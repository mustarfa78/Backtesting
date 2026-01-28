from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from mexc import Candle


@dataclass(frozen=True)
class LaunchHighLowResult:
    highest_close: Optional[float]
    highest_time: Optional[datetime]
    pullback_1_close: Optional[float]
    pullback_1_time: Optional[datetime]
    lowest_close: Optional[float]
    lowest_time: Optional[datetime]
    pullback_2_close: Optional[float]
    pullback_2_time: Optional[datetime]


@dataclass(frozen=True)
class CandleBucket:
    start_time: datetime
    candles: List[Candle]
    close: float  # Close of the last candle in bucket (or any representative close)


def _floor_to_3m(ts: datetime) -> datetime:
    minute = ts.minute - (ts.minute % 3)
    return ts.replace(minute=minute, second=0, microsecond=0)


def _build_3m_series(candles: List[Candle]) -> List[CandleBucket]:
    buckets = {}
    for candle in candles:
        bucket_start = _floor_to_3m(candle.timestamp)
        buckets.setdefault(bucket_start, []).append(candle)

    series = []
    for bucket_start in sorted(buckets.keys()):
        bucket_candles = sorted(buckets[bucket_start], key=lambda c: c.timestamp)
        # Using the close of the last candle in the bucket as the bucket's close,
        # consistent with typical OHLC resampling, although the requirement says
        # "Find the 3-minute bucket with the lowest close".
        # Assuming this means the close price of the 3m bar.
        close_val = bucket_candles[-1].close
        series.append(CandleBucket(start_time=bucket_start, candles=bucket_candles, close=close_val))
    return series


def compute_launch_highlow(candles: List[Candle], launch_time: datetime) -> LaunchHighLowResult:
    window_end = launch_time + timedelta(minutes=90)

    # Filter candles to range [launch_time, launch_time + 90 minutes]
    # Assuming inclusive start, inclusive end or exclusive end. Usually inclusive start.
    window_candles = [
        c for c in candles
        if launch_time <= c.timestamp <= window_end
    ]
    window_candles.sort(key=lambda c: c.timestamp)

    if not window_candles:
        return LaunchHighLowResult(None, None, None, None, None, None, None, None)

    # 1. Highest Close
    high_candle = max(window_candles, key=lambda c: c.close)
    highest_close = high_candle.close
    highest_time = high_candle.timestamp

    # 2. Pullback #1 (Dip after High)
    pullback_1_close = None
    pullback_1_time = None

    after_high_candles = [c for c in window_candles if c.timestamp > highest_time]
    if after_high_candles:
        buckets_after_high = _build_3m_series(after_high_candles)
        if buckets_after_high:
            # Find the 3-minute bucket with the lowest close
            lowest_bucket = min(buckets_after_high, key=lambda b: b.close)

            # Inside that winning 3-minute bucket, find the specific 1-minute candle with the lowest close
            pullback_1_candle = min(lowest_bucket.candles, key=lambda c: c.close)
            pullback_1_close = pullback_1_candle.close
            pullback_1_time = pullback_1_candle.timestamp

    # 3. Lowest Close
    low_candle = min(window_candles, key=lambda c: c.close)
    lowest_close = low_candle.close
    lowest_time = low_candle.timestamp

    # 4. Pullback #2 (Bounce after Low)
    pullback_2_close = None
    pullback_2_time = None

    after_low_candles = [c for c in window_candles if c.timestamp > lowest_time]
    if after_low_candles:
        buckets_after_low = _build_3m_series(after_low_candles)
        if buckets_after_low:
            # Find the 3-minute bucket with the highest close
            highest_bucket = max(buckets_after_low, key=lambda b: b.close)

            # Inside that bucket, find the specific 1-minute candle with the highest close
            pullback_2_candle = max(highest_bucket.candles, key=lambda c: c.close)
            pullback_2_close = pullback_2_candle.close
            pullback_2_time = pullback_2_candle.timestamp

    return LaunchHighLowResult(
        highest_close=highest_close,
        highest_time=highest_time,
        pullback_1_close=pullback_1_close,
        pullback_1_time=pullback_1_time,
        lowest_close=lowest_close,
        lowest_time=lowest_time,
        pullback_2_close=pullback_2_close,
        pullback_2_time=pullback_2_time,
    )


if __name__ == "__main__":
    import unittest

    class TestLaunchHighLow(unittest.TestCase):
        def test_basic_high_low_pullback(self):
            base_time = datetime(2024, 1, 1, 10, 0)
            # Create a sequence: goes up, dips, goes down, bounces
            # 10:00 - 10:10: UP to 100
            # 10:10 - 10:20: Dip to 90
            # 10:20 - 10:40: Down to 50
            # 10:40 - 10:50: Bounce to 70

            candles = []

            # Up to High at 10:10
            for i in range(11):
                t = base_time + timedelta(minutes=i)
                price = 60 + i * 4 # 60, 64, ... 100
                candles.append(Candle(t, float(price)))

            # Dip after high (Pullback 1)
            # 10:11 to 10:15 down
            for i in range(1, 6):
                t = base_time + timedelta(minutes=10 + i)
                price = 100 - i * 2 # 98, 96, 94, 92, 90
                candles.append(Candle(t, float(price)))

            # Continue down to Low at 10:40
            for i in range(1, 26): # 25 mins
                t = base_time + timedelta(minutes=15 + i) # starts 10:16
                price = 90 - i * 1.6 # down to ~50
                candles.append(Candle(t, float(price)))

            # Bounce after low (Pullback 2)
            # 10:41 to 10:50 up
            for i in range(1, 11):
                t = base_time + timedelta(minutes=40 + i)
                price = 50 + i * 2 # up to 70
                candles.append(Candle(t, float(price)))

            # Verify inputs roughly
            # High should be 100 at 10:10
            # Low should be ~50 at 10:40

            res = compute_launch_highlow(candles, base_time)

            self.assertEqual(res.highest_close, 100.0)
            self.assertEqual(res.highest_time, base_time + timedelta(minutes=10))

            self.assertAlmostEqual(res.lowest_close, 50.0, delta=0.1)
            # Low is at 10:40 (15+25)
            self.assertEqual(res.lowest_time, base_time + timedelta(minutes=40))

            # Pullback 1: Dip after High (10:10).
            # Candles after 10:10 go down to 90 at 10:15.
            # 3m buckets after 10:10:
            # 10:11 (starts 10:09 bucket? No, floor to 3m)
            # 10:11 is in 10:09 bucket? 11 - (11%3) = 9. Yes.
            # 10:12 is in 10:12 bucket.
            # 10:15 is in 10:15 bucket.
            # The lowest close during the dip is 90 at 10:15.
            self.assertIsNotNone(res.pullback_1_close)
            self.assertLess(res.pullback_1_close, 100.0)

            # Pullback 2: Bounce after Low (10:40).
            # Candles after 10:40 go up to 70.
            self.assertIsNotNone(res.pullback_2_close)
            self.assertGreater(res.pullback_2_close, 50.0)

        def test_empty_window(self):
            base_time = datetime(2024, 1, 1, 10, 0)
            res = compute_launch_highlow([], base_time)
            self.assertIsNone(res.highest_close)
            self.assertIsNone(res.lowest_close)

    unittest.main()
