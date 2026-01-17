from __future__ import annotations

import argparse
import csv
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from adapters import (
    fetch_binance,
    fetch_bitget,
    fetch_bybit,
    fetch_gate,
    fetch_kraken,
    fetch_kucoin,
    fetch_xt,
)
from adapters.common import Announcement, is_futures_announcement
from config import DEFAULT_DAYS, DEFAULT_TARGET, LOOKAHEAD_BARS, MIN_PULLBACK_PCT
from screening_utils import get_session, mexc_symbols_for
from marketcap import resolve_market_cap
from mexc import MexcFuturesClient
from micro_highs import compute_micro_highs


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build futures listing reaction dataset.")
    parser.add_argument("--target", type=int, default=DEFAULT_TARGET, help="Number of rows to collect")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Lookback window in days")
    parser.add_argument("--out", type=str, default="events.csv", help="Output CSV path")
    return parser.parse_args()


def fetch_all_announcements(session, days: int) -> List[Announcement]:
    adapters = [
        ("Binance", fetch_binance),
        ("Bybit", fetch_bybit),
        ("KuCoin", fetch_kucoin),
        ("XT", fetch_xt),
        ("Gate", fetch_gate),
        ("Kraken", fetch_kraken),
        ("Bitget", fetch_bitget),
    ]
    announcements: List[Announcement] = []
    for name, adapter in adapters:
        try:
            announcements.extend(adapter(session, days=days))
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Adapter %s failed: %s", name, exc)
    announcements.sort(key=lambda a: a.published_at_utc, reverse=True)
    return announcements


def _compute_ma5_at_minus_1m(candles, at_time: datetime) -> Optional[float]:
    target_end = at_time.replace(second=0, microsecond=0) - timedelta(minutes=1)
    window_start = target_end - timedelta(minutes=4)
    window = [c.close for c in candles if window_start <= c.timestamp <= target_end]
    if len(window) != 5:
        return None
    return sum(window) / 5


def _format_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(timezone.utc).isoformat()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    session = get_session()
    announcements = fetch_all_announcements(session, args.days)
    mexc = MexcFuturesClient(session)

    rows: List[Dict[str, str]] = []
    seen = set()

    for announcement in announcements:
        if not is_futures_announcement(announcement.title):
            continue
        for ticker in announcement.tickers:
            if len(rows) >= args.target:
                break
            key = (announcement.source_exchange, ticker, announcement.published_at_utc)
            if key in seen:
                continue
            seen.add(key)

            symbols = mexc_symbols_for(ticker, session)
            if not symbols:
                continue
            at_time = announcement.published_at_utc.replace(second=0, microsecond=0)
            symbol = None
            for candidate_symbol in sorted(symbols):
                try:
                    has_candle, _pre_candles = mexc.ensure_trading(candidate_symbol, at_time)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("MEXC check failed for %s: %s", candidate_symbol, exc)
                    continue
                if not has_candle:
                    LOGGER.info(
                        "Skipping %s at %s: no MEXC candle for %s",
                        ticker,
                        at_time.isoformat(),
                        candidate_symbol,
                    )
                    continue
                symbol = candidate_symbol
                break
            if not symbol:
                continue

            window_start = at_time - timedelta(minutes=10)
            window_end = at_time + timedelta(minutes=60)
            candles = mexc.fetch_klines(symbol, window_start, window_end)
            if not candles:
                continue

            ma5 = _compute_ma5_at_minus_1m(candles, at_time)
            mexc_close_at_minus_1m = mexc.get_close_at(
                candles, at_time.replace(second=0, microsecond=0) - timedelta(minutes=1)
            )
            market_cap, mc_note = resolve_market_cap(
                session,
                ticker,
                at_time - timedelta(minutes=1),
                mexc_close_at_minus_1m,
            )
            micro_result = compute_micro_highs(
                candles,
                window_start=at_time,
                window_end=at_time + timedelta(minutes=60),
                lookahead_bars=LOOKAHEAD_BARS,
                min_pullback_pct=MIN_PULLBACK_PCT,
            )
            notes = []
            if mc_note:
                notes.append(mc_note)
            notes.extend(micro_result.notes)

            row = {
                "source_exchange": announcement.source_exchange,
                "ticker": ticker,
                "mexc_symbol": symbol,
                "listing_type": announcement.listing_type_guess,
                "announcement_datetime_utc": _format_dt(announcement.published_at_utc),
                "launch_datetime_utc": _format_dt(announcement.launch_at_utc),
                "market_cap_usd_at_minus_1m": f"{market_cap:.2f}" if market_cap else "",
                "ma5_close_price_at_minus_1m": f"{ma5:.6f}" if ma5 else "",
                "max_price_1_close": f"{micro_result.max_price_1_close:.6f}"
                if micro_result.max_price_1_close
                else "",
                "max_price_1_time_utc": _format_dt(micro_result.max_price_1_time),
                "lowest_after_1_close": f"{micro_result.lowest_after_1_close:.6f}"
                if micro_result.lowest_after_1_close
                else "",
                "lowest_after_1_time_utc": _format_dt(micro_result.lowest_after_1_time),
                "max_price_2_close": f"{micro_result.max_price_2_close:.6f}"
                if micro_result.max_price_2_close
                else "",
                "max_price_2_time_utc": _format_dt(micro_result.max_price_2_time),
                "lowest_after_2_close": f"{micro_result.lowest_after_2_close:.6f}"
                if micro_result.lowest_after_2_close
                else "",
                "lowest_after_2_time_utc": _format_dt(micro_result.lowest_after_2_time),
                "source_url": announcement.url,
                "notes": "; ".join(notes),
            }
            rows.append(row)
        if len(rows) >= args.target:
            break

    fieldnames = [
        "source_exchange",
        "ticker",
        "mexc_symbol",
        "listing_type",
        "announcement_datetime_utc",
        "launch_datetime_utc",
        "market_cap_usd_at_minus_1m",
        "ma5_close_price_at_minus_1m",
        "max_price_1_close",
        "max_price_1_time_utc",
        "lowest_after_1_close",
        "lowest_after_1_time_utc",
        "max_price_2_close",
        "max_price_2_time_utc",
        "lowest_after_2_close",
        "lowest_after_2_time_utc",
        "source_url",
        "notes",
    ]

    with open(args.out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    LOGGER.info("Wrote %s rows to %s", len(rows), args.out)


if __name__ == "__main__":
    main()
