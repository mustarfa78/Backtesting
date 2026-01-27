from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

LOGGER = logging.getLogger(__name__)


def _fetch_first_candle_binance(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Try Futures first
    try:
        url = "https://fapi.binance.com/fapi/v1/klines"
        params = {
            "symbol": f"{ticker}USDT",
            "interval": "1m",
            "startTime": start_ts * 1000,
            "limit": 1
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                ts = int(data[0][0])
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Binance Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": f"{ticker}USDT",
            "interval": "1m",
            "startTime": start_ts * 1000,
            "limit": 1
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                ts = int(data[0][0])
                return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Binance Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_bybit(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Try Futures (Linear) first
    try:
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "linear",
            "symbol": f"{ticker}USDT",
            "interval": "1",
            "start": start_ts * 1000,
            "limit": 1,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                candles = data["result"]["list"]
                if candles:
                    ts = int(candles[-1][0])
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Bybit Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://api.bybit.com/v5/market/kline"
        params = {
            "category": "spot",
            "symbol": f"{ticker}USDT",
            "interval": "1",
            "start": start_ts * 1000,
            "limit": 1,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                candles = data["result"]["list"]
                if candles:
                    ts = int(candles[-1][0])
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Bybit Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_gate(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Try Futures first
    try:
        url = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
        params = {
            "contract": f"{ticker}_USDT",
            "interval": "1m",
            "limit": 1,
            "from": start_ts,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                item = data[0]
                ts = item.get("t")
                if ts:
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Gate Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://api.gateio.ws/api/v4/spot/candlesticks"
        params = {
            "currency_pair": f"{ticker}_USDT",
            "interval": "1m",
            "limit": 1,
            "from": start_ts,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data and isinstance(data, list) and len(data) > 0:
                # Gate Spot: [time, volume, close, high, low, open]
                ts = int(data[0][0])
                return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Gate Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_bitget(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Strategy A: Metadata (Direct Hit)
    try:
        url = "https://api.bitget.com/api/v2/spot/public/symbols"
        params = {"symbol": f"{ticker}USDT"}
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "00000" and data.get("data"):
                item = data["data"][0]
                open_time = item.get("openTime")
                if open_time and int(open_time) > 0:
                    dt = datetime.fromtimestamp(int(open_time) / 1000, tz=timezone.utc)
                    LOGGER.info("launch_util: Found Bitget launch time via Metadata: %s (Direct hit)", dt.isoformat())
                    return dt
    except Exception as e:
        LOGGER.debug("Bitget Metadata check failed for %s: %s", ticker, e)

    # Strategy B: Kline Fallback
    # Window: start_ts (which is Ann - 24h) to + 5 days.
    # This covers [Ann - 24h, Ann + 4 days].
    end_ts_ms = (start_ts + 5 * 86400) * 1000

    # Try Futures (Mix)
    try:
        url = "https://api.bitget.com/api/v2/mix/market/candles"
        params = {
            "symbol": f"{ticker}USDT",
            "granularity": "1m",
            "startTime": start_ts * 1000,
            "endTime": end_ts_ms,
            "limit": 1000,
            "productType": "usdt-futures",
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "00000" and data.get("data"):
                sorted_candles = sorted(data["data"], key=lambda x: int(x[0]))
                if sorted_candles:
                    ts = int(sorted_candles[0][0])
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Bitget Futures check failed for %s: %s", ticker, e)

    # Try Spot
    try:
        url = "https://api.bitget.com/api/v2/spot/market/candles"
        params = {
            "symbol": f"{ticker}USDT",
            "granularity": "1min",
            "startTime": start_ts * 1000,
            "endTime": end_ts_ms,
            "limit": 1000,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "00000" and data.get("data"):
                sorted_candles = sorted(data["data"], key=lambda x: int(x[0]))
                if sorted_candles:
                    ts = int(sorted_candles[0][0])
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Bitget Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_xt(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Valid window: 7 days from start_ts (Ann - 24h + 7 days = Ann + 6 days)
    limit_ts_ms = (start_ts + 7 * 86400) * 1000

    configs = [
        # Spot V4 (startTime)
        {
            "url": "https://sapi.xt.com/v4/public/kline",
            "params": {
                "symbol": f"{ticker.lower()}_usdt",
                "interval": "1m",
                "startTime": start_ts * 1000,
                "limit": 10,
            },
            "desc": "Spot V4 startTime"
        },
        # Spot V4 (start_time)
        {
            "url": "https://sapi.xt.com/v4/public/kline",
            "params": {
                "symbol": f"{ticker.lower()}_usdt",
                "interval": "1m",
                "start_time": start_ts * 1000,
                "limit": 10,
            },
            "desc": "Spot V4 start_time"
        },
        # Futures (start_time)
        {
            "url": "https://fapi.xt.com/future/market/v1/public/kline",
            "params": {
                "symbol": f"{ticker.lower()}_usdt",
                "period": "1m",
                "start_time": start_ts * 1000,
                "limit": 10,
            },
            "desc": "Futures start_time"
        }
    ]

    last_error_body = None

    for i, cfg in enumerate(configs):
        try:
            resp = session.get(cfg["url"], params=cfg["params"], timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                # Unified result extraction
                result_list = data.get("result") or data.get("data")

                # If result is empty, try next config
                if not result_list or not isinstance(result_list, list):
                    continue

                # Handle result format variations
                # Format A (Spot V4): [{"t": 167..., ...}, ...]
                # Format B (Possible Futures): same or list of lists?
                # Based on previous code, XT returns list of dicts with 't'.

                # Sort just in case
                # We need to handle if items are not dicts (just in case)
                parsed_candles = []
                for item in result_list:
                    t_val = None
                    if isinstance(item, dict):
                        t_val = item.get("t")
                    elif isinstance(item, list) and len(item) > 0:
                        t_val = item[0]

                    if t_val:
                        parsed_candles.append(int(t_val))

                parsed_candles.sort()

                if parsed_candles:
                    first_ts = parsed_candles[0]
                    # Validation
                    if first_ts < limit_ts_ms:
                        # Found it!
                        # LOGGER.info("launch_util: Resolved launch time for %s on XT via %s", ticker, cfg["desc"])
                        return datetime.fromtimestamp(first_ts / 1000, tz=timezone.utc)
                    else:
                        # Too far in future/recent.
                        # This happens if API ignored start_time and gave us "now".
                        # Try next config.
                        pass
            else:
                last_error_body = resp.text
                LOGGER.debug("XT attempt %s (%s) failed status=%s", i + 1, cfg["desc"], resp.status_code)

        except Exception as e:
            LOGGER.debug("XT attempt %s (%s) error: %s", i + 1, cfg["desc"], e)

    if last_error_body:
        LOGGER.debug("XT all attempts failed. Last error body: %s", last_error_body[:200])

    return None

def _fetch_first_candle_kucoin(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Try Futures first
    try:
        url = "https://api-futures.kucoin.com/api/v1/kline/query"
        params = {
            "symbol": f"{ticker}USDTM",
            "granularity": 1,
            "from": start_ts * 1000,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "200000" and data.get("data"):
                candles = data["data"]
                if candles:
                    # KuCoin Futures returns descending (newest first) by default.
                    # We want the oldest in this batch (which is closest to start_ts).
                    ts = int(candles[-1][0])
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("KuCoin Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://api.kucoin.com/api/v1/market/candles"
        params = {
            "symbol": f"{ticker}-USDT",
            "type": "1min",
            "startAt": start_ts,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "200000" and data.get("data"):
                candles = data["data"]
                if candles:
                    ts = int(candles[-1][0])
                    return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("KuCoin Spot check failed for %s: %s", ticker, e)

    return None


def _fetch_first_candle_kraken(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # Spot only
    try:
        url = "https://api.kraken.com/0/public/OHLC"
        params = {"pair": f"{ticker}USD", "since": start_ts}
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if not data.get("error") and data.get("result"):
                res = data["result"]
                for key, val in res.items():
                    if key == "last":
                        continue
                    if isinstance(val, list) and val:
                        ts = int(val[0][0])
                        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Kraken Spot check failed for %s: %s", ticker, e)

    return None


def resolve_launch_time(
    session,
    source_exchange: str,
    ticker: str,
    search_start_time: Optional[datetime] = None
) -> Optional[datetime]:
    """
    Resolves the launch time (start of trading) for a given ticker.

    Args:
        session: Requests session
        source_exchange: Name of the exchange
        ticker: Ticker symbol (e.g. BTC)
        search_start_time: Optional datetime to start searching from (e.g. announcement time).
                           If not provided, defaults to Jan 1 2020.
    """
    # Default to 2020 if no time provided
    start_dt = search_start_time if search_start_time else datetime(2020, 1, 1, tzinfo=timezone.utc)
    # Ensure UTC
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)

    start_ts = int(start_dt.timestamp())

    launch_time = None
    try:
        if source_exchange == "Binance":
            launch_time = _fetch_first_candle_binance(session, ticker, start_ts)
        elif source_exchange == "Bybit":
            launch_time = _fetch_first_candle_bybit(session, ticker, start_ts)
        elif source_exchange == "Gate":
            launch_time = _fetch_first_candle_gate(session, ticker, start_ts)
        elif source_exchange == "Bitget":
            launch_time = _fetch_first_candle_bitget(session, ticker, start_ts)
        elif source_exchange == "KuCoin":
            launch_time = _fetch_first_candle_kucoin(session, ticker, start_ts)
        elif source_exchange == "XT":
            launch_time = _fetch_first_candle_xt(session, ticker, start_ts)
        elif source_exchange == "Kraken":
            launch_time = _fetch_first_candle_kraken(session, ticker, start_ts)

        if launch_time:
            LOGGER.info(
                "launch_util: Resolved launch time for %s on %s: %s",
                ticker,
                source_exchange,
                launch_time.isoformat(),
            )
        else:
            LOGGER.warning(
                "launch_util: Could not resolve launch time for %s on %s (searched from %s)",
                ticker,
                source_exchange,
                start_dt.isoformat(),
            )

        return launch_time

    except Exception as exc:
        LOGGER.error(
            "launch_util: Error resolving launch time for %s on %s: %s",
            ticker,
            source_exchange,
            exc,
            exc_info=True,
        )
        return None
