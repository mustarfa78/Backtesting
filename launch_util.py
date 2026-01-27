from __future__ import annotations

import logging
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
    # Try Futures (Mix) first
    try:
        url = "https://api.bitget.com/api/v2/mix/market/candles"
        params = {
            "symbol": f"{ticker}USDT",
            "granularity": "1m",
            "startTime": start_ts * 1000,
            "endTime": (start_ts + 7 * 86400) * 1000,
            "limit": 1000, # Increased limit to catch data in range
            "productType": "usdt-futures",
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "00000" and data.get("data"):
                candles = data["data"]
                if candles:
                    # Bitget returns DESC order (newest first).
                    # We want the oldest in this batch (which is closest to start_ts).
                    ts = int(candles[-1][0])
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Bitget Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://api.bitget.com/api/v2/spot/market/candles"
        params = {
            "symbol": f"{ticker}USDT",
            "granularity": "1min",
            "startTime": start_ts * 1000,
            "endTime": (start_ts + 7 * 86400) * 1000,
            "limit": 1000,
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("code") == "00000" and data.get("data"):
                candles = data["data"]
                if candles:
                    ts = int(candles[-1][0])
                    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("Bitget Spot check failed for %s: %s", ticker, e)

    return None

def _fetch_first_candle_xt(session, ticker: str, start_ts: int) -> Optional[datetime]:
    # XT returns mixed results or latest if parameters aren't perfect.
    # Logic: Get a batch starting from start_ts and find min timestamp.

    # Try Futures first
    try:
        url = "https://fapi.xt.com/future/market/v1/public/kline"
        params = {
            "symbol": f"{ticker.lower()}_usdt",
            "period": "1m",
            "start_time": start_ts * 1000,
            "limit": 200
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            # Structure: { "result": [ { "t": 123... }, ... ] }
            # Or sometimes list directly? XT documentation varies.
            # Based on curl tests: {"result": [{"t": ...}]}
            res = data.get("result")
            if res and isinstance(res, list):
                # Find min t
                min_t = None
                for item in res:
                    t = item.get("t")
                    if t:
                        if min_t is None or t < min_t:
                            min_t = t
                if min_t:
                    return datetime.fromtimestamp(min_t / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("XT Futures check failed for %s: %s", ticker, e)

    # Fallback to Spot
    try:
        url = "https://sapi.xt.com/v4/public/kline"
        params = {
            "symbol": f"{ticker.lower()}_usdt",
            "interval": "1m",
            "startTime": start_ts * 1000,
            "limit": 200
        }
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            # Structure: { "result": [ { "t": 123... }, ... ] }
            res = data.get("result")
            if res and isinstance(res, list):
                min_t = None
                for item in res:
                    t = item.get("t")
                    if t:
                        if min_t is None or t < min_t:
                            min_t = t
                if min_t:
                    return datetime.fromtimestamp(min_t / 1000, tz=timezone.utc)
    except Exception as e:
        LOGGER.debug("XT Spot check failed for %s: %s", ticker, e)

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
