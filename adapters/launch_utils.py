from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Optional

from http_client import get_json

LOGGER = logging.getLogger(__name__)

_CACHE: Dict[tuple[str, str, str], Optional[datetime]] = {}
_LAST_REQUEST: Dict[str, float] = {}

_WINDOW = timedelta(hours=6)
_MAX_QUERIES = 10
_MAX_LOOKAHEAD = timedelta(days=14)
_MIN_REQUEST_INTERVAL = 0.2


def extract_launch_time(
    session,
    source_exchange: str,
    symbol: str,
    market_type: str,
    published_at_utc: datetime,
) -> Optional[datetime]:
    key = (source_exchange.lower(), symbol.upper(), market_type.lower())
    if key in _CACHE:
        return _CACHE[key]

    lookup = {
        "bybit": bybit_find_launch,
        "bitget": bitget_find_launch,
        "xt": xt_find_launch,
        "binance": binance_find_launch,
        "kucoin": kucoin_find_launch,
    }
    handler = lookup.get(source_exchange.lower())
    if not handler:
        LOGGER.info(
            "LAUNCH_NOT_FOUND exchange=%s symbol=%s reason=unsupported_exchange",
            source_exchange,
            symbol,
        )
        _CACHE[key] = None
        return None
    result = handler(session, symbol, market_type, published_at_utc)
    _CACHE[key] = result
    return result


def bybit_find_launch(
    session,
    symbol: str,
    market_type: str,
    published_at_utc: datetime,
) -> Optional[datetime]:
    category = "spot" if market_type == "spot" else "linear"
    symbol_fmt = _format_symbol(symbol, market_type, underscore=False)
    url = "https://api.bybit.com/v5/market/kline"

    def fetch(start_ms: int, end_ms: int) -> List[int]:
        params = {
            "category": category,
            "symbol": symbol_fmt,
            "interval": "1",
            "start": start_ms,
            "end": end_ms,
            "limit": 200,
        }
        data = _get_json(session, "Bybit", url, params)
        if data.get("retCode") not in (0, "0", None):
            raise ValueError(f"Bybit retCode={data.get('retCode')} retMsg={data.get('retMsg')}")
        items = data.get("result", {}).get("list", []) or []
        return _extract_timestamps(items)

    return _search_first_candle(
        "Bybit",
        symbol_fmt,
        published_at_utc,
        fetch,
    )


def bitget_find_launch(
    session,
    symbol: str,
    market_type: str,
    published_at_utc: datetime,
) -> Optional[datetime]:
    symbol_fmt = _format_symbol(symbol, market_type, underscore=False)
    if market_type == "spot":
        url = "https://api.bitget.com/api/v2/spot/market/candles"
    else:
        url = "https://api.bitget.com/api/v2/mix/market/candles"

    def fetch(start_ms: int, end_ms: int) -> List[int]:
        params = {
            "symbol": symbol_fmt,
            "granularity": "1min",
            "startTime": start_ms,
            "endTime": end_ms,
        }
        data = _get_json(session, "Bitget", url, params)
        if data.get("code") not in ("00000", 0, "0", None):
            raise ValueError(f"Bitget code={data.get('code')} msg={data.get('msg')}")
        items = data.get("data", []) or []
        return _extract_timestamps(items)

    return _search_first_candle(
        "Bitget",
        symbol_fmt,
        published_at_utc,
        fetch,
    )


def xt_find_launch(
    session,
    symbol: str,
    market_type: str,
    published_at_utc: datetime,
) -> Optional[datetime]:
    futures_url = "https://fapi.xt.com/future/market/v1/market/kline"
    spot_url = "https://sapi.xt.com/v4/public/kline"
    symbol_spot = _format_symbol(symbol, market_type="spot", underscore=True)
    symbol_futures = _format_symbol(symbol, market_type="futures", underscore=False)

    def fetch_futures(start_ms: int, end_ms: int) -> List[int]:
        params = {
            "symbol": symbol_futures,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 200,
        }
        data = _get_json(session, "XT", futures_url, params)
        items = _extract_xt_items(data)
        return _extract_timestamps(items)

    def fetch_spot(start_ms: int, end_ms: int) -> List[int]:
        params = {
            "symbol": symbol_spot,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": 200,
        }
        data = _get_json(session, "XT", spot_url, params)
        items = _extract_xt_items(data)
        return _extract_timestamps(items)

    if market_type == "spot":
        result = _search_first_candle("XT", symbol_spot, published_at_utc, fetch_spot)
        if result:
            return result
        return _search_first_candle("XT", symbol_futures, published_at_utc, fetch_futures)

    result = _search_first_candle("XT", symbol_futures, published_at_utc, fetch_futures)
    if result:
        return result
    return _search_first_candle("XT", symbol_spot, published_at_utc, fetch_spot)


def binance_find_launch(
    _session,
    _symbol: str,
    _market_type: str,
    _published_at_utc: datetime,
) -> Optional[datetime]:
    return None


def kucoin_find_launch(
    _session,
    _symbol: str,
    _market_type: str,
    _published_at_utc: datetime,
) -> Optional[datetime]:
    return None


def _format_symbol(symbol: str, market_type: str, underscore: bool) -> str:
    base = symbol.upper()
    if base.endswith("USDT"):
        base = base[:-4]
    if underscore:
        return f"{base}_USDT"
    return f"{base}USDT"


def _extract_xt_items(data: dict) -> Iterable:
    if isinstance(data, dict):
        if "result" in data and isinstance(data["result"], dict):
            result = data["result"]
            if "data" in result:
                return result.get("data") or []
        if "data" in data:
            return data.get("data") or []
    return []


def _extract_timestamps(items: Iterable) -> List[int]:
    timestamps: List[int] = []
    for item in items:
        if isinstance(item, dict):
            ts = item.get("timestamp") or item.get("ts")
        else:
            ts = item[0] if item else None
        if ts is None:
            continue
        try:
            timestamps.append(int(ts))
        except (TypeError, ValueError):
            continue
    return timestamps


def _search_first_candle(
    exchange: str,
    symbol: str,
    published_at_utc: datetime,
    fetcher: Callable[[int, int], List[int]],
) -> Optional[datetime]:
    start = published_at_utc.replace(tzinfo=timezone.utc)
    end = start + _WINDOW
    max_end = start + _MAX_LOOKAHEAD
    last_error = None

    for attempt in range(_MAX_QUERIES):
        if start >= max_end:
            break
        if end > max_end:
            end = max_end
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)
        try:
            timestamps = fetcher(start_ms, end_ms)
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            LOGGER.info(
                "LAUNCH_NOT_FOUND exchange=%s symbol=%s searched_window=%s-%s last_error=%s",
                exchange,
                symbol,
                start.isoformat(),
                end.isoformat(),
                last_error,
            )
            _throttle(exchange)
            start = end
            end = start + _WINDOW
            continue

        if timestamps:
            launch_ms = min(timestamps)
            launch_at = datetime.fromtimestamp(launch_ms / 1000, tz=timezone.utc)
            LOGGER.info(
                "LAUNCH_FOUND exchange=%s symbol=%s launch_at_utc=%s method=kline",
                exchange,
                symbol,
                launch_at.isoformat(),
            )
            return launch_at

        _throttle(exchange)
        start = end
        end = start + _WINDOW

    LOGGER.info(
        "LAUNCH_NOT_FOUND exchange=%s symbol=%s searched_window=%s-%s last_error=%s",
        exchange,
        symbol,
        published_at_utc.isoformat(),
        max_end.isoformat(),
        last_error,
    )
    return None


def _get_json(session, exchange: str, url: str, params: dict) -> dict:
    _throttle(exchange)
    return get_json(session, url, params=params)


def _throttle(exchange: str) -> None:
    now = time.monotonic()
    last = _LAST_REQUEST.get(exchange, 0.0)
    elapsed = now - last
    if elapsed < _MIN_REQUEST_INTERVAL:
        time.sleep(_MIN_REQUEST_INTERVAL - elapsed)
    _LAST_REQUEST[exchange] = time.monotonic()
