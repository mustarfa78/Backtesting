from __future__ import annotations

import argparse
import csv
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from dateutil import parser

from adapters import (
    fetch_binance,
    fetch_bitget,
    fetch_bybit,
    fetch_gate,
    fetch_kraken,
    fetch_kucoin,
    fetch_xt,
)
from adapters.common import Announcement, futures_keyword_match
from config import DEFAULT_DAYS, DEFAULT_TARGET, LOOKAHEAD_BARS, MIN_PULLBACK_PCT
from screening_utils import get_session
from marketcap import resolve_market_cap
from mexc import MexcFuturesClient
from micro_highs import compute_micro_highs


LOGGER = logging.getLogger(__name__)

LAUNCH_KEYWORDS = (
    "will launch",
    "launch",
    "listed",
    "list",
    "introduce",
    "add",
    "open trading",
    "pre-market trading",
    "premarket trading",
)

FUTURES_KEYWORDS = (
    "perpetual",
    "futures",
    "usdt-m",
    "usdⓈ-m",
    "contract",
    "swap",
)

EXCLUDE_KEYWORDS = (
    "funding rate",
    "adjusting",
    "adjustment",
    "tick size",
    "maintenance",
    "system upgrade",
    "replacement",
    "migration",
    "parameter",
    "rules",
    "risk limit",
    "fee",
    "delist",
    "settlement",
    "delivery",
    "index",
    "mark price",
    "community",
    "campaign",
    "week",
)


def _passes_futures_intent(title: str) -> tuple[bool, List[str]]:
    lowered = title.lower()
    hits = [kw for kw in LAUNCH_KEYWORDS if kw in lowered]
    futures_hits = [kw for kw in FUTURES_KEYWORDS if kw in lowered]
    excluded = [kw for kw in EXCLUDE_KEYWORDS if kw in lowered]
    if excluded:
        return False, ["excluded:" + ",".join(excluded)]
    if hits and futures_hits:
        return True, hits + futures_hits
    return False, hits + futures_hits


def _passes_futures_intent_for_source(source: str, text: str) -> tuple[bool, List[str]]:
    if source == "Bitget":
        excluded = [kw for kw in EXCLUDE_KEYWORDS if kw in text.lower()]
        if excluded:
            return False, ["excluded:" + ",".join(excluded)]
        return True, ["bitget_trusted"]
    return _passes_futures_intent(text)

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build futures listing reaction dataset.")
    parser.add_argument("--target", type=int, default=DEFAULT_TARGET, help="Number of rows to collect")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Lookback window in days")
    parser.add_argument("--out", type=str, default="events.csv", help="Output CSV path")
    parser.add_argument("--no-futures-filter", action="store_true", help="Disable futures-only filtering")
    parser.add_argument("--debug-adapters", action="store_true", help="Print sample adapter items")
    parser.add_argument("--no-cache", action="store_true", help="Disable HTTP cache")
    parser.add_argument("--clear-cache", action="store_true", help="Clear HTTP cache before run")
    parser.add_argument("--debug-ticker", type=str, default="", help="Ticker to debug mapping/klines")
    parser.add_argument("--debug-at", type=str, default="", help="UTC time to debug (ISO8601)")
    parser.add_argument("--debug-mexc-symbol", type=str, default="", help="MEXC base ticker to probe klines")
    parser.add_argument("--debug-window-min", type=int, default=60, help="Minutes for debug kline window")
    parser.add_argument("--log-file", type=str, default="run.log", help="Log file path")
    return parser.parse_args()


def fetch_all_announcements(session, days: int) -> tuple[List[Announcement], dict]:
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
    stats = {"counts": {}, "errors": {}, "samples": {}}
    for name, adapter in adapters:
        try:
            items = adapter(session, days=days)
            stats["counts"][name] = len(items)
            stats["samples"][name] = items[:3]
            announcements.extend(items)
            LOGGER.info("adapter=%s announcements=%s", name, len(items))
        except Exception as exc:  # noqa: BLE001
            stats["errors"][name] = str(exc)
            LOGGER.warning("Adapter %s failed: %s", name, exc, exc_info=True)
    announcements.sort(key=lambda a: a.published_at_utc, reverse=True)
    LOGGER.info("total announcements fetched=%s", len(announcements))
    return announcements, stats


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


def _setup_logging(log_file: str) -> None:
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return
    root_logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.ERROR)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    root_logger.addHandler(console_handler)


def main() -> None:
    args = parse_args()
    _setup_logging(args.log_file)

    session = get_session(use_cache=not args.no_cache, clear_cache=args.clear_cache)
    mexc = MexcFuturesClient(session)
    contracts = mexc.list_contracts()

    max_days = max(args.days, 120)
    days_window = args.days
    announcements: List[Announcement] = []
    adapter_stats = {}
    rows: List[Dict[str, str]] = []
    summary_lines: List[str] = []

    while True:
        announcements, adapter_stats = fetch_all_announcements(session, days_window)

        if args.debug_adapters:
            for name, samples in adapter_stats["samples"].items():
                LOGGER.info("adapter=%s sample_count=%s", name, len(samples))
                for item in samples:
                    LOGGER.info(
                        "adapter=%s sample title=%s published=%s url=%s",
                        name,
                        item.title,
                        item.published_at_utc,
                        item.url,
                    )
            return
        if args.debug_ticker and args.debug_at:
            debug_time = parser.isoparse(args.debug_at).astimezone(timezone.utc)
            debug_ticker = args.debug_ticker.upper()
            symbols = mexc.map_ticker_to_symbols(debug_ticker, contracts)
            LOGGER.info("Debug ticker=%s symbols=%s", debug_ticker, symbols)
            for symbol in symbols:
                try:
                    exists, candles = mexc.ensure_trading(symbol, debug_time)
                    LOGGER.info("Symbol %s candle_exists=%s sample=%s", symbol, exists, candles[:5])
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Debug failed for %s: %s", symbol, exc)
            return
        if args.debug_mexc_symbol:
            debug_ticker = args.debug_mexc_symbol.upper()
            symbols = mexc.map_ticker_to_symbols(debug_ticker, contracts)
            LOGGER.info("Debug MEXC ticker=%s symbols=%s", debug_ticker, symbols)
            window_end = datetime.now(timezone.utc)
            window_start = window_end - timedelta(minutes=args.debug_window_min)
            for symbol in symbols:
                try:
                    candles = mexc.fetch_klines(symbol, window_start, window_end)
                    sample_times = [c.timestamp.isoformat() for c in candles[:3]]
                    LOGGER.info(
                        "Symbol %s candle_count=%s sample_times=%s",
                        symbol,
                        len(candles),
                        sample_times,
                    )
                    if candles:
                        break
                except Exception as exc:  # noqa: BLE001
                    LOGGER.warning("Debug kline failed for %s: %s", symbol, exc)
            mexc.probe_first_contracts(contracts)
            return

        if not announcements:
            for name, count in adapter_stats["counts"].items():
                LOGGER.info("adapter=%s announcements=%s", name, count)
            for name, error in adapter_stats["errors"].items():
                LOGGER.warning("adapter=%s error=%s", name, error)

        rows = []
        seen = set()
        futures_filtered = []
        excluded_by_filter = 0
        keyword_hits: Dict[str, int] = {}
        excluded_reasons: Dict[str, int] = {}
        per_source_filtered: Dict[str, int] = {}
        per_source_tickers: Dict[str, int] = {}
        per_source_mapped: Dict[str, int] = {}
        per_source_candle_ok: Dict[str, int] = {}
        per_source_rows: Dict[str, int] = {}

        for announcement in announcements:
            if args.no_futures_filter:
                futures_filtered.append(announcement)
                continue
            text = f"{announcement.title} {announcement.body}".strip()
            match = futures_keyword_match(text)
            allowed, reasons = _passes_futures_intent_for_source(announcement.source_exchange, text)
            if not match or not allowed:
                excluded_by_filter += 1
                reason_key = ";".join(reasons) if reasons else "no_match"
                excluded_reasons[reason_key] = excluded_reasons.get(reason_key, 0) + 1
                continue
            keyword_hits[match] = keyword_hits.get(match, 0) + 1
            futures_filtered.append(announcement)
            per_source_filtered[announcement.source_exchange] = (
                per_source_filtered.get(announcement.source_exchange, 0) + 1
            )

        LOGGER.info("after futures filter=%s excluded=%s", len(futures_filtered), excluded_by_filter)
        if keyword_hits:
            LOGGER.info("futures keyword hits=%s", keyword_hits)
        if excluded_reasons:
            LOGGER.info("futures exclusion reasons=%s", excluded_reasons)
        LOGGER.info("after sort=%s", len(futures_filtered))
        for idx, announcement in enumerate(futures_filtered[:10]):
            LOGGER.info(
                "ticker extraction sample idx=%s title=%s tickers=%s",
                idx,
                announcement.title,
                announcement.tickers,
            )

        candidates_checked = 0
        mapped = 0
        candle_ok = 0
        qualified = 0
        for announcement in futures_filtered:
            if announcement.tickers:
                per_source_tickers[announcement.source_exchange] = (
                    per_source_tickers.get(announcement.source_exchange, 0) + 1
                )
            for ticker in announcement.tickers:
                if len(rows) >= args.target:
                    break
                key = (announcement.source_exchange, ticker, announcement.published_at_utc)
                if key in seen:
                    continue
                seen.add(key)
                candidates_checked += 1

                symbols = mexc.map_ticker_to_symbols(ticker, contracts)
                if not symbols:
                    LOGGER.info("No MEXC symbol mapping for %s", ticker)
                    continue
                mapped += 1
                per_source_mapped[announcement.source_exchange] = (
                    per_source_mapped.get(announcement.source_exchange, 0) + 1
                )
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
                        try:
                            exists_now = mexc.check_symbol_live_now(candidate_symbol)
                            LOGGER.info("Symbol %s live_now=%s", candidate_symbol, exists_now)
                        except Exception as exc:  # noqa: BLE001
                            LOGGER.warning("Live-now check failed for %s: %s", candidate_symbol, exc)
                        continue
                    symbol = candidate_symbol
                    candle_ok += 1
                    per_source_candle_ok[announcement.source_exchange] = (
                        per_source_candle_ok.get(announcement.source_exchange, 0) + 1
                    )
                    break
                if not symbol:
                    continue
                qualified += 1

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
                per_source_rows[announcement.source_exchange] = (
                    per_source_rows.get(announcement.source_exchange, 0) + 1
                )
            if len(rows) >= args.target:
                break
        LOGGER.info(
            "candidates checked=%s mapped=%s candle_ok=%s qualified=%s",
            candidates_checked,
            mapped,
            candle_ok,
            qualified,
        )
        summary_lines = [
            f"candidates checked={candidates_checked} mapped={mapped} candle_ok={candle_ok} qualified={qualified}",
            f"per_source filtered={per_source_filtered}",
            f"per_source tickers_extracted={per_source_tickers}",
            f"per_source mexc_mapped_ok={per_source_mapped}",
            f"per_source mexc_candle_ok={per_source_candle_ok}",
            f"per_source final_rows={per_source_rows}",
        ]
        for name in adapter_stats.get("counts", {}):
            if per_source_rows.get(name, 0) > 0:
                continue
            if adapter_stats["counts"].get(name, 0) == 0:
                reason = "0 announcements fetched"
            elif per_source_filtered.get(name, 0) == 0:
                reason = "0 passed futures filter"
            elif per_source_tickers.get(name, 0) == 0:
                reason = "0 tickers extracted"
            elif per_source_mapped.get(name, 0) == 0:
                reason = "0 mapped to MEXC"
            elif per_source_candle_ok.get(name, 0) == 0:
                reason = "0 passed MEXC candle check"
            else:
                reason = "0 rows after processing"
            summary_lines.append(f"adapter={name} zero rows reason={reason}")
        if rows or days_window >= max_days:
            if len(rows) < args.target:
                summary_lines.append(
                    f"Target {args.target} not reached (rows={len(rows)}) within {days_window} days"
                )
            break
        days_window = min(days_window * 2, max_days)
        summary_lines.append(f"Expanding days window to {days_window} to meet target {args.target}")

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

    summary_lines.append(f"rows_written={len(rows)}")
    summary_lines.append(f"Wrote {len(rows)} rows to {args.out}")
    print("\n".join(summary_lines))


if __name__ == "__main__":
    main()
