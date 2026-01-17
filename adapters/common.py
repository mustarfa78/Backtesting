from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from dateutil import parser


@dataclass(frozen=True)
class Announcement:
    source_exchange: str
    title: str
    published_at_utc: datetime
    launch_at_utc: Optional[datetime]
    url: str
    listing_type_guess: str
    tickers: List[str]


FUTURES_KEYWORDS = (
    "futures",
    "perpetual",
    "perp",
    "premarket",
    "derivatives",
    "contract",
    "swap",
    "innovation",
)

STOP_TOKENS = {
    "USDT",
    "USD",
    "USDC",
    "PERP",
    "PERPETUAL",
    "FUTURES",
    "CONTRACT",
    "SWAP",
    "USDⓈ",
    "USD-M",
    "TRADING",
    "LISTING",
    "LIST",
    "MARGIN",
    "SPOT",
}


def guess_listing_type(title: str) -> str:
    lowered = title.lower()
    if "premarket" in lowered:
        return "premarket"
    if "perpetual" in lowered or "perp" in lowered:
        return "perpetual"
    if "innovation" in lowered:
        return "innovation"
    if "futures" in lowered or "contract" in lowered or "swap" in lowered:
        return "futures"
    return "unknown"


def is_futures_announcement(title: str, extra_keywords: Iterable[str] | None = None) -> bool:
    lowered = title.lower()
    if extra_keywords:
        if any(keyword in lowered for keyword in extra_keywords):
            return True
    return any(keyword in lowered for keyword in FUTURES_KEYWORDS)


def extract_tickers(title: str) -> List[str]:
    import re

    candidates = re.findall(r"\b[A-Z0-9]{2,}\b", title.upper())
    tickers = []
    for token in candidates:
        if token in STOP_TOKENS:
            continue
        if token.isdigit():
            continue
        if token not in tickers:
            tickers.append(token)
    return tickers


def ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = parser.isoparse(value)
    except (ValueError, TypeError):
        return None
    return ensure_utc(parsed)
