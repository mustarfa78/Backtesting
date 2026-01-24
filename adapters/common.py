from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from dateutil import parser

from screening_utils import extract_tickers


@dataclass(frozen=True)
class Announcement:
    source_exchange: str
    title: str
    published_at_utc: datetime
    launch_at_utc: Optional[datetime]
    url: str
    listing_type_guess: str
    market_type: str
    tickers: List[str]
    body: str


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

SPOT_LISTING_KEYWORDS = (
    "will list",
    "listing",
    "listed",
    "spot trading",
    "available to trade",
    "available for trading",
    "trade starts",
    "adds",
    "new listing",
    "asset listing",
    "now available",
    "trading starts",
    "trading begins",
    "opens trading",
    "new asset",
    "introducing",
)

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
    if spot_keyword_match(lowered):
        return "spot"
    return "unknown"


def is_futures_announcement(title: str, extra_keywords: Iterable[str] | None = None) -> bool:
    return futures_keyword_match(title, extra_keywords) is not None


def futures_keyword_match(title: str, extra_keywords: Iterable[str] | None = None) -> Optional[str]:
    lowered = title.lower()
    if extra_keywords:
        for keyword in extra_keywords:
            if keyword in lowered:
                return keyword
    for keyword in FUTURES_KEYWORDS:
        if keyword in lowered:
            return keyword
    return None


def spot_keyword_match(text: str) -> Optional[str]:
    lowered = text.lower()
    for keyword in SPOT_LISTING_KEYWORDS:
        if keyword in lowered:
            return keyword
    return None


def infer_market_type(text: str, default: str = "futures") -> str:
    if futures_keyword_match(text):
        return "futures"
    if spot_keyword_match(text):
        return "spot"
    return default


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


def extract_launch_time(text: str, publish_time: datetime) -> Optional[datetime]:
    """
    Extracts trading launch time from text, assuming UTC if specified or defaulting to it.
    Prioritizes full dates found in text.
    """
    if not text:
        return None

    # 1. ISO-ish format: "2024-09-17 12:30 (UTC)" or "2024-09-17 12:30UTC"
    iso_pattern = r"(\d{4}-\d{2}-\d{2}\s+\d{1,2}:\d{2})\s*\(?UTC\)?"

    # 2. Month Name format: "Jan 14, 2026, 8:00AM UTC" or "January 14 2026 08:00 UTC"
    # Matches: Month (full or abbr), Day, Year (optional comma), Time, AM/PM (optional), UTC
    month_pattern = r"([A-Za-z]+\s+\d{1,2},?\s+\d{4},?\s+(?:at\s+)?\d{1,2}:\d{2}(?:\s*(?:AM|PM))?)\s*\(?UTC\)?"

    # Search for ISO pattern
    iso_matches = re.findall(iso_pattern, text)
    for match in iso_matches:
        try:
            dt = parser.parse(match)
            return ensure_utc(dt)
        except (ValueError, TypeError):
            continue

    # Search for Month pattern
    month_matches = re.findall(month_pattern, text)
    for match in month_matches:
        try:
            # Replace 'at' to help parser if needed, though dateutil is usually smart
            clean_match = match.replace(" at ", " ")
            dt = parser.parse(clean_match)
            return ensure_utc(dt)
        except (ValueError, TypeError):
            continue

    # 3. Time only pattern: "12:00 UTC" or "12:00 (UTC)"
    # Context: "Trading starts: 12:00 UTC"
    # We assume the date is the same as publish_time
    time_pattern = r"(\d{1,2}:\d{2})\s*\(?UTC\)?"
    time_matches = re.findall(time_pattern, text)
    if time_matches:
        # Check if "trading" or "launch" or "list" is near?
        # For now, just take the first one found, assuming it's relevant if present in announcement body.
        # But this is risky if there are other times.
        # Let's try to combine with publish date.
        for match in time_matches:
            try:
                t = parser.parse(match).time()
                dt = datetime.combine(publish_time.date(), t)
                return ensure_utc(dt)
            except (ValueError, TypeError):
                continue

    return None
