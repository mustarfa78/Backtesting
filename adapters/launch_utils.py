import re
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List

from bs4 import BeautifulSoup
from dateutil import parser

from adapters.common import Announcement, ensure_utc
from http_client import get_text

LOGGER = logging.getLogger(__name__)

def fetch_full_body(session, url: str) -> str:
    """
    Fetches the full content of the announcement URL and strips HTML.
    """
    try:
        LOGGER.info("Fetching full body for analysis: %s", url)
        text = get_text(session, url)
        soup = BeautifulSoup(text, 'html.parser')
        # remove scripts and styles
        for script in soup(["script", "style"]):
            script.extract()
        return soup.get_text(separator=' ')
    except Exception as e:
        LOGGER.warning("Failed to fetch full body for %s: %s", url, e)
        return ""

def resolve_launch_time(announcement: Announcement, session) -> datetime:
    """
    Resolves the specific launch time for an announcement.
    Checks title, then body. If no launch time found, fetches the URL content.
    Returns announcement.published_at_utc if no specific launch time is found.
    """
    candidates = []

    LOGGER.info("Scanning for launch time: %s %s", announcement.tickers, announcement.url)

    # 1. Check title
    if announcement.title:
        candidates.extend(_extract_datetimes(announcement.title, announcement.published_at_utc))

    source = "Title"

    # 2. Check body
    if not candidates and announcement.body and announcement.body.strip():
        candidates.extend(_extract_datetimes(announcement.body, announcement.published_at_utc))
        source = "Body"

    # 3. Fetch content if still no candidates
    if not candidates:
        body_text = fetch_full_body(session, announcement.url)
        if body_text:
            candidates.extend(_extract_datetimes(body_text, announcement.published_at_utc))
            source = "ScrapedPage"

    best = _pick_best_candidate(candidates, announcement.published_at_utc)

    if best:
        LOGGER.info("Found Launch Time: %s in %s for %s", best, source, announcement.tickers)
        return best

    LOGGER.info("Launch time not found. Defaulting to publish time: %s for %s", announcement.published_at_utc, announcement.tickers)
    return announcement.published_at_utc


def _extract_datetimes(text: str, relative_base: datetime) -> List[datetime]:
    """
    Extracts datetimes from text using various patterns.
    relative_base is used if a time is found without a date.
    """
    candidates = []
    # Normalize text
    text = re.sub(r'\s+', ' ', text)

    # Pattern 1: ISO-like YYYY-MM-DD HH:MM[:SS] (UTC)
    # 2023-10-12 10:00 (UTC)
    # Refined: (\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?)\s*\(?UTC\)?
    p1 = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(?::\d{2})?)\s*\(?UTC\)?'
    for match in re.finditer(p1, text, re.IGNORECASE):
        try:
            dt = parser.parse(match.group(1))
            candidates.append(ensure_utc(dt))
        except (ValueError, TypeError):
            pass

    # Pattern 2: Textual MMM DD, YYYY HH:MM UTC
    # Oct 12, 2023 10:00 UTC
    # Refined: ([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4},?\s+(?:at\s+)?\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?\s*UTC)
    p2 = r'([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4},?\s+(?:at\s+)?\d{1,2}:\d{2}(?::\d{2})?\s*(?:AM|PM)?\s*UTC)'
    for match in re.finditer(p2, text, re.IGNORECASE):
        try:
            # parser.parse handles "at", "UTC", "AM/PM" quite well
            dt = parser.parse(match.group(1))
            candidates.append(ensure_utc(dt))
        except (ValueError, TypeError):
            pass

    # Pattern 3: "Trading starts: HH:MM UTC"
    # We look for "Trading starts[:\s]+(\d{2}:\d{2})\s*UTC"
    p3 = r'(?:Trading|starts|Launch|Open trading).*?(\d{2}:\d{2})\s*UTC'
    for match in re.finditer(p3, text, re.IGNORECASE):
        try:
            time_str = match.group(1)
            # Combine with relative_base date
            t = parser.parse(time_str).time()
            dt = relative_base.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
            candidates.append(ensure_utc(dt))
        except (ValueError, TypeError):
            pass

    # Pattern 4: "Launch ... on [Date] at [Time]"
    # "Launch on Oct 12, 2023 at 10:00 UTC"
    p4 = r'on\s+([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})\s+at\s+(\d{1,2}:\d{2}(?::\d{2})?)\s*\(?UTC\)?'
    for match in re.finditer(p4, text, re.IGNORECASE):
        try:
            date_str = match.group(1)
            time_str = match.group(2)
            dt = parser.parse(f"{date_str} {time_str}")
            candidates.append(ensure_utc(dt))
        except (ValueError, TypeError):
            pass

    return candidates


def _pick_best_candidate(candidates: List[datetime], published_at: datetime) -> Optional[datetime]:
    if not candidates:
        return None

    # Filter candidates:
    # 1. Must be distinct enough? No, that's done in main.
    # 2. Must be reasonable (e.g. not 1 year ago or 1 year in future)
    valid = []
    for dt in candidates:
        diff_days = (dt - published_at).days
        # Allow it to be up to 30 days in the past (maybe late announcement) or 60 days in future
        if -30 <= diff_days <= 60:
            valid.append(dt)

    if not valid:
        return None

    # Let's filter for future dates (relative to publish) first.
    future_candidates = [d for d in valid if d >= published_at]

    if future_candidates:
        # If multiple, take the earliest future date?
        return sorted(future_candidates)[0]

    # If all candidates are in the past, pick the latest one?
    return sorted(valid)[-1]
