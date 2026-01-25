import re
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, List

from bs4 import BeautifulSoup
from dateutil import parser

from adapters.common import Announcement, ensure_utc
from http_client import get_text

LOGGER = logging.getLogger(__name__)

def resolve_launch_time(announcement: Announcement, session) -> datetime:
    """
    Resolves the specific launch time for an announcement.
    Checks title, then body. If body is empty, fetches the URL content.
    Returns announcement.published_at_utc if no specific launch time is found.
    """
    candidates = []

    # 1. Check title
    if announcement.title:
        candidates.extend(_extract_datetimes(announcement.title, announcement.published_at_utc))

    # 2. Check body
    if announcement.body and announcement.body.strip():
        candidates.extend(_extract_datetimes(announcement.body, announcement.published_at_utc))
    else:
        # 3. Fetch content if body is empty or None
        try:
            # Only fetch if we haven't found a good candidate in title?
            # User said "If the body is empty ... fetch the URL".
            # Implies if body matches, we might not need to fetch.
            # But let's fetch if body is empty.
            LOGGER.info("Fetching content for %s: %s", announcement.tickers, announcement.url)
            text = get_text(session, announcement.url)
            soup = BeautifulSoup(text, 'html.parser')
            # remove scripts and styles
            for script in soup(["script", "style"]):
                script.extract()
            body_text = soup.get_text(separator=' ')
            candidates.extend(_extract_datetimes(body_text, announcement.published_at_utc))
        except Exception as e:
            LOGGER.warning("Failed to fetch content for %s: %s", announcement.url, e)

    best = _pick_best_candidate(candidates, announcement.published_at_utc)

    if best:
        # If the resolved time is effectively the same as publish time (within 1 min),
        # we treat it as "Announcement Time" for the purpose of the distinct check later,
        # but here we just return it. The caller checks distinctness.
        LOGGER.info("Resolved Launch Time: %s (Source: Body/Title) for %s", best, announcement.tickers)
        return best

    LOGGER.info("Launch Time not found, using Announcement Time for %s", announcement.tickers)
    return announcement.published_at_utc


def _extract_datetimes(text: str, relative_base: datetime) -> List[datetime]:
    """
    Extracts datetimes from text using various patterns.
    relative_base is used if a time is found without a date.
    """
    candidates = []
    # Normalize text
    text = re.sub(r'\s+', ' ', text)

    # Pattern 1: ISO-like YYYY-MM-DD HH:MM (UTC)
    # 2023-10-12 10:00 (UTC)
    p1 = r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s*\(?UTC\)?'
    for match in re.finditer(p1, text, re.IGNORECASE):
        try:
            dt = parser.parse(match.group(1))
            candidates.append(ensure_utc(dt))
        except (ValueError, TypeError):
            pass

    # Pattern 2: Textual MMM DD, YYYY HH:MM UTC
    # Oct 12, 2023 10:00 UTC
    p2 = r'([A-Za-z]{3}\s+\d{1,2},?\s+\d{4}\s+\d{2}:\d{2})\s*\(?UTC\)?'
    for match in re.finditer(p2, text, re.IGNORECASE):
        try:
            dt = parser.parse(match.group(1))
            candidates.append(ensure_utc(dt))
        except (ValueError, TypeError):
            pass

    # Pattern 3: "Trading starts: HH:MM UTC"
    # We look for "Trading starts[:\s]+(\d{2}:\d{2})\s*UTC"
    p3 = r'Trading\s+starts[:\s]+(\d{2}:\d{2})\s*UTC'
    for match in re.finditer(p3, text, re.IGNORECASE):
        try:
            time_str = match.group(1)
            # Combine with relative_base date
            # WARNING: This assumes the trading starts on the same day as publication.
            # This is often true for "Trading starts immediately" or "today".
            # But if it's a future listing, the date might be elsewhere.
            # However, without finding the date, this is the best guess we have if we only matched the time.
            # But wait, if we matched Pattern 1 or 2, we have a better candidate.
            # We'll add this to candidates and let the picker decide (or maybe the picker prefers full dates).

            t = parser.parse(time_str).time()
            dt = relative_base.replace(hour=t.hour, minute=t.minute, second=0, microsecond=0)
            candidates.append(ensure_utc(dt))
        except (ValueError, TypeError):
            pass

    # Pattern 4: "Launch ... on [Date] at [Time]"
    # "Launch on Oct 12, 2023 at 10:00 UTC"
    # "Listing on 2023-10-12 at 10:00 UTC"
    p4 = r'on\s+([A-Za-z]{3}\s+\d{1,2},?\s+\d{4}|\d{4}-\d{2}-\d{2})\s+at\s+(\d{2}:\d{2})\s*\(?UTC\)?'
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

    # Preference:
    # - If we have multiple, which one is "Trading"?
    # - The regex didn't capture the context "Trading" vs "Deposit".
    # - Ideally we prefer the one that is AFTER published_at.
    # - If there are multiple after published_at, usually the first one is Deposits, second is Trading?
    # - Or Trading is last?

    # Let's filter for future dates (relative to publish) first.
    future_candidates = [d for d in valid if d >= published_at]

    if future_candidates:
        # If multiple, take the earliest future date?
        # Usually: Deposit (T+1), Trading (T+2), Withdrawal (T+3).
        # We want Trading.
        # But sometimes Deposit is T-1.
        # Without explicit context matching, this is heuristic.
        # User requirement says: "If Launch Time is found...".
        # Let's pick the earliest valid date that is >= published_at.
        # Assuming listing announcements are published before or at the time of listing.
        return sorted(future_candidates)[0]

    # If all candidates are in the past, pick the latest one?
    return sorted(valid)[-1]
