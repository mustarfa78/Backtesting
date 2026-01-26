from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from dateutil import parser

from adapters.common import Announcement, ensure_utc

LOGGER = logging.getLogger(__name__)

# Regex patterns
# 2026-01-23 11:45 (UTC)
ISO_UTC_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})\s*\(?UTC\)?", re.IGNORECASE)
# January 23, 2026 11:45 (UTC) or Jan 23, 2026 at 11:45 UTC
ENGLISH_UTC_PATTERN = re.compile(r"([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4}(?:\s+at)?\s+\d{1,2}:\d{2}(?::\d{2})?)\s*\(?UTC\)?", re.IGNORECASE)
# 13:00 on January 22, 2026 (UTC)
TIME_ON_DATE_PATTERN = re.compile(r"(\d{1,2}:\d{2}(?::\d{2})?)\s+(?:on\s+)?([A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})\s*\(?UTC\)?", re.IGNORECASE)

def fetch_full_body(session, url: str) -> str:
    """
    Fetches the full body of the announcement.
    Handles Binance specially via BAPI.
    """
    if not url:
        return ""

    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    if "binance.com" in domain:
        # Extract code from URL
        # e.g. https://www.binance.com/en/support/announcement/some-title-3edd59bc38c147259945b634a1d2c039
        match = re.search(r"([a-f0-9]{20,})", url)
        if match:
            code = match.group(1)
            api_url = f"https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query?articleCode={code}"
            try:
                LOGGER.info("Fetching Binance full body from API: %s", api_url)
                resp = session.get(api_url, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                # API returns { "code": "000000", "data": { "body": "...", "title": "..." }, ... }
                article = data.get("data", {})
                if not article:
                     LOGGER.warning("Binance API returned no data for %s", url)
                     return ""
                title = article.get("title", "")
                body = article.get("body", "")
                # Concatenate title and body
                return f"{title}\n\n{body}"
            except Exception as e:
                LOGGER.warning("Failed to fetch Binance API for %s: %s", url, e)
                return ""
        else:
            LOGGER.warning("Could not extract article code from Binance URL: %s", url)
            # Fallback to standard scraping? Binance usually fails standard scraping due to 202/JS.
            # But we can try just in case the URL format is different but page is static (unlikely for Binance).
            pass

    # General handling for Bybit, Kucoin, Gate, etc.
    try:
        LOGGER.info("Fetching full body via scraping: %s", url)
        # Some sites might block without headers, but session should have User-Agent.
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove script and style elements
        for script in soup(["script", "style", "nav", "footer", "header", "noscript", "iframe"]):
            script.decompose()

        text = soup.get_text(separator="\n")
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = "\n".join(chunk for chunk in chunks if chunk)
        return text
    except Exception as e:
        LOGGER.warning("Failed to scrape URL %s: %s", url, e)
        return ""

def _extract_date_from_text(text: str) -> Optional[datetime]:
    if not text:
        return None

    # Priority 1: ISO pattern
    match = ISO_UTC_PATTERN.search(text)
    if match:
        try:
            dt_str = match.group(1)
            dt = parser.parse(dt_str)
            return ensure_utc(dt)
        except Exception:
            pass

    # Priority 2: English pattern
    for match in ENGLISH_UTC_PATTERN.finditer(text):
        try:
            dt_str = match.group(1)
            dt = parser.parse(dt_str)
            return ensure_utc(dt)
        except Exception:
            continue

    # Priority 3: Time on Date pattern (e.g. 13:00 on January 22, 2026)
    for match in TIME_ON_DATE_PATTERN.finditer(text):
        try:
            time_str = match.group(1)
            date_str = match.group(2)
            # Combine them for parsing
            dt_str = f"{date_str} {time_str}"
            dt = parser.parse(dt_str)
            return ensure_utc(dt)
        except Exception:
            continue

    return None

def resolve_launch_time(announcement: Announcement, session) -> Optional[datetime]:
    """
    Resolves the trading launch time for an announcement.
    """
    # Step 1: Check title
    launch_dt = _extract_date_from_text(announcement.title)
    if launch_dt:
        return launch_dt

    # Step 2: Check existing body
    launch_dt = _extract_date_from_text(announcement.body)
    if launch_dt:
        return launch_dt

    # Step 3: Fetch full body if not found
    LOGGER.info("Launch time not found in title/summary, fetching full body for %s", announcement.url)
    full_text = fetch_full_body(session, announcement.url)
    if full_text:
        launch_dt = _extract_date_from_text(full_text)
        if launch_dt:
            return launch_dt

    return None
