from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from bs4 import BeautifulSoup

from adapters.common import Announcement, extract_tickers, guess_listing_type, parse_datetime

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://www.gate.tv/announcements/newlisted"
    response = session.get(url, timeout=20)
    LOGGER.info("Gate request url=%s", url)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate response status=%s blocked_or_error", response.status_code)
    LOGGER.info(
        "Gate response status=%s content_type=%s body_preview=%s",
        response.status_code,
        response.headers.get("Content-Type"),
        response.text[:300],
    )
    response.raise_for_status()
    html = response.text
    soup = BeautifulSoup(html, "lxml")
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    items = list(soup.select("a.announcement-item, a.article-item, a.notice-item"))
    for item in items:
        title = item.get_text(strip=True)
        href = item.get("href", "")
        if not href:
            continue
        full_url = href if href.startswith("http") else f"https://www.gate.tv{href}"
        time_el = item.find("time")
        published = None
        if time_el and time_el.get("datetime"):
            published = parse_datetime(time_el["datetime"])
        if not published:
            continue
        if published.timestamp() < cutoff:
            continue
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Gate",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=full_url,
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
                body="",
            )
        )
    LOGGER.info("Gate list parsing announcements=%s", len(announcements))
    return announcements
