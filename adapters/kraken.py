from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from bs4 import BeautifulSoup

from adapters.common import Announcement, extract_tickers, guess_listing_type, parse_datetime
from http_client import get_text

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://blog.kraken.com/category/asset-listings"
    response = session.get(url, timeout=20)
    LOGGER.info("Kraken request url=%s", url)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Kraken response status=%s blocked_or_error", response.status_code)
    LOGGER.info(
        "Kraken response status=%s content_type=%s body_preview=%s",
        response.status_code,
        response.headers.get("Content-Type"),
        response.text[:300],
    )
    response.raise_for_status()
    html = response.text
    soup = BeautifulSoup(html, "lxml")
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    for article in soup.select("article"):
        title_el = article.find(["h2", "h3"])
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        link_el = title_el.find("a")
        if not link_el:
            continue
        full_url = link_el.get("href", "")
        time_el = article.find("time")
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
                source_exchange="Kraken",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=full_url,
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
                body="",
            )
        )
    return announcements
