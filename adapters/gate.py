from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from bs4 import BeautifulSoup

from adapters.common import Announcement, extract_tickers, guess_listing_type, parse_datetime
from http_client import get_text


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://www.gate.io/announcements"
    html = get_text(session, url)
    soup = BeautifulSoup(html, "lxml")
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    for item in soup.select("a.announcement-item, a.article-item"):
        title = item.get_text(strip=True)
        href = item.get("href", "")
        if not href:
            continue
        full_url = href if href.startswith("http") else f"https://www.gate.io{href}"
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
            )
        )
    return announcements
