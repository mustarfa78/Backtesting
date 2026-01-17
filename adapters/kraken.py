from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from bs4 import BeautifulSoup

from adapters.common import Announcement, extract_tickers, guess_listing_type, parse_datetime
from http_client import get_text


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://blog.kraken.com"
    html = get_text(session, url)
    soup = BeautifulSoup(html, "lxml")
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    for article in soup.select("article"):
        title_el = article.find(["h2", "h3"])
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if "futures" not in title.lower() and "derivatives" not in title.lower():
            continue
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
            )
        )
    return announcements
