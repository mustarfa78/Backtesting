from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from bs4 import BeautifulSoup

from adapters.common import Announcement, extract_tickers, guess_listing_type, parse_datetime
from http_client import get_text
from screening_utils import gate_fetch_listing_ids


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://www.gate.io/announcements"
    html = get_text(session, url)
    soup = BeautifulSoup(html, "lxml")
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    items = list(soup.select("a.announcement-item, a.article-item"))
    for item in items:
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
    if announcements:
        return announcements

    ids = gate_fetch_listing_ids(html)
    for article_id in ids[:50]:
        article_html = get_text(session, f"https://www.gate.io/announcements/article/{article_id}")
        article_soup = BeautifulSoup(article_html, "lxml")
        title_el = article_soup.find(["h1", "h2"])
        time_el = article_soup.find("time")
        if not title_el or not time_el or not time_el.get("datetime"):
            continue
        published = parse_datetime(time_el["datetime"])
        if not published or published.timestamp() < cutoff:
            continue
        title = title_el.get_text(strip=True)
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Gate",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=f"https://www.gate.io/announcements/article/{article_id}",
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
            )
        )
    return announcements
