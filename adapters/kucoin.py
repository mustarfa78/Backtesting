from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from adapters.common import Announcement, extract_tickers, guess_listing_type
from http_client import get_json


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://api.kucoin.com/api/v3/announcements"
    data = get_json(session, url)
    items = data.get("data", {}).get("items", [])
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    for item in items:
        published_at = item.get("publishAt")
        if published_at is None:
            continue
        published = datetime.fromtimestamp(int(published_at) / 1000, tz=timezone.utc)
        if published.timestamp() < cutoff:
            continue
        title = item.get("title", "")
        url = item.get("url", "")
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="KuCoin",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=url,
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
            )
        )
    return announcements
