from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from adapters.common import Announcement, extract_tickers, guess_listing_type
from http_client import get_json


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://api.bitget.com/api/v2/public/annoucements"
    params = {"annType": "coin_listings", "language": "en_US"}
    data = get_json(session, url, params=params)
    items = data.get("data", [])
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    for item in items:
        timestamp = item.get("annTime") or item.get("cTime")
        if timestamp is None:
            continue
        published = datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
        if published.timestamp() < cutoff:
            continue
        title = item.get("title", "")
        url = item.get("url", "")
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Bitget",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=url,
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
            )
        )
    return announcements
