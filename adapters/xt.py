from __future__ import annotations

from datetime import datetime, timezone
from typing import List

from adapters.common import Announcement, extract_tickers, guess_listing_type, parse_datetime
from http_client import get_json


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    base_url = "https://xtsupport.zendesk.com/api/v2/help_center/en-us/articles.json"
    announcements: List[Announcement] = []
    page = 1
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    while page <= 2:
        data = get_json(session, base_url, params={"page": page, "per_page": 50})
        items = data.get("articles", [])
        for item in items:
            published_at = item.get("created_at")
            if not published_at:
                continue
            parsed = parse_datetime(published_at)
            if not parsed:
                continue
            published = parsed
            if published.timestamp() < cutoff:
                continue
            title = item.get("title", "")
            if "futures" not in title.lower() and "contract" not in title.lower():
                continue
            url = item.get("html_url", "")
            tickers = extract_tickers(title)
            announcements.append(
                Announcement(
                    source_exchange="XT",
                    title=title,
                    published_at_utc=published,
                    launch_at_utc=None,
                    url=url,
                    listing_type_guess=guess_listing_type(title),
                    tickers=tickers,
                    body="",
                )
            )
        if not data.get("next_page"):
            break
        page += 1
    return announcements
