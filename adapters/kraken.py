from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, infer_market_type
from http_client import get_json

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    LOGGER.info("Kraken adapter using WP JSON feed for asset listings (spot)")
    feed_url = "https://blog.kraken.com/wp-json/wp/v2/posts"
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    try:
        posts = get_json(session, feed_url, params={"per_page": 20})
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Kraken WP JSON fetch failed: %s", exc)
        return announcements
    for post in posts or []:
        title = (post.get("title") or {}).get("rendered", "") or ""
        link = post.get("link", "")
        date_gmt = post.get("date_gmt")
        if not title or not link or not date_gmt:
            continue
        published = datetime.fromisoformat(date_gmt).replace(tzinfo=timezone.utc)
        if published.timestamp() < cutoff:
            continue
        tickers = extract_tickers(title)
        market_type = infer_market_type(title, default="spot")
        announcements.append(
            Announcement(
                source_exchange="Kraken",
                title=title.strip(),
                published_at_utc=published,
                launch_at_utc=None,
                url=link,
                listing_type_guess=guess_listing_type(title),
                market_type=market_type,
                tickers=tickers,
                body="",
            )
        )
    return announcements
