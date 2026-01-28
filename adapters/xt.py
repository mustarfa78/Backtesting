from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import List

from adapters.common import Announcement, extract_tickers, guess_listing_type, infer_market_type, parse_datetime
from http_client import get_json

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    base_url = "https://xtsupport.zendesk.com/api/v2/help_center/en-us/articles.json"
    announcements: List[Announcement] = []
    page = 1
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    while True:
        if page > 50:
            break
        LOGGER.info("Fetching XT page %s...", page)
        data = get_json(session, base_url, params={"page": page, "per_page": 50})
        items = data.get("articles", [])

        if not items:
            break

        batch_oldest_ts = float("inf")
        for item in items:
            published_at = item.get("created_at")
            if not published_at:
                continue
            parsed = parse_datetime(published_at)
            if not parsed:
                continue
            published = parsed

            if published.timestamp() < batch_oldest_ts:
                batch_oldest_ts = published.timestamp()

            if published.timestamp() < cutoff:
                continue
            title = item.get("title", "")
            if "futures" not in title.lower() and "contract" not in title.lower():
                continue
            url = item.get("html_url", "")
            tickers = extract_tickers(title)
            market_type = infer_market_type(title, default="futures")
            announcements.append(
                Announcement(
                    source_exchange="XT",
                    title=title,
                    published_at_utc=published,
                    launch_at_utc=None,
                    url=url,
                    listing_type_guess=guess_listing_type(title),
                    market_type=market_type,
                    tickers=tickers,
                    body="",
                )
            )

        if batch_oldest_ts != float("inf"):
            LOGGER.info("Page %s fetched, oldest item: %s", page, datetime.fromtimestamp(batch_oldest_ts, tz=timezone.utc))
            if batch_oldest_ts < cutoff:
                break

        if not data.get("next_page"):
            break
        page += 1
    return announcements
