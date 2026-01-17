from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://api.kucoin.com/api/ua/v1/market/announcement"
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    page = 1
    total_items = 0
    type_counts: Dict[str, int] = {}
    while True:
        params = {"language": "en_US", "pageNumber": page, "pageSize": 50}
        response = session.get(url, params=params, timeout=20)
        LOGGER.info("KuCoin request url=%s params=%s", url, params)
        if response.status_code in (403, 451) or response.status_code >= 500:
            LOGGER.warning("KuCoin response status=%s blocked_or_error", response.status_code)
        LOGGER.info(
            "KuCoin response status=%s content_type=%s body_preview=%s",
            response.status_code,
            response.headers.get("Content-Type"),
            response.text[:300],
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("data", {}).get("items", []) or data.get("data", {}).get("list", [])
        if not items:
            break
        total_items += len(items)
        for item in items:
            item_type = item.get("type") or item.get("category") or ""
            if isinstance(item_type, list):
                item_type_key = ",".join(str(x) for x in item_type)
            else:
                item_type_key = str(item_type)
            if item_type_key:
                type_counts[item_type_key] = type_counts.get(item_type_key, 0) + 1
            published_at = item.get("publishAt") or item.get("createdAt")
            if published_at is None:
                continue
            published = ensure_utc(datetime.fromtimestamp(int(published_at) / 1000, tz=timezone.utc))
            if published.timestamp() < cutoff:
                continue
            title = item.get("title", "")
            body = item.get("summary", "") or item.get("content", "")
            url_value = item.get("url", "")
            tickers = extract_tickers(f"{title} {body}")
            announcements.append(
                Announcement(
                    source_exchange="KuCoin",
                    title=title,
                    published_at_utc=published,
                    launch_at_utc=None,
                    url=url_value,
                    listing_type_guess=guess_listing_type(title),
                    tickers=tickers,
                    body=body,
                )
            )
        if page >= 10:
            break
        page += 1
    if type_counts:
        LOGGER.info("KuCoin type distribution=%s", type_counts)
    LOGGER.info("KuCoin total_items=%s in_window=%s", total_items, len(announcements))
    return announcements
