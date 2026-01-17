from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://api.bitget.com/api/v2/public/annoucements"
    params = {"annType": "coin_listings", "annSubType": "futures", "language": "en_US"}
    response = session.get(url, params=params, timeout=20)
    LOGGER.info("Bitget request url=%s params=%s", url, params)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Bitget response status=%s blocked_or_error", response.status_code)
    LOGGER.info(
        "Bitget response status=%s content_type=%s body_preview=%s",
        response.status_code,
        response.headers.get("Content-Type"),
        response.text[:300],
    )
    response.raise_for_status()
    data = response.json()
    items = data.get("data", [])
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    for idx, item in enumerate(items):
        timestamp = item.get("annTime") or item.get("cTime")
        if timestamp is None:
            continue
        published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
        if published.timestamp() < cutoff:
            continue
        title = item.get("title", "")
        body = item.get("content", "") or item.get("summary", "")
        url = item.get("url", "")
        tickers = extract_tickers(f"{title} {body}")
        if idx < 10:
            LOGGER.info(
                "Bitget sample title=%s annType=%s annSubType=%s tickers=%s",
                title,
                item.get("annType"),
                item.get("annSubType"),
                tickers,
            )
        announcements.append(
            Announcement(
                source_exchange="Bitget",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=url,
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
                body=body,
            )
        )
    return announcements
