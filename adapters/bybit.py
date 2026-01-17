from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://api.bybit.com/v5/announcements/index"
    params = {"locale": "en-US", "limit": 50}
    response = session.get(url, params=params, timeout=20)
    LOGGER.info("Bybit request url=%s params=%s", url, params)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Bybit response status=%s blocked_or_error", response.status_code)
    LOGGER.info(
        "Bybit response status=%s content_type=%s body_preview=%s",
        response.status_code,
        response.headers.get("Content-Type"),
        response.text[:300],
    )
    response.raise_for_status()
    data = response.json()
    ret_code = data.get("retCode")
    ret_msg = data.get("retMsg")
    LOGGER.info("Bybit retCode=%s retMsg=%s", ret_code, ret_msg)
    if ret_code not in (0, "0", None):
        return []
    items = data.get("result", {}).get("list", [])
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    for item in items:
        timestamp = item.get("dateTimestamp") or item.get("date")
        if not timestamp:
            continue
        published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
        if published.timestamp() < cutoff:
            continue
        title = item.get("title", "")
        body = item.get("summary", "") or item.get("content", "")
        url = item.get("url", "")
        tickers = extract_tickers(f"{title} {body}")
        announcements.append(
            Announcement(
                source_exchange="Bybit",
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
