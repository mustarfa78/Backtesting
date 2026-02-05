from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc, infer_market_type

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://api.bitget.com/api/v2/public/annoucements"
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    cursor = None

    while True:
        # Note: API max limit appears to be 10 or similar small number; 100 fails with 400.
        params = {"annType": "coin_listings", "language": "en_US", "limit": "10"}
        if cursor:
            params["cursor"] = cursor

        response = session.get(url, params=params, timeout=20)
        LOGGER.info("Bitget request url=%s params=%s cursor=%s", url, params, cursor)
        if response.status_code in (403, 451) or response.status_code >= 500:
            LOGGER.warning("Bitget response status=%s blocked_or_error", response.status_code)

        LOGGER.info(
            "Bitget response status=%s content_type=%s body_preview=%s",
            response.status_code,
            response.headers.get("Content-Type"),
            response.text[:300],
        )
        try:
            response.raise_for_status()
        except Exception as exc:
            LOGGER.error("Bitget fetch failed: %s", exc)
            break

        data = response.json()
        items = data.get("data", [])
        if not items:
            break

        batch_min_ts = None
        for idx, item in enumerate(items):
            timestamp = item.get("annTime") or item.get("cTime")
            if timestamp is None:
                continue
            published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))

            ts = published.timestamp()
            if batch_min_ts is None or ts < batch_min_ts:
                batch_min_ts = ts

            if ts < cutoff:
                continue

            title = item.get("title", "") or item.get("annTitle", "")
            body = item.get("content", "") or item.get("summary", "") or item.get("annDesc", "")
            ann_url = item.get("url", "") or item.get("annUrl", "")
            tickers = extract_tickers(f"{title} {body}")
            market_type = infer_market_type(f"{title} {body}", default="futures")
            if idx < 3 and not cursor:
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
                    url=ann_url,
                    listing_type_guess=guess_listing_type(title),
                    market_type=market_type,
                    tickers=tickers,
                    body=body,
                )
            )

        if batch_min_ts is not None and batch_min_ts < cutoff:
            break

        last_item = items[-1]
        cursor = last_item.get("annId")
        if not cursor:
            break

        if len(announcements) > 500: # Safety break if too many items
            LOGGER.warning("Bitget adapter reached item limit 500")
            break

    return announcements
