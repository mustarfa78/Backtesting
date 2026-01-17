from __future__ import annotations

from datetime import datetime, timezone
from typing import List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc, infer_market_type

LOGGER = logging.getLogger(__name__)

_BINANCE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.binance.com/en/support/announcement",
    "clienttype": "web",
    "Accept": "application/json, text/plain, */*",
}


def _fetch_cms_articles(session) -> List[Announcement]:
    cms_url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    params = {"type": 1, "pageNo": 1, "pageSize": 50}
    announcements: List[Announcement] = []
    LOGGER.info("Binance CMS url=%s params=%s", cms_url, params)
    response = session.get(cms_url, params=params, headers=_BINANCE_HEADERS, timeout=20)
    LOGGER.info(
        "Binance CMS response status=%s content_type=%s body_preview=%s",
        response.status_code,
        response.headers.get("Content-Type"),
        response.text[:300],
    )
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Binance CMS response status=%s blocked_or_error", response.status_code)
        return []
    response.raise_for_status()
    cms_data = response.json()
    catalogs = cms_data.get("data", {}).get("catalogs", [])
    for catalog in catalogs:
        for item in catalog.get("articles", []):
            title = (item.get("title") or "").strip()
            code = item.get("code")
            timestamp = item.get("releaseDate")
            if not title or not code or not timestamp:
                continue
            published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
            url = f"https://www.binance.com/en/support/announcement/{code}"
            market_type = infer_market_type(title, default="spot")
            tickers = extract_tickers(title)
            announcements.append(
                Announcement(
                    source_exchange="Binance",
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
    return announcements


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    announcements = _fetch_cms_articles(session)
    if not announcements:
        LOGGER.warning("Binance adapter produced 0 items after fallback attempts")
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    return [a for a in announcements if a.published_at_utc.timestamp() >= cutoff]
