from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc
from loop_guard import LoopDetected, LoopGuard

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://api.kucoin.com/api/ua/v1/market/announcement"
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    page = 1
    last_page = None
    max_pages = 50
    seen_ids: set[str] = set()
    guard = LoopGuard("KuCoin")
    total_items = 0
    type_counts: Dict[str, int] = {}
    while True:
        if last_page is not None and page == last_page:
            raise LoopDetected("KuCoin", "cursor_not_advancing", str(page))
        last_page = page
        params = {"language": "en_US", "pageNumber": page, "pageSize": 50}
        request_sig = f"{url}|{sorted(params.items())}|{page}"
        guard.record_request(request_sig)
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
        page_new = 0
        oldest_ts = None
        content_ids = []
        for idx, item in enumerate(items):
            item_type = item.get("type") or item.get("category") or ""
            if isinstance(item_type, list):
                item_type_key = ",".join(str(x) for x in item_type)
            else:
                item_type_key = str(item_type)
            if item_type_key:
                type_counts[item_type_key] = type_counts.get(item_type_key, 0) + 1
            if idx < 3:
                LOGGER.info(
                    "KuCoin sample title=%s type=%s publishAt=%s",
                    item.get("title"),
                    item_type,
                    item.get("publishAt") or item.get("createdAt") or item.get("releaseTime"),
                )
            published_at = item.get("publishAt") or item.get("createdAt") or item.get("releaseTime")
            if published_at is None:
                continue
            published_val = int(published_at)
            if published_val > 10_000_000_000:
                published_val = int(published_val / 1000)
            published = ensure_utc(datetime.fromtimestamp(published_val, tz=timezone.utc))
            if oldest_ts is None or published_val < oldest_ts:
                oldest_ts = published_val
            if published.timestamp() < cutoff:
                continue
            title = item.get("title", "")
            body = item.get("summary", "") or item.get("content", "")
            url_value = item.get("url", "")
            tickers = extract_tickers(f"{title} {body}")
            event_id = url_value or f"{published.isoformat()}:{title.strip()}"
            if event_id in seen_ids:
                continue
            seen_ids.add(event_id)
            page_new += 1
            content_ids.append(event_id)
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
        if content_ids:
            content_sig = "|".join(sorted(content_ids))
            guard.record_content(content_sig)
        if page >= max_pages:
            raise LoopDetected("KuCoin", "max_pages", str(page))
        if oldest_ts is not None:
            oldest_time = datetime.fromtimestamp(oldest_ts, tz=timezone.utc)
            if oldest_time.timestamp() < cutoff:
                break
        if page_new == 0:
            raise LoopDetected("KuCoin", "zero_new_items", f"page={page}")
        LOGGER.info(
            "adapter=KuCoin iter=%s page=%s items=%s unique_new=%s oldest=%s",
            page,
            page,
            len(items),
            page_new,
            oldest_time.isoformat() if oldest_ts else "n/a",
        )
        page += 1
    if type_counts:
        LOGGER.info("KuCoin type distribution=%s", type_counts)
    LOGGER.info("KuCoin total_items=%s in_window=%s", total_items, len(announcements))
    return announcements
