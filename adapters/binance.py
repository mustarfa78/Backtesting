from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List

from bs4 import BeautifulSoup

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc
from http_client import get_json, get_text

LOGGER = logging.getLogger(__name__)


def _parse_json_list(data: dict) -> List[Announcement]:
    items = data.get("data", {}).get("articles", [])
    announcements: List[Announcement] = []
    for item in items:
        timestamp = item.get("releaseDate")
        if not timestamp:
            continue
        published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
        title = item.get("title", "")
        body = item.get("body", "") or item.get("content", "")
        url = f"https://www.binance.com/en/support/announcement/{item.get('code','')}"
        tickers = extract_tickers(f"{title} {body}")
        announcements.append(
            Announcement(
                source_exchange="Binance",
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


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    cms_url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    cms_param_sets = [
        {"type": 1, "pageNo": 1, "pageSize": 50, "catalogId": 48, "lang": "en"},
        {"type": 1, "pageNo": 1, "pageSize": 50, "catalogId": 49, "lang": "en"},
    ]
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.binance.com/en/support/announcement",
        "clienttype": "web",
    }
    announcements: List[Announcement] = []
    try:
        for cms_params in cms_param_sets:
            response = session.get(cms_url, params=cms_params, headers=headers, timeout=20)
            LOGGER.info(
                "Binance request url=%s params=%s cache=%s",
                cms_url,
                cms_params,
                getattr(response, "from_cache", False),
            )
            if response.status_code in (202, 403, 451) or response.status_code >= 500:
                LOGGER.warning("Binance response status=%s blocked_or_error", response.status_code)
                raise RuntimeError(f"blocked: status {response.status_code}")
            if not response.text:
                raise RuntimeError("blocked: empty body")
            LOGGER.info(
                "Binance response status=%s content_type=%s body_preview=%s",
                response.status_code,
                response.headers.get("Content-Type"),
                response.text[:300],
            )
            response.raise_for_status()
            data = response.json()
            data_block = data.get("data", {})
            LOGGER.info("Binance cms keys=%s data_keys=%s", list(data.keys()), list(data_block.keys()))
            if not data_block:
                continue
            announcements = _parse_json_list(data)
            if announcements:
                break
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"binance parse failed: {exc}") from exc
    if not announcements:
        LOGGER.warning("Binance adapter produced 0 items after CMS attempts")
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    return [a for a in announcements if a.published_at_utc.timestamp() >= cutoff]


if __name__ == "__main__":
    import unittest
    import requests

    class BinanceFeedTests(unittest.TestCase):
        def test_binance_feed_has_items(self):
            session = requests.Session()
            items = fetch_announcements(session, days=30)
            self.assertGreater(len(items), 0)

    unittest.main()
