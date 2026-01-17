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
        url = f"https://www.binance.com/en/support/announcement/{item.get('code','')}"
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Binance",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=url,
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
            )
        )
    return announcements


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://www.binance.com/bapi/composite/v1/public/market/notice/get"
    params = {"page": 1, "rows": 50}
    announcements: List[Announcement] = []
    try:
        response = session.get(url, params=params, timeout=20)
        LOGGER.info("Binance request url=%s params=%s", url, params)
        if response.status_code in (403, 451) or response.status_code >= 500:
            LOGGER.warning("Binance response status=%s blocked_or_error", response.status_code)
        LOGGER.info(
            "Binance response status=%s content_type=%s body_preview=%s",
            response.status_code,
            response.headers.get("Content-Type"),
            response.text[:300],
        )
        response.raise_for_status()
        data = response.json()
        items = data.get("data", {}).get("list", [])
        for item in items:
            timestamp = item.get("releaseDate") or item.get("publishDate")
            if not timestamp:
                continue
            published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
            title = item.get("title", "")
            link = item.get("url") or item.get("link") or ""
            url_value = link if link.startswith("http") else f"https://www.binance.com{link}"
            tickers = extract_tickers(title)
            announcements.append(
                Announcement(
                    source_exchange="Binance",
                    title=title,
                    published_at_utc=published,
                    launch_at_utc=None,
                    url=url_value,
                    listing_type_guess=guess_listing_type(title),
                    tickers=tickers,
                )
            )
    except Exception:
        html = get_text(session, "https://www.binance.com/en/support/announcement")
        soup = BeautifulSoup(html, "lxml")
        script = soup.find("script", {"id": "__APP_DATA"})
        if script and script.text:
            try:
                data = json.loads(script.text)
                articles = (
                    data.get("appState", {})
                    .get("composite", {})
                    .get("articleList", {})
                    .get("articles", [])
                )
                announcements = _parse_json_list({"data": {"articles": articles}})
            except Exception:
                announcements = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    return [a for a in announcements if a.published_at_utc.timestamp() >= cutoff]
