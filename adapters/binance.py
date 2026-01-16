from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List

from bs4 import BeautifulSoup

from adapters.common import Announcement, extract_tickers, guess_listing_type
from http_client import get_json, get_text


def _parse_json_list(data: dict) -> List[Announcement]:
    items = data.get("data", {}).get("articles", [])
    announcements: List[Announcement] = []
    for item in items:
        timestamp = item.get("releaseDate")
        if not timestamp:
            continue
        published = datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)
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
    url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    params = {"type": 1, "pageNo": 1, "pageSize": 50, "catalogId": 48}
    announcements: List[Announcement] = []
    try:
        data = get_json(session, url, params=params)
        announcements = _parse_json_list(data)
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
