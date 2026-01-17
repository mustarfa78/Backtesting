from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import List

from bs4 import BeautifulSoup

import logging

from adapters.common import Announcement, extract_tickers, guess_listing_type, ensure_utc, infer_market_type
from http_client import get_text

LOGGER = logging.getLogger(__name__)

_BINANCE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.binance.com/en/support/announcement",
    "clienttype": "web",
    "Accept": "application/json, text/plain, */*",
}


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
        market_type = infer_market_type(f"{title} {body}", default="spot")
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
                market_type=market_type,
                tickers=tickers,
                body=body,
            )
        )
    return announcements


def _fetch_cms_articles(session) -> List[Announcement]:
    cms_url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    param_sets = [
        {"type": 1, "pageNo": 1, "pageSize": 50, "catalogId": 48, "lang": "en"},
        {"type": 1, "pageNo": 1, "pageSize": 50, "catalogId": 49, "lang": "en"},
        {"type": 1, "pageNo": 1, "pageSize": 50, "lang": "en"},
    ]
    announcements: List[Announcement] = []
    for params in param_sets:
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
            continue
        response.raise_for_status()
        cms_data = response.json()
        announcements.extend(_parse_json_list(cms_data))
        if announcements:
            break
    return announcements


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://www.binance.com/bapi/composite/v1/public/market/notice/get"
    params = {"page": 1, "rows": 50, "type": 1}
    announcements: List[Announcement] = []
    try:
        response = session.get(url, params=params, headers=_BINANCE_HEADERS, timeout=20)
        LOGGER.info(
            "Binance request url=%s params=%s cache=%s",
            url,
            params,
            getattr(response, "from_cache", False),
        )
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
        data_block = data.get("data", {})
        items = data_block.get("list", [])
        LOGGER.info("Binance notice total=%s", data_block.get("total"))
        if not data_block.get("total"):
            LOGGER.info("Binance notice keys=%s data_keys=%s", list(data.keys()), list(data_block.keys()))
        if items:
            LOGGER.info("Binance notice first_item=%s", items[0])
        if items:
            for item in items:
                timestamp = item.get("releaseDate") or item.get("publishDate")
                if not timestamp:
                    continue
                published = ensure_utc(datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc))
                title = item.get("title", "")
                body = item.get("content", "") or item.get("body", "")
                link = item.get("url") or item.get("link") or ""
                url_value = link if link.startswith("http") else f"https://www.binance.com{link}"
                market_type = infer_market_type(f"{title} {body}", default="spot")
                tickers = extract_tickers(f"{title} {body}")
                announcements.append(
                    Announcement(
                        source_exchange="Binance",
                        title=title,
                        published_at_utc=published,
                        launch_at_utc=None,
                        url=url_value,
                        listing_type_guess=guess_listing_type(title),
                        market_type=market_type,
                        tickers=tickers,
                        body=body,
                    )
                )
        else:
            announcements = _fetch_cms_articles(session)
    except Exception:
        announcements = _fetch_cms_articles(session)
        if not announcements:
            try:
                response = session.get(
                    "https://www.binance.com/en/support/announcement",
                    headers=_BINANCE_HEADERS,
                    timeout=20,
                )
                if response.status_code == 202:
                    LOGGER.warning("Binance HTML response status=202 blocked_or_challenge")
                else:
                    response.raise_for_status()
                    html = response.text
                    soup = BeautifulSoup(html, "lxml")
                    script = soup.find("script", {"id": "__APP_DATA"})
                    if script and script.text:
                        data = json.loads(script.text)
                        articles = (
                            data.get("appState", {})
                            .get("composite", {})
                            .get("articleList", {})
                            .get("articles", [])
                        )
                        announcements = _parse_json_list({"data": {"articles": articles}})
                    if not announcements:
                        build_id_match = re.search(r'"buildId"\s*:\s*"([^"]+)"', html)
                        if build_id_match:
                            build_id = build_id_match.group(1)
                            next_url = (
                                f"https://www.binance.com/_next/data/{build_id}/en/support/announcement.json"
                            )
                            next_response = session.get(
                                next_url, headers=_BINANCE_HEADERS, timeout=20
                            )
                            if next_response.status_code != 202:
                                next_response.raise_for_status()
                                next_data = next_response.json()
                                articles = (
                                    next_data.get("pageProps", {})
                                    .get("appState", {})
                                    .get("composite", {})
                                    .get("articleList", {})
                                    .get("articles", [])
                                )
                                announcements = _parse_json_list({"data": {"articles": articles}})
            except Exception:
                announcements = announcements or []
    if not announcements:
        LOGGER.warning("Binance adapter produced 0 items after fallback attempts")
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    return [a for a in announcements if a.published_at_utc.timestamp() >= cutoff]
