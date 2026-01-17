from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import List, Optional

import logging

from bs4 import BeautifulSoup

from adapters.common import (
    Announcement,
    extract_tickers,
    guess_listing_type,
    infer_market_type,
    parse_datetime,
)

LOGGER = logging.getLogger(__name__)


_GATE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_GATE_ARTICLE_ID_RE = re.compile(r"/announcements/article/(\d+)")
_GATE_TIME_RE = re.compile(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+UTC")


def _parse_next_data(html: str) -> List[Announcement]:
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if not script or not script.text:
        return []
    try:
        data = json.loads(script.text)
    except json.JSONDecodeError:
        return []
    payload = (
        data.get("props", {})
        .get("pageProps", {})
        .get("initialState", {})
        .get("notice", {})
        .get("list", [])
    )
    announcements: List[Announcement] = []
    for item in payload:
        title = item.get("title", "").strip()
        url = item.get("url", "")
        published_ms = item.get("date") or item.get("time")
        if not title or not url or not published_ms:
            continue
        published = datetime.fromtimestamp(int(published_ms) / 1000, tz=timezone.utc)
        market_type = infer_market_type(title, default="spot")
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Gate",
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


def _extract_from_html(html: str) -> List[Announcement]:
    soup = BeautifulSoup(html, "lxml")
    items = list(soup.select("a.announcement-item, a.article-item, a.notice-item, a.notice-list-item"))
    announcements: List[Announcement] = []
    for item in items:
        title = item.get_text(strip=True)
        href = item.get("href", "")
        if not title or not href:
            continue
        full_url = href if href.startswith("http") else f"https://www.gate.tv{href}"
        time_el = item.find("time")
        published: Optional[datetime] = None
        if time_el and time_el.get("datetime"):
            published = parse_datetime(time_el["datetime"])
        if not published:
            time_text = time_el.get_text(strip=True) if time_el else ""
            published = parse_datetime(time_text)
        if not published:
            continue
        market_type = infer_market_type(title, default="spot")
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Gate",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=full_url,
                listing_type_guess=guess_listing_type(title),
                market_type=market_type,
                tickers=tickers,
                body="",
            )
        )
    return announcements


def _extract_from_links(html: str) -> List[Announcement]:
    soup = BeautifulSoup(html, "lxml")
    announcements: List[Announcement] = []
    for link in soup.find_all("a", href=_GATE_ARTICLE_ID_RE):
        title = link.get_text(strip=True)
        href = link.get("href", "")
        if not title or not href:
            continue
        parent_text = link.parent.get_text(" ", strip=True) if link.parent else ""
        time_match = _GATE_TIME_RE.search(parent_text)
        if not time_match:
            continue
        published = datetime.strptime(time_match.group(1), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
        full_url = href if href.startswith("http") else f"https://www.gate.tv{href}"
        market_type = infer_market_type(title, default="spot")
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Gate",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=full_url,
                listing_type_guess=guess_listing_type(title),
                market_type=market_type,
                tickers=tickers,
                body="",
            )
        )
    return announcements


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://www.gate.tv/announcements/newlisted"
    response = session.get(url, headers=_GATE_HEADERS, timeout=20)
    LOGGER.info("Gate list url=%s status=%s", url, response.status_code)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate list response status=%s blocked_or_error", response.status_code)
        return []
    response.raise_for_status()
    html = response.text
    announcements = _parse_next_data(html)
    if not announcements:
        announcements = _extract_from_html(html)
    if not announcements:
        announcements = _extract_from_links(html)
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    filtered = [a for a in announcements if a.published_at_utc.timestamp() >= cutoff]
    LOGGER.info(
        "Gate parsed announcements=%s filtered=%s",
        len(announcements),
        len(filtered),
    )
    return filtered
