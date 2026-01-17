from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import List

import logging

from bs4 import BeautifulSoup

from adapters.common import Announcement, extract_tickers, guess_listing_type, parse_datetime

LOGGER = logging.getLogger(__name__)


_GATE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def _parse_announcements(items, cutoff: float) -> List[Announcement]:
    announcements: List[Announcement] = []
    for item in items:
        title = item.get("title", "")
        href = item.get("url", "") or item.get("link", "")
        if not title or not href:
            continue
        published_ms = item.get("date") or item.get("time") or item.get("created_at")
        published = parse_datetime(item.get("dateStr", "")) if item.get("dateStr") else None
        if not published and published_ms:
            published = datetime.fromtimestamp(int(published_ms) / 1000, tz=timezone.utc)
        if not published or published.timestamp() < cutoff:
            continue
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Gate",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=href,
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
                body="",
            )
        )
    return announcements


def _fetch_next_data(session, cutoff: float) -> List[Announcement]:
    url = "https://www.gate.tv/announcements/newlisted"
    response = session.get(url, headers=_GATE_HEADERS, timeout=20)
    LOGGER.info("Gate Next.js request url=%s status=%s", url, response.status_code)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate Next.js response status=%s blocked_or_error", response.status_code)
        return []
    response.raise_for_status()
    html = response.text
    build_id = None
    build_match = re.search(r'"buildId"\s*:\s*"([^"]+)"', html)
    if build_match:
        build_id = build_match.group(1)
    if not build_id:
        script = BeautifulSoup(html, "lxml").find("script", {"id": "__NEXT_DATA__"})
        if script and script.text:
            try:
                data = json.loads(script.text)
                build_id = data.get("buildId")
            except json.JSONDecodeError:
                build_id = None
    if not build_id:
        LOGGER.warning("Gate Next.js buildId not found")
        return []
    data_url = f"https://www.gate.tv/_next/data/{build_id}/en/announcements/newlisted.json"
    data_response = session.get(data_url, headers=_GATE_HEADERS, timeout=20)
    LOGGER.info(
        "Gate Next.js data url=%s status=%s content_type=%s",
        data_url,
        data_response.status_code,
        data_response.headers.get("Content-Type"),
    )
    if data_response.status_code in (403, 451) or data_response.status_code >= 500:
        LOGGER.warning("Gate Next.js data status=%s blocked_or_error", data_response.status_code)
        return []
    data_response.raise_for_status()
    data = data_response.json()
    articles = (
        data.get("pageProps", {})
        .get("initialState", {})
        .get("notice", {})
        .get("list", [])
    )
    LOGGER.info("Gate Next.js articles=%s", len(articles))
    return _parse_announcements(articles, cutoff)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    announcements = _fetch_next_data(session, cutoff)
    if announcements:
        return announcements

    url = "https://www.gate.tv/announcements/newlisted"
    response = session.get(url, headers=_GATE_HEADERS, timeout=20)
    LOGGER.info("Gate request url=%s", url)
    LOGGER.info(
        "Gate response status=%s content_type=%s body_preview=%s",
        response.status_code,
        response.headers.get("Content-Type"),
        response.text[:300],
    )
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate response status=%s blocked_or_error", response.status_code)
    response.raise_for_status()
    html = response.text
    soup = BeautifulSoup(html, "lxml")
    items = list(soup.select("a.announcement-item, a.article-item, a.notice-item, a.notice-list-item"))
    LOGGER.info("Gate candidate nodes=%s", len(items))
    for item in items:
        title = item.get_text(strip=True)
        href = item.get("href", "")
        if not href:
            continue
        full_url = href if href.startswith("http") else f"https://www.gate.tv{href}"
        time_el = item.find("time")
        published = None
        if time_el and time_el.get("datetime"):
            published = parse_datetime(time_el["datetime"])
        if not published or published.timestamp() < cutoff:
            continue
        tickers = extract_tickers(title)
        announcements.append(
            Announcement(
                source_exchange="Gate",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=full_url,
                listing_type_guess=guess_listing_type(title),
                tickers=tickers,
                body="",
            )
        )
    if announcements:
        LOGGER.info("Gate list parsing announcements=%s", len(announcements))
        for sample in announcements[:2]:
            LOGGER.info("Gate sample title=%s url=%s", sample.title, sample.url)
        return announcements

    api_url = "https://www.gate.io/api/web/v2/announcements"
    params = {"type": "newlisted", "page": 1, "size": 50}
    api_headers = {
        **_GATE_HEADERS,
        "Origin": "https://www.gate.io",
        "Referer": "https://www.gate.io/announcements/newlisted",
    }
    response = session.get(api_url, params=params, headers=api_headers, timeout=20)
    LOGGER.info("Gate api url=%s params=%s status=%s", api_url, params, response.status_code)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate api response status=%s blocked_or_error", response.status_code)
    try:
        data = response.json()
        items = data.get("data", {}).get("list", []) or data.get("data", [])
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Gate api parse failed: %s", exc)
        items = []
    LOGGER.info("Gate api items=%s", len(items))
    announcements.extend(_parse_announcements(items, cutoff))
    return announcements
