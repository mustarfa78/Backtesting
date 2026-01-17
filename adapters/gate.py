from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import List

import logging

from bs4 import BeautifulSoup

from adapters.common import Announcement, extract_tickers, guess_listing_type, parse_datetime

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://www.gate.io/announcements/newlisted"
    response = session.get(url, timeout=20)
    LOGGER.info("Gate request url=%s", url)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate response status=%s blocked_or_error", response.status_code)
        url = "https://www.gate.tv/announcements/newlisted"
        response = session.get(url, timeout=20)
        LOGGER.info("Gate fallback url=%s status=%s", url, response.status_code)
    LOGGER.info(
        "Gate response status=%s content_type=%s body_preview=%s",
        response.status_code,
        response.headers.get("Content-Type"),
        response.text[:300],
    )
    response.raise_for_status()
    html = response.text
    soup = BeautifulSoup(html, "lxml")
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    items = list(soup.select("a.announcement-item, a.article-item, a.notice-item, a.notice-list-item"))
    LOGGER.info("Gate candidate nodes=%s", len(items))
    for item in items:
        title = item.get_text(strip=True)
        href = item.get("href", "")
        if not href:
            continue
        full_url = href if href.startswith("http") else f"https://www.gate.io{href}"
        time_el = item.find("time")
        published = None
        if time_el and time_el.get("datetime"):
            published = parse_datetime(time_el["datetime"])
        if not published:
            continue
        if published.timestamp() < cutoff:
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

    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if script and script.text:
        try:
            data = json.loads(script.text)
            articles = (
                data.get("props", {})
                .get("pageProps", {})
                .get("initialState", {})
                .get("notice", {})
                .get("list", [])
            )
            LOGGER.info("Gate __NEXT_DATA__ articles=%s", len(articles))
            for item in articles:
                title = item.get("title", "")
                href = item.get("url", "")
                if not title or not href:
                    continue
                published_ms = item.get("date") or item.get("time")
                if not published_ms:
                    continue
                published = parse_datetime(item.get("dateStr", "")) if item.get("dateStr") else None
                if not published:
                    published = datetime.fromtimestamp(int(published_ms) / 1000, tz=timezone.utc)
                if published.timestamp() < cutoff:
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
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Gate NEXT_DATA parse failed: %s", exc)
    LOGGER.info("Gate list parsing announcements=%s", len(announcements))
    return announcements
