from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import List

import logging

from bs4 import BeautifulSoup

from adapters.common import (
    Announcement,
    extract_tickers,
    guess_listing_type,
    infer_market_type,
)

LOGGER = logging.getLogger(__name__)


_GATE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_GATE_ARTICLE_ID_RE = re.compile(r"/announcements/article/(\d+)")
_GATE_TIME_RE = re.compile(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+UTC")


def _extract_gate_title(soup: BeautifulSoup, html: str) -> str:
    title_el = soup.find("h1")
    if title_el:
        title_text = title_el.get_text(strip=True)
        if title_text:
            return title_text
    og_title = soup.find("meta", {"property": "og:title"})
    if og_title and og_title.get("content"):
        return og_title["content"].strip()
    if soup.title:
        title_text = soup.title.get_text(strip=True)
        if title_text:
            return title_text
    next_data = soup.find("script", {"id": "__NEXT_DATA__"})
    if next_data and next_data.text:
        try:
            data = json.loads(next_data.text)
            title = _find_title_in_json(data)
            if title:
                return title
        except json.JSONDecodeError:
            pass
    nuxt_match = re.search(r"window\\.__NUXT__\\s*=\\s*(\\{.*\\});", html, re.DOTALL)
    if nuxt_match:
        try:
            data = json.loads(nuxt_match.group(1))
            title = _find_title_in_json(data)
            if title:
                return title
        except json.JSONDecodeError:
            pass
    return ""


def _find_title_in_json(data) -> str:
    if isinstance(data, dict):
        for key, value in data.items():
            if key in {"title", "headline"} and isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, (dict, list)):
                nested = _find_title_in_json(value)
                if nested:
                    return nested
    elif isinstance(data, list):
        for item in data:
            nested = _find_title_in_json(item)
            if nested:
                return nested
    return ""


def _fetch_listing_ids(session, base_url: str) -> List[str]:
    response = session.get(base_url, headers=_GATE_HEADERS, timeout=20)
    LOGGER.info("Gate listing url=%s status=%s", base_url, response.status_code)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate listing response status=%s blocked_or_error", response.status_code)
        return []
    response.raise_for_status()
    return list(dict.fromkeys(_GATE_ARTICLE_ID_RE.findall(response.text)))


def _parse_gate_article(session, article_id: str, base_domain: str) -> Announcement | None:
    url = f"{base_domain}/announcements/article/{article_id}"
    response = session.get(url, headers=_GATE_HEADERS, timeout=20)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate article status=%s url=%s", response.status_code, url)
        return None
    response.raise_for_status()
    html = response.text
    time_match = _GATE_TIME_RE.search(html)
    timestamp = time_match.group(1) if time_match else None
    if not timestamp:
        return None
    published = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    soup = BeautifulSoup(html, "lxml")
    title = _extract_gate_title(soup, html)
    if not title:
        return None
    body_text = soup.get_text(" ", strip=True)
    body_snippet = body_text[:2000]
    market_type = infer_market_type(title, default="spot")
    tickers = extract_tickers(title)
    if not tickers and body_snippet:
        tickers = extract_tickers(f"{title} {body_snippet}")
    return Announcement(
        source_exchange="Gate",
        title=title,
        published_at_utc=published,
        launch_at_utc=None,
        url=url,
        listing_type_guess=guess_listing_type(title),
        market_type=market_type,
        tickers=tickers,
        body=body_snippet,
    )


def _fetch_from_domain(session, domain: str, cutoff: float) -> tuple[List[Announcement], dict]:
    listings_url = f"{domain}/announcements/newlisted"
    ids = _fetch_listing_ids(session, listings_url)
    stats = {
        "gate_articles_fetched": len(ids),
        "gate_titles_empty": 0,
        "gate_titles_sample": [],
        "gate_body_contains_ticker_count": 0,
        "tickers_extracted_count": 0,
    }
    announcements: List[Announcement] = []
    for article_id in ids:
        announcement = _parse_gate_article(session, article_id, domain)
        if not announcement:
            stats["gate_titles_empty"] += 1
            continue
        if announcement.published_at_utc.timestamp() < cutoff:
            continue
        if len(stats["gate_titles_sample"]) < 5:
            stats["gate_titles_sample"].append(announcement.title)
        if announcement.body and not extract_tickers(announcement.title):
            if extract_tickers(announcement.body):
                stats["gate_body_contains_ticker_count"] += 1
        if announcement.tickers:
            stats["tickers_extracted_count"] += 1
        announcements.append(announcement)
    LOGGER.info("Gate parsed announcements=%s from %s", len(announcements), domain)
    return announcements, stats


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    announcements, stats = _fetch_from_domain(session, "https://www.gate.com", cutoff)
    if stats:
        LOGGER.info("Gate debug stats=%s", stats)
    if announcements:
        return announcements
    announcements, stats = _fetch_from_domain(session, "https://www.gate.tv", cutoff)
    if stats:
        LOGGER.info("Gate debug stats=%s", stats)
    return announcements
