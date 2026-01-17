from __future__ import annotations

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
)

LOGGER = logging.getLogger(__name__)


_GATE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_GATE_ARTICLE_ID_RE = re.compile(r'href="/announcements/article/(\d+)"')
_GATE_TIME_RE = re.compile(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+UTC")
_GATE_LIST_TOKENS_RE = re.compile(r"\blist\s+([A-Z0-9]{2,15})(?:\s+and\s+([A-Z0-9]{2,15}))?", re.IGNORECASE)


def _extract_listing_ids(html: str) -> List[str]:
    ids = _GATE_ARTICLE_ID_RE.findall(html)
    seen = set()
    out: List[str] = []
    for item_id in ids:
        if item_id in seen:
            continue
        seen.add(item_id)
        out.append(item_id)
    return out


def _extract_article_title(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        return h1.get_text(strip=True)
    og_title = soup.find("meta", {"property": "og:title"})
    if og_title and og_title.get("content"):
        return og_title["content"].strip()
    if soup.title:
        return soup.title.get_text(strip=True)
    return ""


def _extract_gate_tickers(title: str, body_snippet: str) -> List[str]:
    tickers = extract_tickers(f"{title} {body_snippet}".strip())
    if tickers:
        return tickers
    paren_matches = re.findall(r"\(([A-Z0-9]{2,15})\)", title.upper())
    if paren_matches:
        return sorted(set(paren_matches))
    list_match = _GATE_LIST_TOKENS_RE.search(title)
    if list_match:
        tokens = [group for group in list_match.groups() if group]
        return sorted({token.upper() for token in tokens})
    return []


def _fetch_gate_article(session, article_id: str) -> Announcement | None:
    url = f"https://www.gate.tv/announcements/article/{article_id}"
    response = session.get(url, headers=_GATE_HEADERS, timeout=20)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate article status=%s url=%s", response.status_code, url)
        return None
    response.raise_for_status()
    html = response.text
    time_match = _GATE_TIME_RE.search(html)
    if not time_match:
        return None
    published = datetime.strptime(time_match.group(1), "%Y-%m-%d %H:%M:%S").replace(
        tzinfo=timezone.utc
    )
    soup = BeautifulSoup(html, "lxml")
    title = _extract_article_title(soup)
    if not title:
        return None
    body_text = soup.get_text(" ", strip=True)
    body_snippet = body_text[:2000]
    market_type = infer_market_type(title, default="spot")
    tickers = _extract_gate_tickers(title, body_snippet)
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


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    url = "https://www.gate.tv/announcements/newlisted"
    response = session.get(url, headers=_GATE_HEADERS, timeout=20)
    LOGGER.info("Gate list url=%s status=%s", url, response.status_code)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate list response status=%s blocked_or_error", response.status_code)
        return []
    response.raise_for_status()
    html = response.text
    announcements: List[Announcement] = []
    ids = _extract_listing_ids(html)
    for article_id in ids:
        announcement = _fetch_gate_article(session, article_id)
        if announcement:
            announcements.append(announcement)
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    filtered = [a for a in announcements if a.published_at_utc.timestamp() >= cutoff]
    LOGGER.info(
        "Gate parsed announcements=%s filtered=%s",
        len(announcements),
        len(filtered),
    )
    return filtered
