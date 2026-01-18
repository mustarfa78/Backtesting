from __future__ import annotations

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


def _fetch_gate_article(session, article_id: str) -> Announcement | None:
    bases = ("https://www.gate.com", "https://www.gate.tv")
    last_status = None
    html = ""
    url = ""
    for base in bases:
        url = f"{base}/announcements/article/{article_id}"
        response = session.get(url, headers=_GATE_HEADERS, timeout=20)
        last_status = response.status_code
        if response.status_code in (403, 451) or response.status_code >= 500:
            LOGGER.warning("Gate article status=%s url=%s", response.status_code, url)
            continue
        response.raise_for_status()
        html = response.text
        if html:
            break
    if not html:
        if last_status is not None:
            LOGGER.warning("Gate article fetch failed id=%s status=%s", article_id, last_status)
        return None
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


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    primary_url = "https://www.gate.com/announcements/newlisted"
    fallback_url = "https://www.gate.tv/announcements/newlisted"
    response = session.get(primary_url, headers=_GATE_HEADERS, timeout=20)
    url = primary_url
    if response.status_code == 403:
        response = session.get(fallback_url, headers=_GATE_HEADERS, timeout=20)
        url = fallback_url
    LOGGER.info("Gate list url=%s status=%s", url, response.status_code)
    if response.status_code >= 500 or response.status_code in (403, 451):
        LOGGER.warning("Gate list response status=%s blocked_or_error", response.status_code)
        return []
    response.raise_for_status()
    html = response.text
    announcements: List[Announcement] = []
    ids = _extract_listing_ids(html)
    domain = "gate.com" if url == primary_url else "gate.tv"
    LOGGER.info("Gate ids found=%s using domain=%s", len(ids), domain)
    for article_id in ids[:50]:
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
