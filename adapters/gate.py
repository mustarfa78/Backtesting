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
    parse_datetime,
)

LOGGER = logging.getLogger(__name__)


_GATE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

_GATE_ARTICLE_ID_RE = re.compile(r'href="/announcements/article/(\d+)"')
_GATE_TIME_RE = re.compile(r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s+UTC")
_GATE_ANY_ARTICLE_URL_RE = re.compile(
    r'(https?://[^"\s>]*announcements[^"\s>]*article[^"\s>]*|/announcements[^"\s>]*article[^"\s>]*)'
)
_GATE_ANY_ARTICLE_ID_RE = re.compile(r"/announcements/[^\"\s>]*?article/(\d+)")
_GATE_METRICS = {
    "article_403_count": 0,
    "timestamp_missing_count": 0,
}


def get_metrics() -> dict:
    return dict(_GATE_METRICS)


def _log_list_page_debug(html: str) -> None:
    preview = html[:800]
    counts = {
        "announcements/article": html.count("announcements/article"),
        "/article/": html.count("/article/"),
        "__NEXT_DATA__": html.count("__NEXT_DATA__"),
        "window.__NUXT__": html.count("window.__NUXT__"),
    }
    LOGGER.info("Gate list html preview=%s", preview)
    LOGGER.info("Gate list html counts=%s", counts)


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


def _extract_embedded_json(html: str) -> list[dict]:
    payloads: list[dict] = []
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", {"id": "__NEXT_DATA__"})
    if script and script.text:
        try:
            payloads.append(json.loads(script.text))
        except json.JSONDecodeError:
            LOGGER.warning("Gate __NEXT_DATA__ parse failed")
    if "window.__NUXT__" in html:
        match = re.search(r"window\.__NUXT__\s*=\s*(\{.*\})\s*;?", html, re.DOTALL)
        if match:
            raw = match.group(1)
            try:
                payloads.append(json.loads(raw))
            except json.JSONDecodeError:
                LOGGER.warning("Gate window.__NUXT__ parse failed")
    return payloads


def _walk_json_for_items(payload: object) -> list[dict]:
    items: list[dict] = []
    if isinstance(payload, dict):
        lowered_keys = {str(k).lower(): k for k in payload.keys()}
        if "title" in lowered_keys and ("id" in lowered_keys or "article_id" in lowered_keys):
            title_key = lowered_keys["title"]
            id_key = lowered_keys.get("id") or lowered_keys.get("article_id")
            url_key = lowered_keys.get("url") or lowered_keys.get("link")
            items.append(
                {
                    "id": str(payload.get(id_key, "")),
                    "title": str(payload.get(title_key, "")),
                    "url": str(payload.get(url_key, "")) if url_key else "",
                    "published": None,
                }
            )
        for value in payload.values():
            items.extend(_walk_json_for_items(value))
    elif isinstance(payload, list):
        for entry in payload:
            items.extend(_walk_json_for_items(entry))
    return items


def _parse_published_at(container: BeautifulSoup) -> datetime | None:
    time_el = container.find("time")
    if time_el:
        time_value = time_el.get("datetime") or time_el.get_text(strip=True)
        parsed = parse_datetime(time_value)
        if parsed:
            return parsed
    text = container.get_text(" ", strip=True)
    time_match = _GATE_TIME_RE.search(text)
    if time_match:
        return datetime.strptime(time_match.group(1), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=timezone.utc
        )
    return None


def _parse_list_items(html: str, base_url: str) -> List[dict]:
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=_GATE_ARTICLE_ID_RE)
    items = []
    seen = set()
    for anchor in anchors:
        href = anchor.get("href", "")
        match = _GATE_ARTICLE_ID_RE.search(href)
        if not match:
            continue
        article_id = match.group(1)
        if article_id in seen:
            continue
        title = anchor.get_text(" ", strip=True)
        if not title:
            continue
        full_url = href if href.startswith("http") else f"{base_url}{href}"
        published = _parse_published_at(anchor)
        if not published:
            parent = anchor.find_parent()
            if parent:
                published = _parse_published_at(parent)
        items.append(
            {
                "id": article_id,
                "title": title,
                "url": full_url,
                "published": published,
            }
        )
        seen.add(article_id)
    return items


def _parse_items_from_embedded_json(html: str, base_url: str) -> list[dict]:
    payloads = _extract_embedded_json(html)
    items: list[dict] = []
    seen = set()
    for payload in payloads:
        for entry in _walk_json_for_items(payload):
            article_id = entry.get("id", "").strip()
            title = entry.get("title", "").strip()
            if not article_id or not title or article_id in seen:
                continue
            url = entry.get("url") or f"{base_url}/announcements/article/{article_id}"
            if url and url.startswith("/"):
                url = f"{base_url}{url}"
            items.append(
                {
                    "id": article_id,
                    "title": title,
                    "url": url,
                    "published": None,
                }
            )
            seen.add(article_id)
    return items


def _parse_items_from_any_urls(html: str, base_url: str) -> list[dict]:
    matches = _GATE_ANY_ARTICLE_URL_RE.findall(html)
    samples = matches[:10]
    if matches:
        LOGGER.info("Gate fallback url samples=%s", samples)
    items: list[dict] = []
    seen = set()
    for url in matches:
        match = _GATE_ANY_ARTICLE_ID_RE.search(url)
        if not match:
            continue
        article_id = match.group(1)
        if article_id in seen:
            continue
        full_url = url
        if full_url.startswith("/"):
            full_url = f"{base_url}{full_url}"
        items.append(
            {
                "id": article_id,
                "title": "",
                "url": full_url,
                "published": None,
            }
        )
        seen.add(article_id)
    return items


def _fetch_gate_article(session, article_id: str, base_url: str) -> Announcement | None:
    bases = [base_url]
    if base_url == "https://www.gate.com":
        bases.append("https://www.gate.tv")
    last_status = None
    html = ""
    url = ""
    for base in bases:
        url = f"{base}/announcements/article/{article_id}"
        response = session.get(url, headers=_GATE_HEADERS, timeout=20)
        last_status = response.status_code
        if response.status_code == 403:
            _GATE_METRICS["article_403_count"] += 1
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
    _GATE_METRICS["article_403_count"] = 0
    _GATE_METRICS["timestamp_missing_count"] = 0
    url = "https://www.gate.tv/announcements/newlisted"
    response = session.get(url, headers=_GATE_HEADERS, timeout=20)
    LOGGER.info("Gate list url=%s status=%s", url, response.status_code)
    if response.status_code in (403, 451) or response.status_code >= 500:
        LOGGER.warning("Gate list response status=%s blocked_or_error", response.status_code)
        return []
    response.raise_for_status()
    html = response.text
    _log_list_page_debug(html)
    base_url = "https://www.gate.tv" if "gate.tv" in response.url else "https://www.gate.com"
    items = _parse_list_items(html, base_url)
    if not items:
        items = _parse_items_from_embedded_json(html, base_url)
    if not items:
        items = _parse_items_from_any_urls(html, base_url)
    announcements: List[Announcement] = []
    for item in items:
        article = _fetch_gate_article(session, item["id"], base_url)
        if article:
            announcements.append(article)
            continue
        if not item.get("published"):
            _GATE_METRICS["timestamp_missing_count"] += 1
            LOGGER.warning("Gate missing timestamp id=%s url=%s", item["id"], item["url"])
            continue
        if not item.get("title"):
            LOGGER.warning("Gate missing title id=%s url=%s", item["id"], item["url"])
            continue
        market_type = infer_market_type(item["title"], default="spot")
        tickers = extract_tickers(item["title"])
        announcements.append(
            Announcement(
                source_exchange="Gate",
                title=item["title"],
                published_at_utc=item["published"],
                launch_at_utc=None,
                url=item["url"],
                listing_type_guess=guess_listing_type(item["title"]),
                market_type=market_type,
                tickers=tickers,
                body="",
            )
        )
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    filtered = [a for a in announcements if a.published_at_utc.timestamp() >= cutoff]
    LOGGER.info(
        "Gate parsed announcements=%s filtered=%s",
        len(announcements),
        len(filtered),
    )
    return filtered
