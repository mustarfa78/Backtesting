from __future__ import annotations

from datetime import datetime, timezone
from typing import List
from xml.etree import ElementTree

import logging

from dateutil import parser

from adapters.common import Announcement, extract_tickers, guess_listing_type, infer_market_type, parse_datetime
from http_client import get_text

LOGGER = logging.getLogger(__name__)


def fetch_announcements(session, days: int = 30) -> List[Announcement]:
    LOGGER.info("Kraken adapter using RSS feed for asset listings (spot)")
    feed_url = "https://blog.kraken.com/category/asset-listings/feed/"
    try:
        xml_text = get_text(session, feed_url)
    except Exception as exc:  # noqa: BLE001
        LOGGER.warning("Kraken RSS fetch failed: %s", exc)
        return []
    announcements: List[Announcement] = []
    cutoff = datetime.now(timezone.utc).timestamp() - days * 86400
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        LOGGER.warning("Kraken RSS parse failed: %s", exc)
        return []
    for item in root.findall(".//item"):
        title = item.findtext("title", default="").strip()
        link = item.findtext("link", default="").strip()
        pub_date = item.findtext("pubDate", default="").strip()
        if not title or not link or not pub_date:
            continue
        published = parse_datetime(pub_date)
        if not published:
            try:
                published = parser.parse(pub_date)
            except (ValueError, TypeError):
                published = None
        if not published:
            continue
        published = published.astimezone(timezone.utc)
        if published.timestamp() < cutoff:
            continue
        tickers = extract_tickers(title)
        market_type = infer_market_type(title, default="spot")
        announcements.append(
            Announcement(
                source_exchange="Kraken",
                title=title,
                published_at_utc=published,
                launch_at_utc=None,
                url=link,
                listing_type_guess=guess_listing_type(title),
                market_type=market_type,
                tickers=tickers,
                body="",
            )
        )
    return announcements
