from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests
import requests_cache
from tenacity import retry, stop_after_attempt, wait_exponential


LOGGER = logging.getLogger(__name__)


def build_session(cache_name: str = "http_cache", expire_seconds: int = 10800) -> requests.Session:
    session = requests_cache.CachedSession(
        cache_name=cache_name,
        backend="sqlite",
        expire_after=expire_seconds,
    )
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1",
        }
    )
    return session


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6))
def get_json(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    LOGGER.debug("GET %s params=%s", url, params)
    response = session.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6))
def get_text(session: requests.Session, url: str, params: Optional[Dict[str, Any]] = None) -> str:
    LOGGER.debug("GET %s params=%s", url, params)
    response = session.get(url, params=params, timeout=20)
    response.raise_for_status()
    return response.text
