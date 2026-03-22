"""
scraper/google_scraper.py — Google Custom Search API fallback scraper.

Queries Google CSE for LinkedIn posts matching each keyword × country pair,
normalizes results into RawPost dicts, applies a 24h filter, and deduplicates
by URL and text hash. Runs after the Apify scraper to surface posts it missed.

Requires env vars: GOOGLE_CSE_API_KEY, GOOGLE_CSE_ID (opt-in — skipped if absent).
"""

import hashlib
import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set

import requests

from config.config import AppConfig
from .linkedin_scraper import RawPost


# Google Custom Search API endpoint
_CSE_ENDPOINT = "https://www.googleapis.com/customsearch/v1"

# Max results per query (Google CSE hard limit)
_MAX_RESULTS_PER_QUERY = 10

# Seconds to wait between API calls to stay well within quota
_REQUEST_DELAY_RANGE = (0.5, 1.5)

# Regex for extracting author name from CSE result title
# Typical format: "First Last on LinkedIn: ..." or "First Last | LinkedIn"
_AUTHOR_RE = re.compile(r"^(.+?)\s+(?:on LinkedIn|sur LinkedIn|\|)", re.IGNORECASE)


def scrape_google(
    config: AppConfig,
    logger: logging.Logger,
    seen_urls: Optional[Set[str]] = None,
    seen_hashes: Optional[Set[str]] = None,
) -> List[RawPost]:
    """
    Query Google Custom Search API for LinkedIn posts matching each
    keyword × country pair in config.

    Applies 24h recency (via Google's `after:` operator), deduplicates by
    post_url and text hash (including cross-run known posts from the Dedup_Index),
    and returns normalized RawPost dicts ready for Claude scoring.

    Args:
        config: Loaded application configuration (must have google_cse_api_key
                and google_cse_id set, otherwise returns empty list).
        logger: Configured logger instance.
        seen_urls: Set of post URLs already written in previous runs.
        seen_hashes: Set of text hashes already written in previous runs.

    Returns:
        List of deduplicated RawPost dicts from Google CSE results.
    """
    if not config.google_cse_api_key or not config.google_cse_id:
        logger.info("[google_scraper] Skipped — GOOGLE_CSE_API_KEY or GOOGLE_CSE_ID not set.")
        return []

    seen_urls_global: Set[str] = seen_urls if seen_urls is not None else set()
    seen_hashes_global: Set[str] = seen_hashes if seen_hashes is not None else set()

    seen_urls_local: Set[str] = set()
    seen_hashes_local: Set[str] = set()
    all_posts: List[RawPost] = []

    # Use yesterday's date for the `after:` operator (24h window)
    yesterday = (datetime.now(timezone.utc) - timedelta(hours=24)).strftime("%Y-%m-%d")

    pairs = [("", keyword) for keyword in config.search_keywords]
    total = len(pairs)
    logger.info("[google_scraper] %d keyword queries to run.", total)

    for idx, (country, keyword) in enumerate(pairs, start=1):
        query = f'site:linkedin.com/posts "{keyword}" after:{yesterday}'
        logger.debug("[google_scraper] (%d/%d) Query: %s", idx, total, query)

        try:
            results = _call_cse(config.google_cse_api_key, config.google_cse_id, query, logger)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[google_scraper] (%d/%d) Request failed: %s", idx, total, exc)
            results = []

        new_count = 0
        for item in results:
            post = _normalize_item(item, country, keyword, logger)
            if post is None:
                continue

            url = post["post_url"]
            text_hash = _text_hash(post["post_text"])

            # Skip cross-run duplicates
            if url in seen_urls_global or text_hash in seen_hashes_global:
                continue

            # Skip within-run duplicates
            if url in seen_urls_local or text_hash in seen_hashes_local:
                continue

            # 24h check: if date metadata is available, validate it
            if post["post_date"] and not _is_within_24h(post["post_date"], logger):
                continue

            seen_urls_local.add(url)
            seen_hashes_local.add(text_hash)
            all_posts.append(post)
            new_count += 1

        logger.info(
            "[google_scraper] (%d/%d) done — %d new posts (country=%s, keyword='%s')",
            idx, total, new_count, country, keyword,
        )

        # Polite delay between requests
        time.sleep(random.uniform(*_REQUEST_DELAY_RANGE))

    logger.info("[google_scraper] Total unique posts from Google CSE: %d", len(all_posts))
    return all_posts


def _call_cse(api_key: str, cx: str, query: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Call the Google Custom Search JSON API and return the list of result items.

    Args:
        api_key: Google CSE API key.
        cx: Custom Search Engine ID.
        query: Search query string.
        logger: Logger instance.

    Returns:
        List of CSE result item dicts, or empty list if no results / quota exceeded.
    """
    params = {
        "key": api_key,
        "cx": cx,
        "q": query,
        "num": _MAX_RESULTS_PER_QUERY,
        "sort": "date",
    }
    response = requests.get(_CSE_ENDPOINT, params=params, timeout=30)

    if response.status_code == 429:
        logger.warning("[google_scraper] Quota exceeded (HTTP 429). Stopping further queries.")
        return []

    if response.status_code != 200:
        logger.warning(
            "[google_scraper] HTTP %d from CSE API: %s",
            response.status_code, response.text[:200],
        )
        return []

    data = response.json()
    items = data.get("items", [])
    logger.debug("[google_scraper] CSE returned %d items.", len(items))
    return items


def _normalize_item(
    item: Dict[str, Any],
    country: str,
    keyword: str,
    logger: logging.Logger,
) -> Optional[RawPost]:
    """
    Convert a raw Google CSE result item into a RawPost dict.

    Args:
        item: Raw CSE result item dict.
        country: Country used for this search query.
        keyword: Keyword used for this search query.
        logger: Logger instance.

    Returns:
        RawPost dict, or None if the URL is invalid or not a LinkedIn post.
    """
    url: str = item.get("link", "")
    if not url or "linkedin.com" not in url:
        return None

    # Normalize URL: strip tracking params
    url = url.split("?")[0].rstrip("/")

    title: str = item.get("title", "")
    snippet: str = item.get("snippet", "")

    # Parse author name from title ("First Last on LinkedIn: ...")
    author_name = ""
    match = _AUTHOR_RE.match(title)
    if match:
        author_name = match.group(1).strip()

    # Try to extract publication date from pagemap metadata
    post_date = _extract_date(item)

    return RawPost(
        post_url=url,
        author_name=author_name,
        author_title="",
        author_profile_url="",
        post_text=snippet,
        post_date=post_date,
        likes_count=0,
        comments_count=0,
        contact_info=None,
        country=country,
        keyword=keyword,
    )


def _extract_date(item: Dict[str, Any]) -> str:
    """
    Extract publication date from CSE result metadata if available.

    Tries pagemap.metatags[0] fields (article:published_time, og:updated_time),
    then falls back to current UTC datetime so the post passes the 24h check
    (Google's `after:` operator already enforces recency).

    Args:
        item: Raw CSE result item dict.

    Returns:
        ISO 8601 UTC datetime string.
    """
    pagemap = item.get("pagemap", {})
    metatags = pagemap.get("metatags", [{}])
    if metatags:
        meta = metatags[0]
        for field in ("article:published_time", "og:updated_time", "datePublished"):
            raw = meta.get(field, "")
            if raw:
                try:
                    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    return dt.astimezone(timezone.utc).isoformat()
                except (ValueError, TypeError):
                    continue

    # No date found: use current UTC (Google's `after:` already ensures recency)
    return datetime.now(timezone.utc).isoformat()


def _text_hash(text: str) -> str:
    """
    Compute a short hash of the first 300 normalized characters of post text.

    Mirrors linkedin_scraper._text_hash for consistent cross-source deduplication.

    Args:
        text: Post text (may be a truncated snippet from Google CSE).

    Returns:
        MD5 hex digest of the normalized text prefix.
    """
    normalized = " ".join(text[:300].split())
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def _is_within_24h(post_date_iso: str, logger: logging.Logger) -> bool:
    """
    Check whether a post's UTC datetime falls within the last 24 hours.

    Mirrors linkedin_scraper._is_within_24h for consistent filtering.

    Args:
        post_date_iso: ISO 8601 UTC datetime string.
        logger: Logger instance.

    Returns:
        True if the post is within the last 24 hours, False otherwise.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    try:
        post_dt = datetime.fromisoformat(post_date_iso)
        if post_dt.tzinfo is None:
            post_dt = post_dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        logger.warning(
            "[google_scraper] Could not parse post_date '%s' for 24h check — keeping post",
            post_date_iso,
        )
        return True  # Keep when date is unparseable (Google's after: already filters)

    return post_dt >= cutoff
