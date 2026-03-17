"""
scraper/linkedin_scraper.py — Apify-based LinkedIn post scraper.

Calls the Apify LinkedIn Post Search actor for every country × keyword combination,
normalizes results into RawPost dicts, applies a 24h safety filter, deduplicates
by post_url, saves raw JSON to disk, and returns the final list.
"""

import json
import logging
import random
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from apify_client import ApifyClient

from config.config import AppConfig

# Apify actor to use for LinkedIn post search
_APIFY_ACTOR_ID = "apify/linkedin-post-search-scraper"

# Timeout for a single Apify actor run in seconds
_ACTOR_TIMEOUT_SECS = 120

# Regex patterns for contact info extraction
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
_PHONE_RE = re.compile(r"(?:\+?\d[\d\s\-().]{7,}\d)")


class RawPost(dict):
    """
    Typed alias for a raw scraped post dict with the following keys:
        post_url: str
        author_name: str
        author_title: str
        author_profile_url: str
        post_text: str
        post_date: str          — ISO 8601 UTC datetime string
        likes_count: int
        comments_count: int
        contact_info: Optional[str]
        country: str
        keyword: str
    """


def scrape_all_countries(config: AppConfig, logger: logging.Logger) -> List[RawPost]:
    """
    Orchestrate Apify scraping for all countries and keywords in config.

    Iterates every (country, keyword) pair, triggers an Apify actor run for each,
    normalizes and 24h-filters results, then deduplicates globally by post_url.
    Saves raw results to data/raw_posts_{YYYY-MM-DD}.json before returning.

    Args:
        config: Loaded application configuration.
        logger: Configured logger instance.

    Returns:
        List of deduplicated RawPost dicts, all published within the last 24 hours.
    """
    seen_urls: set = set()
    all_posts: List[RawPost] = []
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    pairs = [
        (country, keyword)
        for country in config.target_countries
        for keyword in config.search_keywords
    ]
    total = len(pairs)

    for idx, (country, keyword) in enumerate(pairs, start=1):
        logger.info(
            "[scraper] (%d/%d) country=%s keyword='%s'", idx, total, country, keyword
        )

        raw_items = _run_apify_actor(config, country, keyword, logger)

        batch_added = 0
        for item in raw_items:
            post = _normalize_post(item, country, keyword)
            if post is None:
                continue
            if not _is_within_24h(post["post_date"], logger):
                continue
            if post["post_url"] in seen_urls:
                logger.debug("[scraper] duplicate skipped: %s", post["post_url"])
                continue
            seen_urls.add(post["post_url"])
            all_posts.append(post)
            batch_added += 1

        logger.info(
            "[scraper] batch done — %d new posts (country=%s, keyword='%s')",
            batch_added, country, keyword,
        )

        # Delay between actor calls to respect rate limits
        if idx < total:
            delay = random.uniform(2, 5)
            logger.debug("[scraper] sleeping %.1fs before next call", delay)
            time.sleep(delay)

    logger.info("[scraper] total unique posts within 24h: %d", len(all_posts))
    _save_raw_posts(all_posts, date_str, logger)
    return all_posts


def _run_apify_actor(
    config: AppConfig,
    country: str,
    keyword: str,
    logger: logging.Logger,
) -> List[Dict[str, Any]]:
    """
    Trigger a single Apify actor run for one country + keyword combination.

    Blocks until the run completes (SUCCEEDED) or times out (120s).
    On ApifyApiError, timeout, or FAILED status, logs the error and returns [].

    Args:
        config: Application configuration (provides APIFY_API_TOKEN, MAX_POSTS_PER_COUNTRY).
        country: Target country string (e.g., "France").
        keyword: Search keyword string (e.g., "mission freelance").
        logger: Logger instance.

    Returns:
        List of raw Apify result dicts, or [] on any failure.
    """
    client = ApifyClient(config.apify_api_token)
    run_input = {
        "keywords": keyword,
        "country": country,
        "maxResults": config.max_posts_per_country,
        "datePosted": "past-24h",
    }

    try:
        logger.debug("[scraper] starting Apify actor run — input: %s", run_input)
        run = client.actor(_APIFY_ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=_ACTOR_TIMEOUT_SECS,
        )
    except Exception as exc:
        logger.error(
            "[scraper] Apify actor call failed (country=%s, keyword='%s'): %s",
            country, keyword, exc,
        )
        return []

    if run is None or run.get("status") != "SUCCEEDED":
        status = run.get("status") if run else "None"
        logger.error(
            "[scraper] Apify run did not succeed (status=%s, country=%s, keyword='%s')",
            status, country, keyword,
        )
        return []

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        logger.warning("[scraper] No dataset ID returned for run (country=%s, keyword='%s')", country, keyword)
        return []

    try:
        items = client.dataset(dataset_id).list_items().items
    except Exception as exc:
        logger.error("[scraper] Failed to fetch dataset %s: %s", dataset_id, exc)
        return []

    logger.debug("[scraper] Apify returned %d items", len(items))
    return items


def _normalize_post(
    raw: Dict[str, Any],
    country: str,
    keyword: str,
) -> Optional[RawPost]:
    """
    Map an Apify result dict to the canonical RawPost structure.

    Returns None if essential fields (post_url, post_text) are missing.
    Parses the post date to a UTC-aware ISO string.
    Extracts contact_info via regex from post_text.

    Args:
        raw: Single item from Apify dataset.
        country: Country that produced this result.
        keyword: Keyword that produced this result.

    Returns:
        Normalized RawPost or None if the post is malformed.
    """
    post_url = raw.get("url") or raw.get("postUrl") or raw.get("id", "")
    post_text = raw.get("text") or raw.get("postText") or raw.get("content", "")

    if not post_url or not post_text:
        return None

    # Parse post date — Apify may return ISO strings or relative strings
    raw_date = (
        raw.get("postedAt")
        or raw.get("publishedAt")
        or raw.get("date")
        or raw.get("createdAt")
        or ""
    )
    post_date_iso = _parse_post_date(raw_date)

    author = raw.get("author") or {}
    if isinstance(author, str):
        author_name = author
        author_title = ""
        author_profile_url = ""
    else:
        author_name = author.get("name") or raw.get("authorName", "")
        author_title = author.get("title") or author.get("headline") or raw.get("authorTitle", "")
        author_profile_url = author.get("url") or author.get("profileUrl") or raw.get("authorProfileUrl", "")

    return RawPost(
        post_url=post_url,
        author_name=author_name,
        author_title=author_title,
        author_profile_url=author_profile_url,
        post_text=post_text,
        post_date=post_date_iso,
        likes_count=int(raw.get("likesCount") or raw.get("likes") or 0),
        comments_count=int(raw.get("commentsCount") or raw.get("comments") or 0),
        contact_info=_extract_contact_info(post_text),
        country=country,
        keyword=keyword,
    )


def _parse_post_date(raw_date: str) -> str:
    """
    Attempt to parse a date string from Apify into an ISO 8601 UTC string.

    Handles ISO format strings and falls back to utcnow() if unparseable.
    LinkedIn relative strings ("2h", "1d") are expected to be resolved by
    Apify before reaching this function.

    Args:
        raw_date: Raw date string from the Apify dataset item.

    Returns:
        ISO 8601 UTC datetime string (e.g., "2026-03-17T06:00:00+00:00").
    """
    if not raw_date:
        return datetime.now(timezone.utc).isoformat()

    # Try standard ISO parsing
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(raw_date, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            continue

    # Fallback: use utcnow (conservative — post passes the 24h filter)
    return datetime.now(timezone.utc).isoformat()


def _is_within_24h(post_date_iso: str, logger: logging.Logger) -> bool:
    """
    Check whether a post's UTC datetime falls within the last 24 hours.

    Logs a warning if the post falls outside the window, since the Apify
    datePosted="past-24h" filter should have already excluded it.

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
        logger.warning("[scraper] Could not parse post_date '%s' for 24h check — discarding", post_date_iso)
        return False

    if post_dt < cutoff:
        logger.warning(
            "[scraper] Post outside 24h window (post_date=%s, cutoff=%s) — discarding",
            post_date_iso, cutoff.isoformat(),
        )
        return False
    return True


def _extract_contact_info(text: str) -> Optional[str]:
    """
    Extract the first email address or phone number found in post_text.

    Prioritizes email over phone number. Returns None if nothing is found.

    Args:
        text: Full post text.

    Returns:
        Extracted contact string or None.
    """
    email_match = _EMAIL_RE.search(text)
    if email_match:
        return email_match.group(0)

    phone_match = _PHONE_RE.search(text)
    if phone_match:
        candidate = phone_match.group(0).strip()
        # Require at least 8 digits to avoid false positives
        if sum(c.isdigit() for c in candidate) >= 8:
            return candidate

    return None


def _save_raw_posts(
    posts: List[RawPost],
    date_str: str,
    logger: logging.Logger,
) -> None:
    """
    Serialize and save raw posts to data/raw_posts_{date_str}.json.

    Creates the data/ directory if it does not exist.
    On write failure, logs a warning but does not raise.

    Args:
        posts: List of raw post dicts.
        date_str: Date string in YYYY-MM-DD format.
        logger: Logger instance.
    """
    data_dir = Path(__file__).parent.parent / "data"
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        output_path = data_dir / f"raw_posts_{date_str}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(posts, f, ensure_ascii=False, indent=2)
        logger.info("[scraper] raw posts saved to %s", output_path)
    except Exception as exc:
        logger.warning("[scraper] Could not save raw posts to disk: %s", exc)
