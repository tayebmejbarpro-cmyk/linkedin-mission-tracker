"""
scraper/linkedin_scraper.py — Apify-based LinkedIn post scraper.

Calls the Apify LinkedIn Post Search actor for every country × keyword combination
in parallel (up to 3 concurrent calls), normalizes results into RawPost dicts,
applies a 24h safety filter, deduplicates by post_url AND by post_text hash
(catches reposts with different URLs), saves raw JSON to disk, and returns
the final list.
"""

import hashlib
import json
import logging
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from apify_client import ApifyClient

from config.config import AppConfig

# Apify actor to use for LinkedIn post search
# apimaestro/linkedin-posts-search-scraper-no-cookies — 6.9K users, 4.5★, no login required
_APIFY_ACTOR_ID = "apimaestro/linkedin-posts-search-scraper-no-cookies"

# Timeout for a single Apify actor run in seconds
_ACTOR_TIMEOUT_SECS = 120

# Max concurrent Apify actor calls (limited to avoid account-level throttling)
_MAX_CONCURRENT_APIFY = 3

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


def scrape_all_countries(
    config: AppConfig,
    logger: logging.Logger,
    seen_urls: Optional[Set[str]] = None,
    seen_hashes: Optional[Set[str]] = None,
) -> List[RawPost]:
    """
    Orchestrate Apify scraping for all countries and keywords in config.

    Runs up to _MAX_CONCURRENT_APIFY actor calls in parallel. Normalizes,
    24h-filters, and deduplicates results globally by post_url AND by
    text hash (to catch reposts with different URLs). Saves raw results to
    data/raw_posts_{YYYY-MM-DD}.json before returning.

    Cross-run deduplication: posts whose URL or text hash are already present
    in the Google Sheet (pre-loaded by run.py) are discarded before being
    returned to the scorer, avoiding unnecessary Claude API calls.

    Args:
        config: Loaded application configuration.
        logger: Configured logger instance.
        seen_urls: Optional set of post URLs already written in previous runs.
        seen_hashes: Optional set of text hashes already written in previous runs.

    Returns:
        List of deduplicated RawPost dicts, all published within the last 24 hours,
        with already-known posts (by URL or text hash) removed.
    """
    # Cross-run sets (from Google Sheet) — rename to avoid shadowing local sets
    seen_urls_global: Set[str] = seen_urls if seen_urls is not None else set()
    seen_hashes_global: Set[str] = seen_hashes if seen_hashes is not None else set()

    # In-memory per-run sets (reset each execution)
    seen_urls: set = set()
    seen_text_hashes: set = set()
    all_posts: List[RawPost] = []
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    apify_failures = 0

    pairs = [
        (country, keyword)
        for country in config.target_countries
        for keyword in config.search_keywords
    ]
    total = len(pairs)
    logger.info(
        "[scraper] %d country×keyword pairs — running up to %d in parallel.",
        total, _MAX_CONCURRENT_APIFY,
    )

    with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_APIFY) as executor:
        future_to_pair = {
            executor.submit(_run_apify_actor, config, country, keyword, logger): (country, keyword)
            for country, keyword in pairs
        }

        done_count = 0
        for future in as_completed(future_to_pair):
            country, keyword = future_to_pair[future]
            done_count += 1
            try:
                raw_items = future.result()
            except Exception as exc:
                logger.error(
                    "[scraper] (%d/%d) batch failed country=%s keyword='%s': %s",
                    done_count, total, country, keyword, exc,
                )
                raw_items = []
                apify_failures += 1

            batch_added = 0
            for item in raw_items:
                post = _normalize_post(item, country, keyword)
                if post is None:
                    continue
                if not _is_within_24h(post["post_date"], logger):
                    continue
                # Deduplicate by URL (in-memory, this run)
                if post["post_url"] in seen_urls:
                    logger.debug("[scraper] duplicate URL skipped: %s", post["post_url"])
                    continue
                # Deduplicate by text hash (in-memory, catches reposts with different URLs)
                text_hash = _text_hash(post["post_text"])
                if text_hash in seen_text_hashes:
                    logger.debug("[scraper] near-duplicate text skipped: %s", post["post_url"])
                    continue
                # Cross-run dedup: skip posts already stored in Google Sheet
                if post["post_url"] in seen_urls_global:
                    logger.debug("[scraper] cross-run duplicate URL skipped: %s", post["post_url"])
                    continue
                if text_hash in seen_hashes_global:
                    logger.debug("[scraper] cross-run repost skipped (text hash): %s", post["post_url"])
                    continue

                seen_urls.add(post["post_url"])
                seen_text_hashes.add(text_hash)
                all_posts.append(post)
                batch_added += 1

            logger.info(
                "[scraper] (%d/%d) done — %d new posts (country=%s, keyword='%s')",
                done_count, total, batch_added, country, keyword,
            )

    if apify_failures:
        logger.warning(
            "[scraper] %d/%d Apify batches failed — results may be incomplete.", apify_failures, total
        )
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

    Adds a small random pre-call delay to stagger concurrent requests.
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
    # Stagger concurrent starts to avoid simultaneous actor launches
    time.sleep(random.uniform(0.5, 2.0))

    client = ApifyClient(config.apify_api_token)
    run_input = {
        "keyword": f"{keyword} {country}",
        "sort_type": "date_posted",
        "date_filter": "past-24h",
        "limit": min(config.max_posts_per_country, 50),
        "total_posts": config.max_posts_per_country,
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
        logger.warning(
            "[scraper] No dataset ID returned for run (country=%s, keyword='%s')", country, keyword
        )
        return []

    try:
        items = client.dataset(dataset_id).list_items().items
    except Exception as exc:
        logger.error("[scraper] Failed to fetch dataset %s: %s", dataset_id, exc)
        return []

    logger.debug(
        "[scraper] Apify returned %d items (country=%s, keyword='%s')",
        len(items), country, keyword,
    )
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
    post_url = (
        raw.get("postUrl") or raw.get("url") or raw.get("post_url") or raw.get("id", "")
    )
    post_text = (
        raw.get("text") or raw.get("postText") or raw.get("post_text") or raw.get("content", "")
    )

    if not post_url or not post_text:
        return None

    raw_date = (
        raw.get("postedAt")
        or raw.get("posted_at")
        or raw.get("publishedAt")
        or raw.get("date")
        or raw.get("createdAt")
        or ""
    )
    post_date_iso = _parse_post_date(raw_date)

    author = raw.get("author") or {}
    if isinstance(author, dict):
        author_name = author.get("name") or author.get("fullName") or raw.get("authorName", "")
        author_title = author.get("title") or author.get("headline") or raw.get("authorHeadline", "")
        author_profile_url = author.get("url") or author.get("profileUrl") or raw.get("authorUrl", "")
    else:
        author_name = raw.get("authorName") or raw.get("author_name") or str(author)
        author_title = raw.get("authorHeadline") or raw.get("authorTitle") or raw.get("author_title", "")
        author_profile_url = raw.get("authorUrl") or raw.get("authorProfileUrl") or raw.get("author_profile_url", "")

    return RawPost(
        post_url=post_url,
        author_name=author_name,
        author_title=author_title,
        author_profile_url=author_profile_url,
        post_text=post_text,
        post_date=post_date_iso,
        likes_count=int(raw.get("likesCount") or raw.get("likes") or raw.get("num_likes") or 0),
        comments_count=int(raw.get("commentsCount") or raw.get("comments") or raw.get("num_comments") or 0),
        contact_info=_extract_contact_info(post_text),
        country=country,
        keyword=keyword,
    )


def _parse_post_date(raw_date: str) -> str:
    """
    Attempt to parse a date string from Apify into an ISO 8601 UTC string.

    Handles ISO format strings and dict-typed date objects from some actors.
    Falls back to utcnow() if unparseable.

    Args:
        raw_date: Raw date string or dict from the Apify dataset item.

    Returns:
        ISO 8601 UTC datetime string (e.g., "2026-03-17T06:00:00+00:00").
    """
    if not raw_date:
        return datetime.now(timezone.utc).isoformat()

    if isinstance(raw_date, dict):
        raw_date = (
            raw_date.get("date") or raw_date.get("timestamp") or
            raw_date.get("text") or raw_date.get("value") or ""
        )
    if not raw_date:
        return datetime.now(timezone.utc).isoformat()

    raw_date = str(raw_date).strip()

    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            dt = datetime.strptime(raw_date, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except ValueError:
            continue

    return datetime.now(timezone.utc).isoformat()


def _is_within_24h(post_date_iso: str, logger: logging.Logger) -> bool:
    """
    Check whether a post's UTC datetime falls within the last 24 hours.

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
            "[scraper] Could not parse post_date '%s' for 24h check — discarding", post_date_iso
        )
        return False

    if post_dt < cutoff:
        logger.warning(
            "[scraper] Post outside 24h window (post_date=%s, cutoff=%s) — discarding",
            post_date_iso, cutoff.isoformat(),
        )
        return False
    return True


def _text_hash(text: str) -> str:
    """
    Compute a short hash of the first 300 normalized characters of post text.

    Used for near-duplicate detection: the same mission reposted with a
    slightly different URL will produce the same hash and be discarded.

    Args:
        text: Full post text.

    Returns:
        MD5 hex digest of the normalized text prefix.
    """
    normalized = " ".join(text[:300].split())
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


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
