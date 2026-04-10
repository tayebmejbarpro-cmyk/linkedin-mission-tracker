"""
scraper/linkedin_scraper.py — Shared utility types and helpers for scrapers.

Provides the canonical RawPost type, contact extraction, 24h date filtering,
text hashing, and raw post persistence. Used by bereach_scraper.py.
"""

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from config.config import AppConfig

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
