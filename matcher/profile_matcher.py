"""
matcher/profile_matcher.py — Claude API-based mission scoring engine.

Fetches each consultant's LinkedIn profile once, builds profile vectors,
then scores each raw post with Claude Haiku 4.5 using parallel workers.

Optimizations vs sequential baseline:
  - ThreadPoolExecutor(5) for concurrent Claude API calls
  - 0.5s delay per worker instead of 2-5s sequential delay
  - Pre-filter: skip posts with no mission-related signal before sending to Claude
  - Multi-profile: scores post against all profiles in one Claude call

Filters posts below MIN_MATCH_SCORE and returns the rest sorted by score descending.
"""

import json
import logging
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import anthropic
import requests
from bs4 import BeautifulSoup

from config.config import AppConfig
from scraper.linkedin_scraper import RawPost

# Claude model for scoring — fast and cost-efficient
_CLAUDE_MODEL = "claude-haiku-4-5-20251001"
_CLAUDE_MAX_TOKENS = 1024
_CLAUDE_TEMPERATURE = 0.0

# Concurrent Claude API workers (safe within Haiku rate limits)
_MAX_CONCURRENT_SCORING = 5

# Delay per worker before calling Claude (staggers concurrent calls, seconds)
_WORKER_DELAY_MIN = 0.3
_WORKER_DELAY_MAX = 1.0

# Fallback profile vector used when a LinkedIn profile fetch fails
_FALLBACK_PROFILE = (
    "Mohamed Sid Ahmed — Freelance Consultant. "
    "Skills: Strategy, Digital Transformation, Project Management, Data Analysis, "
    "Business Development, Consulting, Python, SQL, Agile."
)

# Realistic browser User-Agent for profile fetch
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Regex to strip markdown code fences from Claude responses
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)\s*```")

# Keywords that signal a genuine freelance mission post
# Posts missing ALL of these are pre-filtered before Claude scoring
_MISSION_SIGNALS = [
    "freelance", "mission", "consultant", "prestation", "indépendant",
    "tjm", "cdi", "cdd", "recherche", "besoin", "profil", "urgent",
    "contract", "interim", "régie", "forfait",
]


class EnrichedPost(dict):
    """
    Typed alias for an enriched post dict. Contains all RawPost fields plus:
        mission_title: str
        required_skills: List[str]
        duration: str
        daily_rate_tjm: Optional[str]
        location: str
        remote_ok: bool
        claude_contact_info: Optional[str]
        match_score: float
        match_reasons: List[str]
        language: str              — "FR" or "EN"
        profil_name: str           — name of the LinkedIn profile with best match
        scored_at: str             — ISO 8601 UTC
    """


def score_posts(
    raw_posts: List[RawPost],
    config: AppConfig,
    logger: logging.Logger,
) -> List[EnrichedPost]:
    """
    Main entry point for the matcher module.

    Pre-filters posts, fetches all LinkedIn profile vectors in parallel,
    scores each post against all profiles with concurrent Claude workers,
    filters out posts with match_score < MIN_MATCH_SCORE, and returns
    the remaining posts sorted by match_score descending.

    Args:
        raw_posts: List of raw posts from the scraper.
        config: Application configuration.
        logger: Logger instance.

    Returns:
        Filtered, sorted list of EnrichedPost dicts.
    """
    if not raw_posts:
        logger.info("[matcher] No raw posts to score.")
        return []

    # Pre-filter: skip posts with no mission-related signal
    candidates = [p for p in raw_posts if _is_potential_mission(p.get("post_text", ""))]
    logger.info(
        "[matcher] Pre-filter: %d/%d posts pass mission signal check.",
        len(candidates), len(raw_posts),
    )

    if not candidates:
        logger.warning("[matcher] All posts filtered out. Check search keywords.")
        return []

    # Build profile vectors (one HTTP fetch per profile)
    profiles: List[Dict[str, str]] = []
    for profile_cfg in config.linkedin_profiles:
        logger.info("[matcher] Fetching LinkedIn profile '%s'...", profile_cfg["name"])
        html = _fetch_linkedin_profile(profile_cfg["url"], logger)
        vector = _build_profile_vector(html)
        profiles.append({"name": profile_cfg["name"], "vector": vector})
        logger.info(
            "[matcher] Profile '%s' vector built (%d chars).",
            profile_cfg["name"], len(vector),
        )

    anthropic_client = anthropic.Anthropic(api_key=config.anthropic_api_key)
    total = len(candidates)
    enriched: List[EnrichedPost] = []

    logger.info(
        "[matcher] Scoring %d posts with %d concurrent workers...",
        total, _MAX_CONCURRENT_SCORING,
    )

    with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_SCORING) as executor:
        future_to_post = {
            executor.submit(_score_post_with_claude, post, profiles, anthropic_client, logger): post
            for post in candidates
        }
        for done_count, future in enumerate(as_completed(future_to_post), start=1):
            post = future_to_post[future]
            try:
                claude_data = future.result()
            except Exception as exc:
                logger.error(
                    "[matcher] Scoring failed for %s: %s",
                    post.get("post_url", ""), exc,
                )
                claude_data = _make_error_enrichment(profiles)

            logger.info(
                "[matcher] Scored %d/%d — score=%.1f url=%s",
                done_count, total,
                float(claude_data.get("match_score", 0)),
                post.get("post_url", ""),
            )

            scored_at = datetime.now(timezone.utc).isoformat()
            enriched_post = EnrichedPost(
                **post,
                mission_title=claude_data.get("mission_title", ""),
                required_skills=claude_data.get("required_skills", []),
                duration=claude_data.get("duration", ""),
                daily_rate_tjm=claude_data.get("daily_rate_tjm"),
                location=claude_data.get("location", ""),
                remote_ok=bool(claude_data.get("remote_ok", False)),
                claude_contact_info=claude_data.get("contact_info"),
                match_score=float(claude_data.get("match_score", 0)),
                match_reasons=claude_data.get("match_reasons", []),
                language=claude_data.get("language", "FR"),
                profil_name=claude_data.get("best_profil", profiles[0]["name"] if profiles else ""),
                scored_at=scored_at,
            )
            enriched.append(enriched_post)

    # Filter then sort
    before = len(enriched)
    enriched = [p for p in enriched if p["match_score"] >= config.min_match_score]
    enriched.sort(key=lambda p: p["match_score"], reverse=True)

    logger.info(
        "[matcher] %d/%d posts kept (match_score >= %d).",
        len(enriched), before, config.min_match_score,
    )
    return enriched


def _is_potential_mission(text: str) -> bool:
    """
    Quick keyword scan to discard posts with no mission-related signal.

    Avoids sending clearly irrelevant posts (news, branding, congratulations)
    to Claude. Conservative — keeps posts if ANY signal word is present.

    Args:
        text: Post text to scan.

    Returns:
        True if at least one mission signal keyword is found.
    """
    tl = text.lower()
    return any(signal in tl for signal in _MISSION_SIGNALS)


def _fetch_linkedin_profile(profile_url: str, logger: logging.Logger) -> str:
    """
    Fetch the LinkedIn profile page HTML using a realistic browser User-Agent.

    LinkedIn public profiles return partial HTML without authentication.
    Falls back to a hardcoded minimal profile string if the fetch fails,
    so scoring can still run.

    Args:
        profile_url: LinkedIn profile URL.
        logger: Logger instance.

    Returns:
        Raw HTML string of the profile page, or fallback profile text on error.
    """
    try:
        headers = {"User-Agent": _USER_AGENT, "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8"}
        response = requests.get(profile_url, headers=headers, timeout=15)
        response.raise_for_status()
        return response.text
    except Exception as exc:
        logger.warning(
            "[matcher] Could not fetch LinkedIn profile (%s): %s — using fallback vector.",
            profile_url, exc,
        )
        return _FALLBACK_PROFILE


def _build_profile_vector(html: str) -> str:
    """
    Parse LinkedIn profile HTML with BeautifulSoup to extract a plain-text
    profile vector (skills, job titles, headline, about).

    Uses multiple fallback selectors so partial or obfuscated HTML still
    produces some useful text. Never raises.

    Args:
        html: Raw HTML of the LinkedIn profile page (or fallback string).

    Returns:
        Cleaned, concatenated plain-text profile vector string.
    """
    if not html.strip().startswith("<"):
        return html.strip()

    parts: List[str] = []
    try:
        soup = BeautifulSoup(html, "lxml")

        for sel in ["h1", ".text-heading-xlarge", ".pv-text-details__left-panel h1"]:
            el = soup.select_one(sel)
            if el:
                parts.append(el.get_text(separator=" ", strip=True))
                break

        for sel in [".pv-about-section", ".summary", "[data-field='summary']"]:
            el = soup.select_one(sel)
            if el:
                parts.append(el.get_text(separator=" ", strip=True))
                break

        for sel in [".pv-entity__summary-info h3", ".experience-item__title", ".pvs-entity h3"]:
            for el in soup.select(sel)[:5]:
                text = el.get_text(separator=" ", strip=True)
                if text:
                    parts.append(text)

        for sel in [".pv-skill-category-entity__name", ".skill-pill", ".pvs-entity .visually-hidden"]:
            for el in soup.select(sel)[:20]:
                text = el.get_text(separator=" ", strip=True)
                if text and len(text) < 60:
                    parts.append(text)

        for sel in [".pv-accomplishments-block__title", ".certification-name"]:
            for el in soup.select(sel)[:5]:
                text = el.get_text(separator=" ", strip=True)
                if text:
                    parts.append(text)

    except Exception:
        pass

    if not parts:
        return _FALLBACK_PROFILE

    return " | ".join(dict.fromkeys(p for p in parts if p))


def _score_post_with_claude(
    post: RawPost,
    profiles: List[Dict[str, str]],
    anthropic_client: anthropic.Anthropic,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Call the Claude API to extract structured mission data and compute a match score.

    Adds a short random delay before each call to stagger concurrent workers.
    Retries up to 3 times with exponential backoff on rate limit errors (429).
    On any unrecoverable failure, returns safe error defaults (match_score=0).

    Args:
        post: Raw post to score.
        profiles: List of profile dicts with 'name' and 'vector' keys.
        anthropic_client: Initialized Anthropic client instance.
        logger: Logger instance.

    Returns:
        Parsed dict with Claude's extracted fields, or safe defaults on error.
    """
    # Stagger concurrent workers to avoid simultaneous API bursts
    time.sleep(random.uniform(_WORKER_DELAY_MIN, _WORKER_DELAY_MAX))

    prompt = _build_claude_prompt(post.get("post_text", ""), profiles)
    backoff_seconds = [10, 20, 40]

    for attempt in range(4):  # 1 initial + 3 retries
        try:
            response = anthropic_client.messages.create(
                model=_CLAUDE_MODEL,
                max_tokens=_CLAUDE_MAX_TOKENS,
                temperature=_CLAUDE_TEMPERATURE,
                system=(
                    "You are a precise extraction assistant. "
                    "You always respond with valid JSON only — no prose, no markdown fences."
                ),
                messages=[{"role": "user", "content": prompt}],
            )
            raw_text = response.content[0].text
            return _parse_claude_response(raw_text, logger)

        except anthropic.RateLimitError:
            if attempt < 3:
                wait = backoff_seconds[attempt]
                logger.warning(
                    "[matcher] Claude rate limit — retrying in %ds (attempt %d/3)", wait, attempt + 1
                )
                time.sleep(wait)
            else:
                logger.error("[matcher] Claude rate limit persists after 3 retries — returning error defaults.")
                return _make_error_enrichment(profiles)

        except Exception as exc:
            logger.error("[matcher] Claude API error for post %s: %s", post.get("post_url", ""), exc)
            return _make_error_enrichment(profiles)

    return _make_error_enrichment(profiles)


def _build_claude_prompt(post_text: str, profiles: List[Dict[str, str]]) -> str:
    """
    Construct the structured extraction prompt sent to Claude.

    For a single profile, uses a focused single-profile format.
    For multiple profiles, asks Claude to score each and identify the best match.

    Args:
        post_text: Full text of the LinkedIn post.
        profiles: List of profile dicts with 'name' and 'vector' keys.

    Returns:
        Complete prompt string.
    """
    if len(profiles) == 1:
        profile_section = f"## Consultant Profile ({profiles[0]['name']}):\n{profiles[0]['vector']}"
        best_profil_field = f'  "best_profil": "{profiles[0]["name"]}",\n'
    else:
        profile_section = "## Consultant Profiles:\n"
        for p in profiles:
            profile_section += f"### {p['name']}:\n{p['vector']}\n\n"
        best_profil_field = (
            '  "best_profil": "string — name of the profile with the highest match score",\n'
        )

    return f"""You are analyzing a LinkedIn post that may describe a freelance mission opportunity.

## LinkedIn Post:
{post_text}

{profile_section}

## Task:
Extract mission details from the post and score how well each consultant profile matches.

Respond with ONLY a valid JSON object — no preamble, no markdown fences, no explanation.

## Required JSON fields:
{{
  "mission_title": "string — short title of the mission or role (e.g. 'Chef de projet Data')",
  "required_skills": ["list", "of", "skills", "mentioned", "in", "the", "post"],
  "duration": "string — mission duration or contract length (e.g. '3 months', 'CDI', 'unknown')",
  "daily_rate_tjm": "string or null — daily rate if explicitly mentioned (e.g. '600€/jour'), null otherwise",
  "location": "string — city or country of the mission (e.g. 'Paris', 'Remote', 'Casablanca')",
  "remote_ok": "boolean — true if remote work is mentioned or implied",
  "contact_info": "string or null — email or contact method from the post, null if none",
{best_profil_field}  "match_score": "float 0-100 — best match score across all profiles",
  "match_reasons": ["top 3 concise reasons explaining the score"],
  "language": "FR or EN — language of the post"
}}

## Scoring guidelines:
- 80-100: Strong match — most required skills are present in the profile
- 50-79: Partial match — some relevant skills or domain overlap
- 0-49: Weak match — few or no skill overlaps

## Example output:
{{
  "mission_title": "Chef de projet Digital",
  "required_skills": ["gestion de projet", "Agile", "Scrum", "transformation digitale"],
  "duration": "6 mois",
  "daily_rate_tjm": "650€/jour",
  "location": "Paris (Remote possible)",
  "remote_ok": true,
  "contact_info": "contact@example.com",
  "best_profil": "{profiles[0]['name']}",
  "match_score": 82.5,
  "match_reasons": [
    "Profile lists Project Management — key skill for this mission",
    "Digital Transformation expertise matches mission domain",
    "Agile/Scrum mentioned in both profile and post"
  ],
  "language": "FR"
}}"""


def _parse_claude_response(response_text: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Parse Claude's text response as JSON.

    Strips markdown code fences if present. Casts match_score to float safely.
    Validates list fields. On any failure, logs the raw response and returns
    safe error defaults.

    Args:
        response_text: Raw text from Claude API response.
        logger: Logger instance.

    Returns:
        Parsed dict or safe error defaults with match_score=0.
    """
    text = response_text.strip()

    fence_match = _CODE_FENCE_RE.search(text)
    if fence_match:
        text = fence_match.group(1).strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logger.error("[matcher] Claude returned invalid JSON: %r", response_text[:300])
        return _make_error_enrichment([])

    try:
        data["match_score"] = float(str(data.get("match_score", 0)).strip())
    except (ValueError, TypeError):
        data["match_score"] = 0.0

    for field in ("required_skills", "match_reasons"):
        val = data.get(field)
        if isinstance(val, str):
            data[field] = [s.strip() for s in val.split(",") if s.strip()]
        elif not isinstance(val, list):
            data[field] = []

    if not isinstance(data.get("remote_ok"), bool):
        raw_val = str(data.get("remote_ok", "false")).lower()
        data["remote_ok"] = raw_val in ("true", "1", "yes", "oui")

    return data


def _make_error_enrichment(profiles: List[Dict[str, str]]) -> Dict[str, Any]:
    """
    Return a safe default enrichment dict for when Claude scoring fails.

    match_score is set to 0 so the post is filtered out downstream.

    Args:
        profiles: Profile list, used to set a default best_profil name.

    Returns:
        Dict with all Claude fields set to safe defaults.
    """
    return {
        "mission_title": "",
        "required_skills": [],
        "duration": "",
        "daily_rate_tjm": None,
        "location": "",
        "remote_ok": False,
        "contact_info": None,
        "best_profil": profiles[0]["name"] if profiles else "",
        "match_score": 0.0,
        "match_reasons": [],
        "language": "FR",
    }
