"""
matcher/profile_matcher.py — Claude API-based mission scoring engine.

Fetches each consultant's LinkedIn profile once, builds profile vectors,
then scores each raw post with Claude Haiku 4.5 using parallel workers.

Optimizations vs sequential baseline:
  - ThreadPoolExecutor(5) for concurrent Claude API calls
  - 0.5s delay per worker instead of 2-5s sequential delay
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
from apify_client import ApifyClient
from bs4 import BeautifulSoup

from config.config import AppConfig
from scraper.linkedin_scraper import RawPost

# Apify actor for LinkedIn profile scraping (no cookies required)
_APIFY_PROFILE_ACTOR_ID = "apimaestro/linkedin-profile-batch-scraper-no-cookies-required"
_PROFILE_ACTOR_TIMEOUT_SECS = 120

# Claude model for scoring — fast and cost-efficient
_CLAUDE_MODEL = "claude-haiku-4-5-20251001"
_CLAUDE_MAX_TOKENS = 1024
_CLAUDE_TEMPERATURE = 0.0

# Concurrent Claude API workers (safe within Haiku rate limits)
_MAX_CONCURRENT_SCORING = 5

# Delay per worker before calling Claude (staggers concurrent calls, seconds)
_WORKER_DELAY_MIN = 0.3
_WORKER_DELAY_MAX = 1.0

# Fallback profile vector used when a LinkedIn profile fetch fails.
# Intentionally generic — a hardcoded personal name would be wrong for other users.
_FALLBACK_PROFILE = (
    "Freelance Consultant. "
    "Skills: Strategy, Digital Transformation, Project Management, Data Analysis, "
    "Business Development, Consulting, Python, SQL, Agile, Innovation."
)

# Realistic browser User-Agent for profile fetch
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# Regex to strip markdown code fences from Claude responses
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)\s*```")


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


def fetch_profile_vectors(
    config: AppConfig,
    logger: logging.Logger,
    cached: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    """
    Build profile vectors for all profiles in config.

    For each profile URL already present in `cached`, re-uses the stored
    vector without calling Apify. For missing profiles, calls the Apify
    actor and falls back to HTTP scrape if Apify fails.

    Args:
        config: Application configuration (provides linkedin_profiles + apify_api_token).
        logger: Logger instance.
        cached: Optional dict mapping URL → vector text already loaded from cache.

    Returns:
        Dict mapping URL → {"name": str, "vector": str} for all profiles.
        Only entries NOT previously in `cached` are newly fetched (so the caller
        can identify which ones need saving to the sheet cache).
    """
    cached = cached or {}
    result: Dict[str, Dict[str, str]] = {}

    for p in config.linkedin_profiles:
        url = p["url"]
        name = p["name"]

        if url in cached:
            logger.info(
                "[matcher] Profile '%s' loaded from sheet cache (%d chars).",
                name, len(cached[url]),
            )
            result[url] = {"name": name, "vector": cached[url]}
            continue

        # Not cached — fetch via Apify
        logger.info("[matcher] Profile '%s' not cached — fetching via Apify...", name)
        apify_data = _fetch_profile_via_apify(config.apify_api_token, url, logger)
        if apify_data:
            vector = _build_profile_vector_from_apify(apify_data)
            logger.info("[matcher] Profile '%s' fetched via Apify (%d chars).", name, len(vector))
        else:
            logger.warning(
                "[matcher] Apify fetch failed for '%s' — falling back to HTTP scrape.", name
            )
            html = _fetch_linkedin_profile(url, logger)
            vector = _build_profile_vector(html)
            logger.info("[matcher] Profile '%s' built from HTTP fallback (%d chars).", name, len(vector))

        result[url] = {"name": name, "vector": vector}

    return result


def score_posts(
    raw_posts: List[RawPost],
    config: AppConfig,
    logger: logging.Logger,
    profile_vectors: Optional[Dict[str, Dict[str, str]]] = None,
    feedback_examples: Optional[List[Dict[str, str]]] = None,
) -> List[EnrichedPost]:
    """
    Main entry point for the matcher module.

    Builds/uses profile vectors, scores each post against all profiles with
    concurrent Claude workers, filters out posts with match_score < MIN_MATCH_SCORE,
    and returns the remaining posts sorted by match_score descending.

    Args:
        raw_posts: List of raw posts from the scraper.
        config: Application configuration.
        logger: Logger instance.
        profile_vectors: Optional pre-built vectors dict from fetch_profile_vectors().
            If None, vectors are fetched internally (backwards-compatible).
        feedback_examples: Optional list of past user feedback dicts
            (keys: mission_title, required_skills, feedback). Injected into
            the Claude prompt as few-shot corrections.

    Returns:
        Filtered, sorted list of EnrichedPost dicts.
    """
    if not raw_posts:
        logger.info("[matcher] No raw posts to score.")
        return []

    candidates = raw_posts

    # Build profile list from pre-built vectors or fetch on the fly
    if profile_vectors:
        profiles = [
            {"name": info["name"], "vector": info["vector"]}
            for info in profile_vectors.values()
        ]
    else:
        # Backwards-compatible: fetch profiles inline (no cache)
        fetched = fetch_profile_vectors(config, logger)
        profiles = [{"name": info["name"], "vector": info["vector"]} for info in fetched.values()]

    anthropic_client = anthropic.Anthropic(api_key=config.anthropic_api_key)
    total = len(candidates)
    enriched: List[EnrichedPost] = []
    claude_failures = 0
    empty_extractions = 0

    logger.info(
        "[matcher] Scoring %d posts with %d concurrent workers...",
        total, _MAX_CONCURRENT_SCORING,
    )

    with ThreadPoolExecutor(max_workers=_MAX_CONCURRENT_SCORING) as executor:
        future_to_post = {
            executor.submit(
                _score_post_with_claude, post, profiles, anthropic_client, logger,
                feedback_examples or [],
            ): post
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
                claude_failures += 1

            # Force score=0 for posts that are not genuine mission offers
            if not claude_data.get("is_genuine_mission", True):
                claude_data["match_score"] = 0.0
                logger.info(
                    "[matcher] Scored %d/%d — score=0.0 (not a genuine mission) url=%s",
                    done_count, total, post.get("post_url", ""),
                )
            else:
                score = float(claude_data.get("match_score", 0))
                logger.info(
                    "[matcher] Scored %d/%d — score=%.1f url=%s",
                    done_count, total, score, post.get("post_url", ""),
                )
            score = float(claude_data.get("match_score", 0))

            # Warn when Claude failed to extract any mission structure
            title = claude_data.get("mission_title", "")
            skills = claude_data.get("required_skills", [])
            if not title and not skills and score > 0:
                logger.warning(
                    "[matcher] Claude returned score=%.1f but no title/skills for %s — may be a false positive.",
                    score, post.get("post_url", ""),
                )
                empty_extractions += 1

            scored_at = datetime.now(timezone.utc).isoformat()
            enriched_post = EnrichedPost(
                **post,
                mission_title=title,
                required_skills=skills,
                duration=claude_data.get("duration", ""),
                daily_rate_tjm=claude_data.get("daily_rate_tjm"),
                location=claude_data.get("location", ""),
                remote_ok=bool(claude_data.get("remote_ok", False)),
                claude_contact_info=claude_data.get("contact_info"),
                match_score=score,
                match_reasons=claude_data.get("match_reasons", []),
                language=claude_data.get("language", "FR"),
                profil_name=claude_data.get("best_profil", profiles[0]["name"] if profiles else ""),
                scored_at=scored_at,
            )
            enriched.append(enriched_post)

    # Summarise failures
    if claude_failures:
        logger.warning(
            "[matcher] %d/%d Claude scoring calls failed (returned error defaults).", claude_failures, total
        )
    if empty_extractions:
        logger.warning(
            "[matcher] %d posts scored >0 but Claude extracted no title or skills — review prompts.", empty_extractions
        )

    # Filter then sort
    before = len(enriched)
    enriched = [p for p in enriched if p["match_score"] >= config.min_match_score]
    enriched.sort(key=lambda p: p["match_score"], reverse=True)

    logger.info(
        "[matcher] %d/%d posts kept (match_score >= %d).",
        len(enriched), before, config.min_match_score,
    )
    return enriched



def _fetch_profile_via_apify(
    apify_api_token: str,
    profile_url: str,
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    """
    Fetch a LinkedIn profile's structured data via the Apify actor
    apimaestro/linkedin-profile-batch-scraper-no-cookies-required.

    Blocks until the run completes (SUCCEEDED) or times out (120s).
    On any failure, logs a warning and returns None so the caller can fall back.

    Args:
        apify_api_token: Apify API token.
        profile_url: Full LinkedIn profile URL.
        logger: Logger instance.

    Returns:
        First item from the Apify dataset dict, or None on failure.
    """
    client = ApifyClient(apify_api_token)
    run_input = {"profileUrls": [profile_url]}

    try:
        logger.debug("[matcher] Starting Apify profile actor — input: %s", run_input)
        run = client.actor(_APIFY_PROFILE_ACTOR_ID).call(
            run_input=run_input,
            timeout_secs=_PROFILE_ACTOR_TIMEOUT_SECS,
        )
    except Exception as exc:
        logger.warning(
            "[matcher] Apify profile actor call failed (%s): %s", profile_url, exc
        )
        return None

    if run is None or run.get("status") != "SUCCEEDED":
        status = run.get("status") if run else "None"
        logger.warning(
            "[matcher] Apify profile actor did not succeed (status=%s, url=%s)", status, profile_url
        )
        return None

    dataset_id = run.get("defaultDatasetId")
    if not dataset_id:
        logger.warning("[matcher] No dataset ID returned for profile %s", profile_url)
        return None

    try:
        items = client.dataset(dataset_id).list_items().items
    except Exception as exc:
        logger.error("[matcher] Failed to fetch profile dataset %s: %s", dataset_id, exc)
        return None

    if not items:
        logger.warning("[matcher] Apify profile actor returned no items for %s", profile_url)
        return None

    logger.debug("[matcher] Apify profile actor returned %d item(s) for %s", len(items), profile_url)
    return items[0]


def _build_profile_vector_from_apify(data: Dict[str, Any]) -> str:
    """
    Build a plain-text profile vector from structured Apify profile data.

    Extracts name, headline, about text, job titles from experience,
    skills, and certifications. Returns _FALLBACK_PROFILE if nothing useful
    is found.

    Args:
        data: Profile item dict returned by the Apify actor.

    Returns:
        Pipe-separated plain-text profile vector string.
    """
    parts: List[str] = []

    # Basic info — may be nested under "basic_info" or flat at top level
    basic = data.get("basic_info") or {}
    if isinstance(basic, dict):
        name = basic.get("fullname") or basic.get("name") or ""
        headline = basic.get("headline") or basic.get("title") or ""
        location = basic.get("location") or {}
        if isinstance(location, dict):
            loc_str = ", ".join(filter(None, [location.get("city"), location.get("country")]))
        else:
            loc_str = str(location) if location else ""
    else:
        # Flat structure fallback
        name = data.get("fullname") or data.get("name") or ""
        headline = data.get("headline") or data.get("title") or ""
        loc_str = ""

    for val in (name, headline, loc_str):
        if val:
            parts.append(val)

    # About / summary
    for field in ("about", "summary", "description"):
        val = (
            data.get(field)
            or (basic.get(field) if isinstance(basic, dict) else None)
            or ""
        )
        if val and isinstance(val, str):
            parts.append(val[:500])
            break

    # Experience — extract job titles (+ company) for the first 5 roles
    experience = data.get("experience") or []
    if isinstance(experience, list):
        for exp in experience[:5]:
            if not isinstance(exp, dict):
                continue
            title = (
                exp.get("title") or exp.get("role") or exp.get("position") or ""
            )
            company = (
                exp.get("company") or exp.get("companyName") or exp.get("organization") or ""
            )
            if title:
                parts.append(f"{title}" + (f" at {company}" if company else ""))

    # Skills
    skills = data.get("skills") or []
    if isinstance(skills, list):
        skill_names = []
        for sk in skills[:20]:
            if isinstance(sk, dict):
                sk_name = sk.get("name") or sk.get("skill") or ""
                if sk_name:
                    skill_names.append(sk_name)
            elif isinstance(sk, str) and sk:
                skill_names.append(sk)
        if skill_names:
            parts.append("Skills: " + ", ".join(skill_names))

    # Certifications
    certs = data.get("certifications") or data.get("certificates") or []
    if isinstance(certs, list):
        cert_names = []
        for c in certs[:5]:
            if isinstance(c, dict):
                cert_name = c.get("name") or c.get("title") or ""
                if cert_name:
                    cert_names.append(cert_name)
            elif isinstance(c, str) and c:
                cert_names.append(c)
        if cert_names:
            parts.append("Certifications: " + ", ".join(cert_names))

    if not parts:
        return _FALLBACK_PROFILE

    return " | ".join(dict.fromkeys(p for p in parts if p))


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
    feedback_examples: Optional[List[Dict[str, str]]] = None,
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

    prompt = _build_claude_prompt(post.get("post_text", ""), profiles, feedback_examples or [])
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


def _build_claude_prompt(
    post_text: str,
    profiles: List[Dict[str, str]],
    feedback_examples: Optional[List[Dict[str, str]]] = None,
) -> str:
    """
    Construct the structured extraction prompt sent to Claude.

    For a single profile, uses a focused single-profile format.
    For multiple profiles, asks Claude to score each and identify the best match.
    Injects past user feedback as few-shot corrections when provided.

    Args:
        post_text: Full text of the LinkedIn post.
        profiles: List of profile dicts with 'name' and 'vector' keys.
        feedback_examples: Optional list of past feedback dicts to guide scoring.

    Returns:
        Complete prompt string.
    """
    if len(profiles) == 1:
        profile_section = f"## Consultant Profile ({profiles[0]['name']}):\n{profiles[0]['vector']}"
        scoring_instructions = (
            f"Score how well this profile matches the mission requirements (0-100).\n"
            f"Set best_profil to \"{profiles[0]['name']}\"."
        )
        best_profil_field = f'  "best_profil": "{profiles[0]["name"]}",\n'
    else:
        profile_section = "## Consultant Profiles:\n"
        profile_names = []
        for p in profiles:
            profile_section += f"### {p['name']}:\n{p['vector']}\n\n"
            profile_names.append(p["name"])
        names_str = ", ".join(f'"{n}"' for n in profile_names)
        scoring_instructions = (
            f"Score each profile independently against the mission requirements (0-100). "
            f"Then set best_profil to the name of the profile with the highest individual score, "
            f"and set match_score to that highest score. "
            f"Profile names to choose from: {names_str}."
        )
        best_profil_field = (
            f'  "best_profil": "one of {names_str} — the profile with the highest individual match score",\n'
        )

    # Build feedback section from past user corrections
    feedback_section = ""
    if feedback_examples:
        lines = [
            "## Retours utilisateur sur vos suggestions précédentes (apprenez de ces exemples) :",
        ]
        for ex in feedback_examples[:15]:  # cap to avoid prompt bloat
            title = ex.get("mission_title", "?")
            skills = ex.get("required_skills", "")
            fb = ex.get("feedback", "")
            lines.append(f'- "{title}" ({skills}) → Retour utilisateur : "{fb}"')
        feedback_section = "\n".join(lines) + "\n\n"

    return f"""You are analyzing a LinkedIn post that may describe a freelance mission opportunity.

## LinkedIn Post:
{post_text}

{profile_section}

{feedback_section}## Task:
1. Determine if this post is a GENUINE MISSION OFFER (a company, recruiter, or ESN seeking a freelancer).
2. If genuine, extract mission details and score the profile match.
3. If NOT genuine, set is_genuine_mission=false and match_score=0.

Respond with ONLY a valid JSON object — no preamble, no markdown fences, no explanation.

## Required JSON fields:
{{
  "is_genuine_mission": "boolean — TRUE only if a company/recruiter/ESN is SEEKING a freelancer. FALSE if: a freelancer advertises their own availability, a personal branding post, an opinion/news article, or any post NOT offering a mission to fill.",
  "mission_title": "string — short title of the mission or role (e.g. 'Chef de projet Data'), empty string if not a genuine mission",
  "required_skills": ["list", "of", "skills", "mentioned", "in", "the", "post"],
  "duration": "string — mission duration or contract length (e.g. '3 months', 'CDI', 'unknown')",
  "daily_rate_tjm": "string or null — daily rate if explicitly mentioned (e.g. '600€/jour'), null otherwise",
  "location": "string — city of the mission (e.g. 'Paris', 'Casablanca') or 'Remote' if fully remote",
  "remote_ok": "boolean — true if remote work is explicitly mentioned or implied",
  "contact_info": "string or null — email or contact method from the post, null if none",
{best_profil_field}  "match_score": "float 0-100 — match score for best_profil (must be 0 if is_genuine_mission=false)",
  "match_reasons": ["top 3 concise reasons explaining the score, referencing specific skills"],
  "language": "FR or EN — language of the post"
}}

## Critical rule — is_genuine_mission:
Set is_genuine_mission=false (and match_score=0) when:
- A freelancer or consultant announces THEIR OWN availability (e.g. "Je suis disponible pour une mission...")
- The post is personal branding, self-promotion, or availability announcement
- The post is an opinion, article, news, or general content not offering a specific role
- The author IS the consultant, not the client

Set is_genuine_mission=true only when:
- A company, recruiter, manager, or ESN is explicitly LOOKING FOR someone to fill a role
- The post describes a mission/role to be filled (skills required, duration, rate, location)

## Scoring guidelines (only applies when is_genuine_mission=true):
- 80-100: Strong match — most required skills are present in the profile
- 50-79: Partial match — some relevant skills or domain overlap
- 0-49: Weak match — few or no skill overlaps

## Example output (genuine mission):
{{
  "is_genuine_mission": true,
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
}}

## Example output (NOT a genuine mission — freelancer advertising themselves):
{{
  "is_genuine_mission": false,
  "mission_title": "",
  "required_skills": [],
  "duration": "unknown",
  "daily_rate_tjm": null,
  "location": "",
  "remote_ok": false,
  "contact_info": null,
  "best_profil": "{profiles[0]['name']}",
  "match_score": 0,
  "match_reasons": ["Post is a freelancer advertising their own availability, not a mission offer"],
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
        "is_genuine_mission": False,
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
