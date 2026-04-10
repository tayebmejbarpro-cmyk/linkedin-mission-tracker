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
from bs4 import BeautifulSoup

from config.config import AppConfig
from scraper.linkedin_scraper import RawPost

# BeReach API endpoint for LinkedIn profile fetching
_BEREACH_PROFILE_ENDPOINT = "https://api.berea.ch/visit/linkedin/profile"
_PROFILE_REQUEST_TIMEOUT = 30

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

# ---------------------------------------------------------------------------
# Feedback calibration — domain clusters + polarity signals
# Used to aggregate all user feedback into a fixed-size calibration table
# injected into the Claude prompt (~150 tokens, O(1) regardless of volume).
# ---------------------------------------------------------------------------

_DOMAIN_CLUSTERS: Dict[str, list] = {
    "PMO / Pilotage":    ["pmo", "pilotage", "chef de projet", "coordination projet", "programme", "portefeuille"],
    "Service Delivery":  ["service delivery", "sdm", "delivery manager", "service manager", "responsable service"],
    "Incident Manager":  ["incident", "major incident", "war room", "crise it", "gestionnaire d'incidents"],
    "ITSM / Run / MCO":  ["itsm", "run ", "mco", "tma", "exploitation it", "maintien en condition"],
    "Business Analyst":  ["business analyst", " ba ", "analyste fonctionnel", "amoa", "moa", "référent fonctionnel"],
    "Product Owner":     ["product owner", " po ", "product manager", "backlog", "user stories"],
    "Dev / Technique":   ["dev ", "développeur", "developer", "php", "java", "python", "angular", "react", "fullstack"],
    "Data / IA":         ["data engineer", "data scientist", "machine learning", " ia ", "bi ", "analytics", "etl"],
    "Infra / Réseau":    ["infra", "réseau", "network", "sysadmin", "devops", "cloud", "aws", "azure"],
}

_POSITIVE_SIGNALS = [
    "parfait", "excellent", "très bon", "exactement", "idéal", "top",
    "bon match", "intéressant", "bien", "super", "pertinent",
]
_NEGATIVE_SIGNALS = [
    "hors scope", "trop technique", "pas pour moi", "pas moi", "hors cible",
    "non pertinent", "pas adapté", "pas concerné",
]
_CAUTIOUS_SIGNALS = [
    "borderline", "peut-être", "à voir", "pas sûr", "mitigé", "moyen", "limite", "nuancé",
]


def _classify_polarity(feedback_text: str) -> str:
    """
    Classify free-text feedback as 'positive', 'negative', or 'cautious'.

    Checks cautious signals first (most specific), then negative, then positive.
    Defaults to 'positive' as a safety net to avoid losing opportunities.

    Args:
        feedback_text: Raw feedback string from the user.

    Returns:
        One of 'positive', 'negative', 'cautious'.
    """
    text = feedback_text.lower()
    if any(s in text for s in _CAUTIOUS_SIGNALS):
        return "cautious"
    if any(s in text for s in _NEGATIVE_SIGNALS):
        return "negative"
    if any(s in text for s in _POSITIVE_SIGNALS):
        return "positive"
    return "positive"  # safety net


def _build_calibration_table(feedback_examples: List[Dict[str, str]]) -> str:
    """
    Aggregate all feedback examples into a fixed-size domain calibration table.

    Classifies each feedback entry by domain (via keyword matching on
    mission_title + required_skills) and polarity (via _classify_polarity).
    Produces a compact table (~150 tokens) that tells Claude which domains
    to score high, low, or with caution — regardless of input volume.

    Args:
        feedback_examples: List of feedback dicts (keys: mission_title,
            required_skills, feedback, post_date). Already sorted recent-first.

    Returns:
        Formatted calibration table string, ready for prompt injection.
    """
    stats: Dict[str, Dict[str, int]] = {
        d: {"pos": 0, "neg": 0, "cau": 0} for d in _DOMAIN_CLUSTERS
    }

    for ex in feedback_examples:
        text = (ex.get("mission_title", "") + " " + ex.get("required_skills", "")).lower()
        polarity = _classify_polarity(ex.get("feedback", ""))
        key = polarity[:3]  # "pos", "neg", "cau"
        for domain, keywords in _DOMAIN_CLUSTERS.items():
            if any(kw in text for kw in keywords):
                stats[domain][key] += 1
                break  # assign to first matching domain only

    lines = ["## Score calibration table (aggregated from all past user feedback):"]
    lines.append(f"{'Domain':<25} | {'✅ Approved':>11} | {'❌ Rejected':>11} | Target score")
    lines.append("-" * 68)
    has_data = False
    for domain, s in stats.items():
        total = s["pos"] + s["neg"] + s["cau"]
        if total == 0:
            continue
        has_data = True
        if s["pos"] > s["neg"] * 2:
            target = "70+"
        elif s["neg"] > s["pos"] * 2:
            target = "<25"
        else:
            target = "40-55"
        lines.append(f"{domain:<25} | {s['pos']:>11} | {s['neg']:>11} | {target}")

    if not has_data:
        return ""  # no feedback yet — skip section entirely

    lines.append("")
    lines.append(
        "Apply these calibrations: domains with many approvals → score higher; "
        "domains with many rejections → score lower. Use regardless of vocabulary."
    )
    return "\n".join(lines) + "\n\n"

# Consultant context — injected into every scoring prompt.
# Generic: Claude infers the consultant's domains from their profile vector.
_CONSULTANT_PERSONA = """## Consultant Context:
This consultant is an experienced freelancer actively seeking missions.

SCORING PHILOSOPHY — read the profile vector carefully before scoring:
- Score based ENTIRELY on the profile vector provided. Infer core expertise from job titles,
  skills, certifications, and experience listed in the profile.
- Missions strongly aligned with the consultant's demonstrated domain → score 60–100.
- Missions requiring skills entirely absent from the profile → score 0–30.
- When in doubt, favor a higher score: 50 means "worth reviewing", not "perfect match required".
- Do NOT penalize for skills the profile does not explicitly mention if the broader domain matches.

OUT-OF-SCOPE PATTERN (apply when profile is clearly management/functional):
- Pure technical implementation roles (Dev, DevOps, Data Engineering, Infra/Sysadmin/Network)
  without a management or coordination dimension → score <= 30 unless the profile explicitly
  lists that technical skill.

MIXED DOMAIN RULE — when a post combines the consultant's core domain WITH a non-core domain:
Score based on the HIGHEST-MATCHING component, not the average.
Example: if profile shows project management strength, score the PM match (e.g. 72) and ignore
an unrelated secondary skill requirement (e.g. GenAI = 20) → final score 72.
Rationale: the consultant applies for the component they are qualified for."""


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
        is_target_location: bool   — True if mission is in France/Maroc/Remote/unknown
        truly_location_independent: bool — True if role allows working from anywhere worldwide (no residency/onsite constraint)
    """


def fetch_profile_vectors(
    config: AppConfig,
    logger: logging.Logger,
    cached: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    """
    Build profile vectors for all profiles in config.

    For each profile URL already present in `cached`, re-uses the stored
    vector without calling BeReach. For missing profiles, calls the BeReach
    profile endpoint and falls back to HTTP scrape if BeReach fails.

    Args:
        config: Application configuration (provides linkedin_profiles + bereach_api_token).
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

        # Not cached — fetch via BeReach
        logger.info("[matcher] Profile '%s' not cached — fetching via BeReach...", name)
        bereach_data = _fetch_profile_via_bereach(config.bereach_api_token, url, logger)
        if bereach_data:
            vector = _build_profile_vector_from_bereach(bereach_data)
            logger.info("[matcher] Profile '%s' fetched via BeReach (%d chars).", name, len(vector))
        else:
            logger.warning(
                "[matcher] BeReach fetch failed for '%s' — falling back to HTTP scrape.", name
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
    scoring_mode: str = "freelance",
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
        _countries = config.target_countries or ["France", "Maroc"]
        future_to_post = {
            executor.submit(
                _score_post_with_claude, post, profiles, anthropic_client, logger,
                _countries,
                feedback_examples or [],
                scoring_mode,
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
                score = 0.0
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
                is_target_location=claude_data.get("is_target_location", True),
                truly_location_independent=claude_data.get("truly_location_independent", True),
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



def _fetch_profile_via_bereach(
    bereach_api_token: str,
    profile_url: str,
    logger: logging.Logger,
) -> Optional[Dict[str, Any]]:
    """
    Fetch a LinkedIn profile's data via the BeReach API.

    POST https://api.berea.ch/visit/linkedin/profile

    On any failure, logs a warning and returns None so the caller can fall back
    to the HTTP scrape method.

    Args:
        bereach_api_token: BeReach API token.
        profile_url: Full LinkedIn profile URL.
        logger: Logger instance.

    Returns:
        Response JSON dict from BeReach, or None on failure.
    """
    headers = {
        "Authorization": f"Bearer {bereach_api_token}",
        "Content-Type": "application/json",
    }
    payload = {"profileUrl": profile_url}

    try:
        logger.debug("[matcher] Fetching profile via BeReach: %s", profile_url)
        resp = requests.post(
            _BEREACH_PROFILE_ENDPOINT,
            json=payload,
            headers=headers,
            timeout=_PROFILE_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError as exc:
        logger.warning(
            "[matcher] BeReach profile fetch HTTP error (%s): %s", profile_url, exc
        )
        return None
    except Exception as exc:
        logger.warning(
            "[matcher] BeReach profile fetch failed (%s): %s", profile_url, exc
        )
        return None

    if not data:
        logger.warning("[matcher] BeReach returned empty response for profile %s", profile_url)
        return None

    logger.debug(
        "[matcher] BeReach profile response keys: %s",
        list(data.keys()) if isinstance(data, dict) else type(data).__name__,
    )
    return data if isinstance(data, dict) else None


def _build_profile_vector_from_bereach(data: Dict[str, Any]) -> str:
    """
    Build a plain-text profile vector from BeReach profile data.

    BeReach /visit/linkedin/profile returns: name, headline, company,
    connectionDegree, profileUrl, and optionally location, about, experience,
    skills. Extracts all available fields defensively.

    Returns _FALLBACK_PROFILE if nothing useful is found.

    Args:
        data: Profile response dict from the BeReach API.

    Returns:
        Pipe-separated plain-text profile vector string.
    """
    parts: List[str] = []

    # Name
    name = (
        data.get("name") or data.get("fullName") or data.get("fullname") or ""
    )
    if name:
        parts.append(str(name))

    # Headline / title
    headline = (
        data.get("headline") or data.get("title") or data.get("jobTitle") or ""
    )
    if headline:
        parts.append(str(headline))

    # Location
    location = data.get("location") or data.get("city") or ""
    if isinstance(location, dict):
        location = ", ".join(filter(None, [location.get("city"), location.get("country")]))
    if location:
        parts.append(str(location))

    # Current company
    company = (
        data.get("company") or data.get("currentCompany")
        or data.get("companyName") or data.get("organization") or ""
    )
    if isinstance(company, dict):
        company = company.get("name") or company.get("companyName") or ""
    if company:
        parts.append(str(company))

    # About / summary (if returned)
    about = data.get("about") or data.get("summary") or data.get("description") or ""
    if about and isinstance(about, str):
        parts.append(about[:500])

    # Experience — extract job titles (+ company) if returned
    experience = data.get("experience") or data.get("positions") or []
    if isinstance(experience, list):
        for exp in experience[:5]:
            if not isinstance(exp, dict):
                continue
            title = exp.get("title") or exp.get("role") or exp.get("position") or ""
            exp_company = (
                exp.get("company") or exp.get("companyName") or exp.get("organization") or ""
            )
            if title:
                parts.append(str(title) + (f" at {exp_company}" if exp_company else ""))

    # Skills (if returned)
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

    except Exception as exc:
        logging.getLogger(__name__).debug("[matcher] Profile HTML parse failed: %s", exc)

    if not parts:
        return _FALLBACK_PROFILE

    return " | ".join(dict.fromkeys(p for p in parts if p))


def _score_post_with_claude(
    post: RawPost,
    profiles: List[Dict[str, str]],
    anthropic_client: anthropic.Anthropic,
    logger: logging.Logger,
    target_countries: Optional[List[str]] = None,
    feedback_examples: Optional[List[Dict[str, str]]] = None,
    scoring_mode: str = "freelance",
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
        target_countries: List of target country names from config for geo-filtering.
        feedback_examples: Optional list of past feedback dicts to guide scoring.

    Returns:
        Parsed dict with Claude's extracted fields, or safe defaults on error.
    """
    # Stagger concurrent workers to avoid simultaneous API bursts
    time.sleep(random.uniform(_WORKER_DELAY_MIN, _WORKER_DELAY_MAX))

    _countries = target_countries or ["France", "Maroc"]
    prompt = _build_claude_prompt(post.get("post_text", ""), profiles, _countries, feedback_examples or [], scoring_mode=scoring_mode)
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
    target_countries: List[str],
    feedback_examples: Optional[List[Dict[str, str]]] = None,
    scoring_mode: str = "freelance",
) -> str:
    """
    Construct the structured extraction prompt sent to Claude.

    For a single profile, uses a focused single-profile format.
    For multiple profiles, asks Claude to score each and identify the best match.
    Injects past user feedback as few-shot corrections when provided.

    Args:
        post_text: Full text of the LinkedIn post.
        profiles: List of profile dicts with 'name' and 'vector' keys.
        target_countries: List of target country names from config (e.g. ["France", "Maroc"]).
        feedback_examples: Optional list of past feedback dicts to guide scoring.
        scoring_mode: "freelance" (default) or "job" — selects prompt sections for each pipeline.

    Returns:
        Complete prompt string.
    """
    # Build dynamic country strings for the GEO RULE section
    _countries = target_countries or ["France", "Maroc"]
    countries_display = " ou ".join(_countries)
    countries_true_bullets = "\n".join(
        f"  \u2192 true  if mission is in {c}" for c in _countries
    )
    if len(profiles) == 1:
        profile_section = (
            f"{_CONSULTANT_PERSONA}\n\n"
            f"## Consultant Profile ({profiles[0]['name']}):\n{profiles[0]['vector']}"
        )
        scoring_instructions = (
            f"Score how well this profile matches the mission requirements (0-100).\n"
            f"Set best_profil to \"{profiles[0]['name']}\"."
        )
        best_profil_field = f'  "best_profil": "{profiles[0]["name"]}",\n'
    else:
        profile_section = f"{_CONSULTANT_PERSONA}\n\n## Consultant Profiles:\n"
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

    # Build feedback section — aggregated calibration table (~150 tokens fixed,
    # regardless of how many feedback examples exist).
    feedback_section = ""
    if feedback_examples:
        feedback_section = _build_calibration_table(feedback_examples)

    # --- Mode-sensitive prompt sections ---
    if scoring_mode == "job":
        is_genuine_field_desc = (
            "boolean — TRUE only if a company, recruiter, or ESN is SEEKING a full-remote employee "
            "or contractor (CDI, CDD, or freelance). FALSE if: personal branding, availability "
            "announcement, article, opinion, or a position already filled."
        )
        critical_rule_section = """\
## Critical rule — is_genuine_mission:
Set is_genuine_mission=false (and match_score=0) when:
- A freelancer or consultant announces THEIR OWN availability
- The post is personal branding, self-promotion, or an availability announcement
- The post is an opinion, article, news, or general content not offering a specific role
- The post announces a position has been FILLED or closed (e.g. "poste pourvu",
  "nous avons trouvé notre candidat", "clôturé", "profil retenu")

Set is_genuine_mission=true only when:
- A company, recruiter, manager, or ESN is explicitly LOOKING FOR someone to fill a
  full-remote role (CDI, CDD, or freelance contract)
- The post describes a role to be filled (skills required, contract type, start date)"""
        geo_rule_section = """\
## GEO RULE — is_target_location + truly_location_independent:

These search queries already contain "full remote" or "100% télétravail".
Most posts will be remote-first. Apply the following rules:

### is_target_location
Set is_target_location=true when:
  → The role is explicitly fully remote (full remote, 100% télétravail, remote-first)
  → The employer is based in Europe or a France-adjacent francophone country
    (France, Belgium, Switzerland, Luxembourg, Morocco, Tunisia, Senegal, Ivory Coast…)
  → The location is "worldwide remote" or unspecified (safety net)
  → Location is completely unknown after analysis (safety net)

Set is_target_location=false when:
  → The post explicitly requires physical presence in the Americas or Asia-Pacific
  → The post explicitly states on-site only outside Europe

SPECIAL RULE — if is_genuine_mission=false: always return is_target_location=true.

### truly_location_independent
This field determines if the role allows working from ANY country (e.g. Morocco) with
no physical presence, no local-residency requirement, and no onsite obligation.

Set truly_location_independent=true when:
  → Role is "full remote", "100% remote", "work from anywhere", "fully distributed",
    "remote worldwide", "globally remote", "async-first" with no location restriction
  → No mention of required city, country of residence, or onsite visits
  → No phrase indicating employer country residency is required
    (e.g. no "based in France required", "must have right to work in France")

Set truly_location_independent=false when:
  → Post says "full remote" but explicitly restricts where the worker must be located
    (e.g. "full remote en France", "must reside in France", "IDF only")
  → Post mentions required occasional on-site days (e.g. "1 jour/mois sur site")
  → Post specifies a strict timezone requirement (e.g. "CET timezone mandatory")
  → Contract type legally requires local residency (e.g. CDI/portage in France)

SPECIAL RULE — if is_genuine_mission=false: always return truly_location_independent=true.

ANTI-HALLUCINATION RULES:
  - "full remote" alone (without a location restriction) → truly_location_independent=true
  - A French-language post or a French employer is NOT sufficient to set false
  - When genuinely uncertain → return true (safety net — do not discard opportunities)"""
    else:
        is_genuine_field_desc = (
            "boolean — TRUE only if a company/recruiter/ESN is SEEKING a freelancer. "
            "FALSE if: a freelancer advertises their own availability, a personal branding post, "
            "an opinion/news article, or any post NOT offering a mission to fill."
        )
        critical_rule_section = f"""\
## Critical rule — is_genuine_mission:
Set is_genuine_mission=false (and match_score=0) when:
- A freelancer or consultant announces THEIR OWN availability (e.g. "Je suis disponible pour une mission...")
- The post is personal branding, self-promotion, or availability announcement
- The post is an opinion, article, news, or general content not offering a specific role
- The author IS the consultant, not the client
- The post announces a mission has been FILLED or closed (e.g. "mission pourvue", "poste pourvu", "nous avons trouvé notre candidat", "clôturé", "profil retenu")
- The post lists 3 or more unrelated job titles or role types without describing a single specific mission (e.g. "we're looking for a DevOps OR a PMO OR a Data Scientist")
- The post promotes a talent network, talent community, or multi-sector recruitment platform (e.g. "join our network", "we hire across sectors")

Set is_genuine_mission=true only when:
- A company, recruiter, manager, or ESN is explicitly LOOKING FOR someone to fill a role
- The post describes a mission/role to be filled (skills required, duration, rate, location)

## Domain rejection — set match_score <= 35 when the PRIMARY requirement is:
- Scrum Master or Agile Coach as the MAIN role (not PMO who also facilitates ceremonies)
- Data Scientist, ML Engineer, or AI/LLM specialist as PRIMARY (not a data-driven PM)
- SAP/ERP specialist (FICO, SD, MM, S4HANA) as PRIMARY — AMOA in an SAP context is fine
- DevOps Engineer, SRE, or Infrastructure specialist as PRIMARY
- CyberSecurity, SOC, SIEM, or Pentest specialist — NOTE: "RSSI RUN" with ITSM context is NOT this
- Solution/Enterprise/Cloud Architect as PRIMARY
A "Chef de projet" title does not override a primary technical requirement in the above domains."""
        geo_rule_section = f"""\
## GEO RULE — is_target_location (evaluated AFTER scoring, independent of match_score):

Determine if the mission is physically located in {countries_display}.

STEP 1 — Look for an explicit location in the post text:
  - City name, region, department (Paris, Lyon, Île-de-France, Casablanca, Rabat...)
  - Geographic hashtags (#paris #idf #maroc #casablanca #freelancefrance)
  - Direct country mention

STEP 2 — If no explicit location, analyze implicit signals:
  - TJM/rate expressed in € (€) → strong indicator of France
  - Post written entirely in French with ESN/freelance context → likely France
  - Known French ESN or company mentioned → likely France
  - Foreign currency (£, $, CHF) or explicit foreign country → not target

DECISION RULES — set is_target_location to:
{countries_true_bullets}
  → true  if mission is "Remote" / "Télétravail" / "Full Remote" (location-independent)
  → true  if location is completely unknown after analysis (safety net — do not lose opportunities)
  → false if mission is explicitly in a country not listed above
  → false if mission is in DOM-TOM: La Réunion, Guadeloupe, Martinique, Guyane,
           Mayotte, Nouvelle-Calédonie, Polynésie française

SPECIAL RULE — if is_genuine_mission=false:
  Always return is_target_location=true. The post is already excluded by match_score=0
  and geo analysis is irrelevant.

ANTI-HALLUCINATION RULES:
  - Never infer a city from the author's name or company name alone.
  - "Near the border" or "accessible from Paris" does NOT make Brussels or Luxembourg a target.
  - A French-sounding company name does not guarantee the mission is in France.
  - When genuinely uncertain between France and another country → return true (safety net)."""

    task_description = (
        "a GENUINE MISSION OFFER (a company, recruiter, or ESN seeking a freelancer or employee for a full-remote role)"
        if scoring_mode == "job"
        else "a GENUINE MISSION OFFER (a company, recruiter, or ESN seeking a freelancer)"
    )

    return f"""You are analyzing a LinkedIn post that may describe a freelance mission opportunity.

## LinkedIn Post:
{post_text}

{profile_section}

{feedback_section}## Task:
1. Determine if this post is {task_description}.
2. If genuine, extract mission details and score the profile match.
3. If NOT genuine, set is_genuine_mission=false and match_score=0.

Respond with ONLY a valid JSON object — no preamble, no markdown fences, no explanation.

## Required JSON fields:
{{
  "is_genuine_mission": "{is_genuine_field_desc}",
  "mission_title": "string — short title of the mission or role (e.g. 'Chef de projet Data'), empty string if not a genuine mission",
  "required_skills": ["canonical skill names — use standard terms (e.g. 'PMO', 'ITSM', 'Business Analyst', 'Chef de projet', 'Product Owner'). Max 8 skills."],
  "duration": "string — mission duration or contract length (e.g. '3 mois', 'CDI', 'unknown')",
  "daily_rate_tjm": "string or null — daily rate if explicitly mentioned, normalized to 'NNN€/jour' format (e.g. '600€/jour', '700€/jour'). null if not mentioned.",
  "location": "string — city of the mission (e.g. 'Paris', 'Casablanca') or 'Remote' if fully remote",
  "remote_ok": "boolean — true if remote work is explicitly mentioned or implied",
  "contact_info": "string or null — email or contact method from the post, null if none",
{best_profil_field}  "match_score": "float 0-100 — match score for best_profil (must be 0 if is_genuine_mission=false)",
  "match_reasons": ["array of 3 strings — format defined in MATCH_REASONS FORMAT section below"],
  "is_target_location": "boolean — see GEO RULE below",
  "truly_location_independent": "boolean — see GEO RULE below (job mode only: true if worker can be anywhere worldwide with no onsite or residency requirement)"
}}

## match_reasons format (required for all 3 entries):
Each string must follow: "TERM_IN_POST ↔ SKILL_IN_PROFILE (match type)"
Match types: direct match | vocabulary equivalence | adjacent domain | no match
Example: "Pilotage PMO requis ↔ expérience PMO confirmée (direct match)"
If is_genuine_mission=false: use a single entry explaining why (e.g. "Post is a freelancer advertising their own availability, not a mission offer").

{critical_rule_section}

## Scoring guidelines (only applies when is_genuine_mission=true):

SCORE BASED ON DOMAIN MATCH, NOT JUST KEYWORD MATCH.
A mission may use different vocabulary than the profile — look for semantic equivalence.

Score bands:
- 80-100 : Core domain match — mission directly targets the consultant's main expertise
           (PMO, Chef de projet SI, SDM, Service Delivery, Incident Manager,
            ITSM, Run/MCO/TMA, Business Analyst, Product Owner, Pilotage, MOE/MOA)
- 60-79  : Adjacent domain match — mission requires skills the consultant has as secondary
           competencies, OR uses different vocabulary for the same role
- 40-59  : Partial overlap — 1-2 key skills match, domain is related but not core
- 20-39  : Weak match — very few skill overlaps, clearly different domain
- 0-19   : No match — completely unrelated domain or technical stack

SKILL EQUIVALENCES — treat these pairs as synonyms when scoring:
- "pilotage de projet" / "conduite de projet" / "coordination projet"
   = "gestion de projet" / "chef de projet" / "PMO"
- "Service Delivery Manager" / "SDM" / "responsable delivery" / "responsable service client"
   = delivery management / ITIL service management
- "Incident Manager" / "Major Incident Manager" / "gestion des incidents majeurs" / "war room"
   = incident management / coordination de crise IT
- "gestion des incidents" / "gestion des problèmes" / "MCO" / "TMA" / "exploitation IT"
   = "ITSM" / "Run" / "maintien en condition opérationnelle"
- "MOE" / "MOA" / "AMOA" / "recette" / "qualification" / "référent fonctionnel"
   = project management / business analysis adjacent (+10 pts bonus)
- "transformation SI" / "transformation digitale" / "urbanisation SI"
   = digital transformation
- "analyste fonctionnel" / "analyste métier" / "analyste SI" / "études fonctionnelles"
   = "Business Analyst"
- "PO" / "responsable produit" / "Product Manager" / "proxy PO"
   = "Product Owner"
- "RSSI RUN" / "Gouvernance SI opérationnelle" / "Responsable service IT en Run"
   = Run Management / ITSM operations (NOT cybersecurity — RSSI RUN is IT operations governance, not infosec)

SCORING BIAS CORRECTION:
This consultant is ACTIVELY SEEKING missions. When a mission clearly falls within his
domain but uses slightly different vocabulary — favor the higher score band.
A score of 50 means "worth reviewing by the consultant", not "perfect match required".
Do NOT penalize for skills the profile does not mention explicitly if the broader domain matches.

MIXED DOMAIN RULE — when a post combines the consultant's core domain WITH a non-core domain:
Score based on the HIGHEST-MATCHING component, not the average.
Example: "Directeur de projet + AMOA + GenAI/ML" → PMO/AMOA = 72, GenAI = 20 → score 72.
Example: "Gouvernance SSI + RSSI RUN + ITSM" → RUN/ITSM = 78, SSI governance = 40 → score 78.
Rationale: the consultant applies for the component they are qualified for, not the full stack.

{geo_rule_section}

## Example output (genuine mission — vocabulary equivalence, high score):
{{
  "is_genuine_mission": true,
  "mission_title": "Pilote de projet transformation SI",
  "required_skills": ["Chef de projet", "Conduite du changement", "Reporting", "MCO"],
  "duration": "6 mois",
  "daily_rate_tjm": "650€/jour",
  "location": "Lyon",
  "remote_ok": false,
  "contact_info": null,
  "best_profil": "{profiles[0]['name']}",
  "match_score": 75.0,
  "match_reasons": [
    "Pilotage/conduite de projet ↔ expérience Chef de projet SI (vocabulary equivalence)",
    "MCO/exploitation ↔ background ITSM/Run dans le profil (vocabulary equivalence)",
    "Transformation SI ↔ digital transformation background (adjacent domain)"
  ],
  "is_target_location": true,
  "truly_location_independent": false
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
  "is_target_location": true,
  "truly_location_independent": true
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

    # Parse is_target_location — default True (keep post if uncertain)
    raw_loc = data.get("is_target_location")
    if isinstance(raw_loc, bool):
        data["is_target_location"] = raw_loc
    else:
        data["is_target_location"] = True  # safe default: never drop on ambiguity

    # Parse truly_location_independent — default True (safety net: don't miss opportunities)
    raw_tli = data.get("truly_location_independent")
    if isinstance(raw_tli, bool):
        data["truly_location_independent"] = raw_tli
    else:
        raw_tli_str = str(raw_tli or "").lower()
        data["truly_location_independent"] = raw_tli_str not in ("false", "0", "no", "non")

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
        "is_target_location": True,          # safe default — error posts excluded by match_score=0
        "truly_location_independent": True,  # safe default — error posts excluded by match_score=0
    }
