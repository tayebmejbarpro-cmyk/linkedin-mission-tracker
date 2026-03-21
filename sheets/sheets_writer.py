"""
sheets/sheets_writer.py — Google Sheets writer with deduplication and conditional formatting.

Authenticates via a service account JSON string (never written to disk),
manages monthly tabs, deduplicates rows by post_url, appends new rows,
and applies color-coded conditional formatting based on match_score.

Also manages a "Paramètres" tab for editable search configuration.
"""

import hashlib
import json
import logging
from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.config import AppConfig
from matcher.profile_matcher import EnrichedPost

# Google Sheets API scopes
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column layout (A=0 … M=12)
_HEADERS = [
    "date",            # A — post publication date (YYYY-MM-DD)
    "heure",           # B — post publication hour (HH:MM)
    "author_name",     # C
    "mission_title",   # D
    "required_skills", # E
    "match_score",     # F ← conditional formatting
    "tjm",             # G
    "post_url",        # H ← dedup key
    "pays",            # I
    "ville",           # J
    "profil",          # K — LinkedIn profile name with best match
    "match_reasons",   # L — why Claude gave this score
    "feedback",        # M — user feedback (filled manually in sheet)
]

# Column index (0-based) for match_score and post_url
_SCORE_COL_IDX = 5    # F
_URL_COL_LETTER = "H" # post_url column for dedup reads

# Conditional formatting color thresholds
_GREEN_MIN = 80     # >= 80 → green
_YELLOW_MIN = 50    # 50–79 → yellow
# < 50 → red

# Google Sheets hex colors (background)
_COLOR_GREEN = {"red": 0.718, "green": 0.882, "blue": 0.804}   # #b7e1cd
_COLOR_YELLOW = {"red": 0.988, "green": 0.910, "blue": 0.698}  # #fce8b2
_COLOR_RED = {"red": 0.957, "green": 0.780, "blue": 0.765}     # #f4c7c3

# Config tab name
_CONFIG_TAB = "Paramètres"

# Profile vector cache tab name
_PROFILES_CACHE_TAB = "Profils_Cache"
_PROFILES_CACHE_HEADERS = ["profile_name", "url", "vector", "fetched_at"]

# Dedup index tab — persists post_url + text_hash across all runs
_DEDUP_INDEX_TAB = "Dedup_Index"
_DEDUP_INDEX_HEADERS = ["post_url", "text_hash"]


def _text_hash_local(text: str) -> str:
    """
    Compute MD5 of the first 300 normalized characters of post text.

    Mirrors scraper._text_hash — kept as a local copy to avoid a circular import.

    Args:
        text: Full post text string.

    Returns:
        MD5 hex digest string.
    """
    normalized = " ".join(text[:300].split())
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def _ensure_dedup_index_tab(
    service: Any,
    spreadsheet_id: str,
    existing_tabs: set,
    logger: logging.Logger,
) -> None:
    """
    Create the Dedup_Index tab with its header row if it does not already exist.

    No-op when the tab is already present. Called from load_seen_posts_all_tabs()
    so the tab is always available before any read or write.

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        existing_tabs: Set of tab names already in the spreadsheet.
        logger: Logger instance.
    """
    if _DEDUP_INDEX_TAB in existing_tabs:
        return
    body = {"requests": [{"addSheet": {"properties": {"title": _DEDUP_INDEX_TAB}}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{_DEDUP_INDEX_TAB}'!A1",
        valueInputOption="RAW",
        body={"values": [_DEDUP_INDEX_HEADERS]},
    ).execute()
    logger.info("[sheets] Created '%s' tab.", _DEDUP_INDEX_TAB)


def load_seen_posts_all_tabs(
    config: AppConfig,
    logger: logging.Logger,
) -> tuple:
    """
    Load all previously-seen post URLs and text hashes from the Dedup_Index tab.

    Called once at the start of every pipeline run. The returned sets are passed
    to both the scraper (to skip posts before scoring) and the writer (as a
    belt-and-suspenders guard before appending).

    On any failure (auth error, network, tab missing), logs a warning and returns
    two empty sets — the pipeline falls back to in-memory-only deduplication.

    Args:
        config: Application configuration.
        logger: Logger instance.

    Returns:
        Tuple of (seen_urls: Set[str], seen_hashes: Set[str]).
    """
    try:
        service = _get_sheets_service(config.google_service_account_json)
        spreadsheet = service.spreadsheets().get(spreadsheetId=config.spreadsheet_id).execute()
        existing_tabs = {s["properties"]["title"] for s in spreadsheet.get("sheets", [])}
    except Exception as exc:
        logger.warning("[sheets] load_seen_posts_all_tabs: auth failed — %s", exc)
        return set(), set()

    _ensure_dedup_index_tab(service, config.spreadsheet_id, existing_tabs, logger)

    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=config.spreadsheet_id,
            range=f"'{_DEDUP_INDEX_TAB}'!A:B",
        ).execute()
    except Exception as exc:
        logger.warning("[sheets] Could not read '%s' tab: %s", _DEDUP_INDEX_TAB, exc)
        return set(), set()

    rows = result.get("values", [])
    seen_urls: Set[str] = set()
    seen_hashes: Set[str] = set()
    for row in rows[1:]:  # skip header
        if len(row) >= 1 and row[0]:
            seen_urls.add(str(row[0]).strip())
        if len(row) >= 2 and row[1]:
            seen_hashes.add(str(row[1]).strip())

    logger.info(
        "[sheets] Dedup index loaded — %d known URLs, %d known text hashes.",
        len(seen_urls), len(seen_hashes),
    )
    return seen_urls, seen_hashes


def _append_dedup_index(
    service: Any,
    spreadsheet_id: str,
    posts: List[EnrichedPost],
    logger: logging.Logger,
) -> None:
    """
    Append new (post_url, text_hash) rows to the Dedup_Index tab.

    Called only after a successful write to a Missions tab so the index stays
    consistent with what is actually stored. Failure is logged as a warning
    and never propagates.

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        posts: EnrichedPosts that were just successfully written to the sheet.
        logger: Logger instance.
    """
    if not posts:
        return
    rows = [
        [p.get("post_url", ""), _text_hash_local(p.get("post_text", ""))]
        for p in posts
        if p.get("post_url")
    ]
    if not rows:
        return
    try:
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"'{_DEDUP_INDEX_TAB}'!A1",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": rows},
        ).execute()
        logger.info("[sheets] Dedup index updated with %d new entries.", len(rows))
    except Exception as exc:
        logger.warning("[sheets] Could not update dedup index: %s", exc)


def sync_config_tab(
    config: AppConfig,
    logger: logging.Logger,
) -> AppConfig:
    """
    Ensure the "Paramètres" tab exists in the spreadsheet and sync config with it.

    - If the tab does NOT exist: create it and write the current config values.
    - If the tab EXISTS: read values from it and return an updated AppConfig.

    This lets the user edit countries, keywords, and filter thresholds directly
    in the Google Sheet. Changes take effect on the next pipeline run.

    Args:
        config: Current AppConfig (loaded from env + settings.json).
        logger: Logger instance.

    Returns:
        AppConfig potentially overridden with values from the Paramètres tab.
    """
    try:
        service = _get_sheets_service(config.google_service_account_json)
    except Exception as exc:
        logger.warning("[sheets] sync_config_tab: auth failed — using settings.json config. %s", exc)
        return config

    spreadsheet = service.spreadsheets().get(spreadsheetId=config.spreadsheet_id).execute()
    existing_tabs = {s["properties"]["title"] for s in spreadsheet.get("sheets", [])}

    if _CONFIG_TAB not in existing_tabs:
        logger.info("[sheets] Creating '%s' tab with current config...", _CONFIG_TAB)
        _create_config_tab(service, config.spreadsheet_id, config, logger)
        return config  # first run: use settings.json values

    # Tab exists — read and override config
    logger.info("[sheets] Reading config from '%s' tab...", _CONFIG_TAB)
    overrides = _read_config_tab(service, config.spreadsheet_id, logger)
    if not overrides:
        logger.warning("[sheets] '%s' tab is empty or unreadable — using settings.json config.", _CONFIG_TAB)
        return config

    profiles = overrides.get("profiles") or config.linkedin_profiles
    countries = overrides.get("countries") or config.target_countries
    keywords = overrides.get("keywords") or config.search_keywords
    min_score = overrides.get("score_minimum", config.min_match_score)
    max_posts = overrides.get("posts_max_par_pays", config.max_posts_per_country)

    logger.info(
        "[sheets] Config from sheet — profiles: %d | countries: %s | keywords: %d | min_score: %d",
        len(profiles), countries, len(keywords), min_score,
    )

    return replace(
        config,
        linkedin_profiles=profiles[:3],  # max 3 profiles
        target_countries=countries,
        search_keywords=keywords,
        min_match_score=int(min_score),
        max_posts_per_country=int(max_posts),
    )


def load_profile_vectors(
    config: AppConfig,
    logger: logging.Logger,
) -> Dict[str, str]:
    """
    Read cached profile vectors from the "Profils_Cache" sheet tab.

    Returns a dict mapping LinkedIn profile URL → vector text for every
    cached profile found. Returns an empty dict if the tab does not exist,
    is empty, or authentication fails.

    Args:
        config: Application configuration (provides spreadsheet_id + credentials).
        logger: Logger instance.

    Returns:
        Dict mapping URL strings to their cached vector text strings.
    """
    try:
        service = _get_sheets_service(config.google_service_account_json)
    except Exception as exc:
        logger.warning("[sheets] load_profile_vectors: auth failed — no cache. %s", exc)
        return {}

    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=config.spreadsheet_id).execute()
        existing_tabs = {s["properties"]["title"] for s in spreadsheet.get("sheets", [])}
        if _PROFILES_CACHE_TAB not in existing_tabs:
            logger.info("[sheets] '%s' tab not found — cache is empty.", _PROFILES_CACHE_TAB)
            return {}

        result = service.spreadsheets().values().get(
            spreadsheetId=config.spreadsheet_id,
            range=f"'{_PROFILES_CACHE_TAB}'!A:D",
        ).execute()
    except Exception as exc:
        logger.warning("[sheets] Could not read '%s' tab: %s", _PROFILES_CACHE_TAB, exc)
        return {}

    rows = result.get("values", [])
    cache: Dict[str, str] = {}
    for row in rows[1:]:  # skip header
        if len(row) >= 3:
            url = str(row[1]).strip()
            vector = str(row[2]).strip()
            if url and vector:
                cache[url] = vector

    logger.info("[sheets] Loaded %d cached profile vector(s) from '%s'.", len(cache), _PROFILES_CACHE_TAB)
    return cache


def save_profile_vectors(
    vectors: Dict[str, Dict[str, str]],
    config: AppConfig,
    logger: logging.Logger,
) -> None:
    """
    Write (or overwrite) profile vectors to the "Profils_Cache" sheet tab.

    Creates the tab if it does not exist. Clears existing rows (except the
    header) and rewrites all entries. Failure is logged as a warning — the
    main pipeline is never blocked by a cache write error.

    Args:
        vectors: Dict mapping URL → {"name": str, "vector": str}.
        config: Application configuration.
        logger: Logger instance.
    """
    if not vectors:
        return

    try:
        service = _get_sheets_service(config.google_service_account_json)
    except Exception as exc:
        logger.warning("[sheets] save_profile_vectors: auth failed — cache not saved. %s", exc)
        return

    try:
        spreadsheet = service.spreadsheets().get(spreadsheetId=config.spreadsheet_id).execute()
        existing_tabs = {s["properties"]["title"] for s in spreadsheet.get("sheets", [])}

        if _PROFILES_CACHE_TAB not in existing_tabs:
            body = {"requests": [{"addSheet": {"properties": {"title": _PROFILES_CACHE_TAB}}}]}
            service.spreadsheets().batchUpdate(spreadsheetId=config.spreadsheet_id, body=body).execute()
            logger.info("[sheets] Created '%s' tab.", _PROFILES_CACHE_TAB)

        fetched_at = datetime.now(timezone.utc).isoformat()
        rows = [_PROFILES_CACHE_HEADERS]
        for url, info in vectors.items():
            rows.append([info.get("name", ""), url, info.get("vector", ""), fetched_at])

        # Overwrite from A1 (header + all data rows)
        service.spreadsheets().values().update(
            spreadsheetId=config.spreadsheet_id,
            range=f"'{_PROFILES_CACHE_TAB}'!A1",
            valueInputOption="RAW",
            body={"values": rows},
        ).execute()

        # Clear any leftover rows below (in case previous cache had more profiles)
        service.spreadsheets().values().clear(
            spreadsheetId=config.spreadsheet_id,
            range=f"'{_PROFILES_CACHE_TAB}'!A{len(rows) + 1}:D1000",
        ).execute()

        logger.info("[sheets] Saved %d profile vector(s) to '%s'.", len(vectors), _PROFILES_CACHE_TAB)

    except Exception as exc:
        logger.warning("[sheets] Could not save profile vectors to sheet: %s", exc)


def _create_config_tab(
    service: Any,
    spreadsheet_id: str,
    config: AppConfig,
    logger: logging.Logger,
) -> None:
    """
    Create the Paramètres tab and populate it with the current config values.

    Layout: two-column table (parametre | valeur).
    Section headers start with "#" and are ignored when reading.

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        config: Current config to write as defaults.
        logger: Logger instance.
    """
    # Add the sheet
    body = {"requests": [{"addSheet": {"properties": {"title": _CONFIG_TAB}}}]}
    service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

    rows: List[List[str]] = [
        ["parametre", "valeur_1", "valeur_2"],
        ["# Profils LinkedIn (max 3) — format : profil | Nom | URL", "", ""],
    ]
    for p in config.linkedin_profiles:
        rows.append(["profil", p.get("name", ""), p.get("url", "")])

    rows.append(["# Pays cibles (un pays par ligne)", "", ""])
    for country in config.target_countries:
        rows.append(["pays", country, ""])

    rows.append(["# Mots-clés de recherche (un mot-clé par ligne)", "", ""])
    for kw in config.search_keywords:
        rows.append(["keyword", kw, ""])

    rows += [
        ["# Filtres", "", ""],
        ["score_minimum", str(config.min_match_score), ""],
        ["posts_max_par_pays", str(config.max_posts_per_country), ""],
    ]

    range_name = f"'{_CONFIG_TAB}'!A1"
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()
    logger.info("[sheets] '%s' tab created with %d rows.", _CONFIG_TAB, len(rows))


def _read_config_tab(
    service: Any,
    spreadsheet_id: str,
    logger: logging.Logger,
) -> Dict[str, Any]:
    """
    Read the Paramètres tab and return a dict of config overrides.

    Ignores rows where column A starts with "#" (section headers).
    Collects "pays" rows into a list, "keyword" rows into a list,
    and parses scalar values (score_minimum, posts_max_par_pays).

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        logger: Logger instance.

    Returns:
        Dict with keys: countries, keywords, score_minimum, posts_max_par_pays.
        Empty dict on failure.
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"'{_CONFIG_TAB}'!A:C",
        ).execute()
    except Exception as exc:
        logger.warning("[sheets] Could not read '%s' tab: %s", _CONFIG_TAB, exc)
        return {}

    rows = result.get("values", [])
    overrides: Dict[str, Any] = {"countries": [], "keywords": [], "profiles": []}

    for row in rows:
        if not row:
            continue
        key = str(row[0]).strip()
        val1 = str(row[1]).strip() if len(row) > 1 else ""
        val2 = str(row[2]).strip() if len(row) > 2 else ""

        if not key or key.startswith("#") or key == "parametre":
            continue  # skip headers and section markers

        if key == "profil" and val1 and val2:
            overrides["profiles"].append({"name": val1, "url": val2})
        elif key == "pays" and val1:
            overrides["countries"].append(val1)
        elif key == "keyword" and val1:
            overrides["keywords"].append(val1)
        elif key == "score_minimum" and val1:
            try:
                overrides["score_minimum"] = int(val1)
            except ValueError:
                logger.warning("[sheets] Invalid score_minimum value: %r", val1)
        elif key == "posts_max_par_pays" and val1:
            try:
                overrides["posts_max_par_pays"] = int(val1)
            except ValueError:
                logger.warning("[sheets] Invalid posts_max_par_pays value: %r", val1)

    return overrides


def load_feedback_examples(
    config: AppConfig,
    logger: logging.Logger,
) -> List[Dict[str, str]]:
    """
    Read user feedback from all existing monthly mission tabs.

    Scans every tab whose name matches the SHEET_TAB_FORMAT pattern and
    collects rows where column M (feedback) is non-empty. Returns a list
    of dicts with keys: mission_title, required_skills, feedback.

    Used to inject past corrections into Claude's scoring prompt so the
    model learns from explicit user feedback across runs.

    Args:
        config: Application configuration.
        logger: Logger instance.

    Returns:
        List of feedback dicts, empty list on any failure.
    """
    try:
        service = _get_sheets_service(config.google_service_account_json)
        spreadsheet = service.spreadsheets().get(spreadsheetId=config.spreadsheet_id).execute()
    except Exception as exc:
        logger.warning("[sheets] load_feedback_examples: auth failed — %s", exc)
        return []

    # Identify all monthly mission tabs (e.g. "Missions_2026-03")
    tab_prefix = config.sheet_tab_format.split("{")[0]  # e.g. "Missions_"
    mission_tabs = [
        s["properties"]["title"]
        for s in spreadsheet.get("sheets", [])
        if s["properties"]["title"].startswith(tab_prefix)
    ]

    examples: List[Dict[str, str]] = []
    for tab in mission_tabs:
        try:
            result = service.spreadsheets().values().get(
                spreadsheetId=config.spreadsheet_id,
                range=f"'{tab}'!A:M",
            ).execute()
        except Exception as exc:
            logger.warning("[sheets] Could not read tab '%s' for feedback: %s", tab, exc)
            continue

        rows = result.get("values", [])
        for row in rows[1:]:  # skip header
            # Column M is index 12; skip rows that don't reach it
            if len(row) < 13:
                continue
            feedback = str(row[12]).strip()
            if not feedback:
                continue
            examples.append({
                "mission_title": str(row[3]).strip() if len(row) > 3 else "",
                "required_skills": str(row[4]).strip() if len(row) > 4 else "",
                "feedback": feedback,
            })

    logger.info("[sheets] Loaded %d feedback example(s) from %d tab(s).", len(examples), len(mission_tabs))
    return examples


def write_missions(
    enriched_posts: List[EnrichedPost],
    config: AppConfig,
    logger: logging.Logger,
    seen_urls: Optional[Set[str]] = None,
    seen_hashes: Optional[Set[str]] = None,
) -> None:
    """
    Full write pipeline: authenticate → get/create tab → deduplicate →
    append rows → apply conditional formatting → update Dedup_Index.

    Deduplication is two-layered:
      1. Cross-run: uses seen_urls and seen_hashes loaded from Dedup_Index at
         the start of the run (passed in by run.py).
      2. Current-tab belt-and-suspenders: re-reads column H of the current
         monthly tab in case the pre-loaded sets were stale.
    Text-hash dedup catches same-content reposts with a new URL.

    Never raises. On fatal error, attempts to write an ERROR row to the sheet,
    then returns. Formatting failures are logged as warnings only.

    Args:
        enriched_posts: Scored and filtered mission posts.
        config: Application configuration.
        logger: Logger instance.
        seen_urls: Set of post URLs already in the sheet (all tabs, pre-loaded).
        seen_hashes: Set of text hashes already in the sheet (pre-loaded).
    """
    tab_name = _build_tab_name(config.sheet_tab_format)

    try:
        service = _get_sheets_service(config.google_service_account_json)
    except Exception as exc:
        logger.critical("[sheets] Authentication failed: %s", exc)
        return  # Cannot recover without auth

    try:
        sheet_id = _get_or_create_tab(service, config.spreadsheet_id, tab_name, logger)
    except Exception as exc:
        logger.critical("[sheets] Could not get/create tab '%s': %s", tab_name, exc)
        _write_error_row(service, config.spreadsheet_id, tab_name, str(exc), logger)
        return

    # Merge pre-loaded global sets with current-tab URLs (belt-and-suspenders)
    _seen_urls: Set[str] = set(seen_urls) if seen_urls else set()
    _seen_hashes: Set[str] = set(seen_hashes) if seen_hashes else set()
    try:
        tab_urls = _get_existing_urls(service, config.spreadsheet_id, tab_name)
        _seen_urls |= tab_urls
    except Exception as exc:
        logger.warning("[sheets] Could not read current-tab URLs for dedup fallback: %s", exc)

    # Filter: drop posts already seen by URL or by text hash (catches reposts)
    new_posts = []
    for p in enriched_posts:
        url = p.get("post_url", "")
        h = _text_hash_local(p.get("post_text", ""))
        if url in _seen_urls:
            logger.debug("[sheets] dedup: skipping known URL %s", url)
            continue
        if h in _seen_hashes:
            logger.debug("[sheets] dedup: skipping known text hash (repost) %s", url)
            continue
        new_posts.append(p)

    skipped = len(enriched_posts) - len(new_posts)

    if not new_posts:
        logger.info("[sheets] 0 new missions added, %d duplicates skipped.", skipped)
        return

    rows = [_build_row(p) for p in new_posts]

    try:
        start_row = _get_next_empty_row(service, config.spreadsheet_id, tab_name)
        appended = _append_rows(service, config.spreadsheet_id, tab_name, rows, logger)
    except Exception as exc:
        logger.error("[sheets] Row append failed: %s", exc)
        _write_error_row(service, config.spreadsheet_id, tab_name, str(exc), logger)
        return

    # Update the Dedup_Index with newly written posts
    _append_dedup_index(service, config.spreadsheet_id, new_posts, logger)

    # Apply conditional formatting — failure is cosmetic, never blocks
    try:
        _apply_conditional_formatting(
            service, config.spreadsheet_id, sheet_id,
            start_row_index=start_row - 1,  # convert to 0-based
            row_count=appended,
        )
    except Exception as exc:
        logger.warning("[sheets] Conditional formatting failed (data still written): %s", exc)

    logger.info("[sheets] %d new missions added, %d duplicates skipped.", appended, skipped)


def _build_tab_name(sheet_tab_format: str) -> str:
    """
    Resolve the monthly tab name from the format string.

    Replaces {YYYY-MM} with the current UTC year-month (e.g., "Missions_2026-03").

    Args:
        sheet_tab_format: Format string from config, e.g. "Missions_{YYYY-MM}".

    Returns:
        Resolved tab name string.
    """
    now = datetime.now(timezone.utc)
    return sheet_tab_format.replace("{YYYY-MM}", now.strftime("%Y-%m"))


def _get_sheets_service(service_account_json: str) -> Any:
    """
    Build and return an authenticated Google Sheets API service object.

    Parses the service account JSON string in-memory.
    NEVER writes credentials to disk (from_service_account_file is not used).

    Args:
        service_account_json: Raw JSON string of the service account credentials.

    Returns:
        Authenticated Sheets API Resource.
    """
    info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(info, scopes=_SCOPES)
    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def _get_or_create_tab(
    service: Any,
    spreadsheet_id: str,
    tab_name: str,
    logger: logging.Logger,
) -> int:
    """
    Return the sheet ID (gid) of the tab named tab_name.

    If the tab does not exist, creates it and writes the header row.
    Returns the integer sheet ID.

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        tab_name: Tab name, e.g. "Missions_2026-03".
        logger: Logger instance.

    Returns:
        Integer sheet ID (gid) of the tab.
    """
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = spreadsheet.get("sheets", [])

    for sheet in sheets:
        props = sheet.get("properties", {})
        if props.get("title") == tab_name:
            logger.debug("[sheets] Tab '%s' already exists (gid=%d).", tab_name, props["sheetId"])
            # Always refresh the header row so new columns are added to existing tabs
            _write_header_row(service, spreadsheet_id, tab_name)
            return props["sheetId"]

    # Create the tab
    logger.info("[sheets] Creating new tab '%s'...", tab_name)
    body = {"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
    response = service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()
    new_sheet_id = response["replies"][0]["addSheet"]["properties"]["sheetId"]

    _write_header_row(service, spreadsheet_id, tab_name)
    return new_sheet_id


def _write_header_row(service: Any, spreadsheet_id: str, tab_name: str) -> None:
    """
    Write the header row to the tab, reflecting the current _HEADERS list.

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        tab_name: Name of the sheet tab.
    """
    range_name = f"'{tab_name}'!A1"
    body = {"values": [_HEADERS]}
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body,
    ).execute()


def _get_existing_urls(
    service: Any,
    spreadsheet_id: str,
    tab_name: str,
) -> Set[str]:
    """
    Read all values in column H (post_url) to build the deduplication set.

    Reads only column H (lightweight regardless of row count). Skips row 1 (header).

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        tab_name: Name of the sheet tab.

    Returns:
        Set of post URL strings already present in the sheet.
    """
    range_name = f"'{tab_name}'!{_URL_COL_LETTER}:{_URL_COL_LETTER}"
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name
    ).execute()
    values = result.get("values", [])
    # Row 0 is the header; skip it
    return {row[0] for row in values[1:] if row and row[0]}


def _get_next_empty_row(service: Any, spreadsheet_id: str, tab_name: str) -> int:
    """
    Return the 1-based row index of the next empty row in the sheet.

    Used to calculate where new rows will be appended for conditional formatting.

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        tab_name: Name of the sheet tab.

    Returns:
        1-based row index of the next empty row.
    """
    range_name = f"'{tab_name}'!A:A"
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id, range=range_name
    ).execute()
    values = result.get("values", [])
    return len(values) + 1  # next row after last filled


def _build_row(post: EnrichedPost) -> List[Any]:
    """
    Convert an EnrichedPost dict to a flat list of 10 values matching _HEADERS.

    Type conversions:
    - post_date (ISO UTC) → split into date (YYYY-MM-DD) and heure (HH:MM)
    - required_skills (list) → comma-separated string
    - country → pays column
    - location → ville column

    Args:
        post: Enriched mission post.

    Returns:
        List of 10 cell values ready for the Sheets API.
    """
    # Parse publication date/time
    post_date_iso = post.get("post_date", "")
    try:
        post_dt = datetime.fromisoformat(post_date_iso)
        if post_dt.tzinfo is None:
            post_dt = post_dt.replace(tzinfo=timezone.utc)
        post_date = post_dt.strftime("%Y-%m-%d")
        post_hour = post_dt.strftime("%H:%M")
    except (ValueError, TypeError):
        post_date = ""
        post_hour = ""

    required_skills = post.get("required_skills", [])
    if isinstance(required_skills, list):
        required_skills = ", ".join(required_skills)

    match_reasons = post.get("match_reasons", [])
    if isinstance(match_reasons, list):
        match_reasons = " | ".join(match_reasons)

    return [
        post_date,                               # A: date
        post_hour,                               # B: heure
        post.get("author_name", ""),             # C: author_name
        post.get("mission_title", ""),           # D: mission_title
        required_skills,                         # E: required_skills
        post.get("match_score", 0),              # F: match_score
        post.get("daily_rate_tjm") or "",        # G: tjm
        post.get("post_url", ""),                # H: post_url
        post.get("country", ""),                 # I: pays
        post.get("location", ""),                # J: ville
        post.get("profil_name", ""),             # K: profil
        match_reasons,                           # L: match_reasons
        "",                                      # M: feedback (filled by user)
    ]


def _append_rows(
    service: Any,
    spreadsheet_id: str,
    tab_name: str,
    rows: List[List[Any]],
    logger: logging.Logger,
) -> int:
    """
    Batch-append new rows to the sheet using the values.append API endpoint.

    Uses USER_ENTERED value input option so dates and numbers render correctly.

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        tab_name: Name of the sheet tab.
        rows: List of row value lists to append.
        logger: Logger instance.

    Returns:
        Number of rows successfully appended.
    """
    range_name = f"'{tab_name}'!A1"
    body = {"values": rows}
    response = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body=body,
    ).execute()

    updates = response.get("updates", {})
    appended = updates.get("updatedRows", len(rows))
    logger.debug("[sheets] Appended %d rows.", appended)
    return appended


def _apply_conditional_formatting(
    service: Any,
    spreadsheet_id: str,
    sheet_id: int,
    start_row_index: int,
    row_count: int,
) -> None:
    """
    Apply score-based background color formatting to the match_score column (F)
    for newly added rows only.

    Rules: match_score >= 80 → green, 50–79 → yellow, < 50 → red.
    Applies only to new rows to avoid accumulating Sheets rule limits (max 5000).

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        sheet_id: Integer gid of the target tab.
        start_row_index: 0-based index of the first new row.
        row_count: Number of new rows added.
    """
    end_row_index = start_row_index + row_count

    def _range(col_idx: int) -> Dict[str, Any]:
        return {
            "sheetId": sheet_id,
            "startRowIndex": start_row_index,
            "endRowIndex": end_row_index,
            "startColumnIndex": col_idx,
            "endColumnIndex": col_idx + 1,
        }

    def _rule(condition_type: str, values: List[str], color: Dict) -> Dict[str, Any]:
        return {
            "addConditionalFormatRule": {
                "rule": {
                    "ranges": [_range(_SCORE_COL_IDX)],
                    "booleanRule": {
                        "condition": {
                            "type": condition_type,
                            "values": [{"userEnteredValue": v} for v in values],
                        },
                        "format": {"backgroundColor": color},
                    },
                },
                "index": 0,
            }
        }

    requests = [
        _rule("NUMBER_GREATER_THAN_EQ", [str(_GREEN_MIN)], _COLOR_GREEN),
        _rule("NUMBER_BETWEEN", [str(_YELLOW_MIN), str(_GREEN_MIN - 1)], _COLOR_YELLOW),
        _rule("NUMBER_LESS_THAN", [str(_YELLOW_MIN)], _COLOR_RED),
    ]

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests},
    ).execute()


def _write_error_row(
    service: Any,
    spreadsheet_id: str,
    tab_name: str,
    error_message: str,
    logger: logging.Logger,
) -> None:
    """
    Append a single ERROR row to the sheet to make pipeline failures traceable.

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        tab_name: Target tab name.
        error_message: Description of what went wrong.
        logger: Logger instance.
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hour_str = datetime.now(timezone.utc).strftime("%H:%M")
    error_row = [date_str, hour_str, "", "PIPELINE ERROR", "", -1, "", "ERROR", "N/A", "", ""]
    try:
        range_name = f"'{tab_name}'!A1"
        service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="USER_ENTERED",
            insertDataOption="INSERT_ROWS",
            body={"values": [error_row]},
        ).execute()
        logger.info("[sheets] ERROR row written to sheet.")
    except Exception as exc:
        logger.critical("[sheets] Could not write ERROR row: %s", exc)
