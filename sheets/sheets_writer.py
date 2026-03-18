"""
sheets/sheets_writer.py — Google Sheets writer with deduplication and conditional formatting.

Authenticates via a service account JSON string (never written to disk),
manages monthly tabs, deduplicates rows by post_url, appends new rows,
and applies color-coded conditional formatting based on match_score.

Also manages a "Paramètres" tab for editable search configuration.
"""

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

# Column layout (A=0 … J=9)
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

    countries = overrides.get("countries") or config.target_countries
    keywords = overrides.get("keywords") or config.search_keywords
    min_score = overrides.get("score_minimum", config.min_match_score)
    max_posts = overrides.get("posts_max_par_pays", config.max_posts_per_country)

    logger.info(
        "[sheets] Config from sheet — countries: %s | keywords: %d | min_score: %d | max_posts: %d",
        countries, len(keywords), min_score, max_posts,
    )

    return replace(
        config,
        target_countries=countries,
        search_keywords=keywords,
        min_match_score=int(min_score),
        max_posts_per_country=int(max_posts),
    )


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
        ["parametre", "valeur"],
        ["# Pays cibles (un pays par ligne)", ""],
    ]
    for country in config.target_countries:
        rows.append(["pays", country])

    rows.append(["# Mots-clés de recherche (un mot-clé par ligne)", ""])
    for kw in config.search_keywords:
        rows.append(["keyword", kw])

    rows += [
        ["# Filtres", ""],
        ["score_minimum", str(config.min_match_score)],
        ["posts_max_par_pays", str(config.max_posts_per_country)],
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
            range=f"'{_CONFIG_TAB}'!A:B",
        ).execute()
    except Exception as exc:
        logger.warning("[sheets] Could not read '%s' tab: %s", _CONFIG_TAB, exc)
        return {}

    rows = result.get("values", [])
    overrides: Dict[str, Any] = {"countries": [], "keywords": []}

    for row in rows:
        if not row:
            continue
        key = str(row[0]).strip()
        val = str(row[1]).strip() if len(row) > 1 else ""

        if not key or key.startswith("#") or key == "parametre":
            continue  # skip headers and section markers

        if key == "pays" and val:
            overrides["countries"].append(val)
        elif key == "keyword" and val:
            overrides["keywords"].append(val)
        elif key == "score_minimum" and val:
            try:
                overrides["score_minimum"] = int(val)
            except ValueError:
                logger.warning("[sheets] Invalid score_minimum value: %r", val)
        elif key == "posts_max_par_pays" and val:
            try:
                overrides["posts_max_par_pays"] = int(val)
            except ValueError:
                logger.warning("[sheets] Invalid posts_max_par_pays value: %r", val)

    return overrides


def write_missions(
    enriched_posts: List[EnrichedPost],
    config: AppConfig,
    logger: logging.Logger,
) -> None:
    """
    Full write pipeline: authenticate → get/create tab → deduplicate →
    append rows → apply conditional formatting.

    Never raises. On fatal error, attempts to write an ERROR row to the sheet,
    then returns. Formatting failures are logged as warnings only.

    Args:
        enriched_posts: Scored and filtered mission posts.
        config: Application configuration.
        logger: Logger instance.
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

    try:
        existing_urls = _get_existing_urls(service, config.spreadsheet_id, tab_name)
    except Exception as exc:
        logger.error("[sheets] Could not read existing URLs for dedup: %s", exc)
        existing_urls = set()

    new_posts = [p for p in enriched_posts if p.get("post_url") not in existing_urls]
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
    Write the 10-column header row to a newly created tab.

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
    error_row = [date_str, hour_str, "", "PIPELINE ERROR", "", -1, "", "ERROR", "N/A", ""]
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
