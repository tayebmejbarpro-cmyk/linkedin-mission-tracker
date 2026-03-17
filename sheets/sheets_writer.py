"""
sheets/sheets_writer.py — Google Sheets writer with deduplication and conditional formatting.

Authenticates via a service account JSON string (never written to disk),
manages monthly tabs, deduplicates rows by post_url, appends new rows,
and applies color-coded conditional formatting based on match_score.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from config.config import AppConfig
from matcher.profile_matcher import EnrichedPost

# Google Sheets API scopes
_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Column layout (A=0 … P=15)
_HEADERS = [
    "date_found",        # A
    "post_url",          # B  ← dedup key
    "author_name",       # C
    "author_title",      # D
    "author_profile_url",# E
    "mission_title",     # F
    "required_skills",   # G
    "duration",          # H
    "daily_rate_tjm",    # I
    "location",          # J
    "remote_ok",         # K
    "contact_info",      # L
    "match_score",       # M  ← conditional formatting applied here
    "match_reasons",     # N
    "post_text",         # O
    "country",           # P
]

# Column index (0-based) for match_score
_SCORE_COL_IDX = 12   # M

# Conditional formatting color thresholds
_GREEN_MIN = 80     # >= 80 → green
_YELLOW_MIN = 50    # 50–79 → yellow
# < 50 → red

# Google Sheets hex colors (background)
_COLOR_GREEN = {"red": 0.718, "green": 0.882, "blue": 0.804}   # #b7e1cd
_COLOR_YELLOW = {"red": 0.988, "green": 0.910, "blue": 0.698}  # #fce8b2
_COLOR_RED = {"red": 0.957, "green": 0.780, "blue": 0.765}     # #f4c7c3

# Maximum characters for post_text cell to stay within Sheets cell limit
_POST_TEXT_MAX_CHARS = 2000


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
        # Determine the row index where new data will start (for formatting)
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
    Write the 16-column header row to a newly created tab.

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
    Read all values in column B (post_url) to build the deduplication set.

    Reads only column B (lightweight regardless of row count). Skips row 1 (header).

    Args:
        service: Authenticated Sheets service.
        spreadsheet_id: Target spreadsheet ID.
        tab_name: Name of the sheet tab.

    Returns:
        Set of post URL strings already present in the sheet.
    """
    range_name = f"'{tab_name}'!B:B"
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
    Convert an EnrichedPost dict to a flat list of 16 values.

    Type conversions applied:
    - required_skills (list) → comma-separated string
    - match_reasons (list) → newline-separated string
    - remote_ok (bool) → "TRUE" / "FALSE"
    - post_text → truncated to 2000 chars
    - contact_info: prefers claude_contact_info, falls back to raw contact_info

    Args:
        post: Enriched mission post.

    Returns:
        List of 16 cell values ready for the Sheets API.
    """
    date_found = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    required_skills = post.get("required_skills", [])
    if isinstance(required_skills, list):
        required_skills = ", ".join(required_skills)

    match_reasons = post.get("match_reasons", [])
    if isinstance(match_reasons, list):
        match_reasons = "\n".join(match_reasons)

    post_text = post.get("post_text", "")
    if len(post_text) > _POST_TEXT_MAX_CHARS:
        post_text = post_text[:_POST_TEXT_MAX_CHARS] + "…"

    contact_info = post.get("claude_contact_info") or post.get("contact_info") or ""
    remote_ok = "TRUE" if post.get("remote_ok") else "FALSE"

    return [
        date_found,                              # A
        post.get("post_url", ""),                # B
        post.get("author_name", ""),             # C
        post.get("author_title", ""),            # D
        post.get("author_profile_url", ""),      # E
        post.get("mission_title", ""),           # F
        required_skills,                         # G
        post.get("duration", ""),                # H
        post.get("daily_rate_tjm") or "",        # I
        post.get("location", ""),                # J
        remote_ok,                               # K
        contact_info,                            # L
        post.get("match_score", 0),              # M
        match_reasons,                           # N
        post_text,                               # O
        post.get("country", ""),                 # P
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

    Uses USER_ENTERED value input option so dates and booleans render correctly.

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
    Apply score-based background color formatting to the match_score column (M)
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
        # Green: >= 80
        _rule("NUMBER_GREATER_THAN_EQ", [str(_GREEN_MIN)], _COLOR_GREEN),
        # Yellow: >= 50 (rendered after green, so only 50-79 will be yellow in practice)
        _rule("NUMBER_BETWEEN", [str(_YELLOW_MIN), str(_GREEN_MIN - 1)], _COLOR_YELLOW),
        # Red: < 50
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
    date_found = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    error_row = [date_found, "ERROR", "", "", "", "PIPELINE ERROR", "", "", "", "",
                 "FALSE", "", -1, "", error_message[:_POST_TEXT_MAX_CHARS], "N/A"]
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
