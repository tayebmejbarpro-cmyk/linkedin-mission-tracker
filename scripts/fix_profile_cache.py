"""
scripts/fix_profile_cache.py — One-off script to overwrite the Profils_Cache
tab with your profile vector, built from your CV / LinkedIn profile.

Usage:
    1. Fill in PROFILE_NAME, PROFILE_URL, and PROFILE_VECTOR below.
    2. Run: python scripts/fix_profile_cache.py

Requires GOOGLE_SERVICE_ACCOUNT_JSON and SPREADSHEET_ID to be set in the
environment (or in a local .env file loaded via python-dotenv).
"""

import json
import os
import sys
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── Fill in your profile details below ───────────────────────────────────────
# PROFILE_NAME  : displayed name (must match the name in Paramètres tab)
# PROFILE_URL   : your LinkedIn public profile URL
# PROFILE_VECTOR: pipe-separated string describing your profile. Format:
#   "Full Name | Headline | Location | About summary | Job1 | Job2 | Skills: ..."
PROFILE_NAME = "YOUR_NAME"
PROFILE_URL  = "https://www.linkedin.com/in/YOUR_LINKEDIN_PROFILE/"
PROFILE_VECTOR = (
    "YOUR_FULL_NAME | YOUR_HEADLINE | YOUR_LOCATION | "
    "YOUR_ABOUT_SUMMARY | "
    "YOUR_JOB_1 | "
    "YOUR_JOB_2 | "
    "Skills: YOUR_SKILLS"
)
# ─────────────────────────────────────────────────────────────────────────────

_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_PROFILES_CACHE_TAB = "Profils_Cache"
_HEADERS = ["profile_name", "url", "vector", "fetched_at"]


def main() -> None:
    service_account_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")

    if not service_account_json or not spreadsheet_id:
        print("ERROR: GOOGLE_SERVICE_ACCOUNT_JSON and SPREADSHEET_ID must be set.")
        sys.exit(1)

    info = json.loads(service_account_json)
    credentials = service_account.Credentials.from_service_account_info(info, scopes=_SCOPES)
    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

    # Ensure tab exists
    spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {s["properties"]["title"] for s in spreadsheet.get("sheets", [])}

    if _PROFILES_CACHE_TAB not in existing:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": _PROFILES_CACHE_TAB}}}]},
        ).execute()
        print(f"Created '{_PROFILES_CACHE_TAB}' tab.")

    fetched_at = datetime.now(timezone.utc).isoformat()
    rows = [
        _HEADERS,
        [PROFILE_NAME, PROFILE_URL, PROFILE_VECTOR, fetched_at],
    ]

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{_PROFILES_CACHE_TAB}'!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()

    # Clear any leftover rows below row 2
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{_PROFILES_CACHE_TAB}'!A3:D1000",
    ).execute()

    print(f"Done. Profile vector written ({len(PROFILE_VECTOR)} chars).")
    print(f"Vector preview: {PROFILE_VECTOR[:120]}...")


if __name__ == "__main__":
    main()
