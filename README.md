# LinkedIn Freelance Mission Tracker

Daily automation that scrapes LinkedIn for freelance mission posts, scores them against a consultant profile using Claude AI, and writes results to Google Sheets.

Runs automatically every day at **06:00 UTC** (08:00 CEST / 07:00 CET) via GitHub Actions.

> **New user?** Start with [QUICKSTART.md](QUICKSTART.md) for a step-by-step setup guide using the Google Sheets template.

---

## Architecture

```
run.py (orchestrator)
├── config/config.py          — load & validate AppConfig
├── sheets/sheets_writer.py   — read Paramètres tab (config override)
│                             — load Profils_Cache + Dedup_Index
├── scraper/bereach_scraper.py — BeReach API (post scraping + profile fetch)
├── matcher/profile_matcher.py — Claude Haiku scoring + profile fetch
└── sheets/sheets_writer.py   — write results + update Dedup_Index
```

---

## Setup

### 1. Clone & install dependencies

```bash
git clone <repo-url>
cd <repo>
pip install -r requirements.txt
```

### 2. Configure environment variables

Copy `.env.example` to `.env` and fill in all values:

```bash
cp .env.example .env
```

| Variable | Description |
|---|---|
| `BEREACH_API_TOKEN` | BeReach API token (post scraping + LinkedIn profile fetch) |
| `ANTHROPIC_API_KEY` | Claude API key for post scoring |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | Full service account JSON (one line, escaped) |
| `SPREADSHEET_ID` | Google Sheets spreadsheet ID |

> **BeReach API**: Sign up at [bereach.co](https://bereach.co). Free tier may be limited; a paid plan is recommended for daily scraping. Copy your API token from the dashboard.

> **GOOGLE_SERVICE_ACCOUNT_JSON** must be a single-line JSON string — no newlines. Use the Python command in the Google Cloud Setup section below to generate the correct format. Do NOT paste the raw JSON file contents directly.

### 3. Google Cloud Setup

This is the most involved step. Follow carefully:

1. Go to [console.cloud.google.com](https://console.cloud.google.com) and create (or select) a project.
2. Enable the **Google Sheets API**: navigate to *APIs & Services → Library*, search "Google Sheets API", click Enable.
3. Create a **Service Account**: go to *IAM & Admin → Service Accounts → Create Service Account*. Name it anything (e.g. `sheets-writer`).
4. Generate a **JSON key**: click the service account → *Keys → Add Key → Create new key → JSON*. A `.json` file downloads automatically.
5. Convert the key to a single escaped line for your `.env`:

```bash
python -c "import json,sys; d=json.load(open(sys.argv[1])); print(json.dumps(d))" service_account.json
```

Copy the output (starting with `{"type":"service_account",...}`) and paste it as the value of `GOOGLE_SERVICE_ACCOUNT_JSON` in your `.env`.

6. **Create your Google Sheet**: go to [sheets.google.com](https://sheets.google.com), create a blank spreadsheet, and copy the spreadsheet ID from the URL (`https://docs.google.com/spreadsheets/d/**SPREADSHEET_ID**/edit`). Paste it as `SPREADSHEET_ID` in `.env`.
7. **Share the sheet** with the service account email (found in the JSON key as `client_email`) as **Editor**.

The pipeline creates all required tabs automatically on first run.

### 4. Configure settings

Edit `config/settings.json` with your profile and preferences:

```json
{
  "LINKEDIN_PROFILES": [{"name": "YOUR_NAME", "url": "https://linkedin.com/in/YOUR_LINKEDIN_PROFILE/"}],
  "TARGET_COUNTRIES": ["France", "Germany"],
  "SEARCH_KEYWORDS": ["mission freelance", "besoin freelance", "offre mission freelance"],
  "MIN_MATCH_SCORE": 40,
  "MAX_POSTS_PER_COUNTRY": 50
}
```

You can also override settings at runtime via the **Paramètres** tab in the Google Sheet (takes precedence).

### 5. Google Sheets tabs

The pipeline creates the following tabs automatically on first run:

| Tab | Purpose |
|---|---|
| `Missions` | Mission results (all runs, deduplicated) |
| `Remote` | Remote job results |
| `Dedup_Index` | Cross-run deduplication index |
| `Profils_Cache` | LinkedIn profile vector cache |
| `Paramètres` | Editable config (overrides settings.json) |

---

## GitHub Actions (CI/CD)

Secrets to set in **Settings → Secrets and variables → Actions**:

`BEREACH_API_TOKEN`, `ANTHROPIC_API_KEY`, `GOOGLE_SERVICE_ACCOUNT_JSON`, `SPREADSHEET_ID`

To trigger a manual run: **Actions → daily_extract → Run workflow**.

Logs are uploaded as artifacts (retained 30 days).

---

## Cost Estimate

| Service | Free Tier | Paid |
|---------|-----------|------|
| BeReach | Limited free | ~$30–100/month |
| Anthropic Claude | $5 credit | ~$1–5/month (Haiku is very cheap) |
| Google Sheets API | Free (always) | Free |
| GitHub Actions | 2000 min/month free | Free for this use case |

> **Typical total cost**: $30–100/month, mostly driven by your BeReach plan and post volume.

---

## Running locally

```bash
python run.py
```

Logs are written to `logs/run_YYYY-MM-DD.log`.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `KeyError: BEREACH_API_TOKEN` | Missing env variable | Check `.env` has all 4 variables set |
| `APIError: 403` on Sheets | Sheet not shared | Share the sheet with the service account `client_email` as Editor |
| `json.JSONDecodeError` on startup | Malformed `GOOGLE_SERVICE_ACCOUNT_JSON` | Re-run the Python one-liner above to regenerate the escaped value |
| Pipeline returns 0 posts | Keywords returned no results | Check `logs/run_YYYY-MM-DD.log` for API responses; try broader keywords |
| Score always 0 | LinkedIn profile URL not reachable | Verify `LINKEDIN_PROFILES.url` in `settings.json` is a valid public profile |
| Duplicate rows in sheet | `Dedup_Index` tab corrupted | Delete all sheet tabs and re-run — the pipeline recreates them cleanly |
