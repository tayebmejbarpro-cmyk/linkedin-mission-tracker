\# CLAUDE.md — LinkedIn Freelance Mission Tracker



\## Project Purpose

Daily automation that discovers freelance mission posts on LinkedIn for specified countries,

scores them against my profile, and writes structured data to Google Sheets.



\## Architecture

\- Entry point: `run.py` (invoked daily via GitHub Actions)

\- Modules: `scraper/`, `matcher/`, `sheets/`, `config/`

\- Config: `.env` for secrets, `config/settings.json` for user preferences

\- CI/CD: `.github/workflows/daily_extract.yml` — runs every day at 06:00 UTC (= 08:00 CEST / 07:00 CET)



\## Coding Rules

\- Never hardcode credentials. Always use `os.getenv()`.

\- Handle LinkedIn rate limits: add random delays of 2–5s between requests.

\- All functions must have docstrings and return typed values.

\- Log all steps to `logs/run\_{date}.log`.

\- On error, write a failed row to the sheet with status="ERROR" rather than crashing.

\- Deduplicate posts using post URL as unique key before writing to sheet.

\- Only process posts published within the last 24 hours. Discard older posts at scrape time.



\## Key Files

\- `config/settings.json`: MY\_LINKEDIN\_URL, TARGET\_COUNTRIES, SHEET\_ID, KEYWORDS

\- `skills/linkedin-scraper/SKILL.md`: scraping logic instructions

\- `skills/profile-matcher/SKILL.md`: scoring logic instructions

\- `skills/sheets-writer/SKILL.md`: Google Sheets write instructions



\## GitHub Actions Setup

\- Workflow file: `.github/workflows/daily_extract.yml`

\- Schedule: `cron: '0 6 * * *'` (06:00 UTC = 08:00 CEST in summer, 07:00 CET in winter)

\- Trigger: also supports `workflow_dispatch` for manual runs

\- All secrets must be set in the GitHub repo Settings → Secrets and variables → Actions

\- Workflow steps: checkout → install Python deps → run `python run.py` → upload logs as artifact



\## Environment Variables Required

APIFY\_API\_TOKEN, ANTHROPIC\_API\_KEY,

GOOGLE\_SERVICE\_ACCOUNT\_JSON, SPREADSHEET\_ID



