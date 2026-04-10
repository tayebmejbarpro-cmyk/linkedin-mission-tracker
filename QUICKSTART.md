# Quick Start — New User Setup (~15 minutes)

This guide walks you through setting up the LinkedIn Freelance Mission Tracker from scratch,
starting from the Google Sheets template.

---

## Step 1 — Create your Google Sheet

1. Create a **blank** Google Spreadsheet at [sheets.google.com](https://sheets.google.com)
2. From the URL (`https://docs.google.com/spreadsheets/d/**ID**/edit`), copy the spreadsheet **ID** — you'll need it in Step 5
3. The pipeline auto-creates all required tabs on first run — you only need to add the `Paramètres` tab manually:
   - Click the **+** button at the bottom left to add a new tab
   - Name it exactly `Paramètres`
   - Fill it in as described in Step 2

The pipeline will automatically create these additional tabs on first run:

| Tab | Created by |
|-----|-----------|
| `Paramètres` | **You** (Step 2 below) |
| `Missions_YYYY-MM` | Pipeline — monthly tab for scored missions |
| `Remote_YYYY-MM` | Pipeline — monthly tab for remote jobs |
| `Profils_Cache` | Pipeline — caches your LinkedIn profile vector |
| `Dedup_Index` | Pipeline — cross-run deduplication index |

---

## Step 2 — Fill in the Paramètres tab

Open the `Paramètres` tab in your copy and fill in your information:

| Row key | What to enter | Example |
|---------|--------------|---------|
| `profil` | Your LinkedIn **public** profile URL | `https://linkedin.com/in/john-doe/` |
| `pays` | One target country per row | `France`, `Belgium` |
| `keyword` | Boolean search queries for your role (one per row) | `"mission" AND "freelance" AND "PMO"` |
| `remote_keyword` | Same format but for full-remote jobs | `"full remote" AND "PMO"` |
| `score_minimum` | Min relevance score to keep posts (0-100) | `50` — raise once you see results |
| `posts_max_par_pays` | Max posts processed per country per run | `50` |

> **Keyword tip**: Use LinkedIn boolean syntax — `AND`, `OR`, quoted phrases. The more specific, the better the signal-to-noise ratio. You can always adjust later directly in this tab without touching code.

> **LinkedIn profile**: Must be a public profile. The pipeline uses the BeReach API to fetch it once on first run and caches the result — no repeated calls after that.

---

## Step 3 — Get your API tokens

You need accounts and tokens from 2 services:

| Token | Service | Where to get it |
|-------|---------|----------------|
| `BEREACH_API_TOKEN` | BeReach (post scraping + profile fetch) | [bereach.co](https://bereach.co) → dashboard → API |
| `ANTHROPIC_API_KEY` | Claude AI (post scoring) | [console.anthropic.com](https://console.anthropic.com) → API Keys |

> **BeReach**: A paid plan is recommended for daily scraping — the free tier has volume limits. BeReach handles both LinkedIn post scraping and LinkedIn profile fetching.
> **Anthropic**: Claude Haiku is used for scoring — extremely cheap (~$1-5/month typical).

---

## Step 4 — Google Service Account (Sheets access)

The pipeline writes to your Google Sheet using a service account. Follow these steps:

1. Go to [console.cloud.google.com](https://console.cloud.google.com) — create or select a project
2. Enable the **Google Sheets API**: *APIs & Services → Library → search "Google Sheets API" → Enable*
3. Create a **Service Account**: *IAM & Admin → Service Accounts → Create Service Account* (any name, e.g. `sheets-writer`)
4. Generate a **JSON key**: click your service account → *Keys → Add Key → Create new key → JSON* → a file downloads
5. Convert the JSON key to a single escaped line (required for the `.env` / GitHub secret):

```bash
python -c "import json,sys; d=json.load(open(sys.argv[1])); print(json.dumps(d))" service_account.json
```

Copy the full output — it starts with `{"type":"service_account",...}`.

6. **Share your Google Sheet** with the `client_email` value from the JSON (looks like `xxx@yyy.iam.gserviceaccount.com`) as **Editor**

---

## Step 5 — Fork the repo and set GitHub secrets

1. **Fork** this repository on GitHub
2. In your fork: *Settings → Secrets and variables → Actions → New repository secret*
3. Add all 4 secrets:

| Secret name | Value |
|-------------|-------|
| `BEREACH_API_TOKEN` | From Step 3 |
| `ANTHROPIC_API_KEY` | From Step 3 |
| `GOOGLE_SERVICE_ACCOUNT_JSON` | The single-line JSON output from Step 4.5 |
| `SPREADSHEET_ID` | The spreadsheet ID from Step 1.3 |

---

## Step 6 — Trigger your first run

1. In your fork, go to **Actions → daily_extract → Run workflow**
2. Click **Run workflow** (green button)
3. Wait 3–5 minutes
4. Open your Google Sheet — check the **Missions** tab for results

> If the run fails, download the log artifact from the Actions run page and check the error message.

---

## What happens on first run

```
Paramètres tab read         → your real config loaded (profile, countries, keywords)
Profils_Cache checked       → empty → cache miss
BeReach called              → your LinkedIn profile fetched once and cached
Profile vector saved        → cached in Profils_Cache for all future runs
Posts scraped (BeReach)     → for each target country × each keyword
Posts scored (Claude)       → each post matched against your profile
Results written             → Missions tab populated
Dedup_Index updated         → prevents duplicates on future runs
```

## What happens on every subsequent run (daily, 06:00 UTC)

```
Profils_Cache hit           → no BeReach profile call → fast startup
Dedup_Index checked         → only new posts processed (no duplicates)
New results appended        → Missions tab grows daily
```

---

## Troubleshooting

| Error / Symptom | Cause | Fix |
|-----------------|-------|-----|
| `Placeholder values detected` | Paramètres tab not filled | Complete Step 2 |
| `APIError: 403` on Sheets | Sheet not shared | Share sheet with service account email (Step 4.6) |
| `json.JSONDecodeError` | Malformed `GOOGLE_SERVICE_ACCOUNT_JSON` | Re-run the Python command in Step 4.5 |
| 0 posts returned | Keywords too narrow or API issue | Check `logs/run_YYYY-MM-DD.log` in the Actions artifact |
| Score always low | LinkedIn profile not fetched | Verify profile URL is public and correct in Paramètres |

For the full troubleshooting table, see [README.md](README.md#troubleshooting).
