---

name: sheets-writer

description: Reads config from Google Sheets, loads cross-run dedup index, writes enriched
  freelance mission posts to a monthly tab, manages profile vector cache, and syncs a
  Paramètres config tab. Handles deduplication and daily appending without overwriting history.

---



\## Sheet Tabs

| Tab | Purpose |
|---|---|
| `Missions_{YYYY-MM}` | Monthly mission data — one row per unique post |
| `Dedup_Index` | Persistent cross-run dedup index (post_url + text_hash) |
| `Profils_Cache` | Caches LinkedIn profile vectors to avoid re-fetching each run |
| `Paramètres` | Editable config (keywords, countries, score threshold, etc.) |



\## Process Steps

1\. **Config sync** (`sync_config_tab()`): read `Paramètres` tab and override `AppConfig` fields
   (keywords, target countries, min score, etc.) — allows non-code config changes from the sheet.

2\. **Profile cache** (`load_profile_vectors()`): read `Profils_Cache` tab. Returns dict of
   `{name: vector_string}`. Used by `profile_matcher.fetch_profile_vectors()` to skip BeReach profile calls.

3\. **Dedup index** (`load_seen_posts_all_tabs()`): read `Dedup_Index` tab (columns: post_url,
   text_hash). Returns two sets passed to the scraper for cross-run deduplication.

4\. **Write missions** (`write_missions()`):

&nbsp;  a. Create or verify `Missions_{YYYY-MM}` tab exists; write header row if new.

&nbsp;  b. For each enriched post, check URL and text hash against existing rows in the tab.

&nbsp;  c. Append only new, non-duplicate posts.

&nbsp;  d. Apply conditional formatting to column F (match_score): ≥80 green, 50–79 yellow, <50 red.

&nbsp;  e. Add new URLs and hashes to `Dedup_Index` tab.

5\. **Index rejected posts** (`index_rejected_posts()`): posts filtered by geo (`is_target_location=False`)
   are added to `Dedup_Index` only — so they are never re-scored on future runs.

6\. **Error row** (`_write_error_row()`): if the pipeline crashes, write a 13-column ERROR row to
   the sheet so failures are visible without reading logs.



\## Column Schema (`_HEADERS`, 13 columns A–M)

| Col | Field | Notes |
|---|---|---|
| A | date | YYYY-MM-DD |
| B | heure | HH:MM UTC |
| C | author_name | |
| D | mission_title | |
| E | required_skills | comma-separated |
| F | match_score | conditional formatting |
| G | tjm | daily rate if mentioned |
| H | post_url | **dedup key** (not column A) |
| I | pays | country |
| J | ville | city |
| K | profil | LinkedIn profile name with best match |
| L | match_reasons | top 3 Claude reasons |
| M | feedback | filled manually by user |



\## Environment Variables Required

\- `GOOGLE_SERVICE_ACCOUNT_JSON`: service account JSON (parsed in-memory, never written to disk)

\- `SPREADSHEET_ID`: target Google Spreadsheet ID

