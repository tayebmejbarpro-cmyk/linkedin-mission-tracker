---

name: profile-matcher

description: Fetches LinkedIn profile(s) via BeReach API (with sheet caching), then scores each scraped
  post for relevance using Claude Haiku. Returns enriched post dicts with match_score, extracted
  mission details, and geo-filter flag. Handles multi-profile scoring.

---



\## Process Steps

1\. **Profile fetching** (`fetch_profile_vectors()`):

&nbsp;  - Check `Profils_Cache` sheet tab for a cached profile vector (refreshed once per day).

&nbsp;  - On cache miss, fetch the LinkedIn profile via the BeReach API (`POST /visit/linkedin/profile`).

&nbsp;  - Extract name, headline, location, company, about, experience, and skills from the response.

&nbsp;  - On BeReach failure, fall back to HTTP scrape with BeautifulSoup, then `_FALLBACK_PROFILE` (never crashes).

&nbsp;  - Save updated vectors to `Profils_Cache` for next run.

2\. **Feedback loading** (`run.py`): load user feedback from the sheet (column M), aggregate by domain
   cluster into a calibration table (injected into Claude prompt as a compact reference).

3\. **Scoring** (`score_posts()`): for each raw post, call `_score_post_with_claude()` concurrently
   (up to `_MAX_CONCURRENT_SCORING=5` threads), with a random 0.3–1.0s stagger between workers.

4\. **Claude model**: `claude-haiku-4-5-20251001` — fast and cost-efficient for batch scoring.

5\. **Claude prompt** asks Claude to:

&nbsp;  a. Determine `is_genuine_mission` (TRUE only if a company/recruiter/ESN is actively seeking a freelancer).

&nbsp;  b. Extract: `mission_title`, `required_skills`, `duration`, `daily_rate_tjm`, `location`, `remote_ok`, `contact_info`.

&nbsp;  c. Score profile match (0–100): 80–100 strong, 50–79 partial, 0–49 weak.

&nbsp;  d. Determine `is_target_location` based on `config.target_countries`.

&nbsp;  e. Reference the domain calibration table to align score levels with user's past feedback.

6\. **Non-genuine posts**: `match_score` forced to 0, post not written to sheet.

7\. **Location-filtered posts** (`is_target_location=False`): scored normally but indexed in
   `Dedup_Index` so they are never re-scored on future runs.

8\. **Rate limit handling**: up to 3 retries with exponential backoff (10s / 20s / 40s) on
   `anthropic.RateLimitError`. Other errors return safe defaults (`match_score=0`).

9\. **Filter and sort**: keep posts with `match_score >= config.min_match_score`, sort descending.



\## EnrichedPost Schema (output)

`is_genuine_mission`, `mission_title`, `required_skills`, `duration`, `daily_rate_tjm`,
`location`, `remote_ok`, `contact_info`, `best_profil` (profile name with highest score),
`match_score`, `match_reasons`, `language`, `is_target_location`.



\## Environment Variables Required

\- `ANTHROPIC_API_KEY`: Claude API key

\- `BEREACH_API_TOKEN`: used for profile fetching via BeReach API

