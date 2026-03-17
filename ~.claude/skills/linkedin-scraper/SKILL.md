---

name: linkedin-scraper

description: Scrapes LinkedIn posts mentioning freelance missions for given countries using Apify.

&nbsp; Invoked at the start of each daily run. Returns a list of raw post dicts.

---



\## Process Steps



1\. Use the **Apify LinkedIn Post Search scraper** (`apify/linkedin-post-search-scraper` or equivalent actor) via the Apify API.

2\. Authenticate using the `APIFY_API_TOKEN` environment variable. Never hardcode the token.

3\. For each country in TARGET\_COUNTRIES, trigger an Apify actor run with:

&nbsp;  - `keywords`: each entry from SEARCH\_KEYWORDS (e.g. "mission freelance", "TJM freelance")

&nbsp;  - `country`: the target country filter

&nbsp;  - `maxResults`: MAX\_POSTS\_PER\_COUNTRY (50)

&nbsp;  - `datePosted`: `"past-24h"` — only fetch posts published in the last 24 hours

4\. Poll the Apify run status until completion (status = `SUCCEEDED`). On failure, log error and skip that batch.

5\. Fetch results from the Apify dataset using the run's `defaultDatasetId`.

6\. For each post in the dataset, normalize to this structure:

&nbsp;  - post\_url, author\_name, author\_title, author\_profile\_url

&nbsp;  - post\_text (full), post\_date (parsed as UTC datetime), likes\_count, comments\_count

&nbsp;  - Any email or contact info found in the post text

7\. **24h filter (safety net)**: even with `datePosted: past-24h`, verify `post_date >= utcnow() - timedelta(hours=24)`. Discard any post that falls outside this window and log a warning.

8\. Save raw results to `data/raw_posts_{date}.json` before returning.

9\. Return: List\[Dict] of raw posts (all within the last 24h).



\## Apify Integration Notes

\- Use the `apify-client` Python library (`pip install apify-client`).

\- Actor run is triggered via `client.actor(actor_id).call(run_input={...})`.

\- Dataset results are fetched via `client.dataset(dataset_id).list_items().items`.

\- Add a timeout of 120s when polling for run completion.

\- If the actor returns 0 results, log a warning but do not raise an error.



\## Environment Variables Required

\- `APIFY_API_TOKEN`: Apify personal API token (set in GitHub Actions secrets)

