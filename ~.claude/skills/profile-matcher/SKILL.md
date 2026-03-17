---

name: profile-matcher

description: Fetches my LinkedIn profile, extracts skills/experience, then scores

&nbsp; each scraped post for relevance using Claude API. Returns enriched post dicts with match\_score.

---



\## Process Steps



1\. Fetch the profile at MY\_LINKEDIN\_URL using the same Playwright session (already authenticated).

2\. Extract from my profile: skills list, job titles, years of experience, industries, certifications.

3\. Build a "profile\_vector" string: concatenate all skills and titles.

4\. For each raw post:

&nbsp;  a. Send a Claude API call (claude-3-5-haiku for speed/cost) with this prompt:

&nbsp;     ```

&nbsp;     Given this freelance mission post: {post\_text}

&nbsp;     And this consultant profile: {profile\_vector}

&nbsp;     

&nbsp;     Extract and return JSON with:

&nbsp;     - mission\_title: string

&nbsp;     - required\_skills: list\[str]

&nbsp;     - duration: string (e.g. "3 months", "TJM 600€")

&nbsp;     - daily\_rate\_tjm: string or null

&nbsp;     - location: string

&nbsp;     - remote\_ok: bool

&nbsp;     - contact\_info: string or null

&nbsp;     - match\_score: float 0-100 (how well the profile matches)

&nbsp;     - match\_reasons: list\[str] (top 3 reasons for the score)

&nbsp;     - language: "FR" or "EN"

&nbsp;     ```

&nbsp;  b. Parse the JSON response. On parse failure, set match\_score=0 and log error.

5\. Filter: keep only posts with match\_score >= 40.

6\. Return: List\[Dict] enriched posts sorted by match\_score descending.



