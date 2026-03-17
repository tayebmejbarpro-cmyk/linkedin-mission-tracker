---

name: sheets-writer

description: Writes enriched freelance mission posts to Google Sheets. Handles

&nbsp; deduplication and daily appending without overwriting historical data.

---



\## Process Steps



1\. Authenticate with Google Sheets API using service account from GOOGLE\_SERVICE\_ACCOUNT\_JSON env var.

2\. Open spreadsheet by SPREADSHEET\_ID.

3\. Check or create sheet tab named "Missions\_{YYYY-MM}" for current month.

4\. Read existing post\_urls from column A to build a dedup set.

5\. For each enriched post not already in the sheet, append a new row with these columns:

&nbsp;  | date\_found | post\_url | author\_name | author\_title | author\_profile\_url |

&nbsp;  | mission\_title | required\_skills | duration | daily\_rate\_tjm | location |

&nbsp;  | remote\_ok | contact\_info | match\_score | match\_reasons | post\_text | country |

6\. Apply conditional formatting: match\_score >= 80 → green, 50–79 → yellow, < 50 → red.

7\. Log: "X new missions added, Y duplicates skipped."



