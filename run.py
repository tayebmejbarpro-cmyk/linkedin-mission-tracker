"""
run.py — Entry point for the LinkedIn Freelance Mission Tracker.

Orchestrates the full pipeline:
  1. Load config (fail fast if env vars missing)
  2. Scrape LinkedIn posts via Apify
  3. Score posts via Claude API
  4. Write results to Google Sheets

Invoked daily by GitHub Actions at 06:00 UTC.
Can also be run locally: `python run.py`
"""

import logging
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv


def setup_logging(date_str: str) -> logging.Logger:
    """
    Configure a logger that writes to both a daily log file and stdout.

    File handler: DEBUG level → logs/run_{date_str}.log
    Stream handler: INFO level → stdout

    Creates the logs/ directory if it does not exist.

    Args:
        date_str: Date string in YYYY-MM-DD format.

    Returns:
        Configured Logger instance.
    """
    logs_dir = Path(__file__).parent / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    log_path = logs_dir / f"run_{date_str}.log"

    logger = logging.getLogger("freelance_tracker")
    logger.setLevel(logging.DEBUG)

    # Avoid adding duplicate handlers on re-import
    if logger.handlers:
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger


# Known locations for each target country (EN + FR names + major cities)
_LOCATION_MAP = {
    "France": [
        "france", "paris", "lyon", "marseille", "toulouse", "nice", "nantes",
        "bordeaux", "strasbourg", "lille", "montpellier", "rennes", "reims",
        "le havre", "saint-étienne", "toulon", "grenoble", "dijon", "angers",
        "nîmes", "brest", "tours", "amiens", "limoges", "clermont-ferrand",
        "aix-en-provence", "villeurbanne", "metz", "besançon", "caen", "orléans",
        "île-de-france", "idf", "hauts-de-france", "paca",
    ],
    "Morocco": [
        "maroc", "morocco", "casablanca", "rabat", "marrakech", "fès", "tanger",
        "agadir", "meknès", "oujda", "kénitra", "tétouan", "safi", "el jadida",
        "nador", "taza", "settat", "berrechid",
    ],
}
_REMOTE_KEYWORDS = {"remote", "télétravail", "à distance", "full remote", "hybrid", "hybride"}


def _passes_location_filter(post: dict, target_countries: list) -> bool:
    """
    Return True if post location is unknown, remote, or in a target country/city.

    Always keeps posts with no location (can't rule them out from snippet alone).
    Remote/hybrid posts pass regardless of country.

    Args:
        post: Enriched post dict with optional 'location' field.
        target_countries: List of country names from config (e.g. ["France", "Morocco"]).

    Returns:
        True if the post should be kept, False if it should be filtered out.
    """
    location = post.get("location", "").strip().lower()
    if not location:
        return True
    if any(kw in location for kw in _REMOTE_KEYWORDS):
        return True
    for country in target_countries:
        known = _LOCATION_MAP.get(country, [country.lower()])
        if any(city in location for city in known):
            return True
    return False


def main() -> None:
    """
    Run the full LinkedIn Freelance Mission Tracker pipeline.

    Steps:
      1. Setup logging
      2. Load .env (no-op in GitHub Actions where secrets are injected natively)
      3. Load and validate config (raises on missing env vars → exit 1)
      4. Load cross-run dedup index from Google Sheet
      5. Scrape posts via Apify (known posts filtered before scoring)
      6. Score posts via Claude API
      7. Write to Google Sheets + update Dedup_Index
      8. Log final summary

    On any unhandled exception: logs full traceback and exits with code 1
    so GitHub Actions marks the run as failed.
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger = setup_logging(date_str)

    logger.info("=" * 60)
    logger.info("LinkedIn Freelance Mission Tracker — run started")
    logger.info("Date: %s UTC", date_str)
    logger.info("=" * 60)

    # Load .env for local development (no-op when env vars are already set)
    load_dotenv()

    try:
        # Import here so missing deps surface with a clear error after logging is set up
        from config.config import load_config
        from scraper import scrape_all_countries, scrape_google
        from matcher import score_posts, fetch_profile_vectors
        from sheets import write_missions, sync_config_tab, load_profile_vectors, save_profile_vectors, load_feedback_examples, load_seen_posts_all_tabs

        # Step 1 — Config (fail fast)
        logger.info("[run] Loading configuration...")
        config = load_config()

        # Step 1b — Override config with Paramètres tab values (if tab exists)
        logger.info("[run] Syncing config with Google Sheets 'Paramètres' tab...")
        config = sync_config_tab(config, logger)

        logger.info(
            "[run] Config ready. Countries: %s | Keywords: %d | Min score: %d",
            config.target_countries,
            len(config.search_keywords),
            config.min_match_score,
        )

        # Step 1c — Load cached profile vectors from sheet; fetch missing via Apify
        logger.info("[run] Loading profile vectors (from cache or Apify)...")
        cached_vectors = load_profile_vectors(config, logger)
        profile_vectors = fetch_profile_vectors(config, logger, cached=cached_vectors)

        # Save any newly fetched vectors back to the sheet for future runs
        new_vectors = {url: info for url, info in profile_vectors.items() if url not in cached_vectors}
        if new_vectors:
            logger.info("[run] Saving %d new profile vector(s) to sheet cache...", len(new_vectors))
            save_profile_vectors(profile_vectors, config, logger)
        else:
            logger.info("[run] All profile vectors loaded from cache — no Apify call needed.")

        # Step 2 — Load cross-run dedup index from Google Sheet
        logger.info("[run] Loading dedup index (cross-run deduplication)...")
        seen_urls_global, seen_hashes_global = load_seen_posts_all_tabs(config, logger)
        logger.info(
            "[run] Dedup index ready — %d known URLs, %d known text hashes.",
            len(seen_urls_global), len(seen_hashes_global),
        )

        # Step 3 — Scrape via Apify (cross-run known posts filtered out before scoring)
        logger.info("[run] Starting Apify scraping...")
        raw_posts = scrape_all_countries(config, logger, seen_urls=seen_urls_global, seen_hashes=seen_hashes_global)
        logger.info("[run] Apify scraping complete — %d raw posts collected.", len(raw_posts))

        # Step 3b — Google CSE fallback scraper (opt-in, enabled by GOOGLE_CSE_API_KEY + GOOGLE_CSE_ID)
        if config.google_cse_api_key and config.google_cse_id:
            logger.info("[run] Starting Google CSE scraping...")
            google_posts = scrape_google(config, logger, seen_urls=seen_urls_global, seen_hashes=seen_hashes_global)
            raw_posts.extend(google_posts)
            logger.info("[run] Google CSE complete — %d additional posts.", len(google_posts))
        else:
            logger.info("[run] Google CSE scraper disabled (GOOGLE_CSE_API_KEY or GOOGLE_CSE_ID not set).")

        logger.info("[run] Scraping complete — %d total raw posts collected.", len(raw_posts))

        if not raw_posts:
            logger.warning("[run] No posts scraped. Check Apify actor and keyword config.")

        # Step 3 — Score (load past user feedback to inject into Claude prompt)
        logger.info("[run] Loading user feedback examples from sheet...")
        feedback_examples = load_feedback_examples(config, logger)
        if feedback_examples:
            logger.info("[run] %d feedback example(s) will guide scoring.", len(feedback_examples))

        logger.info("[run] Starting Claude scoring...")
        enriched_posts = score_posts(
            raw_posts, config, logger,
            profile_vectors=profile_vectors,
            feedback_examples=feedback_examples,
        )
        logger.info(
            "[run] Scoring complete — %d posts scored >= %d.",
            len(enriched_posts), config.min_match_score,
        )

        # Step 3c — Location filter: keep only posts in target countries (or unknown/remote)
        before_loc = len(enriched_posts)
        enriched_posts = [p for p in enriched_posts if _passes_location_filter(p, config.target_countries)]
        logger.info(
            "[run] Location filter: %d/%d posts kept (target countries: %s).",
            len(enriched_posts), before_loc, config.target_countries,
        )

        # Step 4 — Write to Sheets (seen sets passed for belt-and-suspenders dedup)
        logger.info("[run] Writing to Google Sheets...")
        write_missions(enriched_posts, config, logger, seen_urls=seen_urls_global, seen_hashes=seen_hashes_global)

        # Final summary
        logger.info("=" * 60)
        logger.info(
            "[run] Pipeline complete. %d missions written to Google Sheets.",
            len(enriched_posts),
        )
        logger.info("=" * 60)

    except EnvironmentError as exc:
        logger.critical("[run] Configuration error — cannot proceed: %s", exc)
        sys.exit(1)

    except Exception as exc:  # noqa: BLE001
        logger.critical("[run] Unhandled exception: %s", exc)
        logger.critical(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
