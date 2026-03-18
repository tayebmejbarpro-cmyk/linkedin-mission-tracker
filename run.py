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


def main() -> None:
    """
    Run the full LinkedIn Freelance Mission Tracker pipeline.

    Steps:
      1. Setup logging
      2. Load .env (no-op in GitHub Actions where secrets are injected natively)
      3. Load and validate config (raises on missing env vars → exit 1)
      4. Scrape posts via Apify
      5. Score posts via Claude API
      6. Write to Google Sheets
      7. Log final summary

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
        from scraper import scrape_all_countries
        from matcher import score_posts
        from sheets import write_missions, sync_config_tab

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

        # Step 2 — Scrape
        logger.info("[run] Starting Apify scraping...")
        raw_posts = scrape_all_countries(config, logger)
        logger.info("[run] Scraping complete — %d raw posts collected.", len(raw_posts))

        if not raw_posts:
            logger.warning("[run] No posts scraped. Check Apify actor and keyword config.")

        # Step 3 — Score
        logger.info("[run] Starting Claude scoring...")
        enriched_posts = score_posts(raw_posts, config, logger)
        logger.info(
            "[run] Scoring complete — %d posts scored >= %d.",
            len(enriched_posts), config.min_match_score,
        )

        # Step 4 — Write to Sheets
        logger.info("[run] Writing to Google Sheets...")
        write_missions(enriched_posts, config, logger)

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
