"""
QualityDB Scheduler — runs the Heureka scraper automatically every day.

Usage:
    python3 scraper/scheduler.py

Keep this running in the background (e.g. in a terminal, or as a
startup item). It will wake up once per day at the configured time
and run a full scrape, then go back to sleep.

To run manually right now without waiting:
    python3 scraper/heureka_scraper.py
"""

import time
import datetime
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraper.config import SCHEDULE_HOUR, SCHEDULE_MINUTE
from scraper.heureka_scraper import run_scraper

# German market scrapers
try:
    from scraper.amazon_de_scraper  import scrape_amazon_de
    from scraper.geizhals_scraper   import scrape_geizhals
    from scraper.otto_scraper       import scrape_otto
    DE_SCRAPERS_AVAILABLE = True
except ImportError as e:
    log_msg = f"DE scrapers not loaded: {e}"
    DE_SCRAPERS_AVAILABLE = False

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def seconds_until_next_run(hour: int, minute: int) -> float:
    """Return seconds until the next occurrence of HH:MM today or tomorrow."""
    now  = datetime.datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += datetime.timedelta(days=1)
    return (target - now).total_seconds()


def main():
    log.info(f"QualityDB Scheduler started.")
    log.info(f"Daily scrape scheduled at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}.")
    log.info("Press Ctrl+C to stop.\n")

    while True:
        wait = seconds_until_next_run(SCHEDULE_HOUR, SCHEDULE_MINUTE)
        next_run = datetime.datetime.now() + datetime.timedelta(seconds=wait)
        log.info(f"Next scrape run at {next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                 f"(in {wait/3600:.1f} hours)")

        try:
            time.sleep(wait)
        except KeyboardInterrupt:
            log.info("Scheduler stopped by user.")
            break

        log.info("Starting scheduled scrape run...")
        try:
            result = run_scraper()
            log.info(f"Scheduled run complete: {result['total_added']} new products added.")
        except Exception as e:
            log.error(f"CZ scrape run failed: {e}")

        # German market scrapers
        if DE_SCRAPERS_AVAILABLE:
            for name, fn in [
                ("Amazon.de",   scrape_amazon_de),
                ("Geizhals.de", scrape_geizhals),
                ("Otto.de",     scrape_otto),
            ]:
                log.info(f"Starting {name} scraper...")
                try:
                    fn()
                    log.info(f"{name} scraper complete.")
                except Exception as e:
                    log.error(f"{name} scraper failed: {e}")
        # Brief pause before calculating next sleep
        time.sleep(5)


if __name__ == "__main__":
    main()
