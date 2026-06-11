#!/usr/bin/env python3
"""
scheduler_de_additions.py
──────────────────────────
German-scraper additions for scheduler.py.

HOW TO INTEGRATE
  1. Open your existing scraper/scheduler.py.
  2. At the top, after the existing scraper imports, add:

        from amazon_de_scraper  import scrape_amazon_de
        from otto_scraper       import scrape_otto
        from mediamarkt_scraper import scrape_mediamarkt
        from saturn_scraper     import scrape_saturn

  3. In the main run_all() / run_scrapers() function, after the CZ block, add:

        # ── German market ──────────────────────────────────────────────
        if ENABLE_AMAZON_DE:
            log("Starting Amazon.de scraper …")
            try:
                scrape_amazon_de(DB_PATH)
            except Exception as e:
                log(f"Amazon.de scraper failed: {e}")

        if ENABLE_OTTO_DE:
            log("Starting Otto.de scraper …")
            try:
                scrape_otto(DB_PATH)
            except Exception as e:
                log(f"Otto.de scraper failed: {e}")

        if ENABLE_MEDIAMARKT:
            log("Starting MediaMarkt.de scraper …")
            try:
                scrape_mediamarkt(DB_PATH)
            except Exception as e:
                log(f"MediaMarkt.de scraper failed: {e}")

        if ENABLE_SATURN:
            log("Starting Saturn.de scraper …")
            try:
                scrape_saturn(DB_PATH)
            except Exception as e:
                log(f"Saturn.de scraper failed: {e}")

  4. At the top of scheduler.py, import the flags:
        from config_de_additions import (
            ENABLE_AMAZON_DE, ENABLE_OTTO_DE, ENABLE_MEDIAMARKT, ENABLE_SATURN
        )
     Or add them directly to config.py (see config_de_additions.py).

──────────────────────────────────────────────────────────────────────────────
STANDALONE USAGE — run ALL German scrapers once (for testing):

  python3 scheduler_de_additions.py [/path/to/products.db]

──────────────────────────────────────────────────────────────────────────────
"""

import os
import sys
import time
import datetime

sys.path.insert(0, os.path.dirname(__file__))

from config import DB_PATH
from config_de_additions import (
    ENABLE_AMAZON_DE,
    ENABLE_OTTO_DE,
    ENABLE_MEDIAMARKT,
    ENABLE_SATURN,
)

from amazon_de_scraper  import scrape_amazon_de
from otto_scraper       import scrape_otto
from mediamarkt_scraper import scrape_mediamarkt
from saturn_scraper     import scrape_saturn


def log(msg):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def run_german_scrapers(db_path=None):
    if db_path is None:
        db_path = DB_PATH

    log("═══════════════════════════════════════════")
    log("  QualityDB — German market scrape started")
    log("═══════════════════════════════════════════")
    start = time.time()
    total_inserted = total_updated = 0

    scrapers = [
        ("Amazon.de",    ENABLE_AMAZON_DE,   scrape_amazon_de),
        ("Otto.de",      ENABLE_OTTO_DE,     scrape_otto),
        ("MediaMarkt.de",ENABLE_MEDIAMARKT,  scrape_mediamarkt),
        ("Saturn.de",    ENABLE_SATURN,      scrape_saturn),
    ]

    for name, enabled, fn in scrapers:
        if not enabled:
            log(f"  {name}: DISABLED — skipping")
            continue
        log(f"  {name}: starting …")
        try:
            ins, upd = fn(db_path)
            total_inserted += ins
            total_updated  += upd
            log(f"  {name}: {ins} inserted, {upd} updated")
        except Exception as exc:
            log(f"  {name}: ERROR — {exc}")
        time.sleep(3.0)  # brief pause between sources

    elapsed = time.time() - start
    log("───────────────────────────────────────────")
    log(f"  German scrape done in {elapsed:.0f}s")
    log(f"  Total inserted: {total_inserted:,}  updated: {total_updated:,}")
    log("═══════════════════════════════════════════")
    return total_inserted, total_updated


if __name__ == "__main__":
    db_path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run_german_scrapers(db_path_arg)
