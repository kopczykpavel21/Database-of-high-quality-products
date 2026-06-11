#!/usr/bin/env python3
"""
backfill_details.py — Fetch sub-ratings for warentest products missing details_json.

Queries the DB for all warentest products whose ProductURL contains '/detail/'
but whose details_json column is NULL or empty, then fetches each detail page
and writes the sub-ratings back to the DB.

Usage:
    python3 backfill_details.py              # backfill all missing
    python3 backfill_details.py --limit 100  # backfill first N products
    python3 backfill_details.py --dry-run    # count only, no fetching
"""

import json
import logging
import os
import sqlite3
import sys
import time
import random

# Reuse functions from the main scraper
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scraper"))
from warentest_scraper import (
    fetch, load_cookies, scrape_detail_page, grade_to_stars, grade_to_recommend
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("backfill")

QDB = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(QDB, "products.db")


def _sleep():
    time.sleep(random.uniform(1.2, 2.5))


def backfill(limit=None, dry_run=False):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Find warentest products with detail URLs but no details_json
    query = """
        SELECT rowid, Name, ProductURL, AvgStarRating
        FROM products
        WHERE source = 'warentest'
          AND ProductURL LIKE '%-detail/%'
          AND (details_json IS NULL OR details_json = '' OR details_json = '{}')
        ORDER BY rowid
    """
    if limit:
        query += f" LIMIT {int(limit)}"

    rows = conn.execute(query).fetchall()
    log.info(f"Found {len(rows)} products to backfill")

    if dry_run:
        conn.close()
        return

    cookies = load_cookies()
    if not cookies:
        log.warning("No cookies found — detail pages may be paywalled")

    enriched = 0
    failed = 0
    skipped = 0

    for i, row in enumerate(rows, 1):
        url = row["ProductURL"]
        name = row["Name"] or "(unnamed)"
        log.info(f"[{i}/{len(rows)}] {name[:50]} — {url}")

        detail = scrape_detail_page(url, cookies)
        if not detail:
            log.warning(f"  → No data returned")
            failed += 1
            _sleep()
            continue

        sub = detail.get("sub_ratings", {})
        if not sub:
            log.info(f"  → No sub-ratings found (may be paywalled)")
            skipped += 1
            _sleep()
            continue

        details_json = json.dumps(detail, ensure_ascii=False)
        log.info(f"  → {len(sub)} sub-ratings: {list(sub.keys())}")

        # Update details_json (and price if found)
        update_args = [details_json]
        price = detail.get("price")
        if price:
            conn.execute(
                "UPDATE products SET details_json=?, Price_EUR=COALESCE(Price_EUR, ?) WHERE rowid=?",
                (details_json, price, row["rowid"])
            )
        else:
            conn.execute(
                "UPDATE products SET details_json=? WHERE rowid=?",
                (details_json, row["rowid"])
            )

        # Write sub-ratings to dedicated table
        for key, val in sub.items():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO warentest_sub_ratings
                      (product_url, rating_key, rating_label, grade, stars)
                    VALUES (?,?,?,?,?)
                """, (url, key, val.get("label"), val.get("grade"), val.get("stars")))
            except Exception as e:
                log.warning(f"  sub_rating write error: {e}")

        # If we got an 'overall' grade and the star rating was approximate, update it
        overall = sub.get("overall")
        if overall and overall.get("grade"):
            new_stars = grade_to_stars(overall["grade"])
            new_rec = grade_to_recommend(overall["grade"])
            conn.execute(
                "UPDATE products SET AvgStarRating=?, RecommendRate_pct=? WHERE rowid=?",
                (new_stars, new_rec, row["rowid"])
            )

        conn.commit()
        enriched += 1
        _sleep()

    conn.close()
    log.info(f"\nDone: {enriched} enriched, {skipped} no sub-ratings, {failed} failed")


if __name__ == "__main__":
    limit = None
    dry_run = "--dry-run" in sys.argv

    for arg in sys.argv[1:]:
        if arg == "--limit" or arg.startswith("--limit="):
            if "=" in arg:
                limit = int(arg.split("=")[1])
            else:
                idx = sys.argv.index("--limit")
                if idx + 1 < len(sys.argv):
                    limit = int(sys.argv[idx + 1])

    backfill(limit=limit, dry_run=dry_run)
