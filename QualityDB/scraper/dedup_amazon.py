"""
Remove duplicate amazon_us products — same ASIN appearing more than once.
Keeps the row with the highest ReviewsCount; deletes the rest.

Products share the same ASIN when ProductURL is identical
(URL = https://www.amazon.com/dp/{ASIN}).

Run:
  python3 scraper/dedup_amazon.py
  python3 scraper/dedup_amazon.py --dry-run
"""

import argparse
import logging
import os
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "products.db")


def main():
    parser = argparse.ArgumentParser(description="Deduplicate amazon_us products by ProductURL")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without deleting")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")

    # Count duplicates
    dup_count = conn.execute("""
        SELECT COUNT(*) FROM products
        WHERE source='amazon_us'
          AND ProductURL IN (
              SELECT ProductURL FROM products
              WHERE source='amazon_us'
              GROUP BY ProductURL HAVING COUNT(*) > 1
          )
    """).fetchone()[0]

    url_groups = conn.execute("""
        SELECT ProductURL, COUNT(*) n FROM products
        WHERE source='amazon_us'
        GROUP BY ProductURL HAVING n > 1
    """).fetchall()

    total_dupes = sum(n - 1 for _, n in url_groups)  # rows to delete

    log.info(f"Duplicate ProductURLs found: {len(url_groups):,}")
    log.info(f"Total rows to delete (keeping best per URL): {total_dupes:,}")

    if total_dupes == 0:
        log.info("No duplicates — nothing to do.")
        conn.close()
        return

    # Show a sample of duplicates
    log.info("\nSample duplicate groups:")
    for url, n in url_groups[:5]:
        log.info(f"  {url}  ({n} copies)")
        for row in conn.execute(
            "SELECT id, Name, ReviewsCount, Category FROM products WHERE ProductURL=? AND source='amazon_us'",
            (url,)
        ):
            log.info(f"    id={row[0]}  reviews={row[2]}  cat={row[3]}  {str(row[1])[:60]}")

    if args.dry_run:
        log.info(f"\nDRY RUN — would delete {total_dupes:,} rows.")
        conn.close()
        return

    # Delete: for each duplicated URL, keep the row with max ReviewsCount (highest id as tiebreak)
    log.info("\nDeleting duplicates...")
    cur = conn.execute("""
        DELETE FROM products
        WHERE source='amazon_us'
          AND id NOT IN (
              SELECT MAX(id) FROM (
                  SELECT id,
                         ROW_NUMBER() OVER (
                             PARTITION BY ProductURL
                             ORDER BY COALESCE(ReviewsCount, 0) DESC, id DESC
                         ) AS rn
                  FROM products
                  WHERE source='amazon_us'
              ) ranked
              WHERE rn = 1
          )
    """)
    conn.commit()
    log.info(f"Deleted {cur.rowcount:,} duplicate rows.")

    remaining = conn.execute("SELECT COUNT(*) FROM products WHERE source='amazon_us'").fetchone()[0]
    log.info(f"amazon_us products remaining: {remaining:,}")

    conn.close()


if __name__ == "__main__":
    main()
