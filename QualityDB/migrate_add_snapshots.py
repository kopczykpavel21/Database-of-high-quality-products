"""
migrate_add_snapshots.py — One-time migration to add the product_snapshots table.

Run this ONCE from the QualityDB root directory:
    python3 migrate_add_snapshots.py

It is completely safe to run multiple times — it uses CREATE TABLE IF NOT EXISTS
so nothing is overwritten if the table already exists.

After running this, every future scraper run will automatically start filling
the snapshot table. You don't need to backfill historical data — the longitudinal
panel begins from today.
"""

import sqlite3
import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "products.db")

# Also support the Fly.io volume path via env var
DB_PATH = os.environ.get("DB_PATH", DB_PATH)


def run_migration():
    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}")
        print("Make sure you run this from the QualityDB root directory.")
        sys.exit(1)

    print(f"Connecting to: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute(f"PRAGMA journal_mode={os.environ.get('JOURNAL_MODE', 'wal').upper()}")

    print("Creating product_snapshots table...")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS product_snapshots (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            product_url       TEXT    NOT NULL,
            source            TEXT    NOT NULL,
            country           TEXT    NOT NULL DEFAULT '',
            snapshot_date     TEXT    NOT NULL,
            recommend_pct     REAL,
            review_count      INTEGER,
            avg_star_rating   REAL,
            price_czk         REAL,
            price_eur         REAL,
            UNIQUE(product_url, source, snapshot_date)
        );

        CREATE INDEX IF NOT EXISTS idx_snap_url
            ON product_snapshots(product_url);

        CREATE INDEX IF NOT EXISTS idx_snap_date
            ON product_snapshots(snapshot_date);

        CREATE INDEX IF NOT EXISTS idx_snap_source_date
            ON product_snapshots(source, snapshot_date);
    """)
    conn.commit()

    # Verify
    count = conn.execute("SELECT COUNT(*) FROM product_snapshots").fetchone()[0]
    print(f"Migration complete. product_snapshots table exists with {count} rows.")

    # Backfill: take today's values from the main products table as the first snapshot.
    # This gives you a baseline row dated today, so you're not starting from nothing.
    print("Backfilling today's snapshot from existing products table...")
    conn.execute("""
        INSERT OR IGNORE INTO product_snapshots
            (product_url, source, country, snapshot_date,
             recommend_pct, review_count, avg_star_rating,
             price_czk, price_eur)
        SELECT
            ProductURL,
            COALESCE(source, 'unknown'),
            COALESCE(country, ''),
            date('now'),
            RecommendRate_pct,
            ReviewsCount,
            AvgStarRating,
            Price_CZK,
            Price_EUR
        FROM products
        WHERE ProductURL IS NOT NULL
          AND ProductURL != ''
    """)
    conn.commit()

    backfilled = conn.execute("SELECT COUNT(*) FROM product_snapshots").fetchone()[0]
    print(f"Backfill complete. {backfilled} snapshot rows inserted for today.")
    print()
    print("Next steps:")
    print("  1. Run your scrapers as normal — they will now record snapshots automatically.")
    print("  2. Each monthly run adds one new row per product to product_snapshots.")
    print("  3. After 6+ months you have enough data for trend analysis.")
    conn.close()


if __name__ == "__main__":
    run_migration()
