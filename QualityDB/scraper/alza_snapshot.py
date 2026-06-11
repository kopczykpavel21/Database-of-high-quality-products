"""
alza_snapshot.py — Daily snapshot recorder for alza.cz products.

Alza.cz has the richest data (ReturnRate_pct, full review counts) but no
dedicated live re-scraper. This module reads the current alza rows from
products.db and writes a timestamped snapshot to snapshots.db so that
the 📈 history badge and chart work for those products.

Run via scheduler (daily) or directly:
    python3 -m scraper.alza_snapshot
"""

import os
import sqlite3
import logging
from datetime import date, timedelta

log = logging.getLogger(__name__)

SOURCE = "alza.cz"
COUNTRY = "CZ"


def run_alza_snapshot(db_path: str | None = None, seed_days: int = 0) -> int:
    """
    Write today's snapshot for every alza.cz product in products.db.

    Args:
        db_path:   Path to products.db. Defaults to $DB_PATH env var or /data/products.db.
        seed_days: If > 0, also insert a synthetic 'seed' snapshot dated `seed_days` days
                   ago using the same current data. Used once to bootstrap ≥2 snapshots
                   immediately so badges start appearing right away.
                   Pass 0 (default) for normal daily use.

    Returns:
        Number of snapshot rows written.
    """
    if db_path is None:
        db_path = os.environ.get("DB_PATH", "/data/products.db")

    if not os.path.exists(db_path):
        log.warning(f"alza_snapshot: products.db not found at {db_path}")
        return 0

    try:
        from scraper.snapshots import ensure_snapshot_table, record_snapshot, SNAPSHOTS_DB_PATH
    except ImportError:
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from scraper.snapshots import ensure_snapshot_table, record_snapshot, SNAPSHOTS_DB_PATH

    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row

    ensure_snapshot_table(conn)

    rows = conn.execute("""
        SELECT ProductURL, RecommendRate_pct, ReviewsCount,
               AvgStarRating, Price_CZK, Price_EUR
        FROM   products
        WHERE  source = 'alza.cz'
          AND  ProductURL IS NOT NULL AND ProductURL != ''
    """).fetchall()
    conn.close()

    if not rows:
        log.info("alza_snapshot: no alza.cz products found")
        return 0

    today_str = date.today().isoformat()
    written = 0

    for r in rows:
        product = {
            "ProductURL":        r["ProductURL"],
            "RecommendRate_pct": r["RecommendRate_pct"],
            "ReviewsCount":      r["ReviewsCount"],
            "AvgStarRating":     r["AvgStarRating"],
            "Price_CZK":         r["Price_CZK"],
            "Price_EUR":         r["Price_EUR"],
        }
        record_snapshot(conn, r["ProductURL"], SOURCE, product, country=COUNTRY)
        written += 1

    # One-time seed: insert a historical row dated `seed_days` ago so we
    # immediately have ≥2 snapshots and badges light up on first deploy.
    if seed_days > 0:
        seed_date = (date.today() - timedelta(days=seed_days)).isoformat()
        snap_conn = sqlite3.connect(SNAPSHOTS_DB_PATH, timeout=30)
        try:
            snap_conn.executemany(
                """
                INSERT OR IGNORE INTO product_snapshots
                    (product_url, source, country, snapshot_date,
                     recommend_pct, review_count, avg_star_rating, price_czk, price_eur)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        r["ProductURL"], SOURCE, COUNTRY, seed_date,
                        r["RecommendRate_pct"], r["ReviewsCount"],
                        r["AvgStarRating"], r["Price_CZK"], r["Price_EUR"],
                    )
                    for r in rows
                ],
            )
            snap_conn.commit()
            log.info(f"alza_snapshot: seeded {len(rows)} historical rows dated {seed_date}")
            written += len(rows)
        finally:
            snap_conn.close()

    log.info(f"alza_snapshot: wrote {written} rows for {len(rows)} alza products")
    return written


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    n = run_alza_snapshot(seed_days=7)
    print(f"Done — {n} rows written")
