"""
snapshots.py — Longitudinal tracking for QualityDB ODA (Obsolescence Detection Algorithm).

Every time a scraper runs (daily/monthly), it calls record_snapshot() for every
product it sees — whether new or existing. This builds up a time-series table
that lets us track how ratings, review counts, and prices change over time.

Usage in any scraper (unchanged from before):
    from scraper.snapshots import ensure_snapshot_table, record_snapshot
    ensure_snapshot_table(conn)                      # conn is accepted but ignored
    record_snapshot(conn, url, source, product_dict) # writes to snapshots.db

All snapshot data is written to a SEPARATE snapshots.db file (in the QualityDB
root folder) rather than products.db. This keeps the production database lean
and gives you a dedicated longitudinal research file.
"""

import os
import sqlite3
import logging
import threading
from typing import Optional

log = logging.getLogger(__name__)

# ── Snapshots DB path ─────────────────────────────────────────────────────────
# Default: QualityDB/snapshots.db  (parent of the scraper/ folder)
# Override with SNAPSHOTS_DB_PATH env var if needed.
_DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "..", "snapshots.db")
SNAPSHOTS_DB_PATH = os.environ.get("SNAPSHOTS_DB_PATH", os.path.abspath(_DEFAULT_PATH))

# Module-level connection — opened once, reused across all scraper calls.
_snap_conn: Optional[sqlite3.Connection] = None
_snap_lock = threading.Lock()


def _get_snap_conn() -> sqlite3.Connection:
    """Return (creating if needed) the module-level snapshots.db connection."""
    global _snap_conn
    with _snap_lock:
        if _snap_conn is None:
            _snap_conn = sqlite3.connect(SNAPSHOTS_DB_PATH, check_same_thread=False, timeout=30)
            _snap_conn.row_factory = sqlite3.Row
            _snap_conn.execute("PRAGMA journal_mode=WAL")
            _snap_conn.execute("PRAGMA synchronous=NORMAL")
            _snap_conn.execute("PRAGMA cache_size=-16000")
            _create_table(_snap_conn)
        return _snap_conn


def _create_table(conn: sqlite3.Connection) -> None:
    """Create snapshots table and indexes if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS product_snapshots (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            product_url       TEXT    NOT NULL,
            source            TEXT    NOT NULL,
            country           TEXT    NOT NULL DEFAULT '',
            snapshot_date     TEXT    NOT NULL,         -- 'YYYY-MM-DD'
            recommend_pct     REAL,                     -- RecommendRate_pct at time of snapshot
            review_count      INTEGER,                  -- ReviewsCount at time of snapshot
            avg_star_rating   REAL,                     -- AvgStarRating if available
            price_czk         REAL,                     -- Price_CZK if available
            price_eur         REAL,                     -- Price_EUR if available
            -- One snapshot per product per source per day; second run same day is a no-op.
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


# ── Public API (signatures unchanged — scrapers need zero edits) ──────────────

def ensure_snapshot_table(conn: sqlite3.Connection) -> None:
    """
    Ensure the snapshot table exists in snapshots.db.

    The `conn` parameter (products.db connection) is accepted for backward
    compatibility but not used — all writes go to the dedicated snapshots.db.
    """
    _get_snap_conn()   # triggers table creation if needed


def record_snapshot(
    conn: sqlite3.Connection,
    product_url: str,
    source: str,
    product: dict,
    country: str = "",
) -> None:
    """
    Write one timestamped snapshot row for a product into snapshots.db.

    The `conn` parameter (products.db connection) is accepted for backward
    compatibility but ignored — the snapshot goes to the dedicated snapshots.db.

    Called for EVERY product the scraper sees on each run. One row per
    (product_url, source, date) — if the scraper runs twice in a day the
    second call is silently ignored (INSERT OR IGNORE).
    """
    if not product_url:
        return

    sc = _get_snap_conn()
    try:
        sc.execute(
            """
            INSERT OR IGNORE INTO product_snapshots
                (product_url, source, country, snapshot_date,
                 recommend_pct, review_count, avg_star_rating,
                 price_czk, price_eur)
            VALUES
                (?, ?, ?, date('now'),
                 ?, ?, ?,
                 ?, ?)
            """,
            (
                product_url,
                source,
                country,
                product.get("RecommendRate_pct"),
                product.get("ReviewsCount"),
                product.get("AvgStarRating"),
                product.get("Price_CZK"),
                product.get("Price_EUR"),
            ),
        )
        sc.commit()
    except sqlite3.Error as e:
        log.warning(f"[snapshots] Failed to record snapshot for {product_url}: {e}")


def get_history(conn: sqlite3.Connection, product_url: str) -> list:
    """
    Return the full time-series history for one product URL, oldest first.

    The `conn` parameter is accepted for backward compatibility but ignored.

    Returns a list of dicts:
        [{'snapshot_date': '2025-01-01', 'recommend_pct': 94.0,
          'review_count': 120, 'avg_star_rating': 4.7, ...}, ...]
    """
    sc = _get_snap_conn()
    rows = sc.execute(
        """
        SELECT snapshot_date, recommend_pct, review_count,
               avg_star_rating, price_czk, price_eur
        FROM   product_snapshots
        WHERE  product_url = ?
        ORDER  BY snapshot_date ASC
        """,
        (product_url,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_brand_history(conn: sqlite3.Connection, brand_name: str) -> list:
    """
    Return aggregated daily snapshots for all products matching brand_name,
    joined against products.db for the Name lookup.

    The `conn` parameter here IS used — it should be an open connection to
    products.db (as before), so the JOIN with the products table still works.
    Cross-database join is done via SQLite ATTACH.
    """
    sc = _get_snap_conn()
    try:
        sc.execute(f"ATTACH DATABASE ? AS prod", (
            os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "products.db")),
        ))
        rows = sc.execute(
            """
            SELECT  ps.snapshot_date,
                    AVG(ps.recommend_pct)   AS avg_recommend_pct,
                    SUM(ps.review_count)    AS total_reviews,
                    COUNT(*)                AS product_count
            FROM    product_snapshots ps
            JOIN    prod.products p ON p.ProductURL = ps.product_url
            WHERE   lower(p.Name) LIKE lower(?)
            GROUP   BY ps.snapshot_date
            ORDER   BY ps.snapshot_date ASC
            """,
            (f"{brand_name}%",),
        ).fetchall()
        sc.execute("DETACH DATABASE prod")
        return [dict(r) for r in rows]
    except sqlite3.Error as e:
        log.warning(f"[snapshots] get_brand_history failed: {e}")
        return []
