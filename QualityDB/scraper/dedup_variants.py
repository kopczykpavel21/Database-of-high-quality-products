#!/usr/bin/env python3
"""
Deduplicate Amazon US product variants.
For each group of products with the same name, keeps only the one
with the highest review count (ties broken by lowest rowid).
Also removes known junk-named products.

Run from the QualityDB folder:
    python3 scraper/dedup_variants.py
"""
import sqlite3
import os
import sys
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "products.db")


def dedup(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.execute(f"PRAGMA journal_mode={os.environ.get('JOURNAL_MODE', 'wal').upper()}")
    conn.execute("PRAGMA cache_size=-131072")   # 128 MB cache
    conn.execute("PRAGMA synchronous=NORMAL")

    print(f"DB: {db_path}")
    before = conn.execute("SELECT COUNT(*) FROM products WHERE source='amazon_us'").fetchone()[0]
    print(f"amazon_us rows before: {before:,}")

    # ── Step 1: junk names ────────────────────────────────────────────────────
    junk = ('EMPTY', 'Sold Out', 'Stickers', '', 'N/A', 'TBD')
    placeholders = ','.join('?' * len(junk))
    cur = conn.execute(
        f"DELETE FROM products WHERE source='amazon_us' AND Name IN ({placeholders})", junk
    )
    conn.commit()
    print(f"Junk names removed: {cur.rowcount:,}")

    # ── Step 2: collect all duplicate-name groups ─────────────────────────────
    print("Loading duplicate name groups (this may take a minute)...")
    # Use a temp table to avoid holding everything in Python RAM
    conn.execute("DROP TABLE IF EXISTS _dedup_keep")
    conn.execute("""
        CREATE TEMP TABLE _dedup_keep AS
        SELECT MIN(rowid) as keep_rowid
        FROM (
            SELECT rowid, Name, COALESCE(ReviewsCount, 0) as rc,
                   RANK() OVER (PARTITION BY Name ORDER BY COALESCE(ReviewsCount, 0) DESC) as rnk
            FROM products
            WHERE source = 'amazon_us'
            AND Name IN (
                SELECT Name FROM products WHERE source='amazon_us'
                GROUP BY Name HAVING COUNT(*) > 1
            )
        )
        WHERE rnk = 1
        GROUP BY Name
    """)
    conn.commit()
    keep_count = conn.execute("SELECT COUNT(*) FROM _dedup_keep").fetchone()[0]
    print(f"  Groups found (names with dupes): {keep_count:,}")

    # ── Step 3: delete the non-winners ───────────────────────────────────────
    print("Deleting duplicates...")
    cur = conn.execute("""
        DELETE FROM products
        WHERE source = 'amazon_us'
        AND Name IN (
            SELECT Name FROM products WHERE source='amazon_us'
            GROUP BY Name HAVING COUNT(*) > 1
        )
        AND rowid NOT IN (SELECT keep_rowid FROM _dedup_keep)
    """)
    conn.commit()
    print(f"Variants removed: {cur.rowcount:,}")

    after = conn.execute("SELECT COUNT(*) FROM products WHERE source='amazon_us'").fetchone()[0]
    dupes_left = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT Name FROM products WHERE source='amazon_us'
            GROUP BY Name HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    print(f"\namazon_us rows after:  {after:,}")
    print(f"Names still duplicated: {dupes_left:,}")
    print(f"Total removed: {before - after:,}")
    conn.close()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    dedup(path)
