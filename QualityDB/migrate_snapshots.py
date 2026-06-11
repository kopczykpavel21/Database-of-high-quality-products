#!/usr/bin/env python3
"""
migrate_snapshots.py — Run this ONCE, locally on your Mac.

What it does:
  1. Copies product_snapshots from products.db → snapshots.db  (your dissertation data)
  2. Drops the table from products.db
  3. VACUUMs products.db (shrinks it from ~1.2 GB to ~600 MB)

Run from inside the QualityDB directory:
    cd path/to/QualityDB
    python3 migrate_snapshots.py

Takes ~2-5 minutes on an SSD Mac.  Do NOT run while server.py or any scraper is running.
"""
import sqlite3, os, sys, time

HERE    = os.path.dirname(os.path.abspath(__file__))
SRC     = os.path.join(HERE, "products.db")
DST     = os.path.join(HERE, "snapshots.db")

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

def main():
    if not os.path.exists(SRC):
        sys.exit(f"ERROR: {SRC} not found")
    if os.path.exists(DST):
        sys.exit(f"ERROR: {DST} already exists — delete it first if you want to re-run")

    src_mb = os.path.getsize(SRC) / 1024 / 1024
    log(f"Source: products.db  ({src_mb:.0f} MB)")

    # ── Step 1: Create snapshots.db and copy the table ─────────────────────────
    log("Creating snapshots.db ...")
    dst = sqlite3.connect(DST)
    dst.execute("PRAGMA journal_mode=DELETE")
    dst.execute("PRAGMA synchronous=NORMAL")
    dst.execute("PRAGMA cache_size=-64000")  # 64 MB page cache
    dst.execute("""
        CREATE TABLE product_snapshots (
            id             INTEGER PRIMARY KEY,
            product_url    TEXT NOT NULL,
            source         TEXT NOT NULL,
            country        TEXT NOT NULL DEFAULT '',
            snapshot_date  TEXT NOT NULL,
            recommend_pct  REAL,
            review_count   INTEGER,
            avg_star_rating REAL,
            price_czk      REAL,
            price_eur      REAL
        )
    """)
    # Indexes useful for longitudinal analysis
    dst.execute("CREATE INDEX idx_snap_url    ON product_snapshots(product_url)")
    dst.execute("CREATE INDEX idx_snap_date   ON product_snapshots(snapshot_date)")
    dst.execute("CREATE INDEX idx_snap_source ON product_snapshots(source, snapshot_date)")
    dst.commit()

    log("Copying rows (this may take 1-3 min) ...")
    src = sqlite3.connect(SRC)
    src.execute("PRAGMA journal_mode=DELETE")
    src.execute("PRAGMA cache_size=-64000")

    BATCH = 50_000
    offset = 0
    total_copied = 0
    while True:
        rows = src.execute(
            "SELECT * FROM product_snapshots LIMIT ? OFFSET ?", (BATCH, offset)
        ).fetchall()
        if not rows:
            break
        dst.executemany(
            "INSERT INTO product_snapshots VALUES (?,?,?,?,?,?,?,?,?,?)", rows
        )
        dst.commit()
        total_copied += len(rows)
        offset       += BATCH
        log(f"  ... {total_copied:,} rows copied")

    dst.close()
    dst_mb = os.path.getsize(DST) / 1024 / 1024
    log(f"snapshots.db created ({dst_mb:.0f} MB, {total_copied:,} rows)")

    # ── Step 2: Verify row counts match ────────────────────────────────────────
    src_count  = src.execute("SELECT COUNT(*) FROM product_snapshots").fetchone()[0]
    dst2       = sqlite3.connect(DST)
    dst_count  = dst2.execute("SELECT COUNT(*) FROM product_snapshots").fetchone()[0]
    dst2.close()
    if src_count != dst_count:
        sys.exit(f"ERROR: row count mismatch — source {src_count}, dest {dst_count}. NOT dropping source table.")
    log(f"Verified: {dst_count:,} rows in both files ✓")

    # ── Step 3: Drop from products.db ──────────────────────────────────────────
    log("Dropping product_snapshots from products.db ...")
    src.execute("DROP TABLE product_snapshots")
    src.commit()
    src.close()

    # ── Step 4: VACUUM products.db (reclaims disk space) ───────────────────────
    log("Vacuuming products.db (this may take 1-3 min) ...")
    src_vac = sqlite3.connect(SRC, isolation_level=None)   # autocommit required for VACUUM
    src_vac.execute("PRAGMA journal_mode=DELETE")
    src_vac.execute("VACUUM")
    src_vac.close()

    final_mb = os.path.getsize(SRC) / 1024 / 1024
    log(f"Done!  products.db is now {final_mb:.0f} MB  (was {src_mb:.0f} MB)")
    log(f"       snapshots.db is {dst_mb:.0f} MB  — keep this for your dissertation research")
    log("")
    log("Next: restart server.py so it no longer references product_snapshots.")

if __name__ == "__main__":
    main()
