#!/usr/bin/env python3
"""
prepare_db_for_deploy.py
────────────────────────
Creates a clean, upload-ready products.db by filtering out irrelevant data
(US Amazon products) and adding the image_url column if missing.

Run ONCE before uploading to Fly.io:
    cd /path/to/Database-of-high-quality-products
    python3 prepare_db_for_deploy.py

Output:  products_deploy.db  (upload this to Fly.io)
         snapshots.db is copied as-is (already clean)
"""

import sqlite3
import shutil
import os
import sys

# Canonical data root.  This used to point at a Claude local-agent-mode session
# sandbox under ~/Library/Application Support — a disposable temp folder that
# happened to hold the only current copy of the data.  Both databases now live
# in ~/QualityData/qualitydb, which is also what the server and the scheduler
# read (DB_PATH / SNAPSHOTS_DB_PATH).
DATA_ROOT = os.path.expanduser(os.environ.get("QUALITYDB_DATA_ROOT", "~/QualityData/qualitydb"))

SRC_DB  = os.environ.get("DB_PATH")           or os.path.join(DATA_ROOT, "products.db")
SRC_SNP = os.environ.get("SNAPSHOTS_DB_PATH") or os.path.join(DATA_ROOT, "snapshots.db")
OUT_DB  = os.path.join(os.path.dirname(__file__), "products_deploy.db")
OUT_SNP = os.path.join(os.path.dirname(__file__), "snapshots_deploy.db")

# Sources to KEEP (Central Europe + key EU markets).
# amazon_us (10k US products), amazon (mostly US via CZ domain) are excluded.
KEEP_SOURCES = {
    "alza", "alza.cz", "datart", "datart.cz",
    "planeo", "planeo.cz",
    "heureka", "heureka.cz", "heureka_sk",
    "zbozi", "zbozi.cz",
    "coolblue",
    "digitec",
    "prisjakt",
    "pricerunner", "pricerunner_se",
    "geizhals",
    "fnac",
    "ceneo",
    "mediamarkt",
    "saturn_de",
    "otto_de", "otto",
    "testberichte",
    "conrad",
    "amazon_de",          # German Amazon — EU-relevant
    "dtest",              # Czech testing lab (quality scores, kept)
    "warentest",          # German testing lab (quality scores, kept)
    "mall",
    "czc",
}

def main():
    if not os.path.exists(SRC_DB):
        print(f"ERROR: Source DB not found at:\n  {SRC_DB}")
        print("Is the localhost:8080 server running from that directory?")
        sys.exit(1)

    src_size = os.path.getsize(SRC_DB) / 1024**2
    print(f"Source DB:  {SRC_DB}")
    print(f"  Size:     {src_size:.0f} MB")

    # Count rows before filtering
    src = sqlite3.connect(SRC_DB)
    total_before = src.execute("SELECT count(*) FROM products").fetchone()[0]
    sources = src.execute("SELECT source, count(*) FROM products GROUP BY source ORDER BY count(*) DESC").fetchall()
    print(f"  Products: {total_before:,}")
    print(f"  Sources:  {len(sources)}")
    src.close()

    print(f"\nSources being EXCLUDED:")
    excluded = [s for s, n in sources if s not in KEEP_SOURCES]
    for s in excluded:
        n = next(n for src, n in sources if src == s)
        print(f"  {s:<20} {n:>6,} products")

    print(f"\nBuilding filtered DB → {OUT_DB}")
    if os.path.exists(OUT_DB):
        os.remove(OUT_DB)

    # Use SQLite's backup API for safe copy, then delete unwanted rows
    src = sqlite3.connect(SRC_DB)
    dst = sqlite3.connect(OUT_DB)
    src.backup(dst)
    src.close()

    # Add image_url column if missing
    cols = [r[1] for r in dst.execute("PRAGMA table_info(products)").fetchall()]
    if "image_url" not in cols:
        dst.execute("ALTER TABLE products ADD COLUMN image_url TEXT")
        print("  ✓ Added image_url column")
    if "brand" not in cols:
        dst.execute("ALTER TABLE products ADD COLUMN brand TEXT")
        print("  ✓ Added brand column")

    # Delete excluded sources
    placeholders = ",".join("?" * len(excluded))
    if excluded:
        dst.execute(f"DELETE FROM products WHERE source IN ({placeholders})", excluded)

    total_after = dst.execute("SELECT count(*) FROM products").fetchone()[0]
    dst.commit()
    dst.isolation_level = None   # autocommit mode required for VACUUM
    dst.execute("VACUUM")
    dst.close()

    out_size = os.path.getsize(OUT_DB) / 1024**2
    print(f"  Products kept: {total_after:,}  (removed {total_before - total_after:,})")
    print(f"  Output size:   {out_size:.0f} MB")

    # Copy snapshots DB.  Without this the deployed site loses every price
    # history strip and the has_history / price_drop filters return nothing,
    # so a missing source file is an error, not something to skip quietly.
    if os.path.exists(SRC_SNP):
        shutil.copy2(SRC_SNP, OUT_SNP)
        snp_size = os.path.getsize(OUT_SNP) / 1024**2
        _sc = sqlite3.connect(OUT_SNP)
        _rows, _last = _sc.execute(
            "SELECT COUNT(*), MAX(snapshot_date) FROM product_snapshots").fetchone()
        _sc.close()
        print(f"\nSnapshots DB copied → {OUT_SNP}  ({snp_size:.0f} MB, "
              f"{_rows:,} rows, latest {_last})")
    else:
        print(f"\nERROR: no snapshots DB at {SRC_SNP} — price history would be "
              f"empty on the deployed site.  Set SNAPSHOTS_DB_PATH or "
              f"QUALITYDB_DATA_ROOT and re-run.")
        sys.exit(1)

    print(f"""
Done! Upload to Fly.io with:

  fly sftp shell -a database-of-high-quality-products
  > put {OUT_DB} /data/products.db
  > put {OUT_SNP} /data/snapshots.db
  > exit

Or use the faster sftp pipe:
  fly ssh sftp get /data/products.db  products.db.old   # optional backup
  fly sftp shell -a database-of-high-quality-products <<'EOF'
  put {OUT_DB} /data/products.db
  put {OUT_SNP} /data/snapshots.db
  EOF
""")

if __name__ == "__main__":
    main()
