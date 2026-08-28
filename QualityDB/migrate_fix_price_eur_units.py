#!/usr/bin/env python3
"""
migrate_fix_price_eur_units.py
──────────────────────────────
Repair the EUR price scale for the German retail sources in products.db.

WHAT WAS WRONG
--------------
Two different power-of-ten slips in the old parse_eur() implementations wrote
Price_EUR at the wrong scale.  Both are now fixed in the scrapers; this script
repairs the rows those versions already wrote.

  1. int read as cents.  MediaMarkt/Saturn JSON-LD gives `offers.price` as a
     plain int when the price is a whole number of euros (399, 279, 19).  The
     old code divided ints by 100:  EUR 399  ->  3.99

  2. every dot stripped.  When the same field is a decimal (579.99) or a
     string ("439.00"), the old code stripped all dots before float():
       "579.99" -> 57999      (x100, two decimals)
       "107.2"  -> 1072       (x10,  one decimal)
     Otto always emits a 2-decimal string, so Otto is uniformly x100.

Verified against live pages on 2026-08-26 -- see `test_price_units.py` for the
captured payload shapes.

HOW ROWS ARE REPAIRED
---------------------
  otto, otto_de      factor 0.01 for every row.  Otto's offers.price is always
                     a 2-decimal string, so the dot-strip is always x100.
                     Spot-checked live: stored 1505 -> EUR 15.05 vs EUR 15.10
                     live; 79900 -> EUR 799.00 vs EUR 799.00 live.

  mediamarkt,        resolved per row against the live product page: the factor
  saturn_de          in {0.01, 0.1, 1, 10, 100} whose result best matches the
                     live JSON-LD price.  These two sources mix corrupt and
                     already-correct rows, so a blanket rule is not safe.
                     Rows whose page is gone fall back to a cohort band built
                     from the live-resolved rows of the same source+category,
                     and are marked so they can be told apart.

SAFETY
------
  * Backs the database up before touching it.
  * Writes ONLY Price_EUR plus five new audit columns.  No other column,
    row or table is read-modify-written.
  * Idempotent: a row with price_eur_fix_date set is skipped, so re-running
    is a no-op.  (Contrast fix_otto_prices.py, which divides by 100 every
    time it runs -- do not use that script.)
  * A row whose scale cannot be established has Price_EUR set to NULL and its
    original value kept in price_eur_original, so "Price_EUR is not null"
    becomes a promise that the number is trustworthy.

Usage:
  python3 migrate_fix_price_eur_units.py --evidence <live_prices.json> [--apply]

Without --apply it is a dry run and prints what it would change.
"""

import argparse
import datetime as dt
import json
import os
import shutil
import sqlite3
import sys
from collections import defaultdict

DB_PATH = os.environ.get(
    "DB_PATH", os.path.expanduser("~/QualityData/qualitydb/products.db")
)

OTTO_SOURCES = ("otto", "otto_de")
MM_SOURCES = ("mediamarkt", "saturn_de")
ALL_SOURCES = OTTO_SOURCES + MM_SOURCES

AUDIT_COLUMNS = [
    ("price_eur_original", "REAL"),
    ("price_eur_scale_factor", "REAL"),
    ("price_eur_fix_method", "TEXT"),
    ("price_eur_fix_date", "TEXT"),
    ("price_eur_live_ref", "REAL"),
]

CANDIDATES_INT_VALUED = (1.0, 0.1, 0.01)   # dot-strip inflated it, or it is fine
CANDIDATES_DECIMAL = (1.0, 100.0)          # int-read-as-cents shrank it, or fine


def ensure_columns(conn):
    have = {r[1] for r in conn.execute("PRAGMA table_info(products)")}
    for name, decl in AUDIT_COLUMNS:
        if name not in have:
            conn.execute(f"ALTER TABLE products ADD COLUMN {name} {decl}")
            print(f"  added column products.{name}")


def backup(path):
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_dir = os.path.join(os.path.dirname(path), "backups")
    os.makedirs(dest_dir, exist_ok=True)
    dest = os.path.join(dest_dir, f"products.db.pre_price_units.{stamp}")
    shutil.copy2(path, dest)
    print(f"  backup -> {dest}  ({os.path.getsize(dest)/1e6:.1f} MB)")
    return dest


def candidates_for(stored):
    return CANDIDATES_INT_VALUED if stored == int(stored) else CANDIDATES_DECIMAL


def resolve_live(stored, live, tol=2.5, margin=2.0):
    """Pick the power-of-ten factor whose result best matches the live price.

    Two guards, because a live page is not always the same product any more --
    variants get merged, a 1 TB listing turns into an 8 TB one, a camera page
    becomes a multi-camera kit:

      tol     the winner must be within `tol` of the live price, so a page that
              has drifted too far is treated as no evidence at all rather than
              as a vote for whichever candidate happens to be least wrong.
      margin  the winner must also beat the runner-up by `margin`.  Candidates
              sit a clean 10x or 100x apart, so a genuine match wins by an
              order of magnitude; anything closer means the live price cannot
              actually tell the candidates apart.

    Both failures return None, which sends the row to the cohort fallback.
    """
    if not live or live <= 0 or not stored or stored <= 0:
        return None, None
    scored = sorted(
        ((max(stored * f / live, live / (stored * f)), f) for f in candidates_for(stored)),
        key=lambda t: t[0],
    )
    best_ratio, best = scored[0]
    if best_ratio > tol:
        return None, best_ratio
    if len(scored) > 1 and scored[1][0] < best_ratio * margin:
        return None, best_ratio
    return best, best_ratio


def build_bands(resolved_rows):
    """source+category -> (lo, hi) plausible corrected-price band."""
    by_cat = defaultdict(list)
    by_src = defaultdict(list)
    for src, cat, corrected in resolved_rows:
        by_cat[(src, cat)].append(corrected)
        by_src[src].append(corrected)

    def band(vals):
        vals = sorted(vals)
        n = len(vals)
        lo = vals[max(0, int(0.10 * n) - 1)]
        hi = vals[min(n - 1, int(0.90 * n))]
        return lo * 0.5, hi * 2.0

    bands = {k: band(v) for k, v in by_cat.items() if len(v) >= 5}
    bands.update({("*", s): band(v) for s, v in by_src.items() if len(v) >= 5})
    return bands


def resolve_band(stored, src, cat, bands):
    lo_hi = bands.get((src, cat)) or bands.get(("*", src))
    if not lo_hi:
        return None
    lo, hi = lo_hi
    fits = [f for f in candidates_for(stored) if lo <= stored * f <= hi]
    return fits[0] if len(fits) == 1 else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--evidence", help="JSON from fetch_live_mm.py (mediamarkt/saturn)")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--db", default=DB_PATH)
    args = ap.parse_args()

    if not os.path.exists(args.db):
        sys.exit(f"ERROR: DB not found at {args.db}")
    print(f"database: {args.db}")

    # products.id is NULL for every German-retail row, so ProductURL -- which
    # carries the table's only UNIQUE index -- is the key throughout.
    evidence = {}
    if args.evidence:
        for rec in json.load(open(args.evidence)):
            if rec.get("live") and rec.get("url"):
                evidence[rec["url"]] = rec["live"]
        print(f"evidence: {len(evidence)} live prices from {args.evidence}")

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys=ON")

    have_audit = "price_eur_fix_date" in {
        r[1] for r in conn.execute("PRAGMA table_info(products)")
    }
    if args.apply:
        ensure_columns(conn)
        have_audit = True
    elif not have_audit:
        # A dry run must not touch the schema, so on a first run there is no
        # fix_date column yet and every candidate row is simply unmigrated.
        print("  (dry run: audit columns not present yet, none skipped)")

    already_done = "AND price_eur_fix_date IS NULL" if have_audit else ""
    placeholders = ",".join("?" * len(ALL_SOURCES))
    rows = conn.execute(
        f"""SELECT rowid, source, Price_EUR, Category, Name, ProductURL
              FROM products
             WHERE source IN ({placeholders})
               AND Price_EUR IS NOT NULL
               {already_done}""",
        ALL_SOURCES,
    ).fetchall()
    print(f"rows needing repair: {len(rows)}")
    if not rows:
        print("nothing to do -- already migrated (idempotent no-op)")
        conn.close()
        return

    # Backed up only once there is actually something to write, so that
    # re-running the migration does not pile up copies of a 150 MB database.
    if args.apply:
        backup(args.db)

    plan = []          # (rowid, new_price, factor, method, live_ref)
    resolved_rows = []  # for band building

    # pass 1 -- Otto (uniform) and live-resolved MediaMarkt/Saturn
    deferred = []
    for rid, src, stored, cat, name, url in rows:
        if src in OTTO_SOURCES:
            plan.append((rid, round(stored * 0.01, 2), 0.01, "otto_dotstrip_div100", None))
            resolved_rows.append((src, cat, stored * 0.01))
            continue
        live = evidence.get(url)
        factor, _ratio = resolve_live(stored, live)
        if factor is not None:
            plan.append((rid, round(stored * factor, 2), factor, "live_jsonld", live))
            resolved_rows.append((src, cat, stored * factor))
        else:
            deferred.append((rid, src, stored, cat, name, live))

    # pass 2 -- cohort band for rows with no usable live price
    bands = build_bands(resolved_rows)
    for rid, src, stored, cat, name, live in deferred:
        factor = resolve_band(stored, src, cat, bands)
        if factor is not None:
            plan.append((rid, round(stored * factor, 2), factor, "cohort_band", live))
        else:
            plan.append((rid, None, None, "unresolved", live))

    # report
    by_method = defaultdict(int)
    by_factor = defaultdict(int)
    for _rid, _new, factor, method, _live in plan:
        by_method[method] += 1
        by_factor[factor] += 1
    print("\nplanned repairs by method:")
    for m, n in sorted(by_method.items(), key=lambda kv: -kv[1]):
        print(f"   {m:24s} {n:5d}")
    print("planned repairs by factor:")
    for f, n in sorted(by_factor.items(), key=lambda kv: -kv[1]):
        print(f"   {str(f):24s} {n:5d}")

    print("\nsample of what changes:")
    shown = 0
    for rid, new, factor, method, live in plan:
        if shown >= 12:
            break
        row = conn.execute("SELECT source, Price_EUR, Name FROM products WHERE rowid=?", (rid,)).fetchone()
        if row:
            print(f"   {row[0]:11s} {row[1]:>10} -> {str(new):>9}  (x{factor}, {method})  {row[2][:44]}")
            shown += 1

    if not args.apply:
        print("\nDRY RUN -- nothing written.  Re-run with --apply to commit.")
        conn.close()
        return

    today = dt.date.today().isoformat()
    with conn:
        for rid, new, factor, method, live in plan:
            conn.execute(
                """UPDATE products
                      SET price_eur_original     = Price_EUR,
                          Price_EUR              = ?,
                          price_eur_scale_factor = ?,
                          price_eur_fix_method   = ?,
                          price_eur_fix_date     = ?,
                          price_eur_live_ref     = ?
                    WHERE rowid = ?
                      AND price_eur_fix_date IS NULL""",
                (new, factor, method, today, live, rid),
            )
    print(f"\napplied to {len(plan)} rows.")
    conn.close()


if __name__ == "__main__":
    main()
