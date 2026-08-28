#!/usr/bin/env python3
"""
migrate_repair_sources_20260828.py
──────────────────────────────────
Repair price scale, currency labels and dTest categories in products.db.

Found while building the brand price tiers for durability.institute, where
these defects were changing which brands the site called "budget" and which it
called "premium".

WHAT IS WRONG, AND HOW EACH IS FIXED
────────────────────────────────────

1. mediamarkt Price_EUR is 100x too high  (617 rows, EXACT fix)

   `migrate_fix_price_eur_units.py` resolved the right scale factor per row and
   recorded it in `price_eur_scale_factor`, then wrote the product of the
   original, the factor AND an extra 100. Verified across every one of the 617
   rows: `Price_EUR == price_eur_original * price_eur_scale_factor * 100`, with
   no exceptions. So a PS5 Pro at EUR 899.99 is stored as 89999 and a MacBook
   Air at EUR 999 as 99900; 547 rows exceed EUR 5,000.

   Fix: Price_EUR = price_eur_original * price_eur_scale_factor. Checked against
   the live prices that migration captured in `price_eur_live_ref`: median
   difference 7.7%, 83% within 25% -- ordinary drift between scrape and check,
   which is what a correct repair looks like.

   otto and otto_de already satisfy `Price_EUR == original * factor` and are NOT
   touched. saturn_de carries no audit columns because that migration never
   reached it, and its values are plausible, so it is not touched either.

2. pricerunner prices lost a thousands separator  (693 rows)

   A European "1.449" was parsed as a float, so 1,449 DKK became 1.449. Every
   fractional value in these two sources is this bug -- integers are genuine
   prices, and the two populations do not overlap (fractional median x1000 =
   3,914; integer median = 570).

   Fix: multiply the fractional rows by 1000.

3. Foreign catalogues labelled CZK  (2,889 rows)

   Five sources file non-Czech prices under a CZK label, which creates a
   phantom Czech price level far below the real one:

     pricerunner     -> DKK   a Danish air monitor at "1.449 CZK" is 1,449 DKK
     pricerunner_se  -> SEK
     digitec         -> CHF   a Miele dishwasher at "3,478 CZK" is CHF 3,478
     heureka_sk      -> EUR   an iPad 11 at 439, a Xiaomi 15 Ultra at 1,180
     prisjakt        -> SEK   a 65" QLED at 5,990 is SEK (~EUR 545), not CZK
                              (~EUR 245, well under what that TV costs)

   Fix: relabel the currency. No value changes -- the numbers were always
   right, only the unit was wrong.

4. datart sentinel prices  (159 rows)

   Exactly 30.0 CZK on real appliances -- an American ETA fridge, a Canon EOS
   R50. A placeholder, not a price.

   Fix: Price_CZK -> NULL. Not guessed at: the real price is not recoverable
   from this table.

5. heureka_sk EUR rows whose value sits in Price_CZK  (180 rows)

   currency='EUR' but the number is in Price_CZK and at no consistent scale.
   The same Beko fridge is 5.0 here and 16,065 CZK (~EUR 655) from heureka.cz,
   while a Krups Dolce Gusto at 5.0 is really about EUR 55 -- one stored value,
   two real prices two orders of magnitude apart. No factor recovers these.

   Fix: Price_CZK -> NULL. Same discipline as the earlier migration's
   unresolvable rows: better absent than wrong. heureka_sk's genuine Price_EUR
   rows are correct and are NOT touched -- their cheap entries are cheap
   products (EUR 4 EarPods, EUR 5 descaler), not broken scale.

6. dTest NormalizedCategory is a test TOPIC, not a product type

   dTest groups its catalogue by topic and a topic bundles an appliance with its
   consumables and neighbours, so `NormalizedCategory` puts laundry detergent
   under Washing Machines, kitchen knives under Ovens & Stoves, and cat food
   under Smartphones. `details_json.subgroup` carries the real test name and is
   reliable.

   Two fixes, both narrow:
     a) 366 rows whose subgroup names a product the DB already has a label for,
        filed under the wrong one: soundbars under Headphones, robot and stick
        vacuums under Vacuum Cleaners, monitors under Laptops, printers under
        Smartphones.
     b) 2,672 rows whose subgroup is not a product type at all AND whose current
        label is a real product category they are contaminating -> NULL. The
        topic is not lost: `Category` and `details_json.subgroup` still carry it.

   Rows whose label is already outside the product categories are left alone.
   English "Car Seats" and "Baby Strollers" labels are CORRECT and are not
   rewritten -- only the pricing lookup happens to key on the Czech spellings,
   which is a lookup question, not a data defect.

SAFETY
──────
  * Backs the database up before touching it.
  * Writes only Price_EUR, Price_CZK, currency, NormalizedCategory and the
    audit columns below. No row is deleted and no other column is touched.
  * Idempotent: every repaired row is stamped with `source_repair_date`, and
    stamped rows are skipped, so a second run is a no-op.
  * Every change keeps its previous value in an audit column, so any repair can
    be inspected or reversed.

Usage:
  python3 migrate_repair_sources_20260828.py [--apply]

Without --apply it is a dry run and prints what it would change.
"""

import argparse
import datetime as dt
import json
import os
import re
import shutil
import sqlite3
import sys
from collections import defaultdict

DB_PATH = os.environ.get(
    "DB_PATH", os.path.expanduser("~/QualityData/qualitydb/products.db")
)

AUDIT_COLUMNS = [
    ("source_repair_price_original", "REAL"),
    ("source_repair_currency_original", "TEXT"),
    ("source_repair_category_original", "TEXT"),
    ("source_repair_method", "TEXT"),
    ("source_repair_date", "TEXT"),
]

#: dTest subgroups filed under the wrong product label. Prefix-matched, because
#: dTest versions a test by appending years ("Soundbary 2018-2019").
CATEGORY_CORRECTIONS = [
    (re.compile(r"^Soundbary", re.I), "Headphones", "Soundbars"),
    (re.compile(r"^(Robotické vysavače|Vysavače robotické)", re.I), "Vacuum Cleaners", "Robot Vacuums"),
    (re.compile(r"^(Tyčové vysavače|Vysavače tyčové)", re.I), "Vacuum Cleaners", "Stick Vacuums"),
    (re.compile(r"^(PC ?[Mm]onitory|Monitory \d)", re.I), "Laptops", "Monitors"),
    (re.compile(r"^Tiskárny", re.I), "Smartphones", "Printers"),
]

#: Subgroups that ARE the product of their category. A dTest row whose subgroup
#: matches none of these is not a product of whatever NormalizedCategory says.
#: Kept deliberately in step with
#: qualitytest/data/lookups/dtest_pricing_subgroups.yaml.
PRODUCT_SUBGROUPS = [re.compile(p, re.I) for p in (
    r"^Pračky(?! se sušičkou)", r"^Myčky", r"^Chladničky", r"^Kombinované chladničky",
    r"^Mrazáky", r"^Vestavné trouby", r"^Pyrolytické trouby", r"^Sporáky",
    r"^Podlahové vysavače", r"^Vysavače (?!robotické|tyčové)", r"^Vysavače [IV]+ \d",
    r"^Robotické vysavače", r"^Vysavače robotické", r"^Tyčové vysavače", r"^Vysavače tyčové",
    r"^Mobily a smartphony", r"^Tablety(?! do | a kapsle)", r"^Čtečky elektronických knih",
    r"^Notebooky", r"^PC ?[Mm]onitory", r"^Monitory \d", r"^Tiskárny", r"^Televizory",
    r"^Kompakty", r"^Pokročilé fotoaparáty", r"^Starší testy fotoaparátů",
    r"^Sluchátka", r"^Bezdrátová sluchátka", r"^Přenosné bezdrátové reproduktory",
    r"^Bezdrátové a dokovací reproduktory", r"^Bezdrátové headsety", r"^Soundbary",
    r"^Nástěnné klimatizace", r"^Travní sekačky", r"^Benzínové sekačky",
    r"^Elektrické sekačky", r"^Vřetenové sekačky", r"^Kočárky", r"^Autosedačky",
)]

#: Labels a contaminating dTest row is currently sitting in. Only rows in one of
#: these are nulled -- a row already outside the product categories is harming
#: nothing and is left as it is.
PRODUCT_LABELS = {
    "Washing Machines", "Dishwashers", "Refrigerators", "Ovens & Stoves",
    "Air Conditioners", "Vacuum Cleaners", "Robot Vacuums", "Stick Vacuums",
    "Coffee Machines", "TVs", "Smartphones", "Laptops", "Tablets", "Smartwatches",
    "Headphones", "Speakers", "Soundbars", "Monitors", "Printers",
    "Digital Cameras", "Air Purifiers", "Lawn Mowers",
}


def ensure_columns(conn):
    have = {r[1] for r in conn.execute("PRAGMA table_info(products)")}
    for name, decl in AUDIT_COLUMNS:
        if name not in have:
            conn.execute(f"ALTER TABLE products ADD COLUMN {name} {decl}")


def subgroup_of(details_json):
    if not details_json:
        return None
    try:
        return json.loads(details_json).get("subgroup")
    except (ValueError, AttributeError):
        return None


def plan(conn):
    """Every change this script would make, as (rowid, sql_fragment, params, method)."""
    changes = []

    # 1. mediamarkt: Price_EUR = original * factor  (currently that x100)
    for rowid, price, orig, factor in conn.execute("""
        SELECT rowid, Price_EUR, price_eur_original, price_eur_scale_factor
        FROM products
        WHERE source = 'mediamarkt' AND Price_EUR IS NOT NULL
          AND price_eur_original IS NOT NULL AND price_eur_scale_factor IS NOT NULL
          AND source_repair_date IS NULL
    """):
        correct = orig * factor
        if abs(price - correct * 100) > 0.01:
            continue  # not the known defect; leave it rather than guess
        changes.append((rowid, "Price_EUR = ?", [round(correct, 2)],
                        "mediamarkt_div100", price, None, None))

    # 2. pricerunner thousands separator: fractional values are x1000 too small
    for rowid, czk, eur, src in conn.execute("""
        SELECT rowid, Price_CZK, Price_EUR, source FROM products
        WHERE source IN ('pricerunner','pricerunner_se')
          AND source_repair_date IS NULL
          AND COALESCE(Price_CZK, Price_EUR) > 0
    """):
        val = czk if czk is not None else eur
        if val == int(val):
            continue
        col = "Price_CZK" if czk is not None else "Price_EUR"
        changes.append((rowid, f"{col} = ?", [round(val * 1000)],
                        "pricerunner_thousands_sep", val, None, None))

    # 3. currency label repairs -- no value changes
    for source, wrong, right in (
        ("pricerunner", "CZK", "DKK"),
        ("pricerunner_se", "CZK", "SEK"),
        ("digitec", "CZK", "CHF"),
        # heureka.sk quotes euros: an iPad 11 at 439 and a Xiaomi 15 Ultra at
        # 1180 are euro prices, absurd as koruna.
        ("heureka_sk", "CZK", "EUR"),
        # Prisjakt is Swedish. A 65" QLED at 5,990 is SEK 5,990 (~EUR 545);
        # read as koruna it would be EUR 245, well under what that TV costs.
        ("prisjakt", "CZK", "SEK"),
    ):
        for (rowid,) in conn.execute("""
            SELECT rowid FROM products
            WHERE source = ? AND COALESCE(currency,'CZK') = ? AND source_repair_date IS NULL
        """, (source, wrong)):
            changes.append((rowid, "currency = ?", [right],
                            f"currency_{source}_{wrong}_to_{right}", None, wrong, None))

    # 4. datart sentinel, 5. heureka_sk unresolvable -- both to NULL
    for (rowid,) in conn.execute("""
        SELECT rowid FROM products
        WHERE source = 'datart' AND Price_CZK = 30 AND source_repair_date IS NULL
    """):
        changes.append((rowid, "Price_CZK = NULL", [], "datart_sentinel_price", 30.0, None, None))
    for rowid, czk in conn.execute("""
        SELECT rowid, Price_CZK FROM products
        WHERE source = 'heureka_sk' AND currency = 'EUR'
          AND Price_EUR IS NULL AND Price_CZK > 0 AND source_repair_date IS NULL
    """):
        changes.append((rowid, "Price_CZK = NULL", [], "heureka_sk_unresolvable_scale",
                        czk, None, None))

    # 6. dTest categories
    for rowid, cat, details in conn.execute("""
        SELECT rowid, NormalizedCategory, details_json FROM products
        WHERE source = 'dtest' AND source_repair_date IS NULL
    """):
        sg = subgroup_of(details)
        if not sg:
            continue
        for pattern, wrong_label, right_label in CATEGORY_CORRECTIONS:
            if pattern.search(sg) and cat == wrong_label:
                changes.append((rowid, "NormalizedCategory = ?", [right_label],
                                "dtest_relabel", None, None, cat))
                break
        else:
            is_product = any(p.search(sg) for p in PRODUCT_SUBGROUPS)
            if not is_product and cat in PRODUCT_LABELS:
                changes.append((rowid, "NormalizedCategory = NULL", [],
                                "dtest_not_a_product", None, None, cat))
    return changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    ap.add_argument("--db", default=DB_PATH)
    args = ap.parse_args()

    if not os.path.exists(args.db):
        sys.exit(f"database not found: {args.db}")

    conn = sqlite3.connect(args.db)
    conn.row_factory = None
    ensure_columns(conn)
    changes = plan(conn)

    by_method = defaultdict(int)
    for c in changes:
        by_method[c[3]] += 1
    print(f"{len(changes)} changes across {len(by_method)} repairs:")
    for method, n in sorted(by_method.items(), key=lambda kv: -kv[1]):
        print(f"   {n:6d}  {method}")

    if not args.apply:
        print("\ndry run -- nothing written. Re-run with --apply.")
        conn.close()
        return

    stamp = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
    backup = f"{args.db}.bak_source_repair_{dt.datetime.now():%Y%m%d_%H%M%S}"
    shutil.copy2(args.db, backup)
    print(f"\nbackup: {backup}")

    for rowid, frag, params, method, price_orig, cur_orig, cat_orig in changes:
        conn.execute(
            f"""UPDATE products SET {frag},
                    source_repair_price_original = COALESCE(source_repair_price_original, ?),
                    source_repair_currency_original = COALESCE(source_repair_currency_original, ?),
                    source_repair_category_original = COALESCE(source_repair_category_original, ?),
                    -- 452 pricerunner rows legitimately take two repairs (a
                    -- currency relabel and a value fix, on different columns),
                    -- so methods accumulate rather than overwrite. Re-runs are
                    -- excluded by source_repair_date, so this cannot compound.
                    source_repair_method = CASE
                        WHEN source_repair_method IS NULL THEN ?
                        ELSE source_repair_method || '+' || ? END,
                    source_repair_date = ?
                WHERE rowid = ?""",
            [*params, price_orig, cur_orig, cat_orig, method, method, stamp, rowid],
        )
    conn.commit()
    print(f"applied {len(changes)} changes")
    conn.close()


if __name__ == "__main__":
    main()
