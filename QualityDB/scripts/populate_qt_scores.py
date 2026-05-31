#!/usr/bin/env python3
"""
Fetch brand expert-test scores from the institutkvality API and populate
the qt_brand_score column in QualityDB SQLite.

Run from the repo root:
    python3 QualityDB/scripts/populate_qt_scores.py [db_path]

If db_path is not supplied it defaults to ../../products_deploy.db relative
to this script's directory (i.e. the checked-out deploy database).
"""
import json, sqlite3, sys, urllib.request, os

# ── Config ────────────────────────────────────────────────────────────────────

API_URL  = "https://institutkvality.vercel.app/api/quality/brands"

# Neon category slug → QualityDB NormalizedCategory values (one-to-many)
CATEGORY_MAP: dict[str, list[str]] = {
    "washing_machines":       ["Washing Machines"],
    "dishwashers":            ["Dishwashers"],
    "refrigerators_freezers": ["Refrigerators"],
    "laptops":                ["Laptops"],
    "televisions":            ["TVs"],
    "vacuum_cleaners":        ["Vacuum Cleaners"],
    "smartphones":            ["Smartphones"],
    "headphones_speakers":    ["Headphones", "Speakers"],
    "kitchen_appliances":     ["Coffee Machines", "Kitchen Appliances"],
    "cooking_baking":         ["Kitchen Appliances"],
    "smartphones":            ["Smartphones"],
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def fetch_brand_scores() -> list[dict]:
    """Return brand aggregates from the institutkvality API."""
    req = urllib.request.Request(API_URL, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    brands = data.get("brands", [])
    print(f"  API returned {len(brands)} brand records (source: {data.get('source', '?')})")
    return brands


def build_score_lookup(brands: list[dict]) -> dict[tuple, float]:
    """
    Return a dict mapping (brand_lower, qdb_norm_category) → score.
    One brand can appear in multiple categories.
    """
    lookup: dict[tuple, float] = {}
    for b in brands:
        brand_lower = b["brand"].strip().lower()
        cat_slug    = b.get("category", "")
        score       = float(b.get("score", 0) or 0)
        if not brand_lower or not cat_slug or score == 0:
            continue
        for qdb_cat in CATEGORY_MAP.get(cat_slug, []):
            key = (brand_lower, qdb_cat)
            # Keep the highest score if multiple agencies report same brand/category
            if lookup.get(key, 0) < score:
                lookup[key] = score
    return lookup


def populate_db(db_path: str, lookup: dict[tuple, float]) -> None:
    """Write scores into products.qt_brand_score (adds column if missing)."""
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()

    # Add column if it doesn't exist yet
    existing_cols = {r[1] for r in cur.execute("PRAGMA table_info(products)")}
    if "qt_brand_score" not in existing_cols:
        cur.execute("ALTER TABLE products ADD COLUMN qt_brand_score REAL")
        conn.commit()
        print("  Added qt_brand_score column.")
    else:
        print("  Column qt_brand_score already present.")

    # Pass 1: match by explicit brand column (most accurate)
    total_updated = 0
    for (brand_lower, qdb_cat), score in lookup.items():
        cur.execute(
            """UPDATE products
                  SET qt_brand_score = ?
                WHERE LOWER(TRIM(brand)) = ?
                  AND NormalizedCategory  = ?
                  AND brand IS NOT NULL
                  AND brand != ''""",
            (score, brand_lower, qdb_cat),
        )
        total_updated += cur.rowcount

    # Pass 2: match by brand name appearing in product Name when brand col is NULL.
    # Skip very short brand tokens (≤2 chars) that risk false-positives.
    name_updated = 0
    for (brand_lower, qdb_cat), score in lookup.items():
        if len(brand_lower) <= 2:
            continue  # "lg", "hp" etc — skip (too risky as LIKE patterns)
        # Word-boundary simulation: brand must be preceded by start-of-string or space,
        # and followed by end-of-string or space.
        cur.execute(
            """UPDATE products
                  SET qt_brand_score = ?
                WHERE (brand IS NULL OR brand = '')
                  AND NormalizedCategory = ?
                  AND qt_brand_score IS NULL
                  AND (
                      LOWER(Name) LIKE ? ESCAPE '\\'
                   OR LOWER(Name) LIKE ? ESCAPE '\\'
                  )""",
            (
                score,
                qdb_cat,
                brand_lower + " %",          # starts with brand
                "% " + brand_lower + " %",   # brand in middle
            ),
        )
        name_updated += cur.rowcount

    conn.commit()
    total_updated += name_updated
    if name_updated:
        print(f"  Pass 1 (brand col): {total_updated - name_updated} rows; "
              f"Pass 2 (name infer): {name_updated} rows.")

    # Report coverage
    with_score = cur.execute(
        "SELECT COUNT(*) FROM products WHERE qt_brand_score IS NOT NULL"
    ).fetchone()[0]
    total = cur.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    conn.close()

    print(f"  Updated {total_updated} rows.")
    print(f"  Coverage: {with_score:,} / {total:,} products have qt_brand_score "
          f"({100*with_score/max(total,1):.1f}%)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_db = os.path.normpath(os.path.join(script_dir, "..", "..", "products_deploy.db"))
    db_path = sys.argv[1] if len(sys.argv) > 1 else default_db

    if not os.path.exists(db_path):
        print(f"ERROR: database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Database: {db_path}")
    print(f"Fetching brand scores from {API_URL} …")
    brands = fetch_brand_scores()

    print("Building lookup table …")
    lookup = build_score_lookup(brands)
    print(f"  {len(lookup)} brand×category combinations to populate.")

    print("Populating qt_brand_score …")
    populate_db(db_path, lookup)
    print("Done.")


if __name__ == "__main__":
    main()
