#!/usr/bin/env python3
"""
enrich_warentest.py — Post-processing for Warentest data
─────────────────────────────────────────────────────────
Run after warentest_scraper.py completes to:
  1. Migrate sub-ratings from details_json → warentest_sub_ratings table
  2. Extract and clean brand names for all warentest products
  3. Print a brand quality analysis

Usage:
  python3 enrich_warentest.py
  python3 enrich_warentest.py --analysis   # also print brand analysis
"""

import os, sys, re, json, sqlite3, argparse

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")

# ── Brand extraction ──────────────────────────────────────────────────────────

# Products where the name starts with model/product line, not brand.
# Maps first word (or phrase) → actual brand.
MODEL_TO_BRAND = {
    # Apple products often named by model
    "iphone":   "Apple", "ipad":    "Apple", "macbook": "Apple",
    "imac":     "Apple", "mac":     "Apple", "airpods": "Apple",
    "watch":    "Apple", "homepod": "Apple", "apple":   "Apple",
    # Samsung model lines
    "galaxy":   "Samsung", "frame":  "Samsung",
    # Amazon
    "fire":     "Amazon", "kindle": "Amazon", "echo":   "Amazon",
    # Google
    "pixel":    "Google", "nest":   "Google",
    # Microsoft
    "surface":  "Microsoft",
    # Sony
    "xperia":   "Sony", "playstation": "Sony", "bravia": "Sony",
    "alpha ":   "Sony",   # Sony Alpha camera series
    # Panasonic
    "lumix":    "Panasonic",
    # Canon
    "eos ":     "Canon", "eos-":   "Canon",
    # Bose
    "quietcomfort": "Bose", "soundlink": "Bose", "soundbar": "Bose",
    # JBL
    "partybox": "JBL", "flip ":  "JBL", "charge ": "JBL", "xtreme": "JBL",
    # Olympus / OM System
    "m.zuiko":  "Olympus",
    # Yoga = Lenovo
    "yoga ":    "Lenovo", "ideapad": "Lenovo", "thinkpad": "Lenovo",
    "thinkbook": "Lenovo",
    # Xiaomi sub-brands
    "redmi":    "Xiaomi", "poco":   "Xiaomi",
    # Dyson product lines
    "v7":       "Dyson", "v8": "Dyson", "v10": "Dyson", "v11": "Dyson",
    "v12":      "Dyson", "v15": "Dyson", "gen5": "Dyson",
    # Oral-B (often written without brand prefix)
    "io ":      "Oral-B",
    # Ecotank / Maxify = Epson
    "ecotank":  "Epson", "maxify":  "Canon",
    # FritzBox = AVM
    "fritzbox": "AVM", "fritz!box": "AVM",
    # Geo³ / other false positives → skip via NOT_A_BRAND instead
}

# Known brand list — longest first so "De Longhi" matches before "De"
KNOWN_BRANDS = sorted([
    "AEG", "AKG", "AOC", "APC", "ASRock", "ASUS", "AVM", "Acer", "Agfa", "Amazon",
    "Anker", "Apple", "Ariston", "Asko",
    "Beko", "Belkin", "Bissell", "Blaupunkt", "Bose", "Bosch", "Braun", "Brother",
    "Canon", "Candy", "Casio", "Cecotec", "Chicco", "Comfee", "Corsair",
    "De Longhi", "Dell", "Denon", "Dyson",
    "Ecovacs", "Electrolux", "Epson", "Eufy",
    "Fissler", "Fitbit", "Fujitsu",
    "Garmin", "Gorenje", "Grundig",
    "HP", "Haier", "Harman", "Hitachi", "Honor", "Hoover", "Huawei",
    "Iiyama",
    "JBL", "Jabra",
    "KitchenAid", "Kärcher", "Kenwood",
    "LG", "Lenovo", "Liebherr", "Logitech",
    "Melitta", "Microsoft", "Miele", "Motorola", "Moulinex",
    "Neff", "Nintendo", "Ninja", "Nokia",
    "Olympus", "OnePlus", "Oral-B",
    "Panasonic", "Philips", "Pioneer", "Polar",
    "Remington", "Rowenta",
    "Samsung", "Shark", "Sharp", "Siemens", "Sony", "Stihl",
    "TCL", "TP-Link", "Tefal", "Thomson", "Toshiba",
    "Varta", "Vestel", "Vileda", "Vorwerk",
    "Whirlpool", "Withings", "Xiaomi",
    "Zanussi", "Zeiss",
    # Baby / child
    "Britax", "Joie", "Maxi-Cosi", "Nuna", "Recaro", "Stokke",
    # Sunscreen / personal care
    "Eucerin", "Garnier", "Isdin", "La Roche-Posay", "Nivea", "Vichy",
    # Baby formula
    "Aptamil", "Hipp", "Humana", "Milupa",
], key=len, reverse=True)

# First words that are definitely NOT brands
NOT_A_BRAND = {
    "the", "a", "an", "with", "for", "new", "ultra", "pro", "max", "mini",
    "plus", "no", "smart", "my", "basic", "premium", "bio", "gut",
    "best", "standard", "classic", "original", "eco", "green", "super",
    # German words that slip through
    "junges", "alte", "gute", "sehr", "test", "das", "die", "der",
    "vitamin", "mineral", "balance", "classic", "original", "natural",
    "sensitiv", "intensive", "active", "pure", "fresh", "light",
    # Financial / insurance product terms (warentest tests these but not our domain)
    "basiskonto", "girokonto", "tagesgeld", "festgeld", "fonds", "amundi",
    "etf", "aktienfonds", "anleihenfonds", "mischfonds", "deka",
    "reiserücktrittsversicherung", "krankenversicherung", "zahnschutz",
    "unfallversicherung", "haftpflichtversicherung",
    # Product lines / model series that are NOT brands
    "quietcomfort", "alpha", "lumix", "eos", "partybox", "yoga", "ecotank",
    "maxify", "fritzbox", "soundlink", "ideapad", "thinkpad", "thinkbook",
    "m.zuiko", "soundbar", "flip", "charge", "xtreme", "traumnacht",
    "matratzen", "anfangsmilch", "geo³", "winsafe", "internet",
    # Generic product category names in German
    "matratze", "milch", "kapseln", "tabletten", "saft", "wein",
    "creme", "lotion", "gel", "spray", "öl", "shampoo",
}


def extract_brand(name: str):
    if not name:
        return None
    s = name.strip()
    s_lower = s.lower()

    # Check model-to-brand mapping first (covers Apple, Samsung model lines etc.)
    for model_prefix, brand in MODEL_TO_BRAND.items():
        if s_lower.startswith(model_prefix):
            return brand

    # Known brand list (longest match first)
    for brand in KNOWN_BRANDS:
        if re.match(re.escape(brand), s, re.I):
            return brand

    # First-word fallback — only if it looks like a real brand
    first = s.split()[0]
    first_lower = first.lower().rstrip('.,;:')
    if (first and
            first[0].isupper() and
            not re.match(r'^\d', first) and
            len(first) > 2 and
            first_lower not in NOT_A_BRAND and
            not re.match(r'^[A-Z]{1,2}\d', first)):   # skip model codes like "S24", "V8"
        return first

    return None


# ── Sub-ratings migration ─────────────────────────────────────────────────────

def migrate_sub_ratings(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS warentest_sub_ratings (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            product_url  TEXT NOT NULL,
            rating_key   TEXT NOT NULL,
            rating_label TEXT,
            grade        REAL,
            stars        REAL,
            UNIQUE(product_url, rating_key),
            FOREIGN KEY(product_url) REFERENCES products(ProductURL)
        )
    """)
    conn.commit()

    rows = conn.execute(
        "SELECT ProductURL, details_json FROM products "
        "WHERE source='warentest' AND details_json IS NOT NULL"
    ).fetchall()

    migrated = 0
    for url, dj in rows:
        try:
            d = json.loads(dj)
            for key, val in d.get("sub_ratings", {}).items():
                conn.execute(
                    "INSERT OR REPLACE INTO warentest_sub_ratings "
                    "(product_url, rating_key, rating_label, grade, stars) VALUES (?,?,?,?,?)",
                    (url, key, val.get("label"), val.get("grade"), val.get("stars"))
                )
                migrated += 1
        except Exception:
            pass

    conn.commit()
    return migrated


# ── Brand analysis ────────────────────────────────────────────────────────────

def print_brand_analysis(conn):
    print("\n" + "="*70)
    print("WARENTEST BRAND QUALITY ANALYSIS")
    print("="*70)

    # Overall brand scores (min 3 products to be meaningful)
    print("\n── Top brands by average Warentest rating (min 3 products) ──")
    rows = conn.execute("""
        SELECT brand,
               COUNT(*) as n,
               ROUND(AVG(AvgStarRating), 2) as avg_stars,
               ROUND(AVG(RecommendRate_pct), 1) as avg_rec,
               MIN(AvgStarRating) as worst,
               MAX(AvgStarRating) as best
        FROM products
        WHERE source='warentest' AND brand IS NOT NULL
          AND AvgStarRating IS NOT NULL
        GROUP BY brand
        HAVING COUNT(*) >= 3
        ORDER BY avg_stars DESC
        LIMIT 30
    """).fetchall()

    print(f"  {'Brand':20s} {'N':>4}  {'Avg★':>5}  {'Rec%':>5}  {'Worst':>5}  {'Best':>5}")
    print("  " + "-"*55)
    for brand, n, avg_stars, avg_rec, worst, best in rows:
        print(f"  {brand:20s} {n:>4}  {avg_stars:>5.2f}  "
              f"{avg_rec or 0:>5.1f}  {worst:>5.1f}  {best:>5.1f}")

    # Brand scores by category
    print("\n── Brands with most 'very good' ratings (≥4.5★) ──")
    rows = conn.execute("""
        SELECT brand, COUNT(*) as top_rated, GROUP_CONCAT(DISTINCT Category) as cats
        FROM products
        WHERE source='warentest' AND brand IS NOT NULL AND AvgStarRating >= 4.5
        GROUP BY brand
        ORDER BY top_rated DESC
        LIMIT 15
    """).fetchall()
    for brand, count, cats in rows:
        cat_list = (cats or "")[:60]
        print(f"  {brand:20s} {count:>3} top-rated  ({cat_list})")

    # Sub-rating leaders (if --details data exists)
    r = conn.execute("SELECT COUNT(*) FROM warentest_sub_ratings").fetchone()[0]
    if r > 0:
        print(f"\n── Sub-rating leaders ({r} sub-ratings available) ──")
        rows = conn.execute("""
            SELECT p.brand, s.rating_key,
                   COUNT(*) as n,
                   ROUND(AVG(s.stars), 2) as avg_stars
            FROM warentest_sub_ratings s
            JOIN products p ON p.ProductURL = s.product_url
            WHERE p.brand IS NOT NULL AND s.rating_key != 'overall'
            GROUP BY p.brand, s.rating_key
            HAVING COUNT(*) >= 3
            ORDER BY avg_stars DESC
            LIMIT 20
        """).fetchall()
        print(f"  {'Brand':20s} {'Sub-rating':20s} {'N':>4}  {'Avg★':>5}")
        print("  " + "-"*50)
        for brand, key, n, avg in rows:
            print(f"  {brand:20s} {key:20s} {n:>4}  {avg:>5.2f}")

    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--analysis", action="store_true", help="Print brand analysis after enrichment")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)

    print("Migrating sub-ratings from details_json...")
    migrated = migrate_sub_ratings(conn)
    print(f"  → {migrated} sub-rating rows in warentest_sub_ratings")

    print("\nExtracting brands...")
    rows = conn.execute(
        "SELECT rowid, Name, brand FROM products WHERE source='warentest'"
    ).fetchall()

    updated = skipped = no_brand = 0
    brand_counts = {}
    for rowid, name, existing in rows:
        brand = extract_brand(name)
        if brand:
            if brand != existing:
                conn.execute("UPDATE products SET brand=? WHERE rowid=?", (brand, rowid))
                updated += 1
            else:
                skipped += 1
            brand_counts[brand] = brand_counts.get(brand, 0) + 1
        else:
            no_brand += 1

    conn.commit()
    print(f"  → {updated} updated, {skipped} unchanged, {no_brand} with no brand detected")
    print(f"  → {len(brand_counts)} distinct brands")

    print("\nTop brands:")
    for brand, count in sorted(brand_counts.items(), key=lambda x: -x[1])[:20]:
        print(f"  {brand:20s} {count}")

    if args.analysis:
        print_brand_analysis(conn)

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
