"""
Import Amazon Reviews 2023 item metadata into QualityDB.

SOURCE: McAuley-Lab/Amazon-Reviews-2023 (HuggingFace)
  - 48 million products across 33 categories
  - Each item already has pre-computed average_rating + rating_number
  - Also includes price (USD), product title, category
  - NOTE: This is Amazon.com (US) data — ASINs often overlap with Amazon.de
    but prices are in USD. Products are stored with source='amazon_us', country='US'.

What this does:
  - Downloads item metadata (NOT individual reviews) for selected categories
  - Each row is already a product with avg rating + review count
  - Upserts into products.db

NOTE: Uses huggingface_hub + pandas directly (bypasses 'datasets' library which
      broke loading-script-based datasets in v4.5).

Requirements (run once on your Mac):
  pip3 install huggingface_hub pandas pyarrow

Usage:
  python3 scraper/load_amazon_reviews.py
  python3 scraper/load_amazon_reviews.py --categories Electronics Home_and_Kitchen
  python3 scraper/load_amazon_reviews.py --min-reviews 50 --dry-run
  python3 scraper/load_amazon_reviews.py --list-categories
  python3 scraper/load_amazon_reviews.py --list-files Electronics
"""

import argparse
import logging
import os
import sqlite3
import sys
import tempfile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "products.db")

REPO_ID = "McAuley-Lab/Amazon-Reviews-2023"

# Categories most relevant to the existing DB (electronics, appliances, home)
DEFAULT_CATEGORIES = [
    "Electronics",
    "Home_and_Kitchen",
    "Appliances",
    "Cell_Phones_and_Accessories",
    "Computers",
    "Camera_and_Photo_Products",
    "Musical_Instruments",
    "Office_Products",
    "Sports_and_Outdoors",
    "Toys_and_Games",
    "Automotive",
    "Health_and_Household",
    "Beauty_and_Personal_Care",
    "Tools_and_Home_Improvement",
    "Patio_Lawn_and_Garden",
]

ALL_CATEGORIES = [
    "All_Beauty", "Amazon_Fashion", "Appliances", "Arts_Crafts_and_Sewing",
    "Automotive", "Baby_Products", "Beauty_and_Personal_Care", "Books",
    "CDs_and_Vinyl", "Cell_Phones_and_Accessories", "Clothing_Shoes_and_Jewelry",
    "Camera_and_Photo_Products", "Computers", "Digital_Music",
    "Electronics", "Gift_Cards", "Grocery_and_Gourmet_Food",
    "Handmade_Products", "Health_and_Household", "Home_and_Kitchen",
    "Industrial_and_Scientific", "Kindle_Store", "Magazine_Subscriptions",
    "Movies_and_TV", "Musical_Instruments", "Office_Products",
    "Patio_Lawn_and_Garden", "Pet_Supplies", "Software",
    "Sports_and_Outdoors", "Subscription_Boxes", "Tools_and_Home_Improvement",
    "Toys_and_Games", "Video_Games",
]

# Map Amazon main_category value → (MainCategory CZ, Category CZ)
# Keys are the ACTUAL values stored in the Parquet main_category field
# (NOT the HuggingFace config/dataset names like "Electronics" or "Cell_Phones_and_Accessories")
CATEGORY_MAP = {
    # ── Actual Parquet main_category values ─────────────────────────────────
    "All Electronics":              ("Elektro",                 "Elektronika"),
    "Cell Phones & Accessories":    ("Telefony a tablety",      "Telefony"),
    "Camera & Photo":               ("Foto a video",            "Fotoaparáty"),
    "Home Audio & Theater":         ("Elektro",                 "Domácí kino"),
    "Portable Audio & Accessories": ("Elektro",                 "Přenosný zvuk"),
    "Amazon Devices":               ("Elektro",                 "Streaming zařízení"),
    "Amazon Fire TV":               ("Elektro",                 "Streaming zařízení"),
    "Apple Products":               ("Elektro",                 "Apple"),
    "Fire Phone":                   ("Telefony a tablety",      "Telefony"),
    "Computers":                    ("Počítače a notebooky",    "Počítače"),
    "Software":                     ("Počítače a hry",          "Software"),
    "Video Games":                  ("Počítače a hry",          "Počítačové hry"),
    "Musical Instruments":          ("Hudba",                   "Hudební nástroje"),
    "Toys & Games":                 ("Děti a hračky",           "Hračky"),
    "Baby":                         ("Děti a hračky",           "Dětské zboží"),
    "Appliances":                   ("Velké domácí spotřebiče", "Spotřebiče"),
    "Home & Kitchen":               ("Dům a zahrada",           "Kuchyňské potřeby"),
    "Amazon Home":                  ("Dům a zahrada",           "Domácí potřeby"),
    "Tools & Home Improvement":     ("Dům a zahrada",           "Nástroje"),
    "Patio, Lawn & Garden":         ("Dům a zahrada",           "Zahrada"),
    "Sports & Outdoors":            ("Sport a outdoor",         "Sport"),
    "Automotive":                   ("Auto a moto",             "Auto příslušenství"),
    "Car Electronics":              ("Auto a moto",             "Auto elektronika"),
    "GPS & Navigation":             ("Auto a moto",             "GPS a navigace"),
    "Office Products":              ("Kancelář",                "Kancelářské potřeby"),
    "Industrial & Scientific":      ("Průmysl",                 "Průmyslové zboží"),
    "Health & Personal Care":       ("Zdraví a sport",          "Zdraví"),
    "Health and Beauty":            ("Zdraví a sport",          "Zdraví"),
    "All Beauty":                   ("Kosmetika",               "Kosmetika"),
    "Premium Beauty":               ("Kosmetika",               "Prémiová kosmetika"),
    "AMAZON FASHION":               ("Móda a oblečení",         "Móda"),
    "Clothing, Shoes & Jewelry":    ("Móda a oblečení",         "Oblečení"),
    "Arts, Crafts & Sewing":        ("Hobby",                   "Kreativní práce"),
    "Handmade":                     ("Hobby",                   "Handmade"),
    "Pet Supplies":                 ("Zvířata",                 "Zvířata"),
    "Grocery":                      ("Potraviny",               "Potraviny"),
    "Grocery & Gourmet Food":       ("Potraviny",               "Potraviny"),
    "Books":                        ("Knihy a média",           "Knihy"),
    "Movies & TV":                  ("Knihy a média",           "Filmy"),
    "CDs & Vinyl":                  ("Knihy a média",           "Hudba"),
    "Digital Music":                ("Knihy a média",           "Digitální hudba"),
    "Audible Audiobooks":           ("Knihy a média",           "Audioknihy"),
    "Buy a Kindle":                 ("Knihy a média",           "E-čtečky"),
    "Kindle Store":                 ("Knihy a média",           "E-knihy"),
    "Magazine Subscriptions":       ("Knihy a média",           "Časopisy"),
    "Entertainment":                ("Knihy a média",           "Zábava"),
    "Collectible Coins":            ("Ostatní",                 "Sběratelství"),
    "Collectibles & Fine Art":      ("Ostatní",                 "Umění a sběratelství"),
    "Sports Collectibles":          ("Ostatní",                 "Sběratelství"),
    "Gift Cards":                   ("Ostatní",                 "Dárkové karty"),
    "Subscription Boxes":           ("Ostatní",                 "Předplatné"),
    "Unique Finds":                 ("Ostatní",                 "Ostatní"),
    # ── HF config name fallbacks (in case config name appears as main_category) ─
    "Electronics":                  ("Elektro",                 "Elektronika"),
    "Home_and_Kitchen":             ("Dům a zahrada",           "Kuchyňské potřeby"),
    "Cell_Phones_and_Accessories":  ("Telefony a tablety",      "Telefony"),
    "Camera_and_Photo_Products":    ("Foto a video",            "Fotoaparáty"),
    "Musical_Instruments":          ("Hudba",                   "Hudební nástroje"),
    "Office_Products":              ("Kancelář",                "Kancelářské potřeby"),
    "Sports_and_Outdoors":          ("Sport a outdoor",         "Sport"),
    "Toys_and_Games":               ("Děti a hračky",           "Hračky"),
    "Health_and_Household":         ("Zdraví a sport",          "Zdraví"),
    "Beauty_and_Personal_Care":     ("Kosmetika",               "Kosmetika"),
    "Tools_and_Home_Improvement":   ("Dům a zahrada",           "Nástroje"),
    "Patio_Lawn_and_Garden":        ("Dům a zahrada",           "Zahrada"),
    "Video_Games":                  ("Počítače a hry",          "Počítačové hry"),
    "Baby_Products":                ("Děti a hračky",           "Dětské zboží"),
    "Grocery_and_Gourmet_Food":     ("Potraviny",               "Potraviny"),
    "Industrial_and_Scientific":    ("Průmysl",                 "Průmyslové zboží"),
    "Clothing_Shoes_and_Jewelry":   ("Móda a oblečení",         "Oblečení"),
    "Amazon_Fashion":               ("Móda a oblečení",         "Móda"),
    "All_Beauty":                   ("Kosmetika",               "Kosmetika"),
    "Arts_Crafts_and_Sewing":       ("Hobby",                   "Kreativní práce"),
    "Handmade_Products":            ("Hobby",                   "Handmade"),
    "CDs_and_Vinyl":                ("Knihy a média",           "Hudba"),
    "Digital_Music":                ("Knihy a média",           "Digitální hudba"),
    "Kindle_Store":                 ("Knihy a média",           "E-knihy"),
    "Magazine_Subscriptions":       ("Knihy a média",           "Časopisy"),
    "Subscription_Boxes":           ("Ostatní",                 "Předplatné"),
    "Gift_Cards":                   ("Ostatní",                 "Dárkové karty"),
}


# ── HuggingFace Parquet discovery ──────────────────────────────────────────────
def find_parquet_files(api, cat):
    """
    Discover Parquet files for a given category config in the HF repo.

    The repo uses config names like 'raw_meta_Electronics'.
    After HuggingFace's Parquet conversion, files are stored in patterns such as:
      data/raw_meta_Electronics-00000-of-00003.parquet
      raw_meta_Electronics/full/0000.parquet
      raw_meta_Electronics-*.parquet

    We search all repo files and return any that contain the config name
    and end in .parquet.
    """
    config = f"raw_meta_{cat}"
    all_files = list(api.list_repo_files(REPO_ID, repo_type="dataset"))
    matched = [f for f in all_files if config in f and f.endswith(".parquet")]
    if not matched:
        # Fallback: look for files by category name alone (some repos use shorter names)
        matched = [f for f in all_files if cat in f and f.endswith(".parquet")]
    return sorted(matched)


# ── DB helpers ─────────────────────────────────────────────────────────────────
def open_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def load_existing_urls(conn):
    rows = conn.execute("SELECT ProductURL FROM products WHERE source='amazon_us'").fetchall()
    return {r[0] for r in rows}


# ── Upsert ─────────────────────────────────────────────────────────────────────
def upsert_item(conn, row, existing_urls, dry_run=False):
    """
    row is a pandas Series (or dict-like) with columns from the Parquet file.
    Expected columns: parent_asin, asin, title, average_rating,
                      rating_number, price, main_category
    """
    asin = str(row.get("parent_asin") or row.get("asin") or "").strip()
    if not asin:
        return None

    name = str(row.get("title") or "").strip()
    if not name:
        return None

    avg     = row.get("average_rating")
    count   = row.get("rating_number") or 0
    price   = row.get("price")          # USD float or None
    raw_cat = str(row.get("main_category") or "")
    main_cat, sub_cat = CATEGORY_MAP.get(raw_cat, ("Ostatní", raw_cat or "Ostatní"))

    url    = f"https://www.amazon.com/dp/{asin}"
    is_new = url not in existing_urls

    if dry_run:
        log.debug(f"  {'NEW' if is_new else 'UPD'} {name[:60]} | ★{avg} ({count}) | ${price}")
        existing_urls.add(url)
        return is_new

    # Sanitise numeric values (pandas may return numpy types or NaN)
    try:
        avg_val = float(avg) if avg is not None and avg == avg else None   # NaN check
    except (TypeError, ValueError):
        avg_val = None

    try:
        count_val = int(count) if count is not None and count == count else 0
    except (TypeError, ValueError):
        count_val = 0

    try:
        price_val = float(price) if price is not None and price == price else None
    except (TypeError, ValueError):
        price_val = None

    conn.execute(
        """INSERT INTO products
               (Name, Category, MainCategory, ProductURL,
                AvgStarRating, ReviewsCount, Price_EUR,
                country, source, currency, dataset_source)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(ProductURL) DO UPDATE SET
               AvgStarRating  = COALESCE(excluded.AvgStarRating,  AvgStarRating),
               ReviewsCount   = CASE
                                  WHEN excluded.ReviewsCount > COALESCE(ReviewsCount,0)
                                  THEN excluded.ReviewsCount
                                  ELSE COALESCE(ReviewsCount,0)
                                END,
               Price_EUR      = COALESCE(excluded.Price_EUR, Price_EUR),
               dataset_source = 'amazon_reviews_2023'
        """,
        (
            name[:500], sub_cat, main_cat, url,
            avg_val,
            count_val,
            price_val,   # USD stored in Price_EUR column as proxy
            "US", "amazon_us", "USD",
            "amazon_reviews_2023",
        ),
    )
    existing_urls.add(url)
    return is_new


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Import Amazon Reviews 2023 metadata into QualityDB")
    parser.add_argument("--categories", nargs="+", default=DEFAULT_CATEGORIES,
                        help="Amazon categories to import (default: 15 most relevant)")
    parser.add_argument("--list-categories", action="store_true",
                        help="Print all available categories and exit")
    parser.add_argument("--list-files", metavar="CATEGORY",
                        help="List Parquet files found for a category and exit (useful for debugging)")
    parser.add_argument("--min-reviews", type=int, default=20,
                        help="Only import products with at least N reviews (default: 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without writing to DB")
    args = parser.parse_args()

    if args.list_categories:
        print("Available categories:")
        for c in sorted(ALL_CATEGORIES):
            marker = "* " if c in DEFAULT_CATEGORIES else "  "
            print(f"  {marker}{c}")
        print("\n* = included by default")
        return

    try:
        from huggingface_hub import HfApi
        import pandas as pd
    except ImportError:
        log.error("Run: pip3 install huggingface_hub pandas pyarrow")
        sys.exit(1)

    api = HfApi()

    if args.list_files:
        cat = args.list_files
        log.info(f"Searching for Parquet files for category: {cat}")
        files = find_parquet_files(api, cat)
        if files:
            print(f"\nFound {len(files)} file(s) for '{cat}':")
            for f in files:
                print(f"  {f}")
        else:
            print(f"\nNo Parquet files found for '{cat}'.")
            print("Run with --list-files to see available files for any category.")
            # Show all parquet files in repo for debugging
            all_pq = [f for f in api.list_repo_files(REPO_ID, repo_type="dataset")
                      if f.endswith(".parquet")]
            if all_pq:
                print(f"\nAll Parquet files in repo ({len(all_pq)} total), first 20:")
                for f in all_pq[:20]:
                    print(f"  {f}")
            else:
                print("\nNo .parquet files found anywhere in the repo.")
                print("The dataset may still use only loading scripts.")
                print("Try: pip3 install 'datasets<4.0' to use the old datasets library.")
        return

    conn = open_db() if not args.dry_run else None
    if conn:
        existing_urls = load_existing_urls(conn)
        log.info(f"Existing amazon_us products in DB: {len(existing_urls)}")
    else:
        existing_urls = set()
        log.info("DRY RUN — no changes will be written to DB")

    total_inserted = total_updated = total_skipped = 0

    # Use a persistent temp dir so re-runs reuse cached downloads
    cache_dir = os.path.join(os.path.dirname(__file__), "..", ".hf_cache")
    os.makedirs(cache_dir, exist_ok=True)

    for cat in args.categories:
        if cat not in ALL_CATEGORIES:
            log.warning(f"Unknown category '{cat}' — skipping. Use --list-categories to see options.")
            continue

        log.info(f"── {cat}")

        # Discover Parquet files for this category
        parquet_files = find_parquet_files(api, cat)
        if not parquet_files:
            log.warning(f"   No Parquet files found for '{cat}'.")
            log.warning(f"   Run: python3 scraper/load_amazon_reviews.py --list-files {cat}")
            log.warning(f"   If the repo has no .parquet files, try downgrading: pip3 install 'datasets<4.0'")
            continue

        log.info(f"   Found {len(parquet_files)} Parquet file(s)")

        inserted = updated = skipped = 0
        batch_size = 5000

        for pq_path in parquet_files:
            log.info(f"   Downloading: {pq_path}")
            try:
                local_path = api.hf_hub_download(
                    repo_id=REPO_ID,
                    filename=pq_path,
                    repo_type="dataset",
                    cache_dir=cache_dir,
                )
            except Exception as e:
                log.warning(f"   Download failed for {pq_path}: {e}")
                continue

            log.info(f"   Reading Parquet…")
            try:
                df = pd.read_parquet(local_path)
            except Exception as e:
                log.warning(f"   Could not read {local_path}: {e}")
                continue

            log.info(f"   {len(df):,} rows in file")

            for _, row in df.iterrows():
                count = row.get("rating_number") or 0
                try:
                    count = int(count)
                except (TypeError, ValueError):
                    count = 0

                if count < args.min_reviews:
                    skipped += 1
                    continue

                result = upsert_item(conn, row, existing_urls, dry_run=args.dry_run)
                if result is True:
                    inserted += 1
                elif result is False:
                    updated += 1

                if (inserted + updated) % batch_size == 0 and (inserted + updated) > 0:
                    if conn:
                        conn.commit()
                    log.info(f"   …{inserted+updated:,} processed (ins={inserted} upd={updated} skip={skipped})")

        if conn:
            conn.commit()

        log.info(f"   Done: {inserted} inserted, {updated} updated, {skipped} skipped")
        total_inserted += inserted
        total_updated  += updated
        total_skipped  += skipped

    if conn:
        conn.close()

    log.info("=" * 60)
    log.info(f"TOTAL: {total_inserted} inserted, {total_updated} updated, {total_skipped} skipped")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
