"""
One-shot fix: reclassify amazon_us products that landed in 'Ostatní'
because the CATEGORY_MAP was keyed on HF config names but the Parquet
files store different main_category values.

The raw main_category value was stored in the Category column when no
mapping matched. We use that to apply the correct MainCategory + Category.

Run once:
  python3 scraper/fix_amazon_categories.py
  python3 scraper/fix_amazon_categories.py --dry-run
"""

import argparse
import logging
import os
import sqlite3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "products.db")

# Maps the raw Amazon main_category value → (MainCategory CZ, Category CZ)
REMAP = {
    "Cell Phones & Accessories":   ("Telefony a tablety",      "Telefony"),
    "Toys & Games":                ("Děti a hračky",           "Hračky"),
    "All Electronics":             ("Elektro",                 "Elektronika"),
    "Camera & Photo":              ("Foto a video",            "Fotoaparáty"),
    "Musical Instruments":         ("Hudba",                   "Hudební nástroje"),
    "Home Audio & Theater":        ("Elektro",                 "Domácí kino"),
    "AMAZON FASHION":              ("Móda a oblečení",         "Móda"),
    "Amazon Home":                 ("Dům a zahrada",           "Domácí potřeby"),
    "Industrial & Scientific":     ("Průmysl",                 "Průmyslové zboží"),
    "Sports & Outdoors":           ("Sport a outdoor",         "Sport"),
    "Office Products":             ("Kancelář",                "Kancelářské potřeby"),
    "Tools & Home Improvement":    ("Dům a zahrada",           "Nástroje"),
    "Car Electronics":             ("Auto a moto",             "Auto elektronika"),
    "Amazon Devices":              ("Elektro",                 "Streaming zařízení"),
    "Arts, Crafts & Sewing":       ("Hobby",                   "Kreativní práce"),
    "Portable Audio & Accessories":("Elektro",                 "Přenosný zvuk"),
    "Health & Personal Care":      ("Zdraví a sport",          "Zdraví"),
    "GPS & Navigation":            ("Auto a moto",             "GPS a navigace"),
    "Baby":                        ("Děti a hračky",           "Dětské zboží"),
    "All Beauty":                  ("Kosmetika",               "Kosmetika"),
    "Premium Beauty":              ("Kosmetika",               "Prémiová kosmetika"),
    "Apple Products":              ("Elektro",                 "Apple"),
    "Video Games":                 ("Počítače a hry",          "Počítačové hry"),
    "Pet Supplies":                ("Zvířata",                 "Zvířata"),
    "Grocery":                     ("Potraviny",               "Potraviny"),
    "Amazon Fire TV":              ("Elektro",                 "Streaming zařízení"),
    "Fire Phone":                  ("Telefony a tablety",      "Telefony"),
    "Buy a Kindle":                ("Knihy a média",           "E-čtečky"),
    "Movies & TV":                 ("Knihy a média",           "Filmy"),
    "Digital Music":               ("Knihy a média",           "Digitální hudba"),
    "Audible Audiobooks":          ("Knihy a média",           "Audioknihy"),
    "Magazine Subscriptions":      ("Knihy a média",           "Časopisy"),
    "Collectible Coins":           ("Ostatní",                 "Sběratelství"),
    "Collectibles & Fine Art":     ("Ostatní",                 "Umění a sběratelství"),
    "Sports Collectibles":         ("Ostatní",                 "Sběratelství"),
    "Handmade":                    ("Hobby",                   "Handmade"),
    "Entertainment":               ("Knihy a média",           "Zábava"),
    "Gift Cards":                  ("Ostatní",                 "Dárkové karty"),
    "Unique Finds":                ("Ostatní",                 "Ostatní"),
}


def main():
    parser = argparse.ArgumentParser(description="Fix amazon_us category mappings in DB")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")

    total_updated = 0

    for raw_cat, (main_cat, sub_cat) in REMAP.items():
        # Only touch rows that are currently miscategorised (MainCategory = 'Ostatní'
        # and Category = raw_cat). This avoids touching rows that were correctly mapped.
        cur = conn.execute(
            "SELECT COUNT(*) FROM products WHERE source='amazon_us' AND Category=? AND MainCategory='Ostatní'",
            (raw_cat,),
        )
        count = cur.fetchone()[0]
        if count == 0:
            continue

        log.info(f"  {raw_cat:45} → {main_cat} / {sub_cat}  ({count:,} rows)")
        if not args.dry_run:
            conn.execute(
                "UPDATE products SET MainCategory=?, Category=? WHERE source='amazon_us' AND Category=? AND MainCategory='Ostatní'",
                (main_cat, sub_cat, raw_cat),
            )
        total_updated += count

    if not args.dry_run:
        conn.commit()
        log.info(f"\nDone. {total_updated:,} products reclassified.")
    else:
        log.info(f"\nDRY RUN — {total_updated:,} products would be updated.")

    # Show final category distribution
    log.info("\n=== Final MainCategory distribution (amazon_us) ===")
    for row in conn.execute(
        "SELECT MainCategory, COUNT(*) n FROM products WHERE source='amazon_us' GROUP BY MainCategory ORDER BY n DESC"
    ):
        log.info(f"  {row[0]:35} {row[1]:>8,}")

    conn.close()


if __name__ == "__main__":
    main()
