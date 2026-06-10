"""
PriceRunner.se scraper — finds top-rated products and adds them to QualityDB.

PriceRunner.se is Sweden's largest price-comparison portal (same platform as
pricerunner.dk / pricerunner.no).  Products carry star ratings (1–5) from
verified buyers.  Category slugs and numeric IDs are identical to .dk.

Scraping approach:
  - Category pages sorted by BEST_RATED (?sortByPreset=BEST_RATED)
  - 3 pages per category (~12–13 products/page)
  - No minimum rating threshold — all products are stored
  - HTML parsed with BeautifulSoup from server-rendered product anchors
  - Prices are in SEK, stored as-is in Price_CZK column

Text patterns (Swedish, differs from Danish .dk):
  - "N+ bevakar"  (Swedish "watching") — filtered from product names
  - "N butiker"   (Swedish "stores")   — filtered from product names
  - Price ends with "kr." or "kr"

Category slugs: identical numeric IDs to pricerunner.dk.
All verified working May 2026.

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/pricerunner_se_scraper.py
"""

import re
import time
import logging
import sqlite3
import os
import sys

try:
    from curl_cffi import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install curl_cffi beautifulsoup4\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Settings ──────────────────────────────────────────────────────────────────
MAX_PAGES     = 3      # Pages per category
REQUEST_DELAY = 2.5    # Seconds between requests (site is rate-sensitive)

BASE_URL = "https://www.pricerunner.se"

# ── Category list ─────────────────────────────────────────────────────────────
# Numeric IDs identical to pricerunner.dk; text slug is Swedish.
# Verified working May 2026.
CATEGORIES = [
    {"name": "TVs",              "slug": "2/TV"},
    {"name": "Smartphones",      "slug": "1/Mobiltelefoner"},
    {"name": "Laptops",          "slug": "27/Baerbar"},
    {"name": "Tablets",          "slug": "224/Tablets"},
    {"name": "Headphones",       "slug": "94/Hoeretelefoner"},
    {"name": "Smartwatches",     "slug": "1438/Wearables"},
    {"name": "Speakers",         "slug": "267/Bluetooth-hojttalere"},
    {"name": "Monitors",         "slug": "25/Skaerme"},
    {"name": "Washing Machines", "slug": "14/Vaskemaskiner"},
    {"name": "Dishwashers",      "slug": "13/Opvaskemaskiner"},
    {"name": "Refrigerators",    "slug": "18/Koeleskabe"},
    {"name": "Coffee Machines",  "slug": "82/Kaffemaskiner"},
    {"name": "Vacuum Cleaners",  "slug": "19/Stoevsugere"},
    {"name": "Robot Vacuums",    "slug": "1613/Robotstoevsugere"},
    {"name": "SSD",              "slug": "36/SSD"},
    {"name": "Air Purifiers",    "slug": "453/Indeklima"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "pricerunner_se_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "sv-SE,sv;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": BASE_URL,
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_price_sek(text: str):
    """'24 191 kr.' → 24191.0"""
    if not text:
        return None
    clean = re.sub(r"[^\d,.]", "", text.replace("\xa0", ""))
    clean = clean.replace(",", ".")
    m = re.search(r"(\d+\.?\d*)", clean)
    return float(m.group(1)) if m else None


# ── Fetch & parse ─────────────────────────────────────────────────────────────

def fetch_page(slug: str, page: int, session) -> list:
    """Fetch one category page and return list of raw product dicts."""
    url = f"{BASE_URL}/cl/{slug}?sortByPreset=BEST_RATED&page={page}"
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    products = []

    # Product links: each /pl/ anchor contains all product info as text nodes.
    # Swedish text patterns (differs from Danish .dk):
    #   "N+ bevakar"  — number of price-watchers (skip)
    #   "N butiker"   — number of stores (skip)
    #   "X,X"         — star rating (exactly 3 chars)
    #   "XX XXX kr."  — price in SEK
    links = soup.select("a[href*='/pl/']")
    if not links:
        log.debug("  No /pl/ product links found.")
        return []

    for link in links:
        href        = link.get("href", "")
        url_product = (BASE_URL + href) if href.startswith("/") else href

        parts = [t.strip() for t in link.stripped_strings if t.strip()]

        # Name: first part > 10 chars, not a watcher/store count string
        name = ""
        for p in parts:
            if (len(p) > 10
                    and "bevakar"  not in p
                    and "butiker"  not in p
                    and "övervaka" not in p):
                name = p
                break

        # Rating: standalone "X,X" or "X.X" token
        stars = None
        for p in parts:
            if re.match(r"^\d[,.]\d$", p):
                stars = float(p.replace(",", "."))
                break

        # Price: token containing "kr"
        price_sek = None
        for p in parts:
            if "kr" in p.lower():
                price_sek = _parse_price_sek(p)
                break

        if name:
            products.append({
                "Name":              name,
                "ProductURL":        url_product,
                "Stars":             stars,
                "RecommendRate_pct": round((stars / 5.0) * 100, 1) if stars else None,
                "ReviewsCount":      0,  # not shown on listing page
                "Price_CZK":         price_sek,
            })

    return products


# ── Database helpers ──────────────────────────────────────────────────────────

def load_existing_names(conn) -> set:
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def insert_products(conn, products: list, category: str) -> int:
    from scraper.snapshots import ensure_snapshot_table, record_snapshot
    ensure_snapshot_table(conn)
    existing = load_existing_names(conn)
    inserted = 0
    for p in products:
        record_snapshot(conn, p.get("ProductURL", ""), "pricerunner_se", p, country="SE")
        key = p["Name"].lower()
        if key in existing:
            continue
        conn.execute(
            """INSERT INTO products
               (Name, Category, ProductURL, Price_CZK,
                RecommendRate_pct, ReviewsCount, source)
               VALUES (?,?,?,?,?,?,?)""",
            (
                p["Name"],
                category,
                p.get("ProductURL", ""),
                p.get("Price_CZK"),
                p.get("RecommendRate_pct"),
                p.get("ReviewsCount", 0),
                "pricerunner_se",
            ),
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn) -> int:
    slug     = cat["slug"]
    cat_name = cat["name"]
    total    = 0

    log.info(f"── {cat_name}  (/cl/{slug})")

    for page in range(1, MAX_PAGES + 1):
        log.info(f"   Page {page}")
        products = fetch_page(slug, page, session)

        if not products:
            log.info("   Empty page — stopping.")
            break

        added  = insert_products(conn, products, cat_name)
        total += added
        log.info(f"   Found {len(products)} | New: {added}")

        time.sleep(REQUEST_DELAY)

    return total


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB PriceRunner.se Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome124")
    conn    = sqlite3.connect(DB_PATH)
    summary = {"categories_scraped": 0, "total_added": 0, "errors": []}

    for cat in CATEGORIES:
        try:
            added = scrape_category(cat, session, conn)
            summary["total_added"]        += added
            summary["categories_scraped"] += 1
        except Exception as e:
            log.error(f"Error scraping {cat['name']}: {e}")
            summary["errors"].append({"category": cat["name"], "error": str(e)})
        time.sleep(REQUEST_DELAY)

    conn.close()
    session.close()

    log.info("=" * 60)
    log.info(
        f"Run complete — {summary['total_added']} new products added "
        f"across {summary['categories_scraped']} categories."
    )
    log.info("=" * 60)
    return summary


if __name__ == "__main__":
    result = run_scraper()
    if result.get("errors"):
        print(f"\n⚠  {len(result['errors'])} category error(s) — check pricerunner_se_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
