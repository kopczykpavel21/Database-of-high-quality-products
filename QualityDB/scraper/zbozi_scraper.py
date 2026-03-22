"""
Zbozi.cz scraper — finds top-rated products and adds them to QualityDB.

Zbozi.cz (owned by Seznam.cz) is Czech Republic's largest price comparison site.
It aggregates products from hundreds of Czech shops with verified buyer ratings.

This scraper uses Zbozi's internal JSON API (discovered from browser network requests):
  GET /api/v3/zbozi/zi-search?categoryPath=SLUG&limit=24&offset=N

API fields used:
  displayName     → Name
  url             → ProductURL  (e.g. https://www.zbozi.cz/nabidka/...)
  rating          → RecommendRate_pct  (0–100 percentage, NOT stars)
  experienceCount → ReviewsCount
  minPrice        → Price_CZK  (value is in halíř, divide by 100)

Dependencies (already installed):
    pip3 install curl_cffi

Usage:
    python3 scraper/zbozi_scraper.py
"""

import time
import logging
import sqlite3
import os
import sys
import json

try:
    from curl_cffi import requests
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install curl_cffi\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_RATING_PCT = 80    # Zbozi rating is 0–100%.  80 ≈ 4 stars
MIN_REVIEWS    = 5     # Minimum review count
STOP_BELOW_PCT = 75    # Stop category when rating drops below this
PAGE_SIZE      = 24    # Products per API request
MAX_PAGES      = 8     # Max pages per category
REQUEST_DELAY  = 1.5   # Seconds between requests

# ── Category slugs ────────────────────────────────────────────────────────────
# These map to the URL path on zbozi.cz, e.g.:
#   https://www.zbozi.cz/elektronika/audio/sluchatka/
# which also maps to the API parameter:
#   ?categoryPath=elektronika/audio/sluchatka/
CATEGORIES = [
    {"name": "Headphones",         "slug": "elektronika/audio/sluchatka/"},
    {"name": "Speakers",           "slug": "elektronika/audio/reproduktory/"},
    {"name": "Smartwatches",       "slug": "elektronika/chytre-hodinky-a-fitness/chytre-hodinky/"},
    {"name": "Coffee Machines",    "slug": "domaci-spotrebice/kuchyne/kavovary/"},
    {"name": "Vacuum Cleaners",    "slug": "domaci-spotrebice/uklid/vysavace/"},
    {"name": "Robot Vacuums",      "slug": "domaci-spotrebice/uklid/roboticke-vysavace/"},
    {"name": "Air Purifiers",      "slug": "domaci-spotrebice/klimatizace-a-vzduch/cisticky-vzduchu/"},
    {"name": "Kitchen Appliances", "slug": "domaci-spotrebice/kuchyne/male-kuchynske-spotrebice/"},
    {"name": "Mice",               "slug": "pocitace-a-it/prislusenstvi-k-pc/mysi/"},
    {"name": "Keyboards",          "slug": "pocitace-a-it/prislusenstvi-k-pc/klavesnice/"},
    {"name": "SSD",                "slug": "pocitace-a-it/uloziste/ssd-disky/"},
    {"name": "TVs",                "slug": "elektronika/televize/"},
]

API_BASE = "https://www.zbozi.cz/api/v3/zbozi/zi-search"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "zbozi_scraper.log"),
            encoding="utf-8"
        )
    ]
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.zbozi.cz/",
}


# ── Session ───────────────────────────────────────────────────────────────────

def warm_up_session(session) -> bool:
    """Visit Zbozi.cz homepage to obtain session cookies."""
    try:
        log.info("Warming up session (visiting Zbozi.cz)…")
        resp = session.get("https://www.zbozi.cz/", headers={
            **EXTRA_HEADERS, "Accept": "text/html,application/xhtml+xml,*/*"
        }, timeout=20)
        resp.raise_for_status()
        log.info(f"Session ready. Cookies: {len(session.cookies)}")
        time.sleep(1.5)
        return True
    except Exception as e:
        log.warning(f"Session warmup failed ({e}) — will try scraping anyway.")
        return False


# ── API fetching ──────────────────────────────────────────────────────────────

def fetch_page(slug: str, offset: int, session) -> dict:
    """
    Call the Zbozi JSON API for one page of products.
    Returns the parsed JSON dict, or empty dict on error.
    """
    url = f"{API_BASE}?categoryPath={slug}&limit={PAGE_SIZE}&offset={offset}"
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"  API request failed: {e}")
        return {}


# ── Database helpers ──────────────────────────────────────────────────────────

def load_existing_names(conn):
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def insert_products(conn, products, category):
    existing = load_existing_names(conn)
    inserted = 0
    for p in products:
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
                "zbozi",
            )
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat, session, conn):
    slug     = cat["slug"]
    cat_name = cat["name"]
    total_added = 0

    log.info(f"── {cat_name}  ({slug})")

    for page in range(MAX_PAGES):
        offset = page * PAGE_SIZE
        log.info(f"   Page {page + 1} (offset={offset})")

        data = fetch_page(slug, offset, session)
        items = data.get("products", [])

        if not items:
            log.info("   No products returned — stopping.")
            break

        products = []
        for item in items:
            name    = item.get("displayName", "").strip()
            url     = item.get("url", "")
            rating  = item.get("rating")      # 0–100 percentage
            reviews = item.get("experienceCount", 0)
            price_h = item.get("minPrice")    # in halíř (÷100 = Kč)

            if not name or rating is None:
                continue

            products.append({
                "Name":              name,
                "ProductURL":        url,
                "RecommendRate_pct": float(rating),
                "ReviewsCount":      reviews or 0,
                "Price_CZK":         price_h / 100.0 if price_h else None,
            })

        qualified = [
            p for p in products
            if p["RecommendRate_pct"] >= MIN_RATING_PCT
            and p["ReviewsCount"] >= MIN_REVIEWS
        ]

        rated = [p["RecommendRate_pct"] for p in products if p["RecommendRate_pct"] > 0]
        lowest = min(rated) if rated else 100.0

        added = insert_products(conn, qualified, cat_name)
        total_added += added
        log.info(
            f"   Found {len(products)} | Qualified: {len(qualified)} | "
            f"New: {added} | Lowest rating: {lowest:.0f}%"
        )

        # Stop if we've seen all products or quality drops
        total_docs = data.get("totalDocuments", 0)
        if offset + PAGE_SIZE >= total_docs:
            log.info("   Reached end of category.")
            break
        if lowest < STOP_BELOW_PCT:
            log.info(f"   Rating dropped to {lowest:.0f}% — stopping early.")
            break

        time.sleep(REQUEST_DELAY)

    return total_added


def run_scraper():
    log.info("=" * 60)
    log.info("QualityDB Zbozi.cz Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome120")
    warm_up_session(session)
    time.sleep(REQUEST_DELAY)

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
    log.info(f"Run complete — {summary['total_added']} new products added across {summary['categories_scraped']} categories.")
    log.info("=" * 60)
    return summary


if __name__ == "__main__":
    result = run_scraper()
    if result.get("errors"):
        print(f"\n⚠  {len(result['errors'])} category error(s) — check scraper/zbozi_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
