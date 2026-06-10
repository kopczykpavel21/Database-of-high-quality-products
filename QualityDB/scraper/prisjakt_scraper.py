"""
Prisjakt.nu scraper — finds popular products and adds them to QualityDB.

Prisjakt.nu is Sweden's leading price-comparison portal (sister site of
PriceRunner, operated separately).  Products carry community star ratings
(1–5 scale shown as "X.X av 5 stjärnor" in aria-labels), though many
products have no ratings — those are stored with NULL RecommendRate_pct.

Scraping approach:
  - Category pages at /c/{slug} (default sort = popularity/sales rank)
  - 3 pages per category (~52 products/page) via ?offset=N pagination
  - No minimum rating threshold — all products stored
  - HTML parsed with BeautifulSoup from server-rendered pages
  - Ratings from aria-label attributes: "X.X av 5 stjärnor"
  - Prices in SEK, stored as-is in Price_CZK column
  - Warm-up homepage visit required for cookies

Pagination: offset = (page - 1) * 52
  Page 1: /c/{slug}              (no offset param needed)
  Page 2: /c/{slug}?offset=52
  Page 3: /c/{slug}?offset=104

Category slugs from prisjakt.nu homepage navigation, verified May 2026.

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/prisjakt_scraper.py
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
MAX_PAGES     = 3      # Pages per category (~52 products/page)
PAGE_SIZE     = 52     # Products per page (Prisjakt default)
REQUEST_DELAY = 2.0    # Seconds between requests

BASE_URL = "https://www.prisjakt.nu"

# ── Category list ─────────────────────────────────────────────────────────────
# Slugs from prisjakt.nu homepage navigation, verified May 2026.
# URL pattern: /c/{slug}?offset={N}
CATEGORIES = [
    {"name": "TVs",              "slug": "tv"},
    {"name": "Smartphones",      "slug": "mobiltelefoner"},
    {"name": "Laptops",          "slug": "laptops-barbara-datorer"},
    {"name": "Tablets",          "slug": "surfplattor"},
    {"name": "Headphones",       "slug": "horlurar"},
    {"name": "Monitors",         "slug": "datorskarmar"},
    {"name": "Speakers",         "slug": "mobilhogtalare"},
    {"name": "Smartwatches",     "slug": "smartwatch"},
    {"name": "Keyboards",        "slug": "tangentbord"},
    {"name": "Mice",             "slug": "datormus"},
    {"name": "Washing Machines", "slug": "tvattmaskiner"},
    {"name": "Dishwashers",      "slug": "diskmaskiner"},
    {"name": "Refrigerators",    "slug": "kylskap"},
    {"name": "Coffee Machines",  "slug": "espressomaskiner"},
    {"name": "Vacuum Cleaners",  "slug": "dammsugare"},
    {"name": "Robot Vacuums",    "slug": "robotdammsugare"},
    {"name": "Air Purifiers",    "slug": "luftrenare"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "prisjakt_scraper.log"),
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

def _parse_stars(aria_label: str):
    """'2.3 av 5 stjärnor' → 2.3.  Returns None if unparseable."""
    m = re.search(r"([\d.]+)\s*av\s*5", aria_label)
    return float(m.group(1)) if m else None


def _parse_reviews(rating_div_text: str) -> int:
    """'(42)' or just '42' inside rating div → 42."""
    m = re.search(r"\d+", rating_div_text)
    return int(m.group(0)) if m else 0


def _parse_price_sek(text: str):
    """'24 191 kr' → 24191.0."""
    m = re.search(r"([\d\s]+)\s*kr", text)
    return float(re.sub(r"\s", "", m.group(1))) if m else None


# ── Fetch & parse ─────────────────────────────────────────────────────────────

def fetch_page(slug: str, page: int, session) -> list:
    """Fetch one Prisjakt category page and return list of product dicts."""
    if page == 1:
        url = f"{BASE_URL}/c/{slug}"
    else:
        offset = (page - 1) * PAGE_SIZE
        url = f"{BASE_URL}/c/{slug}?offset={offset}"

    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    articles = soup.select("article")
    if not articles:
        log.debug(f"  No article elements found at {url}")
        return []

    products = []
    for art in articles:
        # Name
        name_el = art.select_one("[data-test='ProductName']")
        if not name_el:
            continue
        name = name_el.get_text(strip=True)

        # URL
        link_el = art.select_one("a[data-test='InternalLink']")
        href = link_el.get("href", "") if link_el else ""
        url_product = (BASE_URL + href) if href.startswith("/") else href

        # Rating: aria-label="X.X av 5 stjärnor"
        rating_el = art.select_one("[aria-label*='av 5']")
        stars = None
        reviews = 0
        if rating_el:
            stars = _parse_stars(rating_el.get("aria-label", ""))
            reviews = _parse_reviews(rating_el.get_text(strip=True))

        pct = round((stars / 5.0) * 100, 1) if stars else None

        # Price
        price_el = art.select_one("[class*='Price'], [class*='price']")
        price_sek = _parse_price_sek(price_el.get_text(strip=True)) if price_el else None

        if name:
            products.append({
                "Name":              name,
                "ProductURL":        url_product,
                "Stars":             stars,
                "RecommendRate_pct": pct,
                "ReviewsCount":      reviews,
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
        record_snapshot(conn, p.get("ProductURL", ""), "prisjakt", p, country="SE")
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
                "prisjakt",
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

    log.info(f"── {cat_name}  (/c/{slug})")

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
    log.info("QualityDB Prisjakt.nu Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome124")

    # Warm-up: visit homepage for cookies
    try:
        log.info("Warming up session (visiting prisjakt.nu)…")
        session.get(BASE_URL + "/", headers=EXTRA_HEADERS, timeout=20)
        time.sleep(2.0)
    except Exception as e:
        log.warning(f"Warm-up failed ({e}) — continuing anyway.")

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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check prisjakt_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
