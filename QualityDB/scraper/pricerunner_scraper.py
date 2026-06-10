"""
Pricerunner scraper — finds top-rated products and adds them to QualityDB.

Pricerunner is Scandinavia's largest price-comparison portal, operating in
Denmark (pricerunner.dk), Sweden (pricerunner.se), and Norway.  It carries
verified buyer ratings (1–5 stars) for hundreds of product categories.

This scraper targets the Danish site (pricerunner.dk) but the category slug
structure is identical across the Nordic markets; to switch to Sweden change
BASE_URL to "https://www.pricerunner.se".

Scraping approach:
  - Category pages sorted by best rated (?sortByPreset=BEST_RATED)
  - HTML parsed with BeautifulSoup (server-side rendered product cards)
  - Stars (1–5) converted to recommend %: (stars / 5) × 100
  - Prices are in DKK, stored as-is in Price_CZK column

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/pricerunner_scraper.py
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

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_STARS     = 4.0    # Minimum average star rating (out of 5)
MIN_REVIEWS   = 0      # Pricerunner doesn't show review count on listing pages
STOP_BELOW    = 3.8    # Stop category when stars drop below this
MAX_PAGES     = 6      # Max pages per category
REQUEST_DELAY = 2.5    # Seconds between requests (site is rate-sensitive)

# ── Category list ─────────────────────────────────────────────────────────────
# Pricerunner category URLs: https://www.pricerunner.dk/cl/{id}/{name}
# IDs verified live May 2026 by checking /pl/{id}-... links in search results.
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

BASE_URL = "https://www.pricerunner.dk"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "pricerunner_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "da-DK,da;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.pricerunner.dk/",
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _parse_stars(text: str):
    """'4,3 ud af 5' or '4.3' → 4.3.  Returns None if unparseable."""
    if not text:
        return None
    m = re.search(r"(\d)[,.](\d)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d+)", text)
    v = float(m.group(1)) if m else None
    return v if v and v <= 5 else None


def _parse_reviews(text: str) -> int:
    """'(1.234 anmeldelser)' → 1234."""
    if not text:
        return 0
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else 0


def _parse_price_dkk(text: str):
    """'1.249 kr.' or '1249,00' → 1249.0 (stored in Price_CZK as DKK value)."""
    if not text:
        return None
    clean = re.sub(r"[^\d,.]", "", text).replace(",", ".")
    # Handle European thousand-separator: "1.249" means 1249
    parts = clean.split(".")
    if len(parts) == 2 and len(parts[1]) == 3:
        clean = "".join(parts)
    m = re.search(r"(\d+(?:\.\d+)?)", clean)
    return float(m.group(1)) if m else None


# ── Fetch & parse ─────────────────────────────────────────────────────────────

def fetch_page(slug: str, page: int, session) -> list[dict]:
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
    # Structure (verified May 2026):
    #   parts[0]  = "N+ overvåger" or absent   (watcher count)
    #   parts[1]  = product name                (longest text node)
    #   parts[?]  = "X,X"                       (rating, if reviews exist)
    #   parts[?]  = specs string                (RAM, SSD, etc.)
    #   parts[-?] = "X.XXX kr."                 (price)
    links = soup.select("a[href*='/pl/']")

    if not links:
        log.debug("  No product links found — possibly last page or layout change.")
        return []

    for link in links:
        href = link.get("href", "")
        url_product = (BASE_URL + href) if href.startswith("/") else href

        parts = [t.strip() for t in link.stripped_strings if t.strip()]

        # Name: first part that's longer than 10 chars and not "N+ overvåger"
        name = ""
        for p in parts:
            if len(p) > 10 and "overvåger" not in p and "butikker" not in p:
                name = p
                break

        # Rating: standalone "X,X" or "X.X" token (exactly 3 chars like "4,8")
        stars = None
        for p in parts:
            if re.match(r"^\d[,.]\d$", p):
                stars = float(p.replace(",", "."))
                break

        # Price: token ending with "kr."
        price_dkk = None
        for p in parts:
            if "kr." in p:
                price_dkk = _parse_price_dkk(p)
                break

        if name:
            products.append({
                "Name":              name,
                "ProductURL":        url_product,
                "Stars":             stars,
                "RecommendRate_pct": round((stars / 5.0) * 100, 1) if stars else None,
                "ReviewsCount":      0,  # not shown on listing page
                "Price_CZK":         price_dkk,
            })

    return products


# ── Database helpers ──────────────────────────────────────────────────────────

def load_existing_names(conn) -> set:
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def insert_products(conn, products: list[dict], category: str) -> int:
    from scraper.snapshots import ensure_snapshot_table, record_snapshot
    ensure_snapshot_table(conn)
    existing = load_existing_names(conn)
    inserted = 0
    for p in products:
        record_snapshot(conn, p.get("ProductURL", ""), "pricerunner", p, country="DK")
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
                "pricerunner",
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

        qualified = [
            p for p in products
            if p.get("Stars") is not None
            and p["Stars"] >= MIN_STARS
            and p["ReviewsCount"] >= MIN_REVIEWS
        ]

        stars_vals = [p["Stars"] for p in products if p.get("Stars")]
        lowest = min(stars_vals) if stars_vals else 5.0

        added = insert_products(conn, qualified, cat_name)
        total += added
        log.info(
            f"   Found {len(products)} | Qualified: {len(qualified)} | "
            f"New: {added} | Lowest ★: {lowest:.1f}"
        )

        if lowest < STOP_BELOW:
            log.info(f"   Stars dropped to {lowest:.1f} — stopping early.")
            break

        time.sleep(REQUEST_DELAY)

    return total


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Pricerunner.dk Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    session = requests.Session(impersonate="chrome120")

    try:
        log.info("Warming up session (visiting Pricerunner.dk)…")
        session.get(
            BASE_URL,
            headers={**EXTRA_HEADERS, "Accept": "text/html,*/*"},
            timeout=20,
        )
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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check pricerunner_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
