"""
Mall.cz scraper — finds top-rated products and adds them to QualityDB.

Mall.cz is one of the largest Czech e-shops. Products are sorted by rating
and filtered to only include high-quality items.

Dependencies (already installed):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/mall_scraper.py          # run once manually
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
MIN_STARS     = 4.0    # Minimum star rating (out of 5)
MIN_REVIEWS   = 5      # Minimum review count
STOP_BELOW    = 3.8    # Stop category when stars drop below this
MAX_PAGES     = 8      # Max pages per category
REQUEST_DELAY = 2.0    # Seconds between requests

# ── Category URLs ─────────────────────────────────────────────────────────────
# Mall.cz category pages, sorted by rating (?sort=rating)
CATEGORIES = [
    # ── Audio ──────────────────────────────────────────────────────────────────
    {"name": "Headphones",          "url": "https://www.mall.cz/sluchatka"},
    {"name": "Speakers",            "url": "https://www.mall.cz/reproduktory"},

    # ── Wearables ──────────────────────────────────────────────────────────────
    {"name": "Smartwatches",        "url": "https://www.mall.cz/chytre-hodinky"},

    # ── Home appliances ────────────────────────────────────────────────────────
    {"name": "Coffee Machines",     "url": "https://www.mall.cz/kavovary"},
    {"name": "Vacuum Cleaners",     "url": "https://www.mall.cz/vysavace"},
    {"name": "Robot Vacuums",       "url": "https://www.mall.cz/roboticke-vysavace"},
    {"name": "Air Purifiers",       "url": "https://www.mall.cz/cisticky-vzduchu"},
    {"name": "Kitchen Appliances",  "url": "https://www.mall.cz/male-kuchynske-spotrebice"},

    # ── Computer peripherals ───────────────────────────────────────────────────
    {"name": "Mice",                "url": "https://www.mall.cz/mysi"},
    {"name": "Keyboards",           "url": "https://www.mall.cz/klavesnice"},
    {"name": "SSD",                 "url": "https://www.mall.cz/ssd-disky"},

    # ── TVs ────────────────────────────────────────────────────────────────────
    {"name": "TVs",                 "url": "https://www.mall.cz/televize"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "mall_scraper.log"),
            encoding="utf-8"
        )
    ]
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Referer": "https://www.mall.cz/",
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_stars(text: str):
    """
    Parse Mall.cz star ratings.
    'Hodnocení: 4.5 z 5' → 4.5
    '4,5' → 4.5
    '90 %' → 4.5  (converts percentage to stars)
    """
    if not text:
        return None
    # Percentage format (convert to stars)
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%", text)
    if m:
        pct = float(m.group(1).replace(",", "."))
        return round(pct / 20.0, 1)  # 100% = 5 stars
    # Star format "4,5 z 5" or "4.5"
    m = re.search(r"(\d+)[,.](\d+)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d+)\s*(?:z|/|out)\s*5", text)
    if m:
        return float(m.group(1))
    return None


def parse_reviews(text: str) -> int:
    """
    '(123 hodnocení)' → 123
    '45 recenzí'      → 45
    """
    if not text:
        return 0
    m = re.search(r"(\d[\d\s]*)", text)
    return int(re.sub(r"\s", "", m.group(1))) if m else 0


def parse_price(text: str):
    """'1 299 Kč' → 1299.0"""
    if not text:
        return None
    m = re.search(r"(\d[\d\s]*)", text)
    return float(re.sub(r"\s", "", m.group(1))) if m else None


# ── Session ───────────────────────────────────────────────────────────────────

def warm_up_session(session) -> bool:
    """Visit Mall.cz homepage to obtain session cookies."""
    try:
        log.info("Warming up session (visiting Mall.cz)…")
        resp = session.get("https://www.mall.cz/", headers=EXTRA_HEADERS, timeout=20)
        resp.raise_for_status()
        log.info(f"Session ready. Cookies: {len(session.cookies)}")
        time.sleep(1.5)
        return True
    except Exception as e:
        log.warning(f"Session warmup failed ({e}) — will try scraping anyway.")
        return False


# ── Page scraping ─────────────────────────────────────────────────────────────

def scrape_page(url: str, session) -> list:
    """Fetch one Mall.cz category page and return product dicts."""
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Mall.cz product cards — try multiple selector patterns
    cards = (
        soup.select("div.product-card") or
        soup.select("div.b-product-item") or
        soup.select("article.product") or
        soup.select("[class*='product-card']") or
        soup.select("[data-product-id]")
    )

    if not cards:
        log.debug(f"  No product cards found at {url}")
        # Log a snippet of the HTML to help debug if needed
        log.debug(f"  Page title: {soup.title.string if soup.title else 'N/A'}")
        return []

    products = []
    for card in cards:
        # ── Name ──────────────────────────────────────────────────────────────
        name_el = (
            card.select_one("h2 a") or
            card.select_one("h3 a") or
            card.select_one("[class*='product-title'] a") or
            card.select_one("[class*='product-name'] a") or
            card.select_one("a[class*='title']")
        )
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        if not name:
            continue

        # ── URL ───────────────────────────────────────────────────────────────
        href = name_el.get("href", "")
        product_url = f"https://www.mall.cz{href}" if href.startswith("/") else href

        # ── Star rating ───────────────────────────────────────────────────────
        # Mall.cz typically shows rating as a number or aria-label on a star widget
        stars = None
        rating_el = (
            card.select_one("[class*='rating'][aria-label]") or
            card.select_one("[class*='star'][title]") or
            card.select_one("[class*='rating']") or
            card.select_one("[itemprop='ratingValue']")
        )
        if rating_el:
            # Try aria-label, title, content, then text
            for attr in ("aria-label", "title", "content"):
                val = rating_el.get(attr, "")
                stars = parse_stars(val)
                if stars is not None:
                    break
            if stars is None:
                stars = parse_stars(rating_el.get_text(strip=True))

        if stars is None:
            continue  # Skip unrated products

        # ── Review count ──────────────────────────────────────────────────────
        review_count = 0
        review_el = next(
            (s for s in card.find_all(["span", "a", "div"])
             if any(kw in s.get_text().lower()
                    for kw in ["hodnocen", "recenz", "reviews", "bewertung"])),
            None
        )
        if review_el:
            review_count = parse_reviews(review_el.get_text(strip=True))

        # ── Price ─────────────────────────────────────────────────────────────
        price_el = card.select_one("[class*='price']")
        price = parse_price(price_el.get_text(strip=True) if price_el else "")

        # Convert stars to recommend %
        recommend_pct = round((stars / 5.0) * 100, 1)

        products.append({
            "Name":              name,
            "ProductURL":        product_url,
            "AvgStarRating":     stars,
            "RecommendRate_pct": recommend_pct,
            "ReviewsCount":      review_count,
            "Price_CZK":         price,
        })

    return products


# ── Database helpers ──────────────────────────────────────────────────────────

def load_existing_names(conn: sqlite3.Connection) -> set:
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def insert_products(conn: sqlite3.Connection, products: list, category: str) -> int:
    existing = load_existing_names(conn)
    inserted = 0
    for p in products:
        key = p["Name"].lower()
        if key in existing:
            continue
        conn.execute(
            """INSERT INTO products
               (Name, Category, ProductURL, Price_CZK, AvgStarRating,
                RecommendRate_pct, ReviewsCount, source)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                p["Name"],
                category,
                p.get("ProductURL", ""),
                p.get("Price_CZK"),
                p.get("AvgStarRating"),
                p.get("RecommendRate_pct"),
                p.get("ReviewsCount", 0),
                "scraper",
            )
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn: sqlite3.Connection) -> int:
    base_url    = cat["url"].rstrip("/")
    cat_name    = cat["name"]
    total_added = 0

    log.info(f"── {cat_name}  ({base_url})")

    for page in range(1, MAX_PAGES + 1):
        # Mall.cz pagination: ?page=2, ?page=3, ...
        # Sort by rating: ?sort=rating (append to base URL)
        if page == 1:
            url = f"{base_url}?sort=rating"
        else:
            url = f"{base_url}?sort=rating&page={page}"

        log.info(f"   Page {page}: {url}")
        products = scrape_page(url, session)

        if not products:
            log.info("   No products returned — stopping.")
            break

        qualified = [
            p for p in products
            if (p.get("AvgStarRating") or 0) >= MIN_STARS
            and p["ReviewsCount"] >= MIN_REVIEWS
        ]

        rated = [p["AvgStarRating"] for p in products if p.get("AvgStarRating") is not None]
        lowest_stars = min(rated) if rated else 5.0

        added = insert_products(conn, qualified, cat_name)
        total_added += added
        log.info(
            f"   Found {len(products)} | Qualified: {len(qualified)} | "
            f"New in DB: {added} | Lowest stars: {lowest_stars:.1f}"
        )

        if lowest_stars < STOP_BELOW:
            log.info(f"   Stars dropped to {lowest_stars:.1f} — stopping early.")
            break

        time.sleep(REQUEST_DELAY)

    return total_added


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Mall.cz Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}. Run load_data.py first.")
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
    log.info(
        f"Run complete — {summary['total_added']} new products added "
        f"across {summary['categories_scraped']} categories."
    )
    log.info("=" * 60)
    return summary


if __name__ == "__main__":
    result = run_scraper()
    if result.get("errors"):
        print(f"\n⚠  {len(result['errors'])} category error(s) — check scraper/mall_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
