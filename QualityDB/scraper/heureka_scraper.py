"""
Heureka.cz scraper — finds top-rated products and adds them to QualityDB.

Dependencies (install once):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/heureka_scraper.py          # run once manually
    python3 scraper/scheduler.py                # run on daily schedule
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

# Allow running from any working directory
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraper.config import (
    MIN_RATING_PCT, MIN_REVIEWS, STOP_BELOW_PCT,
    REQUEST_DELAY, MAX_PAGES, CATEGORIES
)

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "scraper.log"),
            encoding="utf-8"
        )
    ]
)
log = logging.getLogger(__name__)

# curl_cffi handles headers automatically when impersonating Chrome —
# we only need to add language preference
EXTRA_HEADERS = {
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
}


# ── Parsing helpers ──────────────────────────────────────────────────────────

def parse_rating(text: str):
    """'92 %' → 92.0,  '100%' → 100.0,  None if unparseable."""
    if not text:
        return None
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%", text)
    return float(m.group(1).replace(",", ".")) if m else None


def parse_reviews(text: str) -> int:
    """'30 recenzí' → 30,   '162 hodnocení' → 162."""
    if not text:
        return 0
    m = re.search(r"(\d[\d\s]*)", text)
    return int(re.sub(r"\s", "", m.group(1))) if m else 0


def parse_price(text: str):
    """'711 – 1 419 Kč' → 711.0  (takes the lower bound)."""
    if not text:
        return None
    m = re.search(r"(\d[\d\s]*)", text)
    return float(re.sub(r"\s", "", m.group(1))) if m else None


def warm_up_session(session) -> bool:
    """
    Visit the Heureka homepage to obtain session cookies.
    curl_cffi impersonates Chrome's TLS fingerprint, so this should succeed
    even on Cloudflare-protected pages.
    """
    try:
        log.info("Warming up session (visiting homepage)…")
        resp = session.get(
            "https://www.heureka.cz/",
            headers=EXTRA_HEADERS,
            timeout=20
        )
        resp.raise_for_status()
        log.info(f"Session ready. Cookies obtained: {len(session.cookies)}")
        time.sleep(1.2)
        return True
    except Exception as e:
        log.warning(f"Session warmup failed ({e}) — will try scraping anyway.")
        return False


def scrape_page(url: str, session) -> list:
    """
    Fetch one Heureka listing page and return a list of product dicts.
    Returns an empty list on error.
    """
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select(".c-product")
    if not cards:
        log.debug(f"  No .c-product cards found at {url}")
        return []

    products = []
    for card in cards:
        # Name & URL
        name_el = card.select_one(".c-product__link")
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        overlay = card.select_one(".c-product__overlay-link")
        product_url = name_el.get("href") or (overlay.get("href") if overlay else "")

        # Rating %
        rating_el = card.select_one(".c-rating-widget__value")
        rating_pct = parse_rating(rating_el.get_text(strip=True) if rating_el else "")

        # Review count — span that contains "recenzí" or "hodnocení"
        review_span = next(
            (s for s in card.find_all("span")
             if "recenz" in s.get_text().lower() or "hodnocen" in s.get_text().lower()),
            None
        )
        reviews = parse_reviews(review_span.get_text(strip=True) if review_span else "")

        # Price (lowest offer)
        price_el = card.select_one(".c-product__price--bold, .c-product__price")
        price = parse_price(price_el.get_text(strip=True) if price_el else "")

        products.append({
            "Name":              name,
            "ProductURL":        product_url,
            "RecommendRate_pct": rating_pct,
            "ReviewsCount":      reviews,
            "Price_CZK":         price,
        })

    return products


# ── Database helpers ─────────────────────────────────────────────────────────

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
                "heureka",
            )
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn: sqlite3.Connection) -> int:
    base_url    = cat["url"].rstrip("/")
    cat_name    = cat["name"]
    total_added = 0

    log.info(f"── {cat_name}  ({base_url})")

    page = 1
    while True:
        url = f"{base_url}/?sort=rating" if page == 1 else f"{base_url}/?sort=rating&f={page}"

        log.info(f"   Page {page}: {url}")
        products = scrape_page(url, session)

        if not products:
            log.info("   No products returned — stopping.")
            break

        qualified = [
            p for p in products
            if (p["RecommendRate_pct"] or 0) >= MIN_RATING_PCT
            and p["ReviewsCount"] >= MIN_REVIEWS
        ]

        # Only consider rated products for the stop check — unrated ones (None)
        # would falsely trigger an early stop if counted as 0
        rated = [p["RecommendRate_pct"] for p in products if p["RecommendRate_pct"] is not None]
        lowest_on_page = min(rated) if rated else 100

        added = insert_products(conn, qualified, cat_name)
        total_added += added
        log.info(
            f"   Found {len(products)} | Qualified: {len(qualified)} | "
            f"New in DB: {added} | Lowest rating: {lowest_on_page}%"
        )

        if lowest_on_page < STOP_BELOW_PCT:
            log.info(f"   Rating dropped to {lowest_on_page}% — stopping early.")
            break

        if MAX_PAGES and page >= MAX_PAGES:
            log.info(f"   Reached MAX_PAGES={MAX_PAGES} limit.")
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    return total_added


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Heureka Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}. Run load_data.py first.")
        return {"error": "database_not_found"}

    # impersonate="chrome120" makes curl_cffi send Chrome's exact TLS fingerprint
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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check scraper/scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
