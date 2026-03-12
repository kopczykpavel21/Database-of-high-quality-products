"""
Amazon.de scraper — finds top-rated products and adds them to QualityDB.

Dependencies (already installed from Heureka scraper):
    pip3 install curl_cffi beautifulsoup4

Usage:
    python3 scraper/amazon_scraper.py          # run once manually
    python3 scraper/scheduler.py               # run on daily schedule (alongside Heureka)

Notes:
    - Uses curl_cffi with Chrome TLS impersonation to bypass bot detection
    - Searches Amazon.de sorted by review rank, filtered to 4+ stars
    - Amazon uses German number format: "4,5 von 5 Sternen", "12.345 Bewertungen"
    - Star ratings are converted to recommend % (4.5 stars = 90%)
    - REQUEST_DELAY is higher than Heureka — Amazon is more sensitive
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

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Amazon-specific thresholds ───────────────────────────────────────────────
MIN_STARS     = 4.0    # Minimum star rating (out of 5).  4.0 = solid quality
MIN_REVIEWS   = 50     # Minimum reviews — Amazon has many low-review products
STOP_BELOW    = 3.8    # Stop scraping a category when stars drop below this
MAX_PAGES     = 5      # Max pages per category (20 products/page on Amazon)
REQUEST_DELAY = 3.0    # Seconds between requests — be polite, Amazon is sensitive

# ── Category URLs ─────────────────────────────────────────────────────────────
# Each URL searches Amazon.de sorted by "review-rank" (most reviewed first),
# filtered to 4+ stars (rh=p_72:419122031).
# Focused on small electronics and home appliances to match existing DB.
CATEGORIES = [
    # ── Audio ─────────────────────────────────────────────────────────────────
    {"name": "Headphones",          "url": "https://www.amazon.de/s?k=kopfh%C3%B6rer&s=review-rank&rh=p_72%3A419122031"},
    {"name": "Speakers",            "url": "https://www.amazon.de/s?k=bluetooth+lautsprecher&s=review-rank&rh=p_72%3A419122031"},

    # ── Wearables ─────────────────────────────────────────────────────────────
    {"name": "Smartwatches",        "url": "https://www.amazon.de/s?k=smartwatch&s=review-rank&rh=p_72%3A419122031"},

    # ── Home appliances ───────────────────────────────────────────────────────
    {"name": "Coffee Machines",     "url": "https://www.amazon.de/s?k=kaffeemaschine&s=review-rank&rh=p_72%3A419122031"},
    {"name": "Vacuum Cleaners",     "url": "https://www.amazon.de/s?k=staubsauger&s=review-rank&rh=p_72%3A419122031"},
    {"name": "Air Purifiers",       "url": "https://www.amazon.de/s?k=luftreiniger&s=review-rank&rh=p_72%3A419122031"},
    {"name": "Kitchen Appliances",  "url": "https://www.amazon.de/s?k=k%C3%BCchenger%C3%A4te&s=review-rank&rh=p_72%3A419122031"},
    {"name": "Robot Vacuums",       "url": "https://www.amazon.de/s?k=saugroboter&s=review-rank&rh=p_72%3A419122031"},

    # ── Computer peripherals ──────────────────────────────────────────────────
    {"name": "Mice",                "url": "https://www.amazon.de/s?k=computer+maus&s=review-rank&rh=p_72%3A419122031"},
    {"name": "Keyboards",           "url": "https://www.amazon.de/s?k=tastatur&s=review-rank&rh=p_72%3A419122031"},
    {"name": "SSD",                 "url": "https://www.amazon.de/s?k=ssd+festplatte&s=review-rank&rh=p_72%3A419122031"},

    # ── TVs ───────────────────────────────────────────────────────────────────
    {"name": "TVs",                 "url": "https://www.amazon.de/s?k=fernseher&s=review-rank&rh=p_72%3A419122031"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "amazon_scraper.log"),
            encoding="utf-8"
        )
    ]
)
log = logging.getLogger(__name__)

EXTRA_HEADERS = {
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
}


# ── Parsing helpers ───────────────────────────────────────────────────────────

def parse_stars(text: str):
    """
    Parse German Amazon star ratings.
    '4,5 von 5 Sternen' → 4.5
    '4.5 out of 5 stars' → 4.5
    """
    if not text:
        return None
    # German format uses comma as decimal separator: "4,5 von 5"
    m = re.search(r"(\d+)[,.](\d+)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    # Whole number like "4 von 5"
    m = re.search(r"(\d+)\s+von", text)
    if m:
        return float(m.group(1))
    return None


def parse_review_count(text: str) -> int:
    """
    Parse German Amazon review counts.
    '12.345 Bewertungen' → 12345  (dot is thousands separator in German)
    '(1.234)'            → 1234
    '12,345'             → 12345
    """
    if not text:
        return 0
    # Remove everything except digits — dots and commas are just separators
    digits_only = re.sub(r"[^\d]", "", text)
    return int(digits_only) if digits_only else 0


def is_captcha_page(html: str) -> bool:
    """Detect if Amazon returned a CAPTCHA / robot-check page."""
    indicators = [
        "api-services-support@amazon",
        "robot check",
        "captcha",
        "Enter the characters you see below",
        "Geben Sie die Zeichen ein",
        "Sorry, we just need to make sure",
        "Tut uns leid",
    ]
    lower = html.lower()
    return any(ind.lower() in lower for ind in indicators)


# ── Session ───────────────────────────────────────────────────────────────────

def warm_up_session(session) -> bool:
    """Visit Amazon.de homepage to pick up session cookies."""
    try:
        log.info("Warming up session (visiting Amazon.de)…")
        resp = session.get("https://www.amazon.de/", headers=EXTRA_HEADERS, timeout=20)
        resp.raise_for_status()
        if is_captcha_page(resp.text):
            log.warning("CAPTCHA detected on homepage — scraping may be limited.")
            return False
        log.info(f"Session ready. Cookies obtained: {len(session.cookies)}")
        time.sleep(2.5)
        return True
    except Exception as e:
        log.warning(f"Session warmup failed ({e}) — will try scraping anyway.")
        return False


# ── Page scraping ─────────────────────────────────────────────────────────────

def scrape_page(url: str, session) -> list:
    """Fetch one Amazon search results page and return a list of product dicts."""
    try:
        resp = session.get(url, headers=EXTRA_HEADERS, timeout=25)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

    if is_captcha_page(resp.text):
        log.warning("  ⚠ CAPTCHA detected — stopping this category.")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Amazon search result cards — each has data-component-type="s-search-result"
    cards = soup.select("div[data-component-type='s-search-result']")
    if not cards:
        log.debug(f"  No product cards found at {url}")
        return []

    products = []
    for card in cards:
        asin = card.get("data-asin", "")
        if not asin:
            continue  # Skip ad placeholders

        # ── Product name ──────────────────────────────────────────────────────
        title_el = card.select_one("h2 span")
        if not title_el:
            title_el = card.select_one("span.a-size-medium")
        if not title_el:
            continue
        name = title_el.get_text(strip=True)
        if not name:
            continue

        # ── Product URL ───────────────────────────────────────────────────────
        link_el = card.select_one("h2 a")
        product_url = ""
        if link_el and link_el.get("href"):
            href = link_el["href"]
            # href is usually /dp/ASIN/... — make it absolute
            product_url = f"https://www.amazon.de{href}" if href.startswith("/") else href
            # Clean up tracking params — keep only up to /dp/ASIN/
            m = re.match(r"(https://www\.amazon\.de/[^?]+)", product_url)
            if m:
                product_url = m.group(1)

        # ── Star rating ───────────────────────────────────────────────────────
        # Stored in <span class="a-icon-alt">4,5 von 5 Sternen</span>
        stars_el = card.select_one("span.a-icon-alt")
        stars = parse_stars(stars_el.get_text(strip=True) if stars_el else "")

        if stars is None:
            continue  # No rating — skip unrated products

        # ── Review count ──────────────────────────────────────────────────────
        review_count = 0

        # Try aria-label on a span: e.g. aria-label="12.345 Bewertungen"
        review_span = card.select_one("span[aria-label]")
        for span in card.find_all("span", attrs={"aria-label": True}):
            label = span.get("aria-label", "")
            if "bewertung" in label.lower() or "rezension" in label.lower():
                review_count = parse_review_count(label)
                break

        # Fallback: link to customer reviews section
        if review_count == 0:
            for a in card.find_all("a", href=True):
                if "customerReviews" in a["href"] or "customer-reviews" in a["href"]:
                    review_count = parse_review_count(a.get_text(strip=True))
                    break

        # Convert stars to recommend % for consistency with rest of DB
        recommend_pct = round((stars / 5.0) * 100, 1)

        products.append({
            "Name":              name,
            "ProductURL":        product_url,
            "AvgStarRating":     stars,
            "RecommendRate_pct": recommend_pct,
            "ReviewsCount":      review_count,
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
               (Name, Category, ProductURL, AvgStarRating,
                RecommendRate_pct, ReviewsCount, source)
               VALUES (?,?,?,?,?,?,?)""",
            (
                p["Name"],
                category,
                p.get("ProductURL", ""),
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
    base_url    = cat["url"]
    cat_name    = cat["name"]
    total_added = 0

    log.info(f"── {cat_name}  ({base_url})")

    for page in range(1, MAX_PAGES + 1):
        url = base_url if page == 1 else f"{base_url}&page={page}"

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
    log.info("QualityDB Amazon.de Scraper — starting run")
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
        # Extra pause between categories — Amazon watches for rapid sequential requests
        time.sleep(REQUEST_DELAY * 2)

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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check scraper/amazon_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
