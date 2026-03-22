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
import random
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
MIN_STARS            = 4.0   # Minimum star rating (out of 5)
MIN_REVIEWS          = 30    # Lowered — review-rank sort puts most-reviewed first anyway
STOP_BELOW           = 3.8   # Stop scraping a category when stars drop below this
MAX_PAGES            = 8     # Max pages per category (20 products/page on Amazon)
REQUEST_DELAY        = 4.0   # Base seconds between page requests
SESSION_REFRESH_EVERY = 5    # Refresh session cookies every N categories

# ── Base URL builder ──────────────────────────────────────────────────────────
# s=review-rank  → sorted by most reviews first (maximises high-review products)
# rh=p_72:419122031 → filtered to 4+ stars
_BASE = "https://www.amazon.de/s?s=review-rank&rh=p_72%3A419122031&k="

def _url(keyword: str) -> str:
    return _BASE + keyword.replace(" ", "+")

# ── Category list ─────────────────────────────────────────────────────────────
# German keywords used — Amazon.de indexes primarily German product titles.
# Categories mirror the expanded Heureka list for cross-source coverage.
CATEGORIES = [
    # ── Televize & Obraz ──────────────────────────────────────────────────────
    {"name": "TVs",                 "url": _url("fernseher")},
    {"name": "Monitors",            "url": _url("computer monitor")},
    {"name": "Projectors",          "url": _url("beamer projektor")},

    # ── Audio ─────────────────────────────────────────────────────────────────
    {"name": "Headphones",          "url": _url("kopfh\u00f6rer")},
    {"name": "Speakers",            "url": _url("bluetooth lautsprecher")},
    {"name": "Soundbars",           "url": _url("soundbar")},
    {"name": "Earbuds",             "url": _url("kabellose ohrh\u00f6rer")},

    # ── Mobily & Tablety ──────────────────────────────────────────────────────
    {"name": "Mobile Phones",       "url": _url("smartphone")},
    {"name": "Tablets",             "url": _url("tablet")},
    {"name": "Phone Cases",         "url": _url("handyh\u00fclle")},
    {"name": "Phone Chargers",      "url": _url("ladeger\u00e4t usb c")},

    # ── Počítače & Notebooky ──────────────────────────────────────────────────
    {"name": "Laptops",             "url": _url("laptop notebook")},
    {"name": "Mice",                "url": _url("computer maus")},
    {"name": "Keyboards",           "url": _url("tastatur")},
    {"name": "Webcams",             "url": _url("webcam")},
    {"name": "Routers",             "url": _url("wlan router")},
    {"name": "Laptop Accessories",  "url": _url("laptop zubeh\u00f6r")},

    # ── Úložiště ──────────────────────────────────────────────────────────────
    {"name": "SSD",                 "url": _url("ssd festplatte")},
    {"name": "HDD",                 "url": _url("externe festplatte")},
    {"name": "USB Flash Drives",    "url": _url("usb stick")},
    {"name": "RAM",                 "url": _url("arbeitsspeicher ram")},
    {"name": "SD Cards",            "url": _url("speicherkarte sd")},

    # ── Chytré hodinky & fitness ──────────────────────────────────────────────
    {"name": "Smartwatches",        "url": _url("smartwatch")},
    {"name": "Fitness Trackers",    "url": _url("fitness tracker armband")},

    # ── Foto & Video ──────────────────────────────────────────────────────────
    {"name": "Digital Cameras",     "url": _url("digitalkamera")},
    {"name": "Action Cameras",      "url": _url("action kamera")},
    {"name": "Camera Accessories",  "url": _url("kamera zubeh\u00f6r stativ")},

    # ── Domácí spotřebiče ─────────────────────────────────────────────────────
    {"name": "Vacuum Cleaners",     "url": _url("staubsauger")},
    {"name": "Robot Vacuums",       "url": _url("saugroboter")},
    {"name": "Coffee Machines",     "url": _url("kaffeemaschine")},
    {"name": "Coffee Pods",         "url": _url("kapselmaschine")},
    {"name": "Air Purifiers",       "url": _url("luftreiniger")},
    {"name": "Air Fryers",          "url": _url("hei\u00dfluftfritteuse")},
    {"name": "Blenders",            "url": _url("standmixer smoothie")},
    {"name": "Kettles",             "url": _url("wasserkocher")},
    {"name": "Toasters",            "url": _url("toaster")},
    {"name": "Kitchen Robots",      "url": _url("k\u00fcchenmaschine")},
    {"name": "Microwaves",          "url": _url("mikrowelle")},
    {"name": "Kitchen Appliances",  "url": _url("k\u00fcchenger\u00e4te klein")},
    {"name": "Hair Dryers",         "url": _url("haartrockner f\u00f6n")},
    {"name": "Electric Shavers",    "url": _url("elektrorasierer")},
    {"name": "Irons",               "url": _url("b\u00fcgeleisen dampf")},
    {"name": "Electric Toothbrush", "url": _url("elektrische zahnb\u00fcrste")},

    # ── Hry & Gaming ──────────────────────────────────────────────────────────
    {"name": "Game Controllers",    "url": _url("gamepad controller")},
    {"name": "Gaming Headsets",     "url": _url("gaming headset")},
    {"name": "Gaming Mice",         "url": _url("gaming maus")},
    {"name": "Gaming Keyboards",    "url": _url("gaming tastatur")},

    # ── Zabezpečení & Chytrá domácnost ────────────────────────────────────────
    {"name": "IP Cameras",          "url": _url("\u00fcberwachungskamera wlan")},
    {"name": "Smart Home",          "url": _url("smart home ger\u00e4te")},
    {"name": "Smart Lighting",      "url": _url("smart lampe led")},
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
        # Try h2 a first, then any link containing the /dp/ASIN pattern
        link_el = (
            card.select_one("h2 a") or
            card.select_one(f"a[href*='/dp/{asin}']") or
            card.select_one("a[href*='/dp/']")
        )
        product_url = ""
        if link_el and link_el.get("href"):
            href = link_el["href"]
            full = f"https://www.amazon.de{href}" if href.startswith("/") else href
            # Prefer the clean canonical /dp/ASIN URL — strip everything after
            m = re.search(r"(https://www\.amazon\.de(?:/[^/?]*)?/dp/[A-Z0-9]{10})", full)
            product_url = m.group(1) if m else full.split("?")[0]
        # Final fallback: build URL directly from the ASIN (always available)
        if not product_url and asin:
            product_url = f"https://www.amazon.de/dp/{asin}"

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
                "amazon",
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

        # Randomised delay between pages — avoids the fixed-interval bot signature
        time.sleep(random.uniform(REQUEST_DELAY, REQUEST_DELAY * 1.8))

    return total_added


def make_session():
    """Create a fresh curl_cffi session with Chrome TLS impersonation."""
    return requests.Session(impersonate="chrome120")


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Amazon.de Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}. Run load_data.py first.")
        return {"error": "database_not_found"}

    session = make_session()
    warm_up_session(session)
    time.sleep(random.uniform(3.0, 5.0))

    conn    = sqlite3.connect(DB_PATH)
    summary = {"categories_scraped": 0, "total_added": 0, "errors": []}

    for i, cat in enumerate(CATEGORIES):
        # Refresh session cookies every SESSION_REFRESH_EVERY categories.
        # Amazon soft-blocks a session after ~10-15 requests; fresh cookies reset this.
        if i > 0 and i % SESSION_REFRESH_EVERY == 0:
            log.info(f"── Session refresh after {i} categories — re-warming…")
            try:
                session.close()
            except Exception:
                pass
            session = make_session()
            warm_up_session(session)
            # Longer pause after refresh so the new session looks natural
            time.sleep(random.uniform(8.0, 14.0))

        try:
            added = scrape_category(cat, session, conn)
            summary["total_added"]        += added
            summary["categories_scraped"] += 1
        except Exception as e:
            log.error(f"Error scraping {cat['name']}: {e}")
            summary["errors"].append({"category": cat["name"], "error": str(e)})

        # Randomised inter-category pause — avoids the fixed-interval bot signature
        time.sleep(random.uniform(REQUEST_DELAY * 1.5, REQUEST_DELAY * 3.0))

    conn.close()
    try:
        session.close()
    except Exception:
        pass

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
