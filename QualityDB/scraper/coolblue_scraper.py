#!/usr/bin/env python3
"""
coolblue_scraper.py — Coolblue.nl (Dutch retail) scraper
─────────────────────────────────────────────────────────
Coolblue is the Netherlands' largest electronics and appliance retailer.
Category listing pages return JSON-LD ItemList data (22 products/page, up to
20 pages). Product detail pages contain a Dutch-language rating pattern that
yields a 0–10 score and a review count.

What it collects
  • Name, price (EUR), product URL, product ID
  • Rating (0–10 from detail page) → converted to 5-star AvgStarRating
  • Rating * 10 stored as RecommendRate_pct (e.g. 9.3/10 → 93 %)
  • Review count
  • Brand (first word heuristic, with known product-line overrides)
  • source="coolblue", country="NL", currency="EUR"

Usage
  python3 scraper/coolblue_scraper.py                # full run
  python3 scraper/coolblue_scraper.py --limit 50     # first 50 products per category
  python3 scraper/coolblue_scraper.py --no-details   # skip detail-page fetch
  python3 scraper/coolblue_scraper.py --test-url https://www.coolblue.nl/product/...

Dependencies
  pip install curl_cffi beautifulsoup4
"""

from __future__ import annotations

import os
import sys
import re
import json
import time
import sqlite3
import logging
import argparse
import datetime
from urllib.parse import urljoin, urlparse, urlencode, urlunparse, parse_qs

try:
    from curl_cffi import requests as cffi_requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run:\n  pip install curl_cffi beautifulsoup4")
    sys.exit(1)

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from scraper.config import DB_PATH, JOURNAL_MODE
except ImportError:
    DB_PATH = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "products.db"
    )
    JOURNAL_MODE = "WAL"

try:
    from scraper.snapshots import ensure_snapshot_table, record_snapshot
except ImportError:
    def ensure_snapshot_table(conn): pass
    def record_snapshot(conn, url, source, p, country=""): pass

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "coolblue_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────
BASE_URL  = "https://www.coolblue.nl"
DELAY     = 2.0       # seconds between requests (minimum as required)
MAX_PAGES = 20        # per category (22 products/page in JSON-LD)
COUNTRY   = "NL"
CURRENCY  = "EUR"
SOURCE    = "coolblue"

# ── category definitions ──────────────────────────────────────────────────────
# (slug, English label, MainCategory)
# Listing pages use Coolblue's default "aanbevolen" (recommended) sort, which
# reflects sales popularity and is not biased toward highest-rated products.
CATEGORIES = [
    # URL slug                                  EN label              MainCategory
    ("smartphones",                             "Smartphones",        "Phones & Tablets"),
    ("tablets/tablets",                         "Tablets",            "Phones & Tablets"),
    ("laptops/laptops",                         "Laptops",            "Computers"),
    ("televisies/smart-tv",                     "Televisions",        "TV & Audio"),
    ("soundbars/soundbars",                     "Soundbars",          "TV & Audio"),
    ("hoofdtelefoons/bluetooth-hoofdtelefoons", "Headphones",         "TV & Audio"),
    ("bluetooth-speakers",                      "Bluetooth Speakers", "TV & Audio"),
    ("koelkasten/koelkasten",                   "Refrigerators",      "Large Appliances"),
    ("wasmachines/wasmachines",                 "Washing Machines",   "Large Appliances"),
    ("vaatwassers/vaatwassers",                 "Dishwashers",        "Large Appliances"),
    ("stofzuigers/steelstofzuigers",            "Stick Vacuums",      "Small Appliances"),
    ("airfryers/airfryers",                     "Air Fryers",         "Small Appliances"),
    ("smartwatches/smartwatches",               "Smartwatches",       "Wearables"),
]


# ── brand heuristics ──────────────────────────────────────────────────────────
# Product lines where first word is NOT the brand name
_PRODUCT_LINE_TO_BRAND: dict[str, str] = {
    "iphone":   "Apple",
    "ipad":     "Apple",
    "macbook":  "Apple",
    "airpods":  "Apple",
    "imac":     "Apple",
    "galaxy":   "Samsung",
    "xperia":   "Sony",
    "pixel":    "Google",
    "surface":  "Microsoft",
    "thinkpad": "Lenovo",
    "ideapad":  "Lenovo",
    "legion":   "Lenovo",
    "zenbook":  "Asus",
    "vivobook": "Asus",
    "rog":      "Asus",
    "pavilion": "HP",
    "envy":     "HP",
    "spectre":  "HP",
    "inspiron": "Dell",
    "latitude": "Dell",
    "xps":      "Dell",
    "aspire":   "Acer",
    "swift":    "Acer",
    "nitro":    "Acer",
    "roomba":   "iRobot",
}


def extract_brand_from_name(name: str) -> str | None:
    """
    Heuristic brand extraction from product name.
    Checks known product-line → brand overrides first, then falls back to
    the first word (works for Samsung, LG, Sony, Philips, Bosch, etc.).
    """
    if not name:
        return None
    words = name.split()
    if not words:
        return None
    first = words[0].lower().rstrip("®™")
    if first in _PRODUCT_LINE_TO_BRAND:
        return _PRODUCT_LINE_TO_BRAND[first]
    return words[0]


# ── HTTP session ──────────────────────────────────────────────────────────────

def make_session() -> cffi_requests.Session:
    """Create a curl_cffi session impersonating Chrome 131 (avoids bot blocking)."""
    session = cffi_requests.Session(impersonate="chrome131")
    # Warm up with a homepage visit to establish cookies
    try:
        session.get(BASE_URL, headers=_headers(), timeout=20)
        time.sleep(1.0)
        log.info("Session warmed up on Coolblue homepage.")
    except Exception as exc:
        log.warning(f"Warm-up failed ({exc}) — proceeding anyway.")
    return session


def _headers() -> dict:
    return {
        "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Referer": BASE_URL + "/",
    }


def fetch_text(url: str, session: cffi_requests.Session) -> tuple[int, str]:
    """
    Fetch a URL and return (status_code, text).
    Returns (0, "") on network error.
    """
    try:
        resp = session.get(url, headers=_headers(), timeout=30)
        return resp.status_code, resp.text
    except Exception as exc:
        log.error(f"Fetch error: {exc}  [{url}]")
        return 0, ""


# ── listing page — JSON-LD extraction ────────────────────────────────────────

def listing_url(slug: str, page: int) -> str:
    """
    Build a Coolblue category listing URL using the default "aanbevolen" sort
    (recommended / most popular), which reflects sales popularity rather than
    star ratings.
    Page 1 example: https://www.coolblue.nl/smartphones?sorteren=aanbevolen
    Page 2+:        ...&pagina=2
    """
    params: dict[str, str | int] = {
        "sorteren": "aanbevolen",
    }
    if page > 1:
        params["pagina"] = page
    return f"{BASE_URL}/{slug}?{urlencode(params)}"


def parse_listing_jsonld(text: str) -> list[dict]:
    """
    Extract product stubs from a Coolblue category listing page via JSON-LD.

    Coolblue injects a <script type="application/ld+json"> block containing an
    ItemList with up to 22 ListItem entries, each of which has a nested Product
    with name, url, productID, and an Offer with price.

    Returns a list of dicts with keys: Name, ProductURL, Price_EUR, productID.
    """
    # Find all JSON-LD blocks
    soup = BeautifulSoup(text, "html.parser")
    products: list[dict] = []

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue

        # Handle both top-level ItemList and array-wrapped structures
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and item.get("@type") == "ItemList":
                    data = item
                    break
            else:
                continue

        if not isinstance(data, dict) or data.get("@type") != "ItemList":
            continue

        items = data.get("itemListElement", [])
        for entry in items:
            if not isinstance(entry, dict):
                continue
            # ListItem wrapping a Product
            product_node = entry.get("item", entry)
            if not isinstance(product_node, dict):
                continue
            if product_node.get("@type") not in ("Product", "http://schema.org/Product"):
                # Sometimes the ListItem itself has name/url directly
                if entry.get("@type") == "ListItem":
                    product_node = entry
                else:
                    continue

            name = product_node.get("name", "").strip()
            url  = product_node.get("url", "").strip()
            product_id = product_node.get("productID") or product_node.get("sku", "")

            if not name or not url:
                continue
            if not url.startswith("http"):
                url = urljoin(BASE_URL, url)

            # Price from Offer
            price_eur: float | None = None
            offers = product_node.get("offers", {})
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                raw_price = offers.get("price") or offers.get("lowPrice")
                if raw_price is not None:
                    try:
                        price_eur = float(str(raw_price).replace(",", "."))
                    except (ValueError, TypeError):
                        pass

            products.append({
                "Name":       name,
                "ProductURL": url,
                "Price_EUR":  price_eur,
                "productID":  str(product_id) if product_id else None,
            })

        # Found and parsed an ItemList — stop looking
        if products:
            break

    return products


# ── product detail page — rating extraction ───────────────────────────────────

# Pattern verified against live pages:
# "Beoordeling is 9,3 van de 10 op basis van 1247 reviews"
_RATING_RE = re.compile(
    r"Beoordeling is (\d+[,.]\d*) van de 10.*?(\d+) reviews",
    re.DOTALL,
)


def parse_detail_page(text: str) -> dict:
    """
    Extract the Coolblue rating (0–10) and review count from a product detail page.

    Returns a dict with:
      AvgStarRating    — score/10 * 0.5  (0–5 star scale)
      RecommendRate_pct — score * 10     (0–100 %)
      ReviewsCount     — integer
    or an empty dict if the pattern is not found.
    """
    m = _RATING_RE.search(text)
    if not m:
        return {}

    raw_score = m.group(1).replace(",", ".")
    raw_count = m.group(2)

    try:
        score_10 = float(raw_score)          # e.g. 9.3
        review_count = int(raw_count)
    except (ValueError, TypeError):
        return {}

    avg_star = round(score_10 * 0.5, 2)     # convert to 5-star scale
    recommend_pct = round(score_10 * 10, 1) # 9.3 → 93.0 %

    return {
        "AvgStarRating":     avg_star,
        "RecommendRate_pct": recommend_pct,
        "ReviewsCount":      review_count,
    }


def scrape_detail(url: str, session: cffi_requests.Session) -> dict:
    """Fetch a product detail page and return rating fields."""
    time.sleep(DELAY)
    status, text = fetch_text(url, session)
    if status != 200:
        log.warning(f"Detail page returned HTTP {status}: {url}")
        return {}
    return parse_detail_page(text)


# ── database helpers ──────────────────────────────────────────────────────────

def ensure_db_columns(conn: sqlite3.Connection) -> None:
    """
    Ensure all columns used by this scraper exist in the products table.
    Adds missing columns idempotently — safe to call on every run.
    """
    required = [
        ("brand",             "TEXT"),
        ("scraped_at",        "TEXT"),
        ("RecommendRate_pct", "REAL"),
    ]
    cur = conn.execute("PRAGMA table_info(products)")
    existing = {row[1] for row in cur.fetchall()}
    for col, col_type in required:
        if col not in existing:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
    conn.commit()


def upsert_product(conn: sqlite3.Connection, p: dict) -> bool:
    """
    Insert a new product row or update an existing one (matched by ProductURL).
    Returns True if a new row was inserted, False if an existing row was updated.
    """
    url = p.get("ProductURL", "")
    if not url:
        return False

    existing = conn.execute(
        "SELECT id FROM products WHERE ProductURL = ?", (url,)
    ).fetchone()

    brand = p.get("brand") or extract_brand_from_name(p.get("Name", ""))
    now   = datetime.datetime.now().isoformat()

    if existing:
        # Update price, rating, review count, and brand on each run
        conn.execute(
            """
            UPDATE products SET
                Price_EUR        = COALESCE(?, Price_EUR),
                AvgStarRating    = COALESCE(?, AvgStarRating),
                RecommendRate_pct= COALESCE(?, RecommendRate_pct),
                ReviewsCount     = COALESCE(?, ReviewsCount),
                brand            = COALESCE(?, brand),
                scraped_at       = ?
            WHERE ProductURL = ?
            """,
            (
                p.get("Price_EUR"),
                p.get("AvgStarRating"),
                p.get("RecommendRate_pct"),
                p.get("ReviewsCount"),
                brand,
                now,
                url,
            ),
        )
        return False

    # Insert new row
    conn.execute(
        """
        INSERT INTO products (
            Name, Category, MainCategory, ProductURL,
            Price_EUR, AvgStarRating, RecommendRate_pct, ReviewsCount,
            source, country, currency,
            brand, scraped_at
        ) VALUES (
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?
        )
        """,
        (
            p.get("Name"),
            p.get("Category"),
            p.get("MainCategory"),
            url,
            p.get("Price_EUR"),
            p.get("AvgStarRating"),
            p.get("RecommendRate_pct"),
            p.get("ReviewsCount", 0),
            SOURCE,
            COUNTRY,
            CURRENCY,
            brand,
            now,
        ),
    )
    return True


# ── category scraper ──────────────────────────────────────────────────────────

def scrape_category(
    slug: str,
    label: str,
    main_cat: str,
    session: cffi_requests.Session,
    conn: sqlite3.Connection,
    fetch_details: bool = True,
    limit: int = 0,
) -> dict:
    """
    Scrape one Coolblue category (all pages up to MAX_PAGES).
    Returns summary dict with inserted/updated counts.
    """
    log.info(f"── {label}  (/{slug})")
    total_scraped  = 0
    total_inserted = 0
    total_updated  = 0

    ensure_snapshot_table(conn)

    for page_num in range(1, MAX_PAGES + 1):
        url = listing_url(slug, page_num)
        log.info(f"   Page {page_num}: {url}")

        status, text = fetch_text(url, session)

        if status == 403 or status == 429:
            log.warning(f"   HTTP {status} on listing — bot block? Stopping category.")
            break
        if status != 200:
            log.warning(f"   HTTP {status} on listing — stopping category.")
            break

        stubs = parse_listing_jsonld(text)
        if not stubs:
            log.info("   No products found in JSON-LD — category exhausted.")
            break

        log.info(f"   Found {len(stubs)} products on this page.")

        for stub in stubs:
            if limit and total_scraped >= limit:
                break

            product = {
                "Name":        stub["Name"],
                "ProductURL":  stub["ProductURL"],
                "Price_EUR":   stub.get("Price_EUR"),
                "Category":    label,
                "MainCategory": main_cat,
                # Rating fields populated below (if --no-details not set)
                "AvgStarRating":     None,
                "RecommendRate_pct": None,
                "ReviewsCount":      0,
            }

            if fetch_details:
                detail = scrape_detail(stub["ProductURL"], session)
                if detail:
                    product.update(detail)
                else:
                    log.debug(f"   No rating found for: {stub['Name']}")

            inserted = upsert_product(conn, product)
            record_snapshot(conn, product["ProductURL"], SOURCE, product, country=COUNTRY)

            total_scraped += 1
            if inserted:
                total_inserted += 1
            else:
                total_updated += 1

        conn.commit()

        if limit and total_scraped >= limit:
            log.info(f"   Reached --limit {limit} — stopping.")
            break

        # Polite delay between listing pages (detail pages already sleep inside scrape_detail)
        if not fetch_details:
            time.sleep(DELAY)

    log.info(
        f"   {label} done: {total_scraped} products, "
        f"{total_inserted} new, {total_updated} updated."
    )
    return {
        "scraped":  total_scraped,
        "inserted": total_inserted,
        "updated":  total_updated,
    }


# ── single product test mode ──────────────────────────────────────────────────

def test_single_url(url: str, session: cffi_requests.Session) -> None:
    """Scrape a single product URL and print extracted data (for debugging)."""
    log.info(f"Testing single URL: {url}")
    time.sleep(DELAY)
    status, text = fetch_text(url, session)
    print(f"\nHTTP status: {status}")
    detail = parse_detail_page(text)
    if detail:
        print("── Detail page extraction result ──")
        for k, v in sorted(detail.items()):
            print(f"  {k:30s}: {v}")
    else:
        print("  No rating pattern matched.")
        # Show surrounding context to aid debugging
        idx = text.find("Beoordeling")
        if idx >= 0:
            print(f"\n  Raw context around 'Beoordeling' (±200 chars):")
            print(f"  {repr(text[max(0, idx-20):idx+200])}")
        else:
            print("  'Beoordeling' not found in page text.")


# ── main entry point ──────────────────────────────────────────────────────────

def run_scraper(fetch_details: bool = True, limit: int = 0) -> dict:
    """
    Run the full Coolblue scrape.  Called by scheduler.py.
    Returns {"total_added": int, "total_updated": int}.
    """
    log.info("=" * 60)
    log.info("QualityDB Coolblue.nl Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}. Run load_data.py first.")
        return {"total_added": 0, "total_updated": 0, "error": "database_not_found"}

    def _open_conn() -> sqlite3.Connection:
        c = sqlite3.connect(DB_PATH, timeout=30)
        c.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    conn = _open_conn()
    ensure_db_columns(conn)

    session = make_session()

    total_added   = 0
    total_updated = 0
    errors: list[dict] = []

    for slug, label, main_cat in CATEGORIES:
        try:
            result = scrape_category(
                slug, label, main_cat, session, conn,
                fetch_details=fetch_details,
                limit=limit,
            )
            total_added   += result.get("inserted", 0)
            total_updated += result.get("updated", 0)
        except sqlite3.OperationalError as exc:
            log.error(f"DB error scraping {label}: {exc}", exc_info=True)
            errors.append({"category": label, "error": str(exc)})
            # Attempt to reconnect so subsequent categories can proceed
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = _open_conn()
                log.info("Reconnected to database after error.")
            except Exception as re_exc:
                log.error(f"Failed to reconnect: {re_exc}")
                break
        except Exception as exc:
            log.error(f"Error scraping {label}: {exc}", exc_info=True)
            errors.append({"category": label, "error": str(exc)})

    conn.close()

    log.info("=" * 60)
    log.info("Coolblue scrape complete.")
    log.info(f"  New rows   : {total_added}")
    log.info(f"  Updated    : {total_updated}")
    if errors:
        log.warning(f"  Errors     : {len(errors)}")
    log.info("=" * 60)

    return {"total_added": total_added, "total_updated": total_updated}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Coolblue.nl scraper for QualityDB")
    parser.add_argument(
        "--no-details",
        action="store_true",
        help="Skip product detail page fetches (faster, no ratings)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max products per category (0 = unlimited)",
    )
    parser.add_argument(
        "--test-url",
        type=str,
        default=None,
        help="Scrape a single product URL for debugging",
    )
    args = parser.parse_args()

    session = make_session()

    if args.test_url:
        test_single_url(args.test_url, session)
    else:
        run_scraper(
            fetch_details=not args.no_details,
            limit=args.limit,
        )
