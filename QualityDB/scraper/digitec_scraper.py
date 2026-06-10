"""
Digitec scraper — finds top-selling products and adds them to QualityDB.

Digitec.ch is Switzerland's largest electronics retailer.  Products carry
verified buyer star ratings (1–5) with large review volumes.

Scraping approach:
  - Digitec's internal GraphQL API (persisted queries, no auth required)
  - Sorted by BESTSELLER (most sold), top 10 per category, no rating threshold
  - Stars (1–5) converted to recommend %: (stars / 5) × 100
  - Prices are in CHF, stored as-is in Price_CZK column

API details:
  POST https://www.digitec.ch/graphql/o/{QUERY_HASH}/productTypeProductListRelayQuery
  Required custom headers: x-dg-portal, x-dg-language, x-dg-graphql-client-name, etc.
  Sort order: "RATING" (corresponds to ?so=7 in the browser URL)
  Pagination: cursor-based via `after` variable (base64-encoded offset)

Category navigationItemIds were captured via browser DevTools (May 2026).
If this scraper stops working, recapture by visiting a category page with
Chrome DevTools → Network → filter "graphql" → find
productTypeProductListRelayQuery → copy URL hash and request body.

Dependencies (already installed):
    pip3 install curl_cffi

Usage:
    python3 scraper/digitec_scraper.py
"""

import json
import time
import logging
import sqlite3
import os
import sys
import uuid

try:
    from curl_cffi import requests
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install curl_cffi\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Settings ──────────────────────────────────────────────────────────────────
MAX_PAGES     = 3      # Pages of bestsellers per category
PAGE_SIZE     = 48     # Products per GraphQL request (Digitec max is 48)
REQUEST_DELAY = 2.0    # Seconds between requests

# ── Persisted query hash ───────────────────────────────────────────────────────
# Captured from Digitec browser network traffic, May 2026.
# Update if API returns 404: open Chrome DevTools on any Digitec category page,
# Network tab → filter "graphql" → find productTypeProductListRelayQuery request.
QUERY_HASH  = "d9ddfbdfc422608cc3dc2727d10e9ca1"
GRAPHQL_URL = f"https://www.digitec.ch/graphql/o/{QUERY_HASH}/productTypeProductListRelayQuery"

# Sector ID for retail (constant across all categories)
SECTOR_ID = "U2VjdG9yCmkx"

# ── Category list ─────────────────────────────────────────────────────────────
# navigationItemIds captured via browser DevTools (May 2026).
# Each navId encodes the category hierarchy path in base64:
#   e.g. base64("NavigationItem\ndsa:retail/s:1/t:591/t:1383/pt:48") → Headphones
CATEGORIES = [
    {
        "name":  "TVs",
        "label": "TV",
        "url":   "/de/s1/producttype/tv-4",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo1MzgvcHQ6NA==",
    },
    {
        "name":  "Smartphones",
        "label": "Smartphone",
        "url":   "/de/s1/producttype/smartphone-24",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo4Mi9wdDoyNA==",
    },
    {
        "name":  "Laptops",
        "label": "Notebook",
        "url":   "/de/s1/producttype/notebook-6",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo2MTQvcHQ6Ng==",
    },
    {
        "name":  "Headphones",
        "label": "Kopfhörer",
        "url":   "/de/s1/producttype/kopfhoerer-48",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo1OTEvdDoxMzgzL3B0OjQ4",
    },
    {
        "name":  "Monitors",
        "label": "Monitor",
        "url":   "/de/s1/producttype/monitor-31",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo3Ny90OjYyNC9wdDozMQ==",
    },
    {
        "name":  "Smartwatches",
        "label": "Smartwatch",
        "url":   "/de/s1/producttype/smartwatch-2446",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo1MjEvcHQ6MjQ0Ng==",
    },
    {
        "name":  "Speakers",
        "label": "Bluetooth Lautsprecher",
        "url":   "/de/s1/producttype/bluetooth-lautsprecher-536",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo1OTEvdDoxMzg1L3B0OjUzNg==",
    },
    {
        "name":  "Keyboards",
        "label": "Tastatur",
        "url":   "/de/s1/producttype/tastatur-55",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo3Ny90OjUyOS9wdDo1NQ==",
    },
    {
        "name":  "Mice",
        "label": "Maus",
        "url":   "/de/s1/producttype/maus-62",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo3Ny90OjUyOS9wdDo2Mg==",
    },
    {
        "name":  "SSD",
        "label": "SSD",
        "url":   "/de/s1/producttype/ssd-545",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDo3Ni90OjUzNS9wdDo1NDU=",
    },
    {
        "name":  "Coffee Machines",
        "label": "Kaffeevollautomat",
        "url":   "/de/s1/producttype/kaffeevollautomat-125",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDoxMjQ5L3Q6NjAzL3B0OjEyNQ==",
    },
    {
        "name":  "Vacuum Cleaners",
        "label": "Staubsauger",
        "url":   "/de/s1/producttype/staubsauger-118",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDoxMjQ5L3Q6ODkzL3B0OjExOA==",
    },
    {
        "name":  "Robot Vacuums",
        "label": "Staubsauger Roboter",
        "url":   "/de/s1/producttype/staubsauger-roboter-174",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDoxMjQ5L3Q6ODkzL3B0OjE3NA==",
    },
    {
        "name":  "Dishwashers",
        "label": "Geschirrspüler",
        "url":   "/de/s1/producttype/geschirrspueler-einbau-2798",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDoxMjQ5L3Q6NDk1L3Q6OTQzL3B0OjI3OTg=",
    },
    {
        "name":  "Refrigerators",
        "label": "Kühlschrank",
        "url":   "/de/s1/producttype/kuehlschrank-freistehend-139",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDoxMjQ5L3Q6NDk1L3Q6NTA0L3B0OjEzOQ==",
    },
    {
        "name":  "Air Purifiers",
        "label": "Luftreiniger",
        "url":   "/de/s1/producttype/luftreiniger-167",
        "navId": "TmF2aWdhdGlvbkl0ZW0KZHNhOnJldGFpbC9zOjEvdDoxMjQ5L3Q6NDAvcHQ6MTY3",
    },
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "digitec_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

# Required headers reverse-engineered from Digitec browser traffic (May 2026).
# x-dg-graphql-client-version contains a build timestamp and may need updating.
EXTRA_HEADERS = {
    "accept":                      "application/graphql-response+json; charset=utf-8, application/json; charset=utf-8",
    "content-type":                "application/json",
    "accept-language":             "de-CH",
    "origin":                      "https://www.digitec.ch",
    "x-dg-portal":                 "25",
    "x-dg-graphql-client-name":    "isomorph",
    "x-dg-team":                   "endeavour",
    "x-dg-routename":              "/producttype/[titleAndProductTypeId]",
    "x-dg-routeowner":             "endeavour",
    "x-dg-language":               "de-CH",
    "x-dg-graphql-client-version": "master-20260512-0747-25716022907-1",
    "x-dg-xpid":                   "a164f5e4",
}


# ── API fetching ──────────────────────────────────────────────────────────────

def fetch_page(cat: dict, cursor, session) -> dict:
    """POST one GraphQL request for a category page. Returns parsed JSON or {}."""
    headers = {
        **EXTRA_HEADERS,
        "referer":            f"https://www.digitec.ch{cat['url']}",
        "x-dg-correlation-id": str(uuid.uuid4()),
    }
    payload = {
        "variables": {
            "navigationItemId":                  cat["navId"],
            "first":                             PAGE_SIZE,
            "after":                             cursor,
            "filters":                           [],
            "sortOrder":                         "RELEVANCE",
            "sectorId":                          SECTOR_ID,
            "tagIds":                            [],
            "asPath":                            cat["url"],
            "contentRecommenderContext":         None,
            "contentRecommenderDebugInfoEnabled": False,
        }
    }
    try:
        resp = session.post(GRAPHQL_URL, headers=headers, json=payload, timeout=25)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning(f"  GraphQL request failed: {e}")
        return {}


def extract_products(data: dict) -> tuple:
    """
    Extract (products_list, next_cursor) from GraphQL response.
    Returns ([], None) on error.
    """
    try:
        if isinstance(data, list):
            data = data[0]
        products_data = data["data"]["navigationItemById"]["products"]
        edges     = products_data.get("edges", [])
        page_info = products_data.get("pageInfo", {})
        cursor    = page_info.get("endCursor") if page_info.get("hasNextPage") else None
    except (KeyError, TypeError):
        return [], None

    products = []
    for edge in edges:
        node = edge.get("node", {}) or {}

        # Name
        base_name = (node.get("name") or "").strip()
        brand_obj = node.get("brand") or {}
        brand     = (brand_obj.get("name") or "").strip() if brand_obj else ""
        full_name = f"{brand} {base_name}".strip() if brand else base_name

        # Rating
        rating_obj = node.get("ratingSummary") or {}
        rating     = rating_obj.get("averageRating")   # float 1.0–5.0
        reviews    = rating_obj.get("ratingCount", 0)  # int

        # Price (CHF)
        price_obj = node.get("price") or {}
        price_chf = price_obj.get("amountInclusive")

        # URL
        rel_url = node.get("relativeUrl", "") or ""
        url     = ("https://www.digitec.ch" + rel_url) if rel_url else ""

        if full_name and rating is not None:
            products.append({
                "Name":              full_name,
                "ProductURL":        url,
                "Stars":             float(rating),
                "RecommendRate_pct": round((float(rating) / 5.0) * 100, 1),
                "ReviewsCount":      reviews or 0,
                "Price_CZK":         float(price_chf) if price_chf else None,
            })

    return products, cursor


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
        record_snapshot(conn, p.get("ProductURL", ""), "digitec", p, country="CH")
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
                "digitec",
            ),
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn) -> int:
    cat_name = cat["name"]
    total    = 0
    cursor   = None

    log.info(f"── {cat_name}  ({cat['url']})")

    for page_num in range(1, MAX_PAGES + 1):
        data = fetch_page(cat, cursor, session)
        products, next_cursor = extract_products(data)

        if not products:
            log.info("   Empty response — stopping.")
            break

        added = insert_products(conn, products, cat_name)
        total += added
        log.info(
            f"   Page {page_num} | Found {len(products)} | New: {added}"
        )

        if next_cursor is None:
            log.info("   Reached end of category.")
            break

        cursor = next_cursor
        time.sleep(REQUEST_DELAY)

    return total


def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Digitec Scraper — starting run")
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
        print(f"\n⚠  {len(result['errors'])} category error(s) — check digitec_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
