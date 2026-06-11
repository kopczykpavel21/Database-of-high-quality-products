"""
Conrad.de scraper — uses Conrad's REST search API.

Conrad is a full single-page application; category pages render in the
browser via JavaScript and contain no HTML product cards.  This scraper
calls the same JSON search API that the Conrad.de frontend uses.

Reverse-engineered endpoint (visible in browser DevTools → Network → XHR):
  POST https://api.conrad.de/search/1/v3/search/de/de/b2c?apikey=<key>
  Body: {
      "from": 0, "size": 24,
      "globalFilter": [{"field":"categoryId","type":"TERM_OR","values":["c2864444"]}],
      "facetFilter": [], "facets": [], "disabledFeatures": ["FACETS"]
  }

Category IDs are the numeric suffixes of the Conrad.de category URLs:
  https://www.conrad.de/de/c/raspberry-pi-2864444.html → ID 2864444 → "c2864444"

If a category returns 0 products after the warm-up:
  1. Check that the category ID is still valid by visiting Conrad.de.
  2. Open DevTools → Network → filter by "api.conrad.de" — copy the POST body
     from a live category page to see the correct filter field/value.
  3. The API key in this file may also need refreshing (check browser requests).

Products stored with country='DE', source='conrad', Price_EUR.

Usage:
    python3 scraper/conrad_scraper.py
    python3 scraper/conrad_scraper.py --dry-run
"""

import re
import time
import logging
import sqlite3
import os
import sys
import argparse
import json

try:
    from curl_cffi import requests
except ImportError:
    print("\n⚠  Missing dependency. Please run:")
    print("    pip3 install curl_cffi\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from scraper.snapshots import ensure_snapshot_table, record_snapshot

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Conrad search API ─────────────────────────────────────────────────────────
# API key is embedded in the Conrad.de frontend bundle (publicly visible).
# If requests start returning 401/403, update this key from a live browser session.
API_URL   = "https://api.conrad.de/search/1/v3/search/de/de/b2c"
API_KEY   = "hAxfOlAjT77nDfnyZdhhwvB55Yg8mdcj"
PAGE_SIZE = 24
MAX_PAGES = 12

MIN_REVIEWS   = 0      # 0 = collect everything (filter at analysis time)
REQUEST_DELAY = 2.5    # Conrad's CDN may rate-limit aggressive crawlers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "scraper.log"), encoding="utf-8"
        ),
    ],
)
log = logging.getLogger(__name__)


# ── Categories ────────────────────────────────────────────────────────────────
# cat_id = numeric part of the Conrad.de category URL slug.
CATEGORIES = [
    {"name": "Raspberry Pi & SBC",       "main": "Počítače a notebooky",  "cat_id": "2864444"},
    {"name": "Arduino",                   "main": "Průmyslové zboží",      "cat_id": "2871550"},
    {"name": "Měřicí přístroje",          "main": "Průmyslové zboží",      "cat_id": "37381"},
    {"name": "Pájení a elektrotechnika",  "main": "Průmyslové zboží",      "cat_id": "17583"},
    {"name": "Napájecí zdroje",           "main": "Průmyslové zboží",      "cat_id": "17452"},
    {"name": "Smart Home",                "main": "Elektro",               "cat_id": "17200"},
    {"name": "Reproduktory",              "main": "Elektro",               "cat_id": "17483"},
    {"name": "Sluchátka",                 "main": "Elektro",               "cat_id": "1688942"},
    {"name": "Drony",                     "main": "Foto a video",          "cat_id": "221790"},
]

HEADERS_HTML = {
    "Accept": ("text/html,application/xhtml+xml,application/xml;"
               "q=0.9,image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

HEADERS_API = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Content-Type": "application/json",
    "Origin": "https://www.conrad.de",
    "Referer": "https://www.conrad.de/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}


# ── Session setup ─────────────────────────────────────────────────────────────

def warm_up(session) -> bool:
    """Visit Conrad.de to obtain session cookies before calling the API."""
    for url in ["https://www.conrad.de/", "https://www.conrad.de/de/"]:
        try:
            r = session.get(url, headers=HEADERS_HTML, timeout=20)
            if r.status_code < 400:
                log.info(f"Session ready — {url}  (HTTP {r.status_code})")
                time.sleep(2.5)
                return True
        except Exception as e:
            log.warning(f"Warm-up {url}: {e}")
    log.warning("All warm-up URLs failed — attempting API calls anyway.")
    return False


# ── API request ───────────────────────────────────────────────────────────────

def _post(session, body: dict) -> dict:
    url = f"{API_URL}?apikey={API_KEY}"
    try:
        resp = session.post(url, json=body, headers=HEADERS_API, timeout=25)
        if resp.status_code == 200:
            return resp.json()
        log.warning(f"  API HTTP {resp.status_code} — {resp.text[:200]}")
        return {}
    except Exception as e:
        log.warning(f"  API request failed: {e}")
        return {}


def fetch_page(session, cat_id: str, page: int = 0) -> dict:
    body = {
        "from":    page * PAGE_SIZE,
        "size":    PAGE_SIZE,
        "globalFilter": [
            {
                "field":  "categoryId",
                "type":   "TERM_OR",
                "values": [f"c{cat_id}"],
            }
        ],
        "facetFilter":      [],
        "facets":           [],
        "disabledFeatures": ["FACETS"],
    }
    data = _post(session, body)

    # If the first attempt with 'categoryId' yields nothing, retry with
    # alternative filter field names Conrad has used in past API versions.
    if not data.get("hits") and not data.get("products") and page == 0:
        for alt_field in ("level3CategoryId", "category", "categoryPath"):
            log.debug(f"  Retrying with field='{alt_field}'")
            body2 = {**body}
            body2["globalFilter"] = [
                {"field": alt_field, "type": "TERM_OR", "values": [cat_id]}
            ]
            data2 = _post(session, body2)
            hits2, _ = _parse_hits(data2)
            if hits2:
                log.info(f"  Alternative filter '{alt_field}' succeeded.")
                return data2
        log.debug(f"  Response keys for cat {cat_id}: {list(data.keys())[:10]}")

    return data


# ── Response parsing ──────────────────────────────────────────────────────────

def _parse_hits(data: dict) -> tuple:
    """Return (hits_list, total_count) handling various API response shapes."""
    hits = (
        data.get("hits") or
        data.get("products") or
        data.get("results") or
        data.get("items") or
        []
    )
    # Elasticsearch nested shape: {"hits": {"hits": [...], "total": {"value": N}}}
    if isinstance(hits, dict):
        total_raw = hits.get("total", 0)
        hits = hits.get("hits", [])
    else:
        total_raw = (
            (data.get("meta") or {}).get("total") or
            data.get("total") or
            data.get("nbHits") or
            0
        )

    if isinstance(total_raw, dict):
        total_raw = total_raw.get("value", 0)

    return hits, int(total_raw or 0)


def _parse_float(val):
    if val is None:
        return None
    try:
        return float(str(val).replace(",", "."))
    except Exception:
        return None


def _parse_int(val):
    if val is None:
        return 0
    try:
        return int(str(val).replace(",", "").split(".")[0])
    except Exception:
        return 0


def parse_product(hit: dict):
    # Conrad API may wrap the payload under '_source' (Elasticsearch) or expose it flat
    src = hit.get("_source") or hit

    name = (
        src.get("name") or src.get("title") or
        src.get("productName") or src.get("displayName") or ""
    ).strip()
    if not name:
        return None

    # Product URL
    url = (
        src.get("url") or src.get("productUrl") or
        src.get("canonicalUrl") or src.get("slug") or ""
    )
    if url and not url.startswith("http"):
        # Relative URL — prepend Conrad base
        url = "https://www.conrad.de" + (url if url.startswith("/") else "/de/p/" + url)

    # Price (EUR)
    price = None
    pf = src.get("price") or src.get("sellingPrice") or src.get("priceData") or {}
    if isinstance(pf, dict):
        raw = (pf.get("value") or pf.get("amount") or
               pf.get("gross") or pf.get("netValue"))
        if raw is not None:
            price = _parse_float(raw)
    elif pf:
        price = _parse_float(pf)
    if price is None:
        price = _parse_float(src.get("priceValue") or src.get("price_value"))

    # Rating / reviews
    stars   = None
    reviews = 0
    rf = (
        src.get("rating") or src.get("averageRating") or
        src.get("aggregateRating") or src.get("ratingData") or {}
    )
    if isinstance(rf, dict):
        stars   = _parse_float(rf.get("average") or rf.get("value") or
                               rf.get("ratingValue") or rf.get("score"))
        reviews = _parse_int(rf.get("count") or rf.get("reviewCount") or
                             rf.get("ratingCount") or rf.get("numberOfRatings"))
    elif rf:
        stars = _parse_float(rf)

    # Top-level fallbacks
    if stars is None:
        stars = _parse_float(
            src.get("averageRating") or src.get("ratingValue") or src.get("score")
        )
    if not reviews:
        reviews = _parse_int(
            src.get("reviewCount") or src.get("ratingCount") or src.get("numberOfRatings")
        )

    rec_pct = round((stars / 5.0) * 100, 1) if stars else None

    return {
        "Name":             name,
        "ProductURL":       url,
        "AvgStarRating":    stars,
        "ReviewsCount":     reviews,
        "Price_EUR":        price,
        "RecommendRate_pct": rec_pct,
    }


# ── Database ──────────────────────────────────────────────────────────────────

def load_existing(conn):
    rows = conn.execute("SELECT lower(Name), ProductURL FROM products").fetchall()
    names = {r[0] for r in rows}
    urls  = {r[1] for r in rows if r[1]}
    return names, urls


def insert(conn, products, cat_name, main_cat, dry_run=False):
    ensure_snapshot_table(conn)
    existing_names, existing_urls = load_existing(conn)
    added = 0
    for p in products:
        key = p["Name"].lower()
        url = p.get("ProductURL", "")

        if key in existing_names or (url and url in existing_urls):
            record_snapshot(conn, url, "conrad", p, country="DE")
            continue

        if not dry_run:
            conn.execute(
                """INSERT OR IGNORE INTO products
                   (Name, Category, MainCategory, ProductURL,
                    Price_EUR, AvgStarRating, RecommendRate_pct,
                    ReviewsCount, source, country, currency)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    p["Name"], cat_name, main_cat, url,
                    p.get("Price_EUR"), p.get("AvgStarRating"),
                    p.get("RecommendRate_pct"), p.get("ReviewsCount", 0),
                    "conrad", "DE", "EUR",
                ),
            )
        existing_names.add(key)
        if url:
            existing_urls.add(url)
        added += 1
        record_snapshot(conn, url, "conrad", p, country="DE")

    if not dry_run:
        conn.commit()
    return added


# ── Category scraper ──────────────────────────────────────────────────────────

def scrape_category(cat, session, conn, dry_run=False) -> int:
    cat_id   = cat["cat_id"]
    cat_name = cat["name"]
    main_cat = cat.get("main", cat_name)
    total    = 0

    log.info(f"── {cat_name}  (cat_id={cat_id})")

    for page in range(MAX_PAGES):
        log.info(f"   Page {page + 1}  (from={page * PAGE_SIZE})")
        data = fetch_page(session, cat_id, page)

        if not data:
            log.info("   Empty response — stopping.")
            break

        hits, api_total = _parse_hits(data)

        if page == 0:
            log.info(f"   API total: {api_total}")

        if not hits:
            log.info("   No hits in response — stopping.")
            break

        products  = [p for p in (parse_product(h) for h in hits) if p]
        qualified = [p for p in products if (p.get("ReviewsCount") or 0) >= MIN_REVIEWS]

        added  = insert(conn, qualified, cat_name, main_cat, dry_run)
        total += added

        stars_vals = [p["AvgStarRating"] for p in products if p.get("AvgStarRating")]
        lowest = min(stars_vals) if stars_vals else None
        log.info(
            f"   Hits {len(hits)} | Parsed {len(products)} | "
            f"Qualified {len(qualified)} | Added {added}"
            + (f" | Min ★ {lowest:.1f}" if lowest else "")
        )

        # Stop when we've consumed all pages
        if api_total and (page + 1) * PAGE_SIZE >= api_total:
            log.info("   Reached end of category.")
            break
        if not api_total and len(hits) < PAGE_SIZE:
            log.info("   Last page (partial).")
            break

        time.sleep(REQUEST_DELAY)

    return total


# ── Main ──────────────────────────────────────────────────────────────────────

def run_scraper(dry_run=False) -> dict:
    log.info("=" * 60)
    log.info("QualityDB — Conrad.de Scraper  (API mode)")
    log.info("=" * 60)

    session = requests.Session(impersonate="chrome124")
    warm_up(session)

    conn    = sqlite3.connect(DB_PATH, timeout=30)
    summary = {"total_added": 0, "categories_scraped": 0, "errors": []}

    for cat in CATEGORIES:
        try:
            added = scrape_category(cat, session, conn, dry_run)
            summary["total_added"]        += added
            summary["categories_scraped"] += 1
        except Exception as e:
            log.error(f"Error in {cat['name']}: {e}")
            summary["errors"].append(str(e))
            try:
                conn.rollback()
            except Exception:
                pass
        time.sleep(REQUEST_DELAY)

    conn.close()
    log.info(f"Done — {summary['total_added']} new DE products added.")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Conrad.de scraper — REST API mode"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse products but do not write to the database")
    args = parser.parse_args()
    r = run_scraper(dry_run=args.dry_run)
    print(f"\n✓  Done. {r['total_added']} products "
          f"{'would be ' if args.dry_run else ''}added.")
