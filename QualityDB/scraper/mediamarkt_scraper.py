#!/usr/bin/env python3
"""
mediamarkt_scraper.py  (v2 — search-based, slug-independent)
─────────────────────────────────────────────────────────────
Scrapes top-rated products from MediaMarkt.de.

Strategy (more robust than v1 category-slug approach):
  Use MediaMarkt's search endpoint with sortBy=topRated.
  This doesn't rely on category slugs that change frequently.

  Two-stage per query:
    1. PRIMARY  — search endpoint with &format=json  (JSON response)
    2. FALLBACK — parse __NEXT_DATA__ or JSON-LD from the HTML response

  UPSERT on ProductURL.
  country='DE', currency='EUR'.

MULTI-SITE
──────────
MediaMarkt and Saturn are the same Ceconomy storefront with two brands, so the
parsers here work unchanged on either.  Every function that touches a URL takes
an explicit `base_url` instead of reading the module constant, because
saturn_scraper.py used to override only the request headers and silently kept
writing mediamarkt.de product URLs under source='saturn_de'.  `enforce_host`
is the guard that makes a repeat of that fail loudly instead of quietly.
"""

import os
import re
import sys
import time
import json
import sqlite3
from urllib.parse import urlencode, quote, urlparse

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("ERROR: curl_cffi not installed.  Run: pip install curl_cffi")
    sys.exit(1)

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(__file__))
from config import DB_PATH

# ── Config ────────────────────────────────────────────────────────────────────
# Defaults only.  Do not read these inside functions -- pass `base_url` down, so
# a second Ceconomy storefront (Saturn) cannot silently inherit MediaMarkt URLs.
BASE_URL   = "https://www.mediamarkt.de"
SATURN_BASE_URL = "https://www.saturn.de"


def search_url_for(base_url):
    """Search endpoint for a Ceconomy storefront.  Same path on both brands."""
    return base_url.rstrip("/") + "/de/search.html"


SEARCH_URL = search_url_for(BASE_URL)

# These are search terms, not category slugs — will keep working even if
# MediaMarkt reorganises its category tree.
SEARCH_QUERIES = [
    # (search term,              Category label (DE),               MainCategory)
    ("smartphone",               "Smartphones",                     "Telefony a tablety"),
    ("tablet",                   "Tablets",                         "Telefony a tablety"),
    ("laptop notebook",          "Laptops & Notebooks",             "Počítače a notebooky"),
    ("fernseher",                "Fernseher",                       "Televize a video"),
    ("kopfhörer",                "Kopfhörer",                       "Zvuk a hudba"),
    ("bluetooth lautsprecher",   "Bluetooth-Lautsprecher",          "Zvuk a hudba"),
    ("spielkonsole",             "Spielkonsolen",                   "Herní technika"),
    ("gaming headset",           "Gaming-Headsets",                 "Herní technika"),
    ("staubsauger",              "Staubsauger",                     "Vysavače a úklid"),
    ("waschmaschine",            "Waschmaschinen",                  "Velké domácí spotřebiče"),
    ("kühlschrank",              "Kühlschränke",                    "Velké domácí spotřebiče"),
    ("kaffeevollautomat",        "Kaffeevollautomaten",             "Malé domácí spotřebiče"),
    ("wlan router",              "WLAN-Router",                     "Sítě a konektivita"),
    ("smartwatch",               "Smartwatches",                    "Chytré zařízení"),
    ("externe ssd festplatte",   "Externe SSDs & Festplatten",      "Datová úložiště"),
    ("grafikkarte",              "Grafikkarten",                    "PC komponenty"),
    ("drucker",                  "Drucker",                         "Periferie a příslušenství"),
    ("digitalkamera",            "Kameras",                         "Foto a kamery"),
    ("smart home",               "Smart Home",                      "Chytré zařízení"),
]

DELAY_OK   = 3.0
DELAY_BACK = 30.0
MAX_RETRIES = 3


# ── Session ───────────────────────────────────────────────────────────────────

def make_session(base_url=BASE_URL):
    base_url = base_url.rstrip("/")
    s = cffi_requests.Session()
    s.headers.update({
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         base_url + "/",
        "Origin":          base_url,
    })
    try:
        s.get(base_url + "/", impersonate="chrome131", timeout=15)
        time.sleep(1.5)
    except Exception:
        pass
    return s


# ── Host guard ────────────────────────────────────────────────────────────────

def host_of(url):
    """Bare hostname of `url`, without a leading www."""
    host = (urlparse(str(url or "")).hostname or "").lower()
    return host[4:] if host.startswith("www.") else host


def enforce_host(products, base_url):
    """Drop products whose URL is not on `base_url`'s host, and say so.

    A Saturn scrape that returns mediamarkt.de URLs is not partially right, it
    is the wrong shop wearing the right label -- which is exactly the bug that
    put 550 MediaMarkt rows into the DB as source='saturn_de'.  Dropping is
    correct here: a row kept under the wrong source becomes a second
    "independent" retailer downstream.
    """
    want = host_of(base_url)
    kept = [p for p in products if host_of(p.get("ProductURL")) == want]
    dropped = len(products) - len(kept)
    if dropped:
        seen = {host_of(p.get("ProductURL")) for p in products} - {want}
        print(f"    ! {dropped} product(s) not on {want} "
              f"(got {', '.join(sorted(h for h in seen if h)) or 'no host'}) -- dropped")
    return kept


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_eur(value):
    """Parse a price into a float in euros.

    MediaMarkt's search API returns prices as JSON numbers:
      • int   → already EUR (e.g. 599   → 599.00 €)
      • float → already EUR (e.g. 939.99 → 939.99 €)

    Formatted strings use German number notation:
      • "939,99 €"   → 939.99
      • "1.234,56 €" → 1234.56
      • "939.99"     → 939.99   (dot-decimal from some API paths)

    NOTE: int values represent euros directly, NOT euro-cents.
    The old implementation divided ints by 100, turning €599 → €5.99.
    """
    if value is None:
        return None

    # JSON number (int or float) → already in euros
    if isinstance(value, (int, float)):
        return float(value) if value else None

    # String: strip everything except digits, commas, and dots
    text = re.sub(r"[^\d,.]", "", str(value)).strip()
    if not text:
        return None

    # Determine decimal separator by which comes last
    has_dot   = "." in text
    has_comma = "," in text

    if has_dot and has_comma:
        # German "1.234,56" or US "1,234.56"
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")   # German
        else:
            text = text.replace(",", "")                     # US
    elif has_comma:
        # German decimal only: "939,99"
        text = text.replace(",", ".")
    # else: plain integer string or dot-decimal "939.99" — keep as-is

    try:
        return float(text)
    except ValueError:
        return None


# A euro price outside this band is not a price, it is a unit bug.  MediaMarkt
# sells nothing for 20 cents and nothing for six figures, so a value that lands
# outside the band is almost always a factor-of-100 slip.  Drop it loudly
# rather than writing a number that silently poisons every downstream median.
SANE_MIN_EUR = 0.50
SANE_MAX_EUR = 20_000.0


def sane_eur(price, name=None):
    """Return `price`, or None if it cannot plausibly be a euro amount."""
    if price is None:
        return None
    if SANE_MIN_EUR <= price <= SANE_MAX_EUR:
        return price
    print(f"    ! implausible price {price!r} EUR -- dropped ({str(name)[:60]})")
    return None


def parse_float(value):
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


def parse_int(value):
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


# ── JSON response extractor ───────────────────────────────────────────────────

def extract_from_json(data, base_url=BASE_URL):
    """Handle various shapes of MediaMarkt's JSON API response."""
    candidates = (
        data.get("products") or
        (data.get("data") or {}).get("products", {}).get("results") or
        data.get("results") or
        data.get("items") or
        []
    )
    return parse_product_list(candidates, base_url)


def parse_product_list(items, base_url=BASE_URL):
    products = []
    for item in items:
        try:
            name = item.get("name") or item.get("title")
            if not name:
                continue

            url_part = (
                item.get("productUrl") or
                item.get("url") or
                (item.get("links") or {}).get("productUrl", "")
            )
            if not url_part:
                continue
            product_url = (url_part if url_part.startswith("http")
                           else base_url.rstrip("/") + url_part)

            price_field = item.get("price")
            if isinstance(price_field, dict):
                # Prefer the formatted string: "939,99 €" states its own unit,
                # so it cannot be misread as cents.  A bare `value` is only a
                # number and has to be trusted to be euros -- that ambiguity is
                # what produced the 2026 scale corruption (see parse_eur).
                price_raw = price_field.get("formattedValue") or price_field.get("value")
            else:
                price_raw = price_field or item.get("priceValue")
            price = sane_eur(parse_eur(price_raw), item.get("name") or item.get("title"))

            agg       = item.get("aggregateRating") or {}
            rating    = parse_float(item.get("ratingValue") or item.get("rating") or agg.get("ratingValue"))
            rev_count = parse_int(item.get("reviewCount") or item.get("ratingCount") or agg.get("reviewCount"))
            sku       = str(item.get("sku") or item.get("id") or item.get("articleNumber") or "")

            products.append({
                "Name":             name,
                "ProductURL":       product_url,
                "SKU":              sku,
                "Price_EUR":        price,
                "AvgStarRating":    rating,
                "ReviewsCount":     rev_count,
                "StarRatingsCount": rev_count,
            })
        except Exception:
            continue
    return products


# ── __NEXT_DATA__ extractor ───────────────────────────────────────────────────

def extract_next_data(html, base_url=BASE_URL):
    soup = BeautifulSoup(html, "html.parser")
    tag  = soup.find("script", id="__NEXT_DATA__")
    if not tag:
        return []
    try:
        data = json.loads(tag.string or "")
    except Exception:
        return []

    def walk(node, depth=0):
        if depth > 12:
            return []
        if isinstance(node, list) and len(node) >= 3:
            if all(isinstance(x, dict) and ("name" in x or "title" in x) for x in node[:3]):
                result = parse_product_list(node, base_url)
                if result:
                    return result
        if isinstance(node, dict):
            for v in node.values():
                r = walk(v, depth + 1)
                if r:
                    return r
        return []

    return walk(data)


# ── JSON-LD fallback ──────────────────────────────────────────────────────────

def extract_jsonld(html, base_url=BASE_URL):
    products = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            for item in data:
                parse_jsonld_item(item, products, base_url)
        else:
            parse_jsonld_item(data, products, base_url)
    return products


def parse_jsonld_item(item, out, base_url=BASE_URL):
    t = item.get("@type", "")
    if t == "ItemList":
        for el in item.get("itemListElement", []):
            parse_jsonld_item(el.get("item", el), out, base_url)
        return
    if t != "Product":
        return

    name = item.get("name")
    url  = item.get("url") or item.get("@id")
    if not name or not url:
        return

    offers = item.get("offers")
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price = sane_eur(parse_eur((offers or {}).get("price")), name)

    agg       = item.get("aggregateRating") or {}
    rating    = parse_float(agg.get("ratingValue"))
    rev_count = parse_int(agg.get("reviewCount") or agg.get("ratingCount"))

    out.append({
        "Name":             name,
        "ProductURL":       (url if url.startswith("http")
                             else base_url.rstrip("/") + url),
        "SKU":              str(item.get("sku") or ""),
        "Price_EUR":        price,
        "AvgStarRating":    rating,
        "ReviewsCount":     rev_count,
        "StarRatingsCount": rev_count,
    })


# ── Fetch one search query ────────────────────────────────────────────────────

def fetch_query(session, query, base_url=BASE_URL):
    """Search one term on the Ceconomy storefront at `base_url`.

    `base_url` must match the session built by make_session(base_url) -- the
    returned rows are host-checked against it, so a mismatch yields nothing
    rather than the other brand's catalogue.
    """
    base_url = base_url.rstrip("/")
    search_url = search_url_for(base_url)
    params = urlencode({
        "query":   query,
        "sortBy":  "topRated",
        "pageSize": 96,
    })
    # Try JSON mode first
    json_url = f"{search_url}?{params}&format=json"
    html_url = f"{search_url}?{params}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(json_url, impersonate="chrome131", timeout=25)
        except Exception as e:
            print(f"    Request error (attempt {attempt}): {e}")
            time.sleep(DELAY_BACK)
            continue

        if resp.status_code in (429, 503):
            wait = DELAY_BACK * attempt
            print(f"    Rate-limited — waiting {wait:.0f}s")
            time.sleep(wait)
            try:
                session.get(base_url + "/", impersonate="chrome131", timeout=10)
            except Exception:
                pass
            continue

        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code} for '{query}'")
            return []

        # 1. JSON API response
        ct = resp.headers.get("content-type", "")
        if "json" in ct:
            try:
                products = enforce_host(extract_from_json(resp.json(), base_url), base_url)
                if products:
                    return products
            except Exception:
                pass

        # 2. __NEXT_DATA__
        products = enforce_host(extract_next_data(resp.text, base_url), base_url)
        if products:
            return products

        # 3. JSON-LD
        products = enforce_host(extract_jsonld(resp.text, base_url), base_url)
        if products:
            return products

        # 4. Retry without format=json (plain HTML)
        if attempt == 1:
            try:
                resp2 = session.get(html_url, impersonate="chrome131", timeout=25)
                products = enforce_host(
                    extract_next_data(resp2.text, base_url)
                    or extract_jsonld(resp2.text, base_url), base_url)
                if products:
                    return products
            except Exception:
                pass

        return []

    return []


# ── Database ──────────────────────────────────────────────────────────────────

def upsert_products(conn, products, category, main_category, source="mediamarkt_de"):
    from scraper.snapshots import ensure_snapshot_table, record_snapshot
    ensure_snapshot_table(conn)
    cur = conn.cursor()
    inserted = updated = 0
    for p in products:
        if not p.get("ProductURL") or not p.get("Name"):
            continue
        try:
            cur.execute(
                """
                INSERT INTO products
                  (Name, Category, MainCategory, ProductURL, SKU,
                   Price_EUR, AvgStarRating, StarRatingsCount, ReviewsCount,
                   RecommendRate_pct, ReturnRate_pct,
                   source, country, currency)
                VALUES (?,?,?,?,?,?,?,?,?,NULL,NULL,?,?,?)
                ON CONFLICT(ProductURL) DO UPDATE SET
                  Price_EUR        = excluded.Price_EUR,
                  AvgStarRating    = excluded.AvgStarRating,
                  StarRatingsCount = excluded.StarRatingsCount,
                  ReviewsCount     = excluded.ReviewsCount
                """,
                (
                    p["Name"], category, main_category,
                    p["ProductURL"], p.get("SKU"),
                    p.get("Price_EUR"),
                    p.get("AvgStarRating"),
                    p.get("StarRatingsCount"),
                    p.get("ReviewsCount"),
                    source, "DE", "EUR",
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0] == 1:
                inserted += 1
            else:
                updated += 1
            # Record longitudinal snapshot for ODA trend analysis
            record_snapshot(conn, p["ProductURL"], source, p, country="DE")
        except Exception as e:
            print(f"    DB error: {e}")
    conn.commit()
    return inserted, updated


# ── Entry point ───────────────────────────────────────────────────────────────

def scrape_ceconomy(base_url, source, label, db_path=None):
    """Run the search sweep against one Ceconomy storefront.

    Saturn and MediaMarkt differ only in `base_url` and `source`; everything
    else -- session, search path, parsers, category map -- is shared.
    """
    if db_path is None:
        db_path = DB_PATH
    conn    = sqlite3.connect(db_path)
    session = make_session(base_url)
    total_ins = total_upd = 0

    for query, cat_label, main_cat in SEARCH_QUERIES:
        print(f"  {label}  [{cat_label}]")
        products = fetch_query(session, query, base_url)
        ins, upd = upsert_products(conn, products, cat_label, main_cat, source=source)
        total_ins += ins
        total_upd += upd
        print(f"    {len(products)} found → {ins} new, {upd} updated")
        time.sleep(DELAY_OK)

    conn.close()
    print(f"\n{label} finished: {total_ins} inserted, {total_upd} updated")
    return total_ins, total_upd


def scrape_mediamarkt(db_path=None):
    # NOTE: writes source='mediamarkt_de', but the 542 MediaMarkt rows already in
    # products.db carry source='mediamarkt' (written by server.py's scan path).
    # Left as-is deliberately -- unifying the two labels is a data decision, not
    # a scraper one.
    return scrape_ceconomy(BASE_URL, "mediamarkt_de", "MediaMarkt.de", db_path)


if __name__ == "__main__":
    db_path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    scrape_mediamarkt(db_path_arg)
