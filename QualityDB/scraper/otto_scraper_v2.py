#!/usr/bin/env python3
"""
otto_scraper.py  (v3 — Next.js __NEXT_DATA__ + JSON-LD fallback)
────────────────────────────────────────────────────────────────
Scrapes top-rated / most-reviewed products from Otto.de.

Otto.de is built on Next.js (server-side rendered).  Product listing pages
embed all product data inside a <script id="__NEXT_DATA__"> JSON blob, which
we can parse without JavaScript rendering — same approach as zbozi_scraper.

Strategy
  1. PRIMARY  — Walk __NEXT_DATA__ JSON looking for product arrays
  2. FALLBACK — JSON-LD schema.org Product tags

Usage
  python3 otto_scraper.py                   # from QualityDB/scraper/
  python3 scraper/otto_scraper.py           # from QualityDB/
  python3 otto_scraper.py --debug           # print raw JSON structure and exit
"""

import os
import re
import sys
import time
import json
import sqlite3
import logging

try:
    from curl_cffi import requests as cffi_requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run:  pip3 install curl_cffi beautifulsoup4")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper.config import DB_PATH, REQUEST_DELAY, MAX_PAGES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.otto.de"
DELAY    = max(REQUEST_DELAY, 2.5)

# ── Category pages ─────────────────────────────────────────────────────────────
MIN_PRICE_EUR = 100  # Skip products below this price

CATEGORIES = [
    # ── Expensive Electronics ─────────────────────────────────────────────────
    ("technik/smartphone/",              "Smartphones",                "Telefony a tablety"),
    ("technik/tablet/",                  "Tablets",                    "Telefony a tablety"),
    ("technik/notebook/",                "Laptops & Notebooks",        "Počítače a notebooky"),
    ("technik/fernseher/",               "Fernseher",                  "Televize a video"),
    ("technik/spielkonsolen/",           "Spielkonsolen",              "Herní technika"),
    ("technik/smartwatch/",              "Smartwatches",               "Chytré zařízení"),

    # ── Household Appliances (expensive) ──────────────────────────────────────
    ("haushalt/waschmaschinen/",         "Waschmaschinen",             "Velké domácí spotřebiče"),
    ("haushalt/trockner/",              "Wäschetrockner",             "Velké domácí spotřebiče"),
    ("haushalt/geschirrspueler/",       "Geschirrspüler",             "Velké domácí spotřebiče"),
    ("haushalt/kuehlschraenke/",        "Kühlschränke",               "Velké domácí spotřebiče"),
    ("haushalt/gefrierschraenke/",      "Gefrierschränke",            "Velké domácí spotřebiče"),
    ("haushalt/staubsauger/",            "Staubsauger",                "Vysavače a úklid"),
    ("haushalt/kaffeemaschinen/",        "Kaffeemaschinen",            "Malé domácí spotřebiče"),

    # ── Audio & Peripherals ───────────────────────────────────────────────────
    ("technik/kopfhoerer/",              "Kopfhörer",                  "Zvuk a hudba"),
    ("technik/lautsprecher/",            "Lautsprecher & Soundbars",   "Zvuk a hudba"),
    ("technik/drucker/",                 "Drucker",                    "Periferie a příslušenství"),
]


# ── Session ────────────────────────────────────────────────────────────────────

def make_session():
    s = cffi_requests.Session()
    s.headers.update({
        "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer":         BASE_URL + "/",
    })
    try:
        s.get(BASE_URL + "/", impersonate="chrome131", timeout=15)
        time.sleep(1.5)
    except Exception:
        pass
    return s


# ── Parsers ────────────────────────────────────────────────────────────────────

def parse_eur(value):
    if value is None:
        return None
    text = re.sub(r"[^\d,.]", "", str(value)).replace(".", "").replace(",", ".")
    try:
        return float(text) or None
    except ValueError:
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


# ── __NEXT_DATA__ walker ───────────────────────────────────────────────────────

def _looks_like_products(lst):
    if not isinstance(lst, list) or len(lst) < 2:
        return False
    sample = lst[0]
    if not isinstance(sample, dict):
        return False
    keys = set(k.lower() for k in sample)
    return bool(
        keys & {"name", "title", "productname"} and
        keys & {"url", "link", "producturl", "variationid", "ean", "sku", "id"}
    )


def _walk_for_products(node, depth=0):
    if depth > 15:
        return []
    if isinstance(node, list):
        if _looks_like_products(node):
            return node
        for item in node:
            result = _walk_for_products(item, depth + 1)
            if result:
                return result
    elif isinstance(node, dict):
        for priority_key in ("products", "items", "results", "elements", "variations", "tileList"):
            if priority_key in node:
                result = _walk_for_products(node[priority_key], depth + 1)
                if result:
                    return result
        for v in node.values():
            result = _walk_for_products(v, depth + 1)
            if result:
                return result
    return []


def _parse_otto_product(item):
    name = (
        item.get("name") or item.get("title") or
        item.get("productName") or item.get("brandName")
    )
    if not name:
        return None

    url_raw = (
        item.get("url") or item.get("productUrl") or
        item.get("link") or item.get("canonicalUrl") or ""
    )
    if not url_raw:
        return None
    product_url = url_raw if url_raw.startswith("http") else BASE_URL + url_raw

    price = None
    p = item.get("price") or item.get("priceData") or {}
    if isinstance(p, dict):
        price = parse_eur(
            p.get("value") or
            (p.get("regular") or {}).get("value") or
            (p.get("current") or {}).get("value") or
            p.get("formattedValue")
        )
    elif isinstance(p, (int, float, str)):
        price = parse_eur(p)

    agg    = item.get("aggregateRating") or item.get("rating") or {}
    rating = parse_float(
        item.get("ratingValue") or
        (agg.get("ratingValue") if isinstance(agg, dict) else None) or
        (agg if isinstance(agg, (int, float)) else None)
    )
    rev_count = parse_int(
        item.get("reviewCount") or item.get("ratingCount") or
        (agg.get("reviewCount") if isinstance(agg, dict) else None) or
        (agg.get("ratingCount") if isinstance(agg, dict) else None)
    )
    sku = str(item.get("sku") or item.get("variationId") or item.get("id") or item.get("ean") or "")

    return {
        "Name":             name,
        "ProductURL":       product_url,
        "SKU":              sku,
        "Price_EUR":        price,
        "AvgStarRating":    rating,
        "ReviewsCount":     rev_count,
        "StarRatingsCount": rev_count,
    }


def extract_next_data(html):
    soup = BeautifulSoup(html, "html.parser")
    tag  = soup.find("script", id="__NEXT_DATA__")
    if not tag:
        return []
    try:
        data = json.loads(tag.string or "")
    except Exception:
        return []
    results = []
    for item in _walk_for_products(data):
        p = _parse_otto_product(item)
        if p:
            results.append(p)
    return results


# ── JSON-LD fallback ───────────────────────────────────────────────────────────

def extract_jsonld(html):
    products = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        items = [data] if isinstance(data, dict) else data
        for item in items:
            if item.get("@type") == "ItemList":
                for el in item.get("itemListElement", []):
                    _parse_jsonld(el.get("item", el), products)
            elif item.get("@type") == "Product":
                _parse_jsonld(item, products)
    return products


def _parse_jsonld(item, out):
    name = item.get("name")
    url  = item.get("url") or item.get("@id", "")
    if not name or not url:
        return
    offers = item.get("offers") or {}
    if isinstance(offers, list):
        offers = offers[0] if offers else {}
    price = parse_eur((offers or {}).get("price"))
    agg   = item.get("aggregateRating") or {}
    out.append({
        "Name":             name,
        "ProductURL":       url if url.startswith("http") else BASE_URL + url,
        "SKU":              str(item.get("sku") or ""),
        "Price_EUR":        price,
        "AvgStarRating":    parse_float(agg.get("ratingValue")),
        "ReviewsCount":     parse_int(agg.get("reviewCount") or agg.get("ratingCount")),
        "StarRatingsCount": parse_int(agg.get("reviewCount") or agg.get("ratingCount")),
    })


# ── Fetch one category ─────────────────────────────────────────────────────────

def fetch_category(session, slug, page=1):
    url = f"{BASE_URL}/{slug}?sortby=RATING"
    if page > 1:
        url += f"&page={page}"

    for attempt in range(1, 4):
        try:
            resp = session.get(url, impersonate="chrome131", timeout=25)
        except Exception as e:
            log.warning(f"    Request error (attempt {attempt}): {e}")
            time.sleep(20)
            continue

        if resp.status_code == 429:
            time.sleep(30 * attempt)
            continue
        if resp.status_code == 404:
            log.warning(f"    404 for slug '{slug}'")
            return [], False
        if resp.status_code != 200:
            log.warning(f"    HTTP {resp.status_code}")
            return [], False

        products = extract_next_data(resp.text)
        if products:
            soup     = BeautifulSoup(resp.text, "html.parser")
            has_next = bool(soup.select_one("a[rel='next']"))
            return products, has_next

        products = extract_jsonld(resp.text)
        return products, False

    return [], False


# ── Database ───────────────────────────────────────────────────────────────────

def upsert_products(conn, products, category, main_category):
    cur = conn.cursor()
    inserted = updated = 0
    for p in products:
        if not p.get("ProductURL") or not p.get("Name"):
            continue
        # Skip cheap products (focus on expensive electronics/appliances)
        if p.get("Price_EUR") and p["Price_EUR"] < MIN_PRICE_EUR:
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
                    p["ProductURL"], p.get("SKU", ""),
                    p.get("Price_EUR"),
                    p.get("AvgStarRating"),
                    p.get("StarRatingsCount"),
                    p.get("ReviewsCount"),
                    "otto_de", "DE", "EUR",
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0] == 1:
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            log.error(f"    DB error: {e}")
    conn.commit()
    return inserted, updated


# ── Debug mode ─────────────────────────────────────────────────────────────────

def debug_print(session):
    url  = f"{BASE_URL}/smartphones/?sortby=RATING"
    resp = session.get(url, impersonate="chrome131", timeout=25)
    print(f"Status: {resp.status_code}  Body: {len(resp.text)} chars")
    soup = BeautifulSoup(resp.text, "html.parser")
    tag  = soup.find("script", id="__NEXT_DATA__")
    if not tag:
        print("No __NEXT_DATA__ found")
        print(resp.text[:500])
        return
    data = json.loads(tag.string)
    print(f"__NEXT_DATA__ size: {len(tag.string)} chars")

    def show(node, depth=0):
        if depth > 5:
            return
        indent = "  " * depth
        if isinstance(node, dict):
            for k, v in list(node.items())[:8]:
                extra = f"  len={len(v)}" if isinstance(v, (list, dict)) else f"  = {str(v)[:50]}"
                print(f"{indent}[{k}] {type(v).__name__}{extra}")
                show(v, depth + 1)
        elif isinstance(node, list) and node:
            print(f"{indent}list[{len(node)}]  first={type(node[0]).__name__}")
            if isinstance(node[0], dict):
                show(node[0], depth + 1)

    show(data)
    raw = _walk_for_products(data)
    print(f"\nProduct arrays found: {len(raw)}")
    if raw:
        print("Keys:", list(raw[0].keys()))


# ── Entry point ────────────────────────────────────────────────────────────────

def scrape_otto(db_path=None):
    if db_path is None:
        db_path = DB_PATH

    if "--debug" in sys.argv:
        debug_print(make_session())
        return 0, 0

    conn    = sqlite3.connect(db_path)
    session = make_session()
    total_ins = total_upd = 0

    for slug, cat_label, main_cat in CATEGORIES:
        log.info(f"  Otto.de  [{cat_label}]")
        for page in range(1, (MAX_PAGES or 5) + 1):
            products, has_next = fetch_category(session, slug, page)
            ins, upd = upsert_products(conn, products, cat_label, main_cat)
            total_ins += ins
            total_upd += upd
            log.info(f"    page {page}: {len(products)} found → {ins} new, {upd} updated")
            if not products or not has_next:
                break
            time.sleep(DELAY)

    conn.close()
    log.info(f"\nOtto.de finished: {total_ins} inserted, {total_upd} updated")
    return total_ins, total_upd


if __name__ == "__main__":
    db_path_arg = None
    for arg in sys.argv[1:]:
        if not arg.startswith("--"):
            db_path_arg = arg
    scrape_otto(db_path_arg)
