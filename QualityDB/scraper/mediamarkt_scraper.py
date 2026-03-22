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
"""

import os
import re
import sys
import time
import json
import sqlite3
from urllib.parse import urlencode, quote

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("ERROR: curl_cffi not installed.  Run: pip install curl_cffi")
    sys.exit(1)

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(__file__))
from config import DB_PATH

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL   = "https://www.mediamarkt.de"
SEARCH_URL = BASE_URL + "/de/search.html"

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

def make_session():
    s = cffi_requests.Session()
    s.headers.update({
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_eur(value):
    if value is None:
        return None
    text = re.sub(r"[^\d,.]", "", str(value))
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
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


# ── JSON response extractor ───────────────────────────────────────────────────

def extract_from_json(data):
    """Handle various shapes of MediaMarkt's JSON API response."""
    candidates = (
        data.get("products") or
        (data.get("data") or {}).get("products", {}).get("results") or
        data.get("results") or
        data.get("items") or
        []
    )
    return parse_product_list(candidates)


def parse_product_list(items):
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
            product_url = url_part if url_part.startswith("http") else BASE_URL + url_part

            price_field = item.get("price")
            if isinstance(price_field, dict):
                price_raw = price_field.get("value") or price_field.get("formattedValue")
            else:
                price_raw = price_field or item.get("priceValue")
            price = parse_eur(price_raw)

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

def extract_next_data(html):
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
                result = parse_product_list(node)
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

def extract_jsonld(html):
    products = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        if isinstance(data, list):
            for item in data:
                parse_jsonld_item(item, products)
        else:
            parse_jsonld_item(data, products)
    return products


def parse_jsonld_item(item, out):
    t = item.get("@type", "")
    if t == "ItemList":
        for el in item.get("itemListElement", []):
            parse_jsonld_item(el.get("item", el), out)
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
    price = parse_eur((offers or {}).get("price"))

    agg       = item.get("aggregateRating") or {}
    rating    = parse_float(agg.get("ratingValue"))
    rev_count = parse_int(agg.get("reviewCount") or agg.get("ratingCount"))

    out.append({
        "Name":             name,
        "ProductURL":       url if url.startswith("http") else BASE_URL + url,
        "SKU":              str(item.get("sku") or ""),
        "Price_EUR":        price,
        "AvgStarRating":    rating,
        "ReviewsCount":     rev_count,
        "StarRatingsCount": rev_count,
    })


# ── Fetch one search query ────────────────────────────────────────────────────

def fetch_query(session, query):
    params = urlencode({
        "query":   query,
        "sortBy":  "topRated",
        "pageSize": 96,
    })
    # Try JSON mode first
    json_url = f"{SEARCH_URL}?{params}&format=json"
    html_url = f"{SEARCH_URL}?{params}"

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
                session.get(BASE_URL + "/", impersonate="chrome131", timeout=10)
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
                products = extract_from_json(resp.json())
                if products:
                    return products
            except Exception:
                pass

        # 2. __NEXT_DATA__
        products = extract_next_data(resp.text)
        if products:
            return products

        # 3. JSON-LD
        products = extract_jsonld(resp.text)
        if products:
            return products

        # 4. Retry without format=json (plain HTML)
        if attempt == 1:
            try:
                resp2 = session.get(html_url, impersonate="chrome131", timeout=25)
                products = extract_next_data(resp2.text) or extract_jsonld(resp2.text)
                if products:
                    return products
            except Exception:
                pass

        return []

    return []


# ── Database ──────────────────────────────────────────────────────────────────

def upsert_products(conn, products, category, main_category, source="mediamarkt_de"):
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
        except Exception as e:
            print(f"    DB error: {e}")
    conn.commit()
    return inserted, updated


# ── Entry point ───────────────────────────────────────────────────────────────

def scrape_mediamarkt(db_path=None):
    if db_path is None:
        db_path = DB_PATH
    conn    = sqlite3.connect(db_path)
    session = make_session()
    total_ins = total_upd = 0

    for query, cat_label, main_cat in SEARCH_QUERIES:
        print(f"  MediaMarkt.de  [{cat_label}]")
        products = fetch_query(session, query)
        ins, upd = upsert_products(conn, products, cat_label, main_cat)
        total_ins += ins
        total_upd += upd
        print(f"    {len(products)} found → {ins} new, {upd} updated")
        time.sleep(DELAY_OK)

    conn.close()
    print(f"\nMediaMarkt.de finished: {total_ins} inserted, {total_upd} updated")
    return total_ins, total_upd


if __name__ == "__main__":
    db_path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    scrape_mediamarkt(db_path_arg)
