#!/usr/bin/env python3
"""
amazon_de_scraper.py
────────────────────
Scrapes top-rated / most-reviewed products from Amazon.de bestseller lists.

Strategy
  • Iterates Amazon.de bestseller category pages (each page = 50 products).
  • Uses curl_cffi with Chrome 131 impersonation — same approach as heureka_scraper.
  • Rotates a small set of realistic Accept-Language / User-Agent combos.
  • Polite 2.5 s delay between pages; backs off on HTTP 503/429.
  • UPSERT on ProductURL (= https://www.amazon.de/dp/{ASIN}).

Fields populated
  Name, Category, MainCategory, ProductURL, SKU (= ASIN),
  Price_EUR, AvgStarRating, StarRatingsCount / ReviewsCount,
  source='amazon_de', country='DE', currency='EUR'

Fields NOT available from bestseller pages (left NULL)
  RecommendRate_pct, ReturnRate_pct, Stars1-5_Count, Description, keywords

NOTE  Amazon's HTML structure changes frequently.  If selectors stop matching,
      inspect the page and update SELECTOR_* constants at the top of this file.
"""

import os
import re
import sys
import time
import sqlite3

from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as cffi_requests
except ImportError:
    print("ERROR: curl_cffi not installed.  Run: pip install curl_cffi")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper.config import DB_PATH

# ── Selectors (update here if Amazon changes its HTML) ────────────────────────
# Amazon refreshes CSS class names periodically — listed from most to least stable
SEL_ITEM        = "div.zg-grid-general-faceout, li.zg-item-immersion, div[class*='zg-item']"
SEL_NAME        = [
    "._cDEzb_p13n-sc-css-line-clamp-3_g3dy1",
    "._cDEzb_p13n-sc-css-line-clamp-1_1Fn3P",
    "div.p13n-sc-line-clamp-2",
    "div.p13n-sc-line-clamp-3",
    "span.zg-text-center-align",
    "a.a-link-normal span",
    "div[class*='line-clamp']",
]
SEL_PRICE       = [
    "span._cDEzb_p13n-sc-price_3mJ9Z",
    "span.p13n-sc-price",
    "span.a-color-price",
]
SEL_RATING_ALT  = "span.a-icon-alt"          # e.g. "4,5 von 5 Sternen"
SEL_REVIEW_CNT  = [
    "span.a-size-small a",
    "div.a-section span.a-size-small",
]

# ── Category list (Amazon node slug → DB fields) ──────────────────────────────
# Each page returns up to 50 items; we scrape 2 pages → ~100 per category.
CATEGORIES = [
    # slug                       Category label (DE)            MainCategory (matches CZ DB)
    ("electronics",              "Elektronik",                  "Telefony a tablety"),
    ("computers",                "Computer & Zubehör",          "Počítače a notebooky"),
    ("videogames",               "Videospiele",                 "Herní technika"),
    ("photo",                    "Kamera & Foto",               "Foto a kamery"),
    ("ce-de",                    "TV & Video",                  "Televize a video"),
    ("headphones",               "Kopfhörer",                   "Zvuk a hudba"),
    ("software",                 "Software",                    "PC komponenty"),
    ("office-products",          "Bürobedarf & Schreibwaren",   "Periferie a příslušenství"),
    ("kitchen",                  "Küche & Haushalt",            "Malé domácí spotřebiče"),
    ("large-appliances",         "Große Haushaltsgeräte",       "Velké domácí spotřebiče"),
    ("wireless",                 "Handys & Zubehör",            "Telefony a tablety"),
    ("personal-computers",       "Laptops",                     "Počítače a notebooky"),
    ("pc-hardware",              "PC-Hardware",                  "PC komponenty"),
    ("networking-device",        "Netzwerk & Zubehör",          "Sítě a konektivita"),
    ("data-storage",             "Datenspeicher",               "Datová úložiště"),
    ("smart-home",               "Smart Home",                  "Chytré zařízení"),
    ("vacuum-cleaners",          "Staubsauger",                 "Vysavače a úklid"),
    ("gaming",                   "Gaming",                      "Herní technika"),
]

BASE_URL   = "https://www.amazon.de/gp/bestsellers/{node}/ref=zg_bs_pg_{page}?pg={page}"
DELAY_OK   = 2.5   # seconds between pages
DELAY_BACK = 30.0  # seconds after rate-limit response


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_session():
    s = cffi_requests.Session()
    s.headers.update({
        "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
    })
    return s


def _first_text(soup_el, selectors):
    for sel in selectors:
        el = soup_el.select_one(sel)
        if el:
            return el.get_text(strip=True)
    return None


def parse_eur(text):
    """'29,99 €'  →  29.99"""
    if not text:
        return None
    text = re.sub(r"[^\d,.]", "", text.strip())
    # German format: 1.299,99 → strip thousands dot, swap decimal comma
    text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_rating(text):
    """'4,5 von 5 Sternen'  →  4.5"""
    if not text:
        return None
    m = re.search(r"([\d,]+)\s+von\s+5", text)
    if m:
        try:
            return float(m.group(1).replace(",", "."))
        except ValueError:
            pass
    return None


def parse_int(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


# ── Core scraping ─────────────────────────────────────────────────────────────

def scrape_page(session, node, page):
    url = BASE_URL.format(node=node, page=page)
    products = []

    try:
        resp = session.get(url, impersonate="chrome131", timeout=25)
    except Exception as e:
        print(f"    Request error ({url}): {e}")
        return products, False   # (products, should_continue)

    if resp.status_code in (429, 503):
        print(f"    Rate-limited (HTTP {resp.status_code}) — backing off {DELAY_BACK}s")
        time.sleep(DELAY_BACK)
        return products, False

    if resp.status_code != 200:
        print(f"    HTTP {resp.status_code} — skipping page")
        return products, False

    soup = BeautifulSoup(resp.text, "html.parser")
    items = soup.select(SEL_ITEM)

    if not items:
        # Possible CAPTCHA / empty page
        print(f"    No items found on page {page} — possible CAPTCHA, stopping category")
        return products, False

    for item in items:
        try:
            name = _first_text(item, SEL_NAME)
            if not name:
                continue

            # ASIN + URL
            link_el = item.select_one("a.a-link-normal[href]")
            if not link_el:
                continue
            href = link_el["href"]
            m = re.search(r"/dp/([A-Z0-9]{10})", href)
            if not m:
                continue
            asin = m.group(1)
            product_url = f"https://www.amazon.de/dp/{asin}"

            price = parse_eur(_first_text(item, SEL_PRICE))

            rating_el = item.select_one(SEL_RATING_ALT)
            rating = parse_rating(
                rating_el.get("title", "") or rating_el.get_text()
                if rating_el else None
            )

            review_text = _first_text(item, SEL_REVIEW_CNT)
            review_count = parse_int(review_text)

            products.append({
                "Name":             name,
                "ProductURL":       product_url,
                "SKU":              asin,
                "Price_EUR":        price,
                "AvgStarRating":    rating,
                "ReviewsCount":     review_count,
                "StarRatingsCount": review_count,
            })
        except Exception:
            continue

    return products, True


# ── Database ──────────────────────────────────────────────────────────────────

def upsert_products(conn, products, category, main_category):
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
                    "amazon_de", "DE", "EUR",
                ),
            )
            if cur.lastrowid and conn.execute(
                "SELECT changes()"
            ).fetchone()[0] == 1:
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            print(f"    DB error: {e}")
    conn.commit()
    return inserted, updated


# ── Entry point ───────────────────────────────────────────────────────────────

def scrape_amazon_de(db_path=None):
    if db_path is None:
        db_path = DB_PATH
    conn = sqlite3.connect(db_path)
    session = make_session()
    total_ins = total_upd = 0

    for node, cat_label, main_cat in CATEGORIES:
        print(f"  Amazon.de  [{cat_label}]")
        for page in range(1, 3):          # pages 1 and 2
            products, ok = scrape_page(session, node, page)
            ins, upd = upsert_products(conn, products, cat_label, main_cat)
            total_ins += ins
            total_upd += upd
            print(f"    page {page}: {len(products)} found → {ins} new, {upd} updated")
            if not ok:
                break
            time.sleep(DELAY_OK)

    conn.close()
    print(f"Amazon.de finished: {total_ins} inserted, {total_upd} updated\n")
    return total_ins, total_upd


if __name__ == "__main__":
    db_path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    scrape_amazon_de(db_path_arg)
