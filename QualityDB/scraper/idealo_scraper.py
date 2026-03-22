#!/usr/bin/env python3
"""
idealo_scraper.py
─────────────────
Scrapes top-rated / most-reviewed products from Idealo.de — the German
equivalent of Heureka.cz.  Uses the same curl_cffi + BeautifulSoup approach
as heureka_scraper.py.

Why Idealo instead of MediaMarkt / Saturn?
  MediaMarkt and Saturn both use client-side JavaScript rendering — all product
  data is fetched by the browser after page load, so there is nothing to parse
  in the raw HTML.  Idealo is a server-side-rendered price comparison site that
  embeds full product data (name, price, rating, review count) in its HTML,
  exactly like Heureka.cz does for the Czech market.

Data available per product
  Name, ProductURL, Price_EUR, AvgStarRating, ReviewsCount, SKU
  (RecommendRate_pct and ReturnRate_pct are not available on Idealo)

Usage
  python3 idealo_scraper.py                  # from QualityDB/scraper/
  python3 scraper/idealo_scraper.py          # from QualityDB/
"""

import os
import re
import sys
import time
import sqlite3
import logging

try:
    from curl_cffi import requests as cffi_requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run:  pip3 install curl_cffi beautifulsoup4")
    sys.exit(1)

# Allow running from both QualityDB/ and QualityDB/scraper/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scraper.config import DB_PATH, REQUEST_DELAY, MAX_PAGES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL  = "https://www.idealo.de"
DELAY     = max(REQUEST_DELAY, 2.0)   # be polite — at least 2 s between requests
MAX_PAGES = MAX_PAGES or 5            # fall back to 5 if unlimited set in config

# ── Category list ─────────────────────────────────────────────────────────────
# URL pattern: https://www.idealo.de/preisvergleich/CategoryID.html
#              optionally with ?sortby=rating (most sites accept this)
# Each entry: (idealo category path,  Category label (DE),  MainCategory)
#
# Finding category IDs: navigate to idealo.de, click a category, copy the URL.
# The numeric IDs below are stable — Idealo rarely changes them.

CATEGORIES = [
    # ── Phones & Tablets ──────────────────────────────────────────────────────
    ("ProductCategory/3513I16-705.html",     "Smartphones",               "Telefony a tablety"),
    ("ProductCategory/8095.html",            "Tablets",                   "Telefony a tablety"),

    # ── Computers ─────────────────────────────────────────────────────────────
    ("ProductCategory/703.html",             "Laptops & Notebooks",       "Počítače a notebooky"),
    ("ProductCategory/701.html",             "Desktop-PCs",               "Počítače a notebooky"),

    # ── PC Components ─────────────────────────────────────────────────────────
    ("ProductCategory/1670.html",            "Grafikkarten",              "PC komponenty"),
    ("ProductCategory/1668.html",            "CPUs / Prozessoren",        "PC komponenty"),
    ("ProductCategory/1672.html",            "RAM-Speicher",              "PC komponenty"),
    ("ProductCategory/3513I16-1199.html",    "SSDs",                      "Datová úložiště"),
    ("ProductCategory/3513I16-1198.html",    "Externe Festplatten",       "Datová úložiště"),

    # ── Peripherals ───────────────────────────────────────────────────────────
    ("ProductCategory/3513I16-769.html",     "Drucker",                   "Periferie a příslušenství"),
    ("ProductCategory/3513I16-746.html",     "Tastaturen",                "Periferie a příslušenství"),
    ("ProductCategory/3513I16-745.html",     "Mäuse",                     "Periferie a příslušenství"),

    # ── Networking ────────────────────────────────────────────────────────────
    ("ProductCategory/3513I16-778.html",     "WLAN-Router",               "Sítě a konektivita"),

    # ── Audio ─────────────────────────────────────────────────────────────────
    ("ProductCategory/14013.html",           "Kopfhörer",                 "Zvuk a hudba"),
    ("ProductCategory/3513I16-836.html",     "Bluetooth-Lautsprecher",    "Zvuk a hudba"),

    # ── TV & Video ────────────────────────────────────────────────────────────
    ("ProductCategory/691.html",             "Fernseher",                 "Televize a video"),

    # ── Photo ─────────────────────────────────────────────────────────────────
    ("ProductCategory/684.html",             "Digitalkameras",            "Foto a kamery"),

    # ── Gaming ────────────────────────────────────────────────────────────────
    ("ProductCategory/11178.html",           "Spielkonsolen",             "Herní technika"),
    ("ProductCategory/3513I16-829.html",     "Gaming-Headsets",           "Herní technika"),

    # ── Smart devices ─────────────────────────────────────────────────────────
    ("ProductCategory/14139.html",           "Smartwatches",              "Chytré zařízení"),
    ("ProductCategory/3513I16-830.html",     "Smart-Home-Geräte",         "Chytré zařízení"),

    # ── Home Appliances ───────────────────────────────────────────────────────
    ("ProductCategory/3513I16-810.html",     "Waschmaschinen",            "Velké domácí spotřebiče"),
    ("ProductCategory/3513I16-811.html",     "Kühlschränke",              "Velké domácí spotřebiče"),
    ("ProductCategory/3513I16-813.html",     "Kaffeevollautomaten",       "Malé domácí spotřebiče"),
    ("ProductCategory/3513I16-814.html",     "Staubsauger",               "Vysavače a úklid"),
    ("ProductCategory/3513I16-815.html",     "Saugroboter",               "Vysavače a úklid"),
]


# ── Session ───────────────────────────────────────────────────────────────────

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
        time.sleep(1.0)
    except Exception:
        pass
    return s


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_eur(text):
    """'29,99 €' or '1.299,00' → 29.99"""
    if not text:
        return None
    text = re.sub(r"[^\d,.]", "", text.strip())
    text = text.replace(".", "").replace(",", ".")
    try:
        v = float(text)
        return v if v > 0 else None
    except ValueError:
        return None


def parse_rating(text):
    """'4,5' or '4.5 / 5' or '90%' → float"""
    if not text:
        return None
    text = text.strip()
    # percentage → scale to 5
    m = re.search(r"([\d,\.]+)\s*%", text)
    if m:
        try:
            return round(float(m.group(1).replace(",", ".")) / 20, 2)
        except ValueError:
            pass
    # decimal rating
    m = re.search(r"([\d]+[,\.][\d]+)", text)
    if m:
        try:
            v = float(m.group(1).replace(",", "."))
            return v if v <= 5 else round(v / 20, 2)
        except ValueError:
            pass
    return None


def parse_int(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


# ── HTML parsing ──────────────────────────────────────────────────────────────

def parse_product_card(card):
    """
    Extract product data from a single Idealo product card element.
    Idealo's HTML uses several possible selector patterns; we try them all.
    """
    # Product name
    name_el = (
        card.select_one("a.sr-resultItemLink span[class*='title']") or
        card.select_one("a.productOffers-listItemTitleLink") or
        card.select_one("[class*='productTitle']") or
        card.select_one("h2 a") or
        card.select_one("a[class*='title']")
    )
    name = name_el.get_text(strip=True) if name_el else None
    if not name:
        return None

    # Product URL
    link_el = (
        card.select_one("a.sr-resultItemLink") or
        card.select_one("a.productOffers-listItemTitleLink") or
        card.select_one("a[href*='/preisvergleich/']")
    )
    if not link_el:
        return None
    href = link_el.get("href", "")
    product_url = href if href.startswith("http") else BASE_URL + href
    if not product_url:
        return None

    # Price (lowest listed price)
    price_el = (
        card.select_one("[class*='price']") or
        card.select_one("span.price") or
        card.select_one("[data-testid*='price']")
    )
    price = parse_eur(price_el.get_text() if price_el else None)

    # Rating
    rating_el = (
        card.select_one("[class*='rating']") or
        card.select_one("[class*='stars']") or
        card.select_one("[aria-label*='Stern']") or
        card.select_one("[aria-label*='Bewertung']")
    )
    rating = None
    if rating_el:
        aria = rating_el.get("aria-label", "")
        rating = parse_rating(aria) or parse_rating(rating_el.get_text())

    # Review count
    review_el = (
        card.select_one("[class*='ratingCount']") or
        card.select_one("[class*='reviewCount']") or
        card.select_one("[class*='testCount']")
    )
    review_count = parse_int(review_el.get_text() if review_el else None)

    return {
        "Name":             name,
        "ProductURL":       product_url,
        "SKU":              "",
        "Price_EUR":        price,
        "AvgStarRating":    rating,
        "ReviewsCount":     review_count,
        "StarRatingsCount": review_count,
    }


def scrape_category_page(session, cat_path, page=1):
    """Fetch one page of an Idealo category, sorted by rating."""
    # Idealo sort options: ?sortby=rating  (highest rated first)
    url = f"{BASE_URL}/preisvergleich/{cat_path}"
    if "?" in url:
        url += "&sortby=rating"
    else:
        url += "?sortby=rating"
    if page > 1:
        url += f"&page={page}"

    try:
        resp = session.get(url, impersonate="chrome131", timeout=25)
    except Exception as e:
        log.warning(f"    Request error: {e}")
        return [], False

    if resp.status_code == 429:
        log.warning(f"    Rate-limited — waiting 30s")
        time.sleep(30)
        return [], True   # retry
    if resp.status_code == 404:
        log.warning(f"    404 — category path may have changed: {cat_path}")
        return [], False
    if resp.status_code != 200:
        log.warning(f"    HTTP {resp.status_code}")
        return [], False

    soup = BeautifulSoup(resp.text, "html.parser")

    # Idealo product card selectors (tries multiple known patterns)
    cards = (
        soup.select("div.sr-resultItem") or
        soup.select("article[class*='productCard']") or
        soup.select("li[class*='productOffers-listItem']") or
        soup.select("[data-testid*='product-card']") or
        soup.select("div[class*='resultItem']")
    )

    products = []
    for card in cards:
        p = parse_product_card(card)
        if p:
            products.append(p)

    has_next = bool(soup.select_one("a[rel='next'], [class*='pagination'] a[aria-label*='nächste'], [class*='nextPage']"))
    return products, has_next


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
                    p["ProductURL"], p.get("SKU", ""),
                    p.get("Price_EUR"),
                    p.get("AvgStarRating"),
                    p.get("StarRatingsCount"),
                    p.get("ReviewsCount"),
                    "idealo_de", "DE", "EUR",
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


# ── Entry point ───────────────────────────────────────────────────────────────

def scrape_idealo(db_path=None):
    if db_path is None:
        db_path = DB_PATH
    conn    = sqlite3.connect(db_path)
    session = make_session()
    total_ins = total_upd = 0

    for cat_path, cat_label, main_cat in CATEGORIES:
        log.info(f"  Idealo.de  [{cat_label}]")
        for page in range(1, (MAX_PAGES or 5) + 1):
            products, has_next = scrape_category_page(session, cat_path, page)
            ins, upd = upsert_products(conn, products, cat_label, main_cat)
            total_ins += ins
            total_upd += upd
            log.info(f"    page {page}: {len(products)} found → {ins} new, {upd} updated")
            if not products or not has_next:
                break
            time.sleep(DELAY)

    conn.close()
    log.info(f"\nIdealo.de finished: {total_ins} inserted, {total_upd} updated")
    return total_ins, total_upd


if __name__ == "__main__":
    db_path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    scrape_idealo(db_path_arg)
