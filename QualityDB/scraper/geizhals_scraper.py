#!/usr/bin/env python3
"""
geizhals_scraper.py
────────────────────
Scrapes top-rated products from Geizhals.de — Germany's most popular
hardware & electronics price comparison site (same concept as Heureka.cz).

Why Geizhals instead of Idealo / MediaMarkt?
  • Server-side rendered HTML — product data is in the raw page, no JS needed.
  • Same architecture as Heureka.cz (price comparison site, not a shop).
  • Lower bot detection aggression than Idealo or MediaMarkt.
  • curl_cffi Chrome impersonation is sufficient.

URL format
  https://geizhals.de/?cat={category_code}&sort=r&pg={page}
    sort=r  → sort by user rating (descending)
    pg=N    → page number

Usage
  python3 geizhals_scraper.py               # from QualityDB/scraper/
  python3 scraper/geizhals_scraper.py       # from QualityDB/
  python3 geizhals_scraper.py --debug       # show HTML structure and exit
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

BASE_URL = "https://geizhals.de"
DELAY    = max(REQUEST_DELAY, 2.0)

# ── Category codes (verified from live site) ──────────────────────────────────
# Format: (cat_code, Category label DE, MainCategory)
# Pagination: ?cat=CODE&sort=r&pg=N
#
# sort=r  = sort by user rating (Bewertung), highest first
CATEGORIES = [
    # Phones & Tablets
    ("umtsover",         "Smartphones",                  "Telefony a tablety"),
    ("umtstab",          "Tablets",                      "Telefony a tablety"),

    # Computers
    ("nb",               "Laptops & Notebooks",          "Počítače a notebooky"),
    ("desktop",          "Desktop-PCs",                  "Počítače a notebooky"),

    # PC Components
    ("gra16_512",        "Grafikkarten",                 "PC komponenty"),
    ("cpu",              "Prozessoren (CPUs)",            "PC komponenty"),
    ("ramddr4",          "Arbeitsspeicher (RAM)",         "PC komponenty"),

    # Storage
    ("hdx",              "Externe Festplatten & SSDs",   "Datová úložiště"),
    ("hd",               "Interne Festplatten",          "Datová úložiště"),

    # Peripherals
    ("drucker",          "Drucker",                      "Periferie a příslušenství"),
    ("tastatur",         "Tastaturen",                   "Periferie a příslušenství"),
    ("maus",             "Mäuse",                        "Periferie a příslušenství"),

    # Networking
    ("wlanrouter",       "WLAN-Router",                  "Sítě a konektivita"),

    # Audio
    ("koph",             "Kopfhörer",                    "Zvuk a hudba"),
    ("multls",           "Lautsprecher & Soundbars",     "Zvuk a hudba"),

    # TV & Video
    ("tvger",            "Fernseher",                    "Televize a video"),

    # Photo
    ("dcam",             "Digitalkameras",               "Foto a kamery"),

    # Gaming
    ("spielkons",        "Spielkonsolen",                "Herní technika"),

    # Smart devices
    ("uhrpm",            "Smartwatches",                 "Chytré zařízení"),
    ("smarthomehub",     "Smart-Home-Geräte",            "Chytré zařízení"),

    # Large appliances
    ("waschma",          "Waschmaschinen",               "Velké domácí spotřebiče"),
    ("kuehlschrank",     "Kühlschränke",                 "Velké domácí spotřebiče"),

    # Small appliances
    ("kaffeeauto",       "Kaffeevollautomaten",          "Malé domácí spotřebiče"),

    # Vacuums
    ("staubsauger",      "Staubsauger",                  "Vysavače a úklid"),
    ("saugroboter",      "Saugroboter",                  "Vysavače a úklid"),
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
        time.sleep(1.5)
    except Exception:
        pass
    return s


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_eur(text):
    if not text:
        return None
    text = re.sub(r"[^\d,.]", "", str(text)).replace(".", "").replace(",", ".")
    try:
        v = float(text)
        return v if v > 0 else None
    except ValueError:
        return None


def parse_rating(text):
    """'87%' → 4.35,  '4,5' → 4.5"""
    if not text:
        return None
    text = str(text).strip()
    m = re.search(r"([\d]+[,\.][\d]+)", text)
    if m:
        try:
            v = float(m.group(1).replace(",", "."))
            return v if v <= 5 else round(v / 20, 2)
        except ValueError:
            pass
    m = re.search(r"(\d+)\s*%", text)
    if m:
        try:
            return round(int(m.group(1)) / 20, 2)
        except ValueError:
            pass
    return None


def parse_int(text):
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", str(text))
    return int(digits) if digits else None


# ── HTML parsing ──────────────────────────────────────────────────────────────

def parse_product_card(card):
    """
    Extract product data from a Geizhals product list item.
    Geizhals uses class names like 'productlist__item', 'productlist-title', etc.
    Multiple selector patterns cover different versions of the HTML.
    """
    # Name
    name_el = (
        card.select_one("a.productlist-fullname") or
        card.select_one("a.productlist__name") or
        card.select_one(".productlist-title a") or
        card.select_one("span.productlist-fullname") or
        card.select_one("a[class*='productname']") or
        card.select_one("h3 a") or
        card.select_one("h2 a")
    )
    name = name_el.get_text(strip=True) if name_el else None
    if not name:
        return None

    # URL
    link_el = (
        card.select_one("a.productlist-fullname") or
        card.select_one("a.productlist__name") or
        card.select_one("a[href*='/a']")     # Geizhals product URLs: /a1234567.html
    )
    if not link_el:
        return None
    href = link_el.get("href", "")
    product_url = href if href.startswith("http") else BASE_URL + href
    if not product_url or product_url == BASE_URL:
        return None

    # Price (best price listed)
    price_el = (
        card.select_one("span.price_amount") or
        card.select_one(".price span") or
        card.select_one("[class*='price']")
    )
    price = parse_eur(price_el.get_text() if price_el else None)

    # Rating  (Geizhals shows % or star score)
    rating_el = (
        card.select_one(".userrating-score") or
        card.select_one("[class*='rating-score']") or
        card.select_one("[class*='userrating']") or
        card.select_one("[aria-label*='Bewertung']") or
        card.select_one("[class*='stars']")
    )
    rating = None
    if rating_el:
        aria = rating_el.get("aria-label", "")
        rating = parse_rating(aria) or parse_rating(rating_el.get_text())

    # Review / rating count
    count_el = (
        card.select_one(".userrating-votes") or
        card.select_one("[class*='votes']") or
        card.select_one("[class*='rating-count']") or
        card.select_one("[class*='reviewcount']")
    )
    review_count = parse_int(count_el.get_text() if count_el else None)

    return {
        "Name":             name,
        "ProductURL":       product_url,
        "SKU":              "",
        "Price_EUR":        price,
        "AvgStarRating":    rating,
        "ReviewsCount":     review_count,
        "StarRatingsCount": review_count,
    }


def scrape_page(session, cat_code, page=1):
    url = f"{BASE_URL}/?cat={cat_code}&sort=r&pg={page}"
    try:
        resp = session.get(url, impersonate="chrome131", timeout=25)
    except Exception as e:
        log.warning(f"    Request error: {e}")
        return [], False

    if resp.status_code == 429:
        log.warning("    Rate-limited — waiting 30s")
        time.sleep(30)
        return [], False
    if resp.status_code == 404:
        log.warning(f"    404 for cat='{cat_code}' — code may have changed")
        return [], False
    if resp.status_code != 200:
        log.warning(f"    HTTP {resp.status_code}")
        return [], False

    soup  = BeautifulSoup(resp.text, "html.parser")

    # Product list items — try multiple selectors
    cards = (
        soup.select("li.productlist__item") or
        soup.select("article.productlist__item") or
        soup.select("div.productlist__item") or
        soup.select(".productlist li") or
        soup.select("li[class*='productlist']")
    )

    products = [p for p in (parse_product_card(c) for c in cards) if p]

    # Check for next page link
    has_next = bool(
        soup.select_one("a[rel='next']") or
        soup.select_one("a.nav_next") or
        soup.select_one("[class*='pagination'] a[class*='next']")
    )

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
                    "geizhals_de", "DE", "EUR",
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


# ── Debug mode ────────────────────────────────────────────────────────────────

def debug_print(session):
    url  = f"{BASE_URL}/?cat=umtsover&sort=r"
    resp = session.get(url, impersonate="chrome131", timeout=25)
    print(f"Status : {resp.status_code}  Body: {len(resp.text)} chars")
    if resp.status_code != 200:
        print(resp.text[:500])
        return
    soup  = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.string if soup.title else "(no title)"
    print(f"Title  : {title}")

    # Show what product-like elements exist
    for sel in ["li.productlist__item", "article.productlist__item",
                "div.productlist__item", "li[class*='productlist']"]:
        found = soup.select(sel)
        print(f"Selector '{sel}': {len(found)} items")
        if found:
            first = found[0]
            print(f"  First item classes: {first.get('class')}")
            print(f"  First item text (100 chars): {first.get_text(strip=True)[:100]}")
            # Try to find name
            for name_sel in ["a.productlist-fullname", "a.productlist__name",
                             ".productlist-title a", "h3 a", "h2 a"]:
                el = first.select_one(name_sel)
                if el:
                    print(f"  Name via '{name_sel}': {el.get_text(strip=True)[:60]}")
                    break


# ── Entry point ───────────────────────────────────────────────────────────────

def scrape_geizhals(db_path=None):
    if db_path is None:
        db_path = DB_PATH

    if "--debug" in sys.argv:
        debug_print(make_session())
        return 0, 0

    conn    = sqlite3.connect(db_path)
    session = make_session()
    total_ins = total_upd = 0

    for cat_code, cat_label, main_cat in CATEGORIES:
        log.info(f"  Geizhals.de  [{cat_label}]")
        for page in range(1, (MAX_PAGES or 5) + 1):
            products, has_next = scrape_page(session, cat_code, page)
            ins, upd = upsert_products(conn, products, cat_label, main_cat)
            total_ins += ins
            total_upd += upd
            log.info(f"    page {page}: {len(products)} found → {ins} new, {upd} updated")
            if not products or not has_next:
                break
            time.sleep(DELAY)

    conn.close()
    log.info(f"\nGeizhals.de finished: {total_ins} inserted, {total_upd} updated")
    return total_ins, total_upd


if __name__ == "__main__":
    db_path_arg = None
    for arg in sys.argv[1:]:
        if not arg.startswith("--"):
            db_path_arg = arg
    scrape_geizhals(db_path_arg)
