#!/usr/bin/env python3
"""
darty_scraper.py — Darty.fr (French retail) scraper
─────────────────────────────────────────────────────
Darty is one of France's largest electronics & appliance retailers.
Like Fnac, it is legally required to display the French "Indice de
réparabilité" (repairability, since 2021) and "Indice de durabilité"
(durability, since 2024) for covered product categories.

What it collects
  • Name, price (EUR), star rating, review count
  • Repairability score (0–10) for covered categories
  • Durability score (0–10) for covered categories
  • EAN, model number, brand (from JSON-LD on detail pages)

Categories
  Mirrors Fnac's electronics lineup + major household appliances.

Usage
  python3 -m scraper.darty_scraper
  python3 -m scraper.darty_scraper --dry-run      # print, don't save
  python3 -m scraper.darty_scraper --limit 50     # 50 products per category
  python3 -m scraper.darty_scraper --no-details   # skip detail page fetches

Dependencies
  pip install curl_cffi beautifulsoup4
"""

from __future__ import annotations   # X | None syntax on Python 3.9

import os
import sys
import re
import json
import time
import sqlite3
import logging
import argparse
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

try:
    from curl_cffi import requests as cffi_requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run:\n  pip install curl_cffi beautifulsoup4")
    sys.exit(1)

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
            os.path.join(os.path.dirname(__file__), "darty_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────
BASE_URL    = "https://www.darty.com"
DELAY       = 2.5        # seconds between requests
MAX_PAGES   = 20         # per category (24 products/page)
PAGE_SIZE   = 24
MIN_REVIEWS = 10
COUNTRY     = "FR"
SOURCE      = "darty"

# ── categories ────────────────────────────────────────────────────────────────
# Darty pagination: ?pas=24&debut=N  (debut = offset, 0-based)
# Darty sort by reviews/ratings: ?tri=note (highest rating first)
CATEGORIES = [
    # ── Smartphones (réparabilité + durabilité) ───────────────────────────────
    {
        "name":     "Smartphones",
        "main_cat": "Telefony a tablety",
        "url":      "https://www.darty.com/nav/achat/telephonie/mobile_smartphone/",
        "has_repairability": True,
        "has_durability":    True,
    },
    # ── Tablets ───────────────────────────────────────────────────────────────
    {
        "name":     "Tablets",
        "main_cat": "Telefony a tablety",
        "url":      "https://www.darty.com/nav/achat/telephonie/tablette/",
        "has_repairability": True,
        "has_durability":    False,
    },
    # ── Laptops (réparabilité + durabilité) ───────────────────────────────────
    {
        "name":     "Laptops",
        "main_cat": "Počítače a notebooky",
        "url":      "https://www.darty.com/nav/achat/informatique/ordinateur_portable/",
        "has_repairability": True,
        "has_durability":    True,
    },
    # ── Desktops ──────────────────────────────────────────────────────────────
    {
        "name":     "Desktops",
        "main_cat": "Počítače a notebooky",
        "url":      "https://www.darty.com/nav/achat/informatique/ordinateur_de_bureau/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── TVs (réparabilité + durabilité) ───────────────────────────────────────
    {
        "name":     "TVs",
        "main_cat": "Televize a video",
        "url":      "https://www.darty.com/nav/achat/image_son/television/",
        "has_repairability": True,
        "has_durability":    True,
    },
    # ── Monitors ─────────────────────────────────────────────────────────────
    {
        "name":     "Monitors",
        "main_cat": "Počítače a notebooky",
        "url":      "https://www.darty.com/nav/achat/informatique/ecran/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Headphones ───────────────────────────────────────────────────────────
    {
        "name":     "Headphones",
        "main_cat": "Zvuk a hudba",
        "url":      "https://www.darty.com/nav/achat/image_son/casque_audio/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Speakers ─────────────────────────────────────────────────────────────
    {
        "name":     "Speakers",
        "main_cat": "Zvuk a hudba",
        "url":      "https://www.darty.com/nav/achat/image_son/enceinte/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Soundbars ────────────────────────────────────────────────────────────
    {
        "name":     "Soundbars",
        "main_cat": "Zvuk a hudba",
        "url":      "https://www.darty.com/nav/achat/image_son/barre_de_son/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Cameras ──────────────────────────────────────────────────────────────
    {
        "name":     "Cameras",
        "main_cat": "Foto a kamery",
        "url":      "https://www.darty.com/nav/achat/image_son/appareil_photo/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Smartwatches ─────────────────────────────────────────────────────────
    {
        "name":     "Smartwatches",
        "main_cat": "Chytré zařízení",
        "url":      "https://www.darty.com/nav/achat/peripherique/montre_connectee/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Washing Machines (réparabilité + durabilité) ──────────────────────────
    {
        "name":     "Washing Machines",
        "main_cat": "Velké domácí spotřebiče",
        "url":      "https://www.darty.com/nav/achat/gros_electromenager/lave_linge/",
        "has_repairability": True,
        "has_durability":    True,
    },
    # ── Tumble Dryers ─────────────────────────────────────────────────────────
    {
        "name":     "Tumble Dryers",
        "main_cat": "Velké domácí spotřebiče",
        "url":      "https://www.darty.com/nav/achat/gros_electromenager/seche_linge/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Dishwashers (réparabilité + durabilité) ───────────────────────────────
    {
        "name":     "Dishwashers",
        "main_cat": "Velké domácí spotřebiče",
        "url":      "https://www.darty.com/nav/achat/gros_electromenager/lave_vaisselle/",
        "has_repairability": True,
        "has_durability":    True,
    },
    # ── Fridges & Freezers ────────────────────────────────────────────────────
    {
        "name":     "Fridges & Freezers",
        "main_cat": "Velké domácí spotřebiče",
        "url":      "https://www.darty.com/nav/achat/gros_electromenager/refrigerateur/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Ovens ─────────────────────────────────────────────────────────────────
    {
        "name":     "Ovens",
        "main_cat": "Velké domácí spotřebiče",
        "url":      "https://www.darty.com/nav/achat/gros_electromenager/four/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Vacuum Cleaners (durabilité 2024) ─────────────────────────────────────
    {
        "name":     "Vacuum Cleaners",
        "main_cat": "Vysavače a úklid",
        "url":      "https://www.darty.com/nav/achat/electromenager/aspirateur/",
        "has_repairability": False,
        "has_durability":    True,
    },
    # ── Robot Vacuums ─────────────────────────────────────────────────────────
    {
        "name":     "Robot Vacuums",
        "main_cat": "Vysavače a úklid",
        "url":      "https://www.darty.com/nav/achat/electromenager/aspirateur_robot/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Coffee Machines ───────────────────────────────────────────────────────
    {
        "name":     "Coffee Machines",
        "main_cat": "Malé domácí spotřebiče",
        "url":      "https://www.darty.com/nav/achat/electromenager/machine_a_cafe/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Air Conditioners ──────────────────────────────────────────────────────
    {
        "name":     "Air Conditioners",
        "main_cat": "Velké domácí spotřebiče",
        "url":      "https://www.darty.com/nav/achat/electromenager/climatiseur/",
        "has_repairability": False,
        "has_durability":    False,
    },
    # ── Lawnmowers (réparabilité) ─────────────────────────────────────────────
    {
        "name":     "Lawnmowers",
        "main_cat": "Zahrada a dílna",
        "url":      "https://www.darty.com/nav/achat/jardin/tondeuse/",
        "has_repairability": True,
        "has_durability":    False,
    },
]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

# Top-level nav pages to walk through during warm-up, mimicking a user who
# arrives at the homepage then browses into electronics before landing on a
# category listing. This establishes a realistic navigation trail and lets
# Darty's WAF see a credible cookie/referer chain before we fetch listings.
_WARMUP_TRAIL = [
    BASE_URL + "/",
    BASE_URL + "/nav/achat/image_son/",           # Audio / Image top-level
    BASE_URL + "/nav/achat/telephonie/",           # Phones top-level
    BASE_URL + "/nav/achat/informatique/",         # IT top-level
    BASE_URL + "/nav/achat/gros_electromenager/",  # Appliances top-level
]


def make_session() -> cffi_requests.Session:
    session = cffi_requests.Session(impersonate="chrome131")
    prev = BASE_URL + "/"
    for step_url in _WARMUP_TRAIL:
        try:
            r = session.get(step_url, headers=_nav_headers(prev), timeout=20)
            log.debug(f"Warm-up {step_url} → HTTP {r.status_code}")
            time.sleep(1.5)
            prev = step_url
        except Exception as e:
            log.warning(f"Warm-up step {step_url} failed ({e}) — continuing.")
    log.info("Session warmed up (walked Darty nav trail).")
    time.sleep(2.0)   # extra pause before first real fetch
    return session


def _nav_headers(referer: str = BASE_URL + "/") -> dict:
    """Full navigation-style headers that real Chrome sends for page loads."""
    return {
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language":           "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding":           "gzip, deflate, br",
        "Cache-Control":             "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "same-origin",
        "Sec-Fetch-User":            "?1",
        "Referer":                   referer,
    }


# Keep old _headers() as an alias so any other callers don't break.
def _headers() -> dict:
    return _nav_headers()


def fetch(url: str, session, referer: str | None = None) -> BeautifulSoup | None:
    hdrs = _nav_headers(referer or BASE_URL + "/")
    for attempt in range(1, 3):   # up to 2 attempts
        try:
            resp = session.get(url, headers=hdrs, timeout=30)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "html.parser")
            log.warning(f"HTTP {resp.status_code}  {url}  (attempt {attempt})")
            if resp.status_code == 403 and attempt == 1:
                time.sleep(4.0)   # back off and retry once
                continue
            return None
        except Exception as e:
            log.error(f"Fetch error: {e}  [{url}]")
            return None
    return None


def page_url(base: str, page: int) -> str:
    """Darty paginates with ?pas=24&debut=N (offset-based)."""
    if page == 1:
        return base
    offset = (page - 1) * PAGE_SIZE
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}pas={PAGE_SIZE}&debut={offset}"


_TRACKING_PARAMS = frozenset({"utm_source", "utm_medium", "utm_campaign", "ref", "origin"})


def canonical_url(url: str) -> str:
    """Strip tracking params so the same product always has the same key."""
    if not url:
        return url
    if not url.startswith("http"):
        url = urljoin(BASE_URL, url)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=False)
    clean_qs = {k: v for k, v in qs.items() if k not in _TRACKING_PARAMS}
    return urlunparse(parsed._replace(query=urlencode(clean_qs, doseq=True)))


# ── parsers ───────────────────────────────────────────────────────────────────

def parse_price(text: str) -> float | None:
    """'249,99 €' or '1 299 €' → float."""
    if not text:
        return None
    text = text.replace("\xa0", " ").replace("\u202f", " ").strip()
    # Remove currency symbols and normalise
    text = re.sub(r"[€$£]", "", text).strip()
    # French thousands separator (space) + decimal comma
    # e.g. "1 299,99" → "1299.99"
    text = text.replace(" ", "")
    text = text.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(m.group(1)) if m else None


def parse_stars(text: str) -> float | None:
    """'4,5/5' '4.2 sur 5' '4.2' → float 0–5."""
    if not text:
        return None
    text = text.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:/|sur)\s*5", text)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    if m:
        val = float(m.group(1))
        return val if 0.0 <= val <= 5.0 else None
    return None


def parse_reviews(text: str) -> int:
    """'(123 avis)' '123' → int."""
    if not text:
        return 0
    m = re.search(r"(\d[\d\s\xa0]*)", text)
    return int(re.sub(r"[\s\xa0]", "", m.group(1))) if m else 0


def parse_index_score(text: str) -> float | None:
    """'7,2/10' '8.5 / 10' 'Indice : 6.4' → float 0–10."""
    if not text:
        return None
    text = text.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*/\s*10", text)
    if m:
        val = float(m.group(1))
        return val if 0.0 <= val <= 10.0 else None
    m = re.search(r"(\d+\.\d+)", text)
    if m:
        val = float(m.group(1))
        return val if 0.0 <= val <= 10.0 else None
    return None


def stars_to_pct(stars: float | None) -> float | None:
    if stars is None:
        return None
    return round((stars / 5.0) * 100.0, 1)


# ── listing page scraper ──────────────────────────────────────────────────────

def scrape_listing(soup: BeautifulSoup, cat: dict) -> list[dict]:
    """
    Extract product stubs from one Darty category listing page.

    Darty renders products inside:
      - <li class="product-list-item"> or <article class="c-product-item">
      - JSON embedded in <script type="application/ld+json"> (ItemList)
    We try multiple strategies in order.
    """
    products = []

    # ── Strategy 1: JSON-LD ItemList (most reliable if present) ──────────────
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                for item in data:
                    if item.get("@type") == "ItemList":
                        data = item
                        break
            if data.get("@type") == "ItemList":
                for element in data.get("itemListElement", []):
                    item = element.get("item", element)
                    name = item.get("name")
                    url = item.get("url") or item.get("@id")
                    if not name or not url:
                        continue
                    url = canonical_url(url)
                    price = None
                    offers = item.get("offers", {})
                    if isinstance(offers, list) and offers:
                        offers = offers[0]
                    if isinstance(offers, dict):
                        price = parse_price(str(offers.get("price", "")))
                    stars = None
                    agg = item.get("aggregateRating", {})
                    if agg:
                        stars = agg.get("ratingValue")
                        if stars:
                            stars = float(str(stars).replace(",", "."))
                    reviews = 0
                    if agg:
                        reviews = int(agg.get("reviewCount", 0) or agg.get("ratingCount", 0))
                    products.append({
                        "Name":                   name,
                        "ProductURL":             url,
                        "Price_EUR":              price,
                        "AvgStarRating":          stars,
                        "RecommendRate_pct":      stars_to_pct(stars),
                        "ReviewsCount":           reviews,
                        "repairability_score_fr": None,
                        "durability_score_fr":    None,
                    })
                if products:
                    log.debug(f"  JSON-LD ItemList gave {len(products)} products.")
                    return products
        except Exception:
            pass

    # ── Strategy 2: HTML product cards ────────────────────────────────────────
    # Darty uses several card variants depending on page type:
    #   <li class="product-list-item">
    #   <article class="c-product-item"> or <div class="c-product-item">
    #   <div class="product-hit"> (Algolia-based search results)
    cards = (
        soup.select("li.product-list-item")
        or soup.select("article.c-product-item")
        or soup.select("div.c-product-item")
        or soup.select("div.product-hit")
        or soup.select("[class*='product-item']")
        or soup.select("[class*='ProductItem']")
    )

    if not cards:
        log.debug("  No product cards found on page.")
        return []

    for card in cards:
        # Name + URL
        name_el = (
            card.select_one("a[class*='title']")
            or card.select_one("a[class*='Title']")
            or card.select_one("h2 a")
            or card.select_one("h3 a")
            or card.select_one("p[class*='title'] a")
            or card.select_one("a[class*='name']")
        )
        if not name_el:
            # Try to get name from any anchor with substantial text
            for a in card.find_all("a", href=True):
                txt = a.get_text(strip=True)
                if len(txt) > 15:
                    name_el = a
                    break
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        url = name_el.get("href", "")
        if not name or not url:
            continue
        url = canonical_url(url)

        # Price
        price_el = (
            card.select_one("[class*='price']:not([class*='old']):not([class*='before'])")
            or card.select_one("[class*='Price']:not([class*='Old'])")
            or card.select_one(".price")
        )
        price = parse_price(price_el.get_text(strip=True) if price_el else "")

        # Star rating
        stars = None
        review_count = 0

        # Darty uses aria-label="X sur 5" on star containers, or data-note="X"
        star_el = (
            card.select_one("[aria-label*='sur 5']")
            or card.select_one("[aria-label*='/5']")
            or card.select_one("[data-note]")
            or card.select_one("[class*='star']")
            or card.select_one("[class*='Star']")
            or card.select_one("[class*='rating']")
            or card.select_one("[class*='Rating']")
        )
        if star_el:
            # Try aria-label first ("4,5 sur 5 étoiles")
            aria = star_el.get("aria-label", "")
            if aria:
                stars = parse_stars(aria)
            # Try data-note attribute
            if stars is None:
                dn = star_el.get("data-note", "")
                if dn:
                    stars = parse_stars(dn)
            # Try text content
            if stars is None:
                stars = parse_stars(star_el.get_text(strip=True))

        # Review count
        review_el = (
            card.select_one("[class*='review']:not([class*='Review'])")
            or card.select_one("[class*='avis']")
            or card.select_one("[class*='Avis']")
        )
        if review_el:
            review_count = parse_reviews(review_el.get_text(strip=True))
        else:
            # Search text for N avis pattern
            block_text = card.get_text(separator=" ", strip=True)
            m = re.search(r"(\d[\d\s\xa0]{0,5})\s*avis", block_text, re.IGNORECASE)
            if m:
                review_count = parse_reviews(m.group(1))

        # Repairability / durability index badges on card
        repair_score = None
        durability_score = None
        for el in card.select(
            "[class*='sustainability'], [class*='repairabilit'], "
            "[class*='reparabilit'], [class*='durabilit'], "
            "[class*='indice'], [data-repair], [data-durability]"
        ):
            text = el.get_text(" ", strip=True)
            score = parse_index_score(text)
            cls = " ".join(el.get("class", [])).lower()
            if "durab" in cls:
                if durability_score is None:
                    durability_score = score
            elif "repair" in cls or "répar" in cls or "reparab" in cls:
                if repair_score is None:
                    repair_score = score

        products.append({
            "Name":                   name,
            "ProductURL":             url,
            "Price_EUR":              price,
            "AvgStarRating":          stars,
            "RecommendRate_pct":      stars_to_pct(stars),
            "ReviewsCount":           review_count,
            "repairability_score_fr": repair_score,
            "durability_score_fr":    durability_score,
        })

    return products


# ── product detail page scraper ───────────────────────────────────────────────

def scrape_detail(url: str, session, listing_url: str | None = None) -> dict:
    """
    Fetch a Darty product page and extract:
      • repairability_score_fr + sub-criteria
      • durability_score_fr + sub-criteria
      • EAN, model number, brand (from JSON-LD)
    Returns dict of extra fields (may be empty).
    """
    time.sleep(DELAY)
    # Use the listing page we came from as referer, just like a real browser would.
    referer = listing_url or BASE_URL + "/"
    soup = fetch(url, session, referer=referer)
    if soup is None:
        return {}

    extras = {}

    # JSON-LD Product schema
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = next((d for d in data if d.get("@type") == "Product"), data[0])
            if data.get("@type") == "Product":
                brand = data.get("brand")
                extras["brand"] = brand.get("name") if isinstance(brand, dict) else brand
                extras["ean"]   = (data.get("gtin13") or data.get("gtin")
                                   or data.get("gtin8") or data.get("mpn"))
                extras["model_number"] = data.get("mpn") or data.get("sku")
                rel = data.get("releaseDate") or data.get("productionDate")
                if rel:
                    extras["release_date"] = rel[:10]
                    extras["release_year"] = int(rel[:4])
                break
        except Exception:
            pass

    # Repairability / durability scores from page
    repair_score = None
    durability_score = None
    repair_sub = {}
    durability_sub = {}

    for el in soup.select(
        "[class*='sustainability'], [class*='repairabilit'], "
        "[class*='reparabilit'], [class*='durabilit'], "
        "[class*='indice-rep'], [class*='indice-dur'], "
        "[class*='IndiceRep'], [class*='indiceDurab']"
    ):
        text = el.get_text(" ", strip=True)
        score = parse_index_score(text)
        cls = " ".join(el.get("class", [])).lower()
        if "durab" in cls:
            if durability_score is None and score is not None:
                durability_score = score
        else:
            if repair_score is None and score is not None:
                repair_score = score

    # Labelled headings pattern ("Indice de réparabilité X/10")
    for heading in soup.find_all(
        ["h2", "h3", "h4", "div", "span", "p"],
        string=re.compile(r"indice de (répar|durab)", re.I),
    ):
        nxt = heading.find_next(string=re.compile(r"\d[,.]?\d?\s*/\s*10"))
        if nxt:
            sc = parse_index_score(nxt)
            if "durab" in heading.get_text().lower() and durability_score is None:
                durability_score = sc
            elif repair_score is None:
                repair_score = sc

    # Sub-criteria (dt/dd or table rows)
    REPAIR_CRITERIA = {
        "documentation": re.compile(r"document|notice|manuel", re.I),
        "spare_parts":   re.compile(r"pi[eè]ces détach|spare", re.I),
        "price_parts":   re.compile(r"prix des pi[eè]ces", re.I),
        "software":      re.compile(r"logiciel|software|mise à jour", re.I),
        "ease":          re.compile(r"facilit[eé]|démontage", re.I),
    }
    DURABILITY_CRITERIA = {
        "reliability":     re.compile(r"fiabilit[eé]|robustesse", re.I),
        "spare_parts":     re.compile(r"disponibilit[eé].*pi[eè]ces", re.I),
        "spare_parts_yrs": re.compile(r"\d+\s*ans.*pi[eè]ces", re.I),
        "software":        re.compile(r"mise à jour|maj", re.I),
        "repairability":   re.compile(r"r[eé]parabilit[eé]", re.I),
    }

    for dt in soup.find_all(["dt", "th", "td", "li", "div"]):
        label = dt.get_text(strip=True)
        dd = dt.find_next_sibling() or dt.find_next(["dd", "td"])
        if not dd:
            continue
        val_text = dd.get_text(strip=True)
        score = parse_index_score(val_text)
        if score is None:
            continue
        for key, pattern in REPAIR_CRITERIA.items():
            if pattern.search(label):
                repair_sub[key] = score
        for key, pattern in DURABILITY_CRITERIA.items():
            if pattern.search(label):
                durability_sub[key] = score

    if repair_score is not None:
        extras["repairability_score_fr"] = repair_score
    if repair_sub:
        extras["repairability_sub_fr"] = json.dumps(repair_sub, ensure_ascii=False)
    if durability_score is not None:
        extras["durability_score_fr"] = durability_score
    if durability_sub:
        extras["durability_sub_fr"] = json.dumps(durability_sub, ensure_ascii=False)

    return extras


# ── database ──────────────────────────────────────────────────────────────────

def open_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")
    return conn


def upsert_products(conn: sqlite3.Connection, products: list[dict],
                    cat_name: str, main_cat: str) -> tuple[int, int]:
    """Manual upsert — works with or without UNIQUE index on ProductURL."""
    ensure_snapshot_table(conn)
    cur = conn.cursor()
    inserted = updated = 0

    for p in products:
        url = p.get("ProductURL", "")
        if not url:
            continue

        existing = cur.execute(
            "SELECT rowid FROM products WHERE ProductURL = ? LIMIT 1", (url,)
        ).fetchone()

        if existing:
            cur.execute(
                """UPDATE products SET
                   Price_EUR=?, AvgStarRating=?, RecommendRate_pct=?,
                   ReviewsCount=?, Category=?, MainCategory=?,
                   repairability_score_fr=?, durability_score_fr=?,
                   repairability_sub_fr=?, durability_sub_fr=?
                   WHERE ProductURL=?""",
                (
                    p.get("Price_EUR"),
                    p.get("AvgStarRating"),
                    p.get("RecommendRate_pct"),
                    p.get("ReviewsCount", 0),
                    cat_name,
                    main_cat,
                    p.get("repairability_score_fr"),
                    p.get("durability_score_fr"),
                    p.get("repairability_sub_fr"),
                    p.get("durability_sub_fr"),
                    url,
                ),
            )
            updated += 1
        else:
            cur.execute(
                """INSERT INTO products
                   (Name, Category, MainCategory, ProductURL,
                    Price_EUR, AvgStarRating, RecommendRate_pct, ReviewsCount,
                    repairability_score_fr, durability_score_fr,
                    repairability_sub_fr, durability_sub_fr,
                    source, country)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    p["Name"], cat_name, main_cat, url,
                    p.get("Price_EUR"),
                    p.get("AvgStarRating"),
                    p.get("RecommendRate_pct"),
                    p.get("ReviewsCount", 0),
                    p.get("repairability_score_fr"),
                    p.get("durability_score_fr"),
                    p.get("repairability_sub_fr"),
                    p.get("durability_sub_fr"),
                    SOURCE, COUNTRY,
                ),
            )
            inserted += 1

        record_snapshot(conn, url, SOURCE, p, country=COUNTRY)

    conn.commit()
    return inserted, updated


# ── main scrape loop ───────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn, dry_run=False,
                    limit=None, fetch_details=True) -> dict:
    base = cat["url"]
    name = cat["name"]
    log.info(f"── {name}  ({base})")

    # Build the parent-category referer from the base URL by going one level up.
    # e.g. /nav/achat/telephonie/mobile_smartphone/ → /nav/achat/telephonie/
    parts = base.rstrip("/").rsplit("/", 1)
    parent_referer = parts[0] + "/" if len(parts) > 1 else BASE_URL + "/"

    all_products: list[dict] = []
    prev_url = parent_referer  # start referer chain from parent nav page
    for page in range(1, MAX_PAGES + 1):
        url = page_url(base, page)
        log.info(f"   Page {page}: {url}")
        soup = fetch(url, session, referer=prev_url)
        prev_url = url  # next page's referer = this page
        if soup is None:
            log.info("   Fetch failed — stopping.")
            break

        products = scrape_listing(soup, cat)
        if not products:
            log.info("   No products found — stopping.")
            break

        all_products.extend(products)
        log.info(f"   Got {len(products)} products (total so far: {len(all_products)})")

        if limit and len(all_products) >= limit:
            all_products = all_products[:limit]
            log.info(f"   Reached --limit {limit}, stopping.")
            break

        time.sleep(DELAY)

    # Filter by min reviews
    qualified = [p for p in all_products if p["ReviewsCount"] >= MIN_REVIEWS]
    log.info(f"   Qualified (≥{MIN_REVIEWS} reviews): {len(qualified)} / {len(all_products)}")

    # Fetch detail pages for repairability/durability if not already present
    if fetch_details and not dry_run:
        needs_detail = [
            p for p in qualified
            if (cat.get("has_repairability") or cat.get("has_durability"))
            and p.get("repairability_score_fr") is None
            and p.get("durability_score_fr") is None
        ]
        log.info(f"   Fetching detail pages for {len(needs_detail)} products...")
        listing_ref = page_url(base, 1)  # use page-1 listing as referer
        for p in needs_detail:
            extras = scrape_detail(p["ProductURL"], session, listing_url=listing_ref)
            p.update(extras)

    if dry_run:
        for p in qualified[:5]:
            log.info(f"   DRY-RUN: {p['Name'][:60]}  €{p.get('Price_EUR')}  "
                     f"★{p.get('AvgStarRating')}  {p.get('ReviewsCount')} reviews  "
                     f"IR={p.get('repairability_score_fr')}  DUR={p.get('durability_score_fr')}")
        return {"inserted": 0, "updated": 0, "found": len(qualified)}

    inserted, updated = upsert_products(
        conn, qualified, cat["name"], cat.get("main_cat", cat["name"])
    )
    log.info(f"   → {inserted} inserted, {updated} updated")
    return {"inserted": inserted, "updated": updated, "found": len(qualified)}


def run(dry_run=False, limit=None, fetch_details=True):
    log.info("=" * 60)
    log.info("QualityDB — Darty.fr Scraper")
    log.info("=" * 60)

    session = make_session()
    conn = open_db() if not dry_run else None
    totals = {"inserted": 0, "updated": 0, "errors": []}

    for cat in CATEGORIES:
        try:
            result = scrape_category(cat, session, conn, dry_run=dry_run,
                                     limit=limit, fetch_details=fetch_details)
            totals["inserted"] += result.get("inserted", 0)
            totals["updated"]  += result.get("updated", 0)
        except Exception as e:
            log.error(f"Error in {cat['name']}: {e}", exc_info=True)
            totals["errors"].append(str(e))
        time.sleep(DELAY)

    if conn:
        conn.close()

    log.info(f"Done — {totals['inserted']} inserted, {totals['updated']} updated, "
             f"{len(totals['errors'])} errors.")
    return totals


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Darty.fr scraper for QualityDB")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Print products without saving to DB")
    parser.add_argument("--limit",      type=int, default=None,
                        help="Max products per category (for testing)")
    parser.add_argument("--no-details", action="store_true",
                        help="Skip detail-page fetches (faster, less data)")
    args = parser.parse_args()

    result = run(
        dry_run=args.dry_run,
        limit=args.limit,
        fetch_details=not args.no_details,
    )
    print(f"\n✓  Done. {result['inserted']} inserted, {result['updated']} updated.")
