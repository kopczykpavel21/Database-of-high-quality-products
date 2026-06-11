#!/usr/bin/env python3
"""
fnac_scraper.py — Fnac.fr (French retail) scraper with durability index support
─────────────────────────────────────────────────────────────────────────────────
What it collects
  • Standard product data: name, price, star rating, review count
  • Indice de réparabilité  (0–10, loi AGEC, mandatory since 2021)
  • Indice de durabilité    (0–10, loi AGEC, mandatory since 2024)
  • Sub-criterion scores for both indices (documentation, spare parts,
    software updates, ease of repair, reliability…)
  • EAN, model number, energy class (from product detail page)
  • Brand, normalised model name

Legal basis
  France's loi AGEC (anti-gaspillage pour une économie circulaire) mandates
  that manufacturers publish repairability and durability scores on all covered
  product pages.  These scores are public information legally required to be
  displayed; scraping them for academic research on planned obsolescence is
  consistent with their stated public-interest purpose.

Covered product categories (as of 2024)
  • Smartphones              (réparabilité 2021, durabilité 2024)
  • Laptops / notebooks      (réparabilité 2022, durabilité 2024)
  • Televisions              (réparabilité 2022, durabilité 2024)
  • Washing machines         (réparabilité 2021, durabilité 2024)
  • Dishwashers              (réparabilité 2022, durabilité 2024)
  • Vacuum cleaners          (durabilité 2024)
  • Lawnmowers               (réparabilité 2022)

Usage
  python3 scraper/fnac_scraper.py                # full run
  python3 scraper/fnac_scraper.py --limit 50     # first 50 products per category
  python3 scraper/fnac_scraper.py --no-details   # skip detail-page fetch (faster, less data)
  python3 scraper/fnac_scraper.py --test-url https://www.fnac.com/mp…  # single product test

Dependencies
  pip install curl_cffi beautifulsoup4
"""

from __future__ import annotations   # ← makes X | None syntax work on Python 3.9

import os
import sys
import re
import json
import time
import sqlite3
import logging
import argparse
import datetime
from urllib.parse import urljoin, urlencode, urlparse, urlunparse, parse_qs, urlencode as _urlencode

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
            os.path.join(os.path.dirname(__file__), "fnac_scraper.log"),
            encoding="utf-8"
        ),
    ]
)
log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────
BASE_URL       = "https://www.fnac.com"
DELAY          = 2.5        # seconds between requests
MAX_PAGES      = 20         # per category (typically 30 products/page)
COUNTRY        = "FR"
CURRENCY       = "EUR"
SOURCE         = "fnac"

# ── category definitions ──────────────────────────────────────────────────────
# URL pattern for Fnac category pages. w-1 = pertinence (default relevance/popularity sort).
# w-4 was "meilleures notes" (top rated) — replaced with w-1 to avoid star-rating selection bias.
# PageIndex=N for pagination. nsh/sh IDs are Fnac internal category identifiers.
#
# Sustainability score coverage (French loi AGEC):
#   Réparabilité (since 2021): smartphones, laptops, TVs, washing machines,
#                               dishwashers, lawnmowers, high-pressure cleaners
#   Durabilité   (since 2024): smartphones, laptops, TVs, washing machines,
#                               dishwashers, vacuum cleaners (incl. robot vacuums)
CATEGORIES = [

    # ══════════════════════════════════════════════════════════════════════════
    # TÉLÉPHONIE & TABLETTES
    # ══════════════════════════════════════════════════════════════════════════

    # Smartphones (réparabilité + durabilité)
    {
        "name":      "Smartphones",
        "main_cat":  "Telefony a tablety",
        "url":       "https://www.fnac.com/Tous-les-telephones-portables-et-smartphones/Tous-les-telephones/nsh130385/w-1",
        "has_repairability": True,
        "has_durability":    True,
    },
    # Tablets
    {
        "name":      "Tablets",
        "main_cat":  "Telefony a tablety",
        "url":       "https://www.fnac.com/Toutes-les-tablettes/Toutes-les-tablettes/nsh227099/w-1",
        "has_repairability": True,
        "has_durability":    False,
    },
    # E-readers / liseuses
    {
        "name":      "E-readers",
        "main_cat":  "Telefony a tablety",
        "url":       "https://www.fnac.com/Liseuse-ebook/sh227100/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },

    # ══════════════════════════════════════════════════════════════════════════
    # INFORMATIQUE
    # ══════════════════════════════════════════════════════════════════════════

    # Laptops (réparabilité + durabilité)
    {
        "name":      "Laptops",
        "main_cat":  "Počítače a notebooky",
        "url":       "https://www.fnac.com/Tous-les-ordinateurs-portables/Ordinateurs-portables/nsh154425/w-1",
        "has_repairability": True,
        "has_durability":    True,
    },
    # Desktop computers
    {
        "name":      "Desktop Computers",
        "main_cat":  "Počítače a notebooky",
        "url":       "https://www.fnac.com/Tous-les-PC-de-bureau/Ordinateur-de-bureau/nsh51426/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Monitors
    {
        "name":      "Monitors",
        "main_cat":  "Počítače a notebooky",
        "url":       "https://www.fnac.com/Ecran-PC/sh1730/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Printers
    {
        "name":      "Printers",
        "main_cat":  "Počítače a notebooky",
        "url":       "https://www.fnac.com/Toutes-les-imprimantes/Imprimante-scanner/nsh50260/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },

    # ══════════════════════════════════════════════════════════════════════════
    # TV / SON
    # ══════════════════════════════════════════════════════════════════════════

    # TVs (réparabilité + durabilité)
    {
        "name":      "TVs",
        "main_cat":  "Televize a video",
        "url":       "https://www.fnac.com/Toutes-les-TV/TV-Television/nsh474940/w-1",
        "has_repairability": True,
        "has_durability":    True,
    },
    # Soundbars
    {
        "name":      "Soundbars",
        "main_cat":  "Zvuk a hudba",
        "url":       "https://www.fnac.com/Barre-de-son/sh450492/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Headphones
    {
        "name":      "Headphones",
        "main_cat":  "Zvuk a hudba",
        "url":       "https://www.fnac.com/Tous-les-casques/Casque-ecouteur/nsh450498/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Bluetooth Speakers
    {
        "name":      "Speakers",
        "main_cat":  "Zvuk a hudba",
        "url":       "https://www.fnac.com/Enceinte-Dock/shi450491/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Hi-Fi systems / amplifiers
    {
        "name":      "Hi-Fi & Amplifiers",
        "main_cat":  "Zvuk a hudba",
        "url":       "https://www.fnac.com/Hifi-Ampli/sh450493/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },

    # ══════════════════════════════════════════════════════════════════════════
    # PHOTO / VIDÉO
    # ══════════════════════════════════════════════════════════════════════════

    # Digital Cameras
    {
        "name":      "Cameras",
        "main_cat":  "Foto a kamery",
        "url":       "https://www.fnac.com/Photo-camescope/shi56352/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Drones
    {
        "name":      "Drones",
        "main_cat":  "Foto a kamery",
        "url":       "https://www.fnac.com/Drone/sh389045/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },

    # ══════════════════════════════════════════════════════════════════════════
    # GAMING
    # ══════════════════════════════════════════════════════════════════════════

    # Gaming headsets
    {
        "name":      "Gaming Headsets",
        "main_cat":  "Hry a konzole",
        "url":       "https://www.fnac.com/Casque-gaming/sh450499/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },

    # ══════════════════════════════════════════════════════════════════════════
    # MONTRES / GPS / MAISON CONNECTÉE
    # ══════════════════════════════════════════════════════════════════════════

    # Smartwatches
    {
        "name":      "Smartwatches",
        "main_cat":  "Chytré zařízení",
        "url":       "https://www.fnac.com/Montres-et-bracelets-connectes/shi389044/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # GPS navigation
    {
        "name":      "GPS Navigation",
        "main_cat":  "Chytré zařízení",
        "url":       "https://www.fnac.com/Tous-les-GPS/GPS/nsh130951/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },

    # ══════════════════════════════════════════════════════════════════════════
    # GROS ÉLECTROMÉNAGER — Large appliances
    # ══════════════════════════════════════════════════════════════════════════

    # Washing Machines — front-loading (réparabilité + durabilité)
    {
        "name":      "Washing Machines (hublot)",
        "main_cat":  "Velké domácí spotřebiče",
        "url":       "https://www.fnac.com/Lave-linge-hublot/Lave-Linge/nsh569195/w-1",
        "has_repairability": True,
        "has_durability":    True,
    },
    # Washing Machines — top-loading (réparabilité + durabilité)
    {
        "name":      "Washing Machines (top)",
        "main_cat":  "Velké domácí spotřebiče",
        "url":       "https://www.fnac.com/Lave-linge-top/Lave-Linge/nsh569196/w-1",
        "has_repairability": True,
        "has_durability":    True,
    },
    # Dishwashers (réparabilité + durabilité)
    {
        "name":      "Dishwashers",
        "main_cat":  "Velké domácí spotřebiče",
        "url":       "https://www.fnac.com/Tous-les-lave-vaisselle/Lave-vaisselle/nsh501335/w-1",
        "has_repairability": True,
        "has_durability":    True,
    },
    # Dryers (réparabilité)
    {
        "name":      "Dryers",
        "main_cat":  "Velké domácí spotřebiče",
        "url":       "https://www.fnac.com/Tous-les-seches-linge/Seche-linge/nsh501271/w-1",
        "has_repairability": True,
        "has_durability":    False,
    },
    # Refrigerators & freezers
    {
        "name":      "Refrigerators",
        "main_cat":  "Velké domácí spotřebiče",
        "url":       "https://www.fnac.com/Tous-les-refrigerateurs/Refrigerateur/nsh501296/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Ovens (fours encastrables)
    {
        "name":      "Ovens",
        "main_cat":  "Velké domácí spotřebiče",
        "url":       "https://www.fnac.com/Tous-les-fours/Four/nsh501435/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },

    # ══════════════════════════════════════════════════════════════════════════
    # PETIT ÉLECTROMÉNAGER — Small appliances
    # ══════════════════════════════════════════════════════════════════════════

    # Vacuum Cleaners (durabilité 2024)
    {
        "name":      "Vacuum Cleaners",
        "main_cat":  "Vysavače a úklid",
        "url":       "https://www.fnac.com/Aspirateur-Nettoyeur/Aspirateurs/sh502079/w-1",
        "has_repairability": False,
        "has_durability":    True,
    },
    # Robot Vacuums (durabilité 2024)
    {
        "name":      "Robot Vacuums",
        "main_cat":  "Vysavače a úklid",
        "url":       "https://www.fnac.com/Aspirateur-robot/sh502080/w-1",
        "has_repairability": False,
        "has_durability":    True,
    },
    # Coffee machines / espresso
    {
        "name":      "Coffee Machines",
        "main_cat":  "Malé domácí spotřebiče",
        "url":       "https://www.fnac.com/Toutes-les-machines-a-cafe/Cafetiere-Expresso/nsh556982/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Air fryers & fryers
    {
        "name":      "Air Fryers",
        "main_cat":  "Malé domácí spotřebiče",
        "url":       "https://www.fnac.com/Friteuse-sans-huile/Friteuse-electrique/nsh534281/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
    # Hair dryers & styling
    {
        "name":      "Hair Care",
        "main_cat":  "Malé domácí spotřebiče",
        "url":       "https://www.fnac.com/Seche-cheveux/Coiffure/nsh530885/w-1",
        "has_repairability": False,
        "has_durability":    False,
    },
]


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def make_session():
    """Create a curl_cffi session impersonating Chrome."""
    # chrome124 has a well-established TLS fingerprint that triggers fewer
    # bot-detection systems than newer versions which are less common in the wild.
    session = cffi_requests.Session(impersonate="chrome124")
    # Two-step warm-up: homepage → category hub.  A single homepage hit is
    # enough to receive session cookies, but visiting one more page before the
    # scrape begins makes the session look more like organic browsing.
    try:
        session.get(BASE_URL, headers=_lang_headers(), timeout=20)
        time.sleep(3.0)
        # Visit the main electronics hub so the session has a realistic referer
        session.get(BASE_URL + "/Electronique/sh3/w-1", headers=_lang_headers(), timeout=20)
        time.sleep(2.0)
        log.info("Session warmed up on Fnac homepage + electronics hub.")
    except Exception as e:
        log.warning(f"Warm-up failed ({e}) — proceeding anyway.")
    return session


def _lang_headers():
    return {
        "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Referer": BASE_URL + "/",
    }


def fetch(url: str, session, _retries: int = 2) -> BeautifulSoup | None:
    """Fetch a URL and return a BeautifulSoup tree, or None on failure.

    Retries once on 403/429/503 with an exponential back-off so that a
    transient bot-detection challenge doesn't silently kill a whole category.
    """
    for attempt in range(1, _retries + 2):
        try:
            resp = session.get(url, headers=_lang_headers(), timeout=30)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "html.parser")
            if resp.status_code in (403, 429, 503) and attempt <= _retries:
                wait = 15 * attempt          # 15 s, then 30 s
                log.warning(f"HTTP {resp.status_code}  {url}  — retrying in {wait}s "
                            f"(attempt {attempt}/{_retries})")
                time.sleep(wait)
                continue
            log.warning(f"HTTP {resp.status_code}  {url}")
            return None
        except Exception as e:
            if attempt <= _retries:
                log.warning(f"Fetch error ({e})  [{url}]  — retrying in 15s")
                time.sleep(15)
                continue
            log.error(f"Fetch error: {e}  [{url}]")
            return None
    return None


# ── listing page parsers ──────────────────────────────────────────────────────

# Query parameters that are tracking/session noise — strip these from product URLs
# so the same product isn't stored twice with different tracking suffixes.
_TRACKING_PARAMS = frozenset({
    "from", "Origin", "position", "sl", "shId",
    "Libelle", "SHOP_MKT", "SHOP_MKP", "SHOP_PFID",
    "awc", "gclid", "fbclid",
})


def canonical_url(url: str) -> str:
    """
    Strip tracking/session query parameters from a Fnac product URL so that
    the same product is always stored under the same canonical key.

    e.g. 'https://www.fnac.com/mp1234?from=search&sl=abc' → 'https://www.fnac.com/mp1234'
    """
    parsed = urlparse(url)
    qs = parse_qs(parsed.query, keep_blank_values=False)
    # Keep only non-tracking params
    clean_qs = {k: v for k, v in qs.items() if k not in _TRACKING_PARAMS}
    clean = parsed._replace(query=_urlencode(clean_qs, doseq=True))
    return urlunparse(clean)


def page_url(base: str, page: int) -> str:
    """Build a paginated URL for Fnac category listings."""
    if page == 1:
        return base
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}PageIndex={page}"


def parse_star_rating(text: str):
    """'4,5 / 5' → 4.5,   '4.2' → 4.2,   None if unparseable."""
    if not text:
        return None
    # French decimal comma
    text = text.replace(",", ".")
    m = re.search(r"(\d+(?:\.\d+)?)\s*/?\s*5", text)
    if m:
        return float(m.group(1))
    m = re.search(r"(\d+(?:\.\d+)?)", text)
    return float(m.group(1)) if m else None


def stars_to_recommend_pct(stars) -> float | None:
    """Convert 0–5 star rating to 0–100 recommendation %."""
    if stars is None:
        return None
    return round((stars / 5.0) * 100.0, 1)


def parse_price_eur(text: str):
    """'249,99 €' → 249.99"""
    if not text:
        return None
    text = text.replace("\xa0", "").replace(" ", "")
    m = re.search(r"([\d]+[,.][\d]{2})", text)
    if m:
        return float(m.group(1).replace(",", "."))
    m = re.search(r"(\d+)", text)
    return float(m.group(1)) if m else None


def parse_index_score(text: str):
    """
    Parse durability/repairability index score from text like:
      '7,2/10'  '8.5 / 10'  'Indice : 6,4'  '6.4'
    Returns float 0–10 or None.
    """
    if not text:
        return None
    # Normalise decimal separator
    text = text.replace(",", ".")
    # e.g. "7.2/10" or "7.2 / 10"
    m = re.search(r"(\d+(?:\.\d+)?)\s*/\s*10", text)
    if m:
        val = float(m.group(1))
        return val if 0.0 <= val <= 10.0 else None
    # Bare decimal: only trust if in range
    m = re.search(r"(\d+\.\d+)", text)
    if m:
        val = float(m.group(1))
        return val if 0.0 <= val <= 10.0 else None
    return None


def scrape_listing_page(soup: BeautifulSoup, cat: dict) -> list[dict]:
    """
    Extract product stubs from one Fnac category listing page.
    Returns list of dicts with: Name, ProductURL, Price_EUR, AvgStarRating,
    ReviewsCount, repairability_score_fr, durability_score_fr.

    Fnac uses two listing layouts:
      - article.Article-itemGroup  (nsh/sh full-category pages, paginated)
      - article.thumbnail          (shi curated landing pages, ~24 items)
    """
    products = []

    cards = soup.select("article.Article-itemGroup") or soup.select("article.thumbnail")

    if not cards:
        log.debug("No product cards found on page.")
        return []

    for card in cards:
        # ── name & URL ────────────────────────────────────────────────────────
        # Article-itemGroup uses <a class="Article-title js-Search-hashLink">
        # thumbnail uses <a class="thumbnail-titleLink js-Search-hashLink">
        name_el = (
            card.select_one("a.Article-title")
            or card.select_one("a.thumbnail-titleLink")
            or card.select_one("[class*='title'] a")
            or card.select_one("h2 a")
            or card.select_one("h3 a")
        )
        if not name_el:
            continue
        name = name_el.get_text(strip=True)
        url  = name_el.get("href", "")
        if url and not url.startswith("http"):
            url = urljoin(BASE_URL, url)
        if not name or not url:
            continue
        url = canonical_url(url)   # strip tracking params before storing

        # ── price ─────────────────────────────────────────────────────────────
        # Article-itemGroup: .userPrice (clean price, no delivery)
        # thumbnail: .thumbnail-price
        price_el = (
            card.select_one(".userPrice")
            or card.select_one(".thumbnail-price")
            or card.select_one(".Article-price")
        )
        price_eur = parse_price_eur(price_el.get_text(strip=True) if price_el else "")

        # ── star rating & review count ─────────────────────────────────────────
        # .f-star-score holds a bare number ("5", "4.5") — directly usable
        # .f-article-reviews-rating text is "4.5\n\n  \n23 avis" — parse both
        stars = None
        reviews = 0

        score_el = card.select_one(".f-star-score")
        if score_el:
            score_text = score_el.get_text(strip=True).replace(",", ".")
            try:
                val = float(score_text)
                if 0.0 <= val <= 5.0:
                    stars = val
            except (ValueError, TypeError):
                pass

        review_block = card.select_one(".f-article-reviews-rating")
        if review_block:
            # Try the dedicated count element first
            count_el = review_block.select_one(".customerReviewsRating__countTotal")
            if count_el:
                m = re.search(r"(\d[\d\xa0\s]{0,6})\s*avis", count_el.get_text(strip=True), re.IGNORECASE)
                if m:
                    reviews = int(re.sub(r"[\s\xa0]", "", m.group(1)))
            else:
                # Fallback: parse the block text; "N avis" pattern (no thousands gap ambiguity)
                block_text = review_block.get_text(separator="\n", strip=True)
                m = re.search(r"(\d[\d\xa0]{0,6})\s*avis", block_text, re.IGNORECASE)
                if m:
                    reviews = int(re.sub(r"[\s\xa0]", "", m.group(1)))
            # Stars: if not yet set, grab the f-star-score inside the block
            if stars is None:
                inner_score = review_block.select_one(".f-star-score")
                if inner_score:
                    try:
                        val = float(inner_score.get_text(strip=True).replace(",", "."))
                        if 0.0 <= val <= 5.0:
                            stars = val
                    except (ValueError, TypeError):
                        pass

        # ── repairability index (listing page badge) ──────────────────────────
        # Fnac shows "Indice de réparabilité" / "Indice de durabilité" badges
        # on product cards in covered categories.
        repair_score = None
        durability_score = None

        for el in card.select("[class*='sustainability'], [class*='repairability'], "
                              "[class*='reparabilite'], [class*='durabilite'], "
                              "[class*='indice'], [data-repairability], [data-durability]"):
            text = el.get_text(strip=True)
            score = parse_index_score(text)
            # Distinguish by class name or label text
            cls = " ".join(el.get("class", []))
            label = text.lower()
            if "durab" in cls.lower() or "durab" in label:
                if durability_score is None:
                    durability_score = score
            elif "repair" in cls.lower() or "répar" in label or "reparab" in label:
                if repair_score is None:
                    repair_score = score
            else:
                # Generic sustainability badge — try to infer from context
                if score is not None:
                    if repair_score is None and cat.get("has_repairability"):
                        repair_score = score
                    elif durability_score is None and cat.get("has_durability"):
                        durability_score = score

        # Also check aria-labels and data attributes
        for attr_name in ["data-repairability-score", "data-repair-index", "data-indice"]:
            el = card.select_one(f"[{attr_name}]")
            if el and repair_score is None:
                repair_score = parse_index_score(el.get(attr_name, ""))

        for attr_name in ["data-durability-score", "data-durability-index"]:
            el = card.select_one(f"[{attr_name}]")
            if el and durability_score is None:
                durability_score = parse_index_score(el.get(attr_name, ""))

        products.append({
            "Name":                    name,
            "ProductURL":              url,
            "Price_EUR":               price_eur,
            "AvgStarRating":           stars,
            "RecommendRate_pct":       stars_to_recommend_pct(stars),
            "ReviewsCount":            reviews,
            "repairability_score_fr":  repair_score,
            "durability_score_fr":     durability_score,
        })

    return products


# ── product detail page parser ────────────────────────────────────────────────

def scrape_product_detail(url: str, session) -> dict:
    """
    Fetch a Fnac product detail page and extract:
      • repairability_score_fr + sub-criteria JSON
      • durability_score_fr + sub-criteria JSON
      • EAN / barcode
      • model number
      • energy class
      • release year / date
      • brand (from product schema or breadcrumb)

    Returns a dict of extra fields (may be empty if page unreachable).
    """
    time.sleep(DELAY)
    soup = fetch(url, session)
    if soup is None:
        return {}

    extras = {}

    # ── JSON-LD product schema (most reliable source for EAN, brand, model) ──
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if data.get("@type") in ("Product", "http://schema.org/Product"):
                extras["brand"] = _nested_get(data, "brand", "name") or data.get("brand")
                extras["ean"]   = data.get("gtin13") or data.get("gtin") or data.get("gtin8")
                if not extras.get("ean"):
                    offers = data.get("offers", {})
                    if isinstance(offers, list) and offers:
                        offers = offers[0]
                    extras["ean"] = offers.get("gtin13") or offers.get("gtin")
                # mpn is often the model number
                extras["model_number"] = data.get("mpn") or data.get("sku")
                # releaseDate
                rel = data.get("releaseDate") or data.get("productionDate")
                if rel:
                    extras["release_date"] = rel[:10]
                    extras["release_year"] = int(rel[:4])
                break
        except Exception:
            pass

    # ── Repairability index section ───────────────────────────────────────────
    repair_section = (
        soup.find(id=re.compile(r"repairabilit|reparabilite|indice-rep", re.I))
        or soup.find(class_=re.compile(r"repairabilit|reparabilite|indice-rep", re.I))
        or soup.find("section", string=re.compile(r"réparabilit", re.I))
    )

    # Also try searching for the score badge anywhere on page
    repair_score = None
    durability_score = None
    repair_sub = {}
    durability_sub = {}

    # Score badges / widgets
    for el in soup.select("[class*='sustainability'], [class*='repairabilit'], "
                          "[class*='reparabilit'], [class*='durabilit'], "
                          "[class*='indice-rep'], [class*='indice-dur']"):
        text = el.get_text(" ", strip=True)
        score = parse_index_score(text)
        cls = " ".join(el.get("class", [])).lower()
        if "durab" in cls:
            if durability_score is None and score is not None:
                durability_score = score
        else:
            if repair_score is None and score is not None:
                repair_score = score

    # Labelled headings with score nearby (common pattern)
    for heading in soup.find_all(["h2", "h3", "h4", "div", "span"],
                                 string=re.compile(r"indice de (répar|durab)", re.I)):
        heading_text = heading.get_text().lower()
        # Look for score in sibling / next elements
        score_container = heading.find_next(string=re.compile(r"\d[,.]?\d+\s*/\s*10"))
        if score_container:
            sc = parse_index_score(score_container)
            if "durab" in heading_text and durability_score is None:
                durability_score = sc
            elif repair_score is None:
                repair_score = sc

    # ── Sub-criteria rows ─────────────────────────────────────────────────────
    # Fnac renders sub-criteria as dt/dd pairs or table rows within the index section
    REPAIR_CRITERIA = {
        "documentation": re.compile(r"document|notice|manuel", re.I),
        "spare_parts":   re.compile(r"pi[eè]ces détach", re.I),
        "price_parts":   re.compile(r"prix des pi[eè]ces", re.I),
        "software":      re.compile(r"logiciel|software|mise à jour", re.I),
        "ease":          re.compile(r"facilit[eé]|démontage|ease", re.I),
    }
    DURABILITY_CRITERIA = {
        "reliability":     re.compile(r"fiabilit[eé]|robustesse|reliability", re.I),
        "spare_parts":     re.compile(r"disponibilit[eé].*pi[eè]ces|pi[eè]ces disponib", re.I),
        "spare_parts_yrs": re.compile(r"dur[eé]e.*pi[eè]ces|\d+\s*ans.*pi[eè]ces", re.I),
        "software":        re.compile(r"mise à jour|maj|software update", re.I),
        "repairability":   re.compile(r"r[eé]parabilit[eé]|indice de répar", re.I),
    }

    def extract_sub_criteria(container, criteria_map):
        results = {}
        if container is None:
            return results
        rows = container.select("tr, li, [class*='criteria'], [class*='row']")
        if not rows:
            rows = [container]
        for row in rows:
            row_text = row.get_text(" ", strip=True)
            for key, pattern in criteria_map.items():
                if pattern.search(row_text) and key not in results:
                    sc = parse_index_score(row_text)
                    if sc is not None:
                        results[key] = sc
        return results

    # Try to find sub-criteria containers
    for section_selector in [
        "[class*='repairabilit']", "[class*='reparabilit']", "#repairability-details",
    ]:
        section = soup.select_one(section_selector)
        if section:
            repair_sub = extract_sub_criteria(section, REPAIR_CRITERIA)
            break

    for section_selector in [
        "[class*='durabilit']", "#durability-details",
    ]:
        section = soup.select_one(section_selector)
        if section:
            durability_sub = extract_sub_criteria(section, DURABILITY_CRITERIA)
            break

    # ── Energy class ──────────────────────────────────────────────────────────
    energy_el = (
        soup.select_one("[class*='energy'] .label")
        or soup.select_one("[class*='energy-class']")
        or soup.find(string=re.compile(r"Classe énergétique\s*:?\s*[A-G]"))
    )
    if energy_el:
        energy_text = (
            energy_el.get_text(strip=True)
            if hasattr(energy_el, "get_text") else str(energy_el)
        )
        m = re.search(r"Classe[^:]*:?\s*([A-G][+]*)", energy_text, re.I)
        if m:
            extras["energy_class"] = m.group(1)

    # ── Warranty ──────────────────────────────────────────────────────────────
    for el in soup.find_all(string=re.compile(r"garantie|warranty", re.I)):
        m = re.search(r"(\d+)\s*an", str(el), re.I)
        if m and "warranty_years" not in extras:
            extras["warranty_years"] = float(m.group(1))
            break

    # ── Normalise brand / model from page heading if not in schema ────────────
    if not extras.get("brand"):
        brand_el = (
            soup.select_one("[class*='brand']")
            or soup.select_one("[itemprop='brand']")
        )
        if brand_el:
            extras["brand"] = brand_el.get_text(strip=True)

    if not extras.get("model_number"):
        for el in soup.find_all(string=re.compile(r"ref[.:]|référence|model\s*:", re.I)):
            m = re.search(r":\s*([A-Z0-9][-A-Z0-9/]{3,})", str(el), re.I)
            if m:
                extras["model_number"] = m.group(1)
                break

    # ── Assemble results ──────────────────────────────────────────────────────
    today = datetime.date.today().isoformat()
    if repair_score is not None:
        extras["repairability_score_fr"]       = repair_score
        extras["repairability_score_date"]     = today
        extras["repairability_sub_scores_json"] = json.dumps(repair_sub) if repair_sub else None

    if durability_score is not None:
        extras["durability_score_fr"]          = durability_score
        extras["durability_score_date"]        = today
        extras["durability_sub_scores_json"]   = json.dumps(durability_sub) if durability_sub else None

    return extras


# ── helpers ───────────────────────────────────────────────────────────────────

def _nested_get(d, *keys):
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key)
        else:
            return None
    return d


# Known product lines that are NOT the brand name (first word is model, not brand)
_PRODUCT_LINE_TO_BRAND = {
    "iphone":    "Apple",
    "ipad":      "Apple",
    "macbook":   "Apple",
    "airpods":   "Apple",
    "imac":      "Apple",
    "galaxy":    "Samsung",
    "xperia":    "Sony",
    "pixel":     "Google",
    "surface":   "Microsoft",
    "thinkpad":  "Lenovo",
    "ideapad":   "Lenovo",
    "legion":    "Lenovo",
    "zenbook":   "Asus",
    "vivobook":  "Asus",
    "rog":       "Asus",
    "pavilion":  "HP",
    "envy":      "HP",
    "spectre":   "HP",
    "inspiron":  "Dell",
    "latitude":  "Dell",
    "xps":       "Dell",
    "aspire":    "Acer",
    "swift":     "Acer",
    "nitro":     "Acer",
    "roomba":    "iRobot",
    "dyson":     "Dyson",   # first word is already brand for Dyson
}


def extract_brand_from_name(name: str) -> str | None:
    """
    Heuristic brand extraction from product name.

    First checks if the first word is a known product-line name (e.g. "iPhone")
    that implies a different brand (e.g. "Apple").  Falls back to returning the
    first word directly, which works for "Samsung Galaxy S24", "LG OLED55…", etc.
    """
    if not name:
        return None
    words = name.split()
    if not words:
        return None
    first = words[0].lower().rstrip("®™")
    # Check known product-line → brand mapping
    if first in _PRODUCT_LINE_TO_BRAND:
        return _PRODUCT_LINE_TO_BRAND[first]
    # Default: first word is the brand (works for Samsung, LG, Sony, Philips…)
    return words[0]


def normalize_model(name: str, brand: str | None = None) -> str:
    """
    Remove brand prefix and common noise words to get a normalised model name
    suitable for fuzzy matching.
    """
    if not name:
        return ""
    s = name
    if brand:
        s = re.sub(re.escape(brand), "", s, flags=re.IGNORECASE)
    # Strip common French stopwords and noise
    noise = re.compile(
        r"\b(reconditionn[eé]|neuf|occasion|certifi[eé]|grade|smartphone|"
        r"tablette|ordinateur|portable|lave-linge|aspirateur|t[eé]l[eé]viseur|"
        r"télévision|notebook|laptop|coloris|couleur|po[iï]ds|version)\b",
        re.IGNORECASE
    )
    s = noise.sub("", s)
    # Collapse whitespace
    return re.sub(r"\s+", " ", s).strip()


# ── database helpers ──────────────────────────────────────────────────────────

def ensure_columns(conn: sqlite3.Connection):
    """
    Add French-index columns if the migration hasn't been run yet.
    Mirrors migrate_add_french_durability.py (idempotent).
    """
    required = [
        ("repairability_score_fr",       "REAL"),
        ("repairability_score_date",      "TEXT"),
        ("repairability_sub_scores_json", "TEXT"),
        ("durability_score_fr",           "REAL"),
        ("durability_score_date",         "TEXT"),
        ("durability_sub_scores_json",    "TEXT"),
        ("fr_source_url",                 "TEXT"),
        ("brand",                         "TEXT"),
        ("model_normalized",              "TEXT"),
        ("model_number",                  "TEXT"),
        ("ean",                           "TEXT"),
        ("release_year",                  "INTEGER"),
        ("release_date",                  "TEXT"),
        ("energy_class",                  "TEXT"),
        ("warranty_years",                "REAL"),
    ]
    cur = conn.execute("PRAGMA table_info(products)")
    existing = {row[1] for row in cur.fetchall()}
    for col, col_type in required:
        if col not in existing:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
    conn.commit()


def upsert_product(conn: sqlite3.Connection, p: dict, category: str) -> bool:
    """
    Insert a new product or update its French index score if it already exists.
    Returns True if a new row was inserted.
    """
    url = p.get("ProductURL", "")

    # Check for existing row by URL (most reliable key for FR products)
    existing = conn.execute(
        "SELECT id FROM products WHERE ProductURL=?", (url,)
    ).fetchone()

    brand      = p.get("brand") or extract_brand_from_name(p.get("Name", ""))
    model_norm = normalize_model(p.get("Name", ""), brand)

    if existing:
        # Update the French scores and extra fields on the existing row
        conn.execute("""
            UPDATE products SET
                repairability_score_fr       = COALESCE(?, repairability_score_fr),
                repairability_score_date     = COALESCE(?, repairability_score_date),
                repairability_sub_scores_json= COALESCE(?, repairability_sub_scores_json),
                durability_score_fr          = COALESCE(?, durability_score_fr),
                durability_score_date        = COALESCE(?, durability_score_date),
                durability_sub_scores_json   = COALESCE(?, durability_sub_scores_json),
                fr_source_url                = COALESCE(?, fr_source_url),
                brand                        = COALESCE(?, brand),
                model_normalized             = COALESCE(?, model_normalized),
                model_number                 = COALESCE(?, model_number),
                ean                          = COALESCE(?, ean),
                release_year                 = COALESCE(?, release_year),
                release_date                 = COALESCE(?, release_date),
                energy_class                 = COALESCE(?, energy_class),
                warranty_years               = COALESCE(?, warranty_years)
            WHERE ProductURL = ?
        """, (
            p.get("repairability_score_fr"),
            p.get("repairability_score_date"),
            p.get("repairability_sub_scores_json"),
            p.get("durability_score_fr"),
            p.get("durability_score_date"),
            p.get("durability_sub_scores_json"),
            url,
            brand,
            model_norm,
            p.get("model_number"),
            p.get("ean"),
            p.get("release_year"),
            p.get("release_date"),
            p.get("energy_class"),
            p.get("warranty_years"),
            url,
        ))
        return False

    # Insert new FR product row
    conn.execute("""
        INSERT INTO products (
            Name, Category, MainCategory, ProductURL,
            Price_EUR, AvgStarRating, RecommendRate_pct, ReviewsCount,
            source, country, currency,
            repairability_score_fr, repairability_score_date, repairability_sub_scores_json,
            durability_score_fr, durability_score_date, durability_sub_scores_json,
            fr_source_url,
            brand, model_normalized, model_number, ean,
            release_year, release_date, energy_class, warranty_years
        ) VALUES (
            ?,?,?,?,  ?,?,?,?,  ?,?,?,  ?,?,?,  ?,?,?,  ?,  ?,?,?,?,  ?,?,?,?
        )
    """, (
        p.get("Name"),
        category,
        p.get("main_cat", ""),
        url,
        p.get("Price_EUR"),
        p.get("AvgStarRating"),
        p.get("RecommendRate_pct"),
        p.get("ReviewsCount", 0),
        SOURCE,
        COUNTRY,
        CURRENCY,
        p.get("repairability_score_fr"),
        p.get("repairability_score_date"),
        p.get("repairability_sub_scores_json"),
        p.get("durability_score_fr"),
        p.get("durability_score_date"),
        p.get("durability_sub_scores_json"),
        url,
        brand,
        model_norm,
        p.get("model_number"),
        p.get("ean"),
        p.get("release_year"),
        p.get("release_date"),
        p.get("energy_class"),
        p.get("warranty_years"),
    ))
    return True


def also_upsert_canonical_table(conn: sqlite3.Connection, p: dict):
    """
    Mirror French index scores into the `french_durability_scores` table
    (if it exists — requires migrate_add_french_durability.py to have been run).
    """
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='french_durability_scores'"
    )
    if not cur.fetchone():
        return

    brand  = p.get("brand") or extract_brand_from_name(p.get("Name", ""))
    model  = normalize_model(p.get("Name", ""), brand)
    today  = datetime.date.today().isoformat()

    repair_sub = json.loads(p.get("repairability_sub_scores_json") or "{}") or {}
    dur_sub    = json.loads(p.get("durability_sub_scores_json")    or "{}") or {}

    conn.execute("""
        INSERT INTO french_durability_scores (
            brand, model_normalized, model_number, ean,
            repairability_score, repairability_score_date,
            repair_documentation, repair_spare_parts, repair_price_spare_parts,
            repair_software, repair_ease,
            durability_score, durability_score_date,
            durability_reliability, durability_spare_parts_avail,
            durability_spare_parts_years, durability_software_updates,
            durability_repairability,
            fr_product_url, fr_retailer, category, main_category, scraped_at
        ) VALUES (
            ?,?,?,?,  ?,?,  ?,?,?,?,?,  ?,?,  ?,?,?,?,?,  ?,?,?,?,?
        )
        ON CONFLICT(model_number, brand, fr_retailer) DO UPDATE SET
            repairability_score          = COALESCE(excluded.repairability_score, repairability_score),
            repairability_score_date     = excluded.repairability_score_date,
            durability_score             = COALESCE(excluded.durability_score, durability_score),
            durability_score_date        = excluded.durability_score_date,
            scraped_at                   = excluded.scraped_at
    """, (
        brand, model, p.get("model_number"), p.get("ean"),
        p.get("repairability_score_fr"), p.get("repairability_score_date"),
        repair_sub.get("documentation"), repair_sub.get("spare_parts"),
        repair_sub.get("price_parts"),   repair_sub.get("software"),
        repair_sub.get("ease"),
        p.get("durability_score_fr"), p.get("durability_score_date"),
        dur_sub.get("reliability"),       dur_sub.get("spare_parts"),
        dur_sub.get("spare_parts_yrs"),   dur_sub.get("software"),
        dur_sub.get("repairability"),
        p.get("ProductURL"), SOURCE,
        p.get("Category", ""), p.get("main_cat", ""), today,
    ))


# ── category scraper ──────────────────────────────────────────────────────────

def scrape_category(cat: dict, session, conn: sqlite3.Connection,
                    fetch_details: bool = True, limit: int = 0) -> dict:
    """Scrape one Fnac category and return summary stats."""
    log.info(f"── {cat['name']}  ({cat['url']})")
    total_scraped = 0
    total_inserted = 0
    total_updated  = 0
    total_scored   = 0

    ensure_snapshot_table(conn)

    for page_num in range(1, MAX_PAGES + 1):
        url = page_url(cat["url"], page_num)
        log.info(f"   Page {page_num}: {url}")

        soup = fetch(url, session)
        if soup is None:
            log.warning("   Fetch failed — stopping category.")
            break

        products = scrape_listing_page(soup, cat)
        if not products:
            log.info("   No products found — category exhausted.")
            break

        log.info(f"   Found {len(products)} products on this page.")

        for p in products:
            if limit and total_scraped >= limit:
                break

            # Enrich with detail-page data (EAN, model, full index scores)
            # Note: scrape_product_detail() already sleeps DELAY internally.
            if fetch_details:
                extras = scrape_product_detail(p["ProductURL"], session)
                p.update({k: v for k, v in extras.items() if v is not None})

            # Ensure category metadata is on the dict
            p["Category"] = cat["name"]
            p["main_cat"] = cat["main_cat"]

            inserted = upsert_product(conn, p, cat["name"])
            also_upsert_canonical_table(conn, p)
            record_snapshot(conn, p["ProductURL"], SOURCE, p, country=COUNTRY)

            total_scraped += 1
            if inserted:
                total_inserted += 1
            else:
                total_updated += 1
            if p.get("repairability_score_fr") or p.get("durability_score_fr"):
                total_scored += 1

        conn.commit()

        if limit and total_scraped >= limit:
            log.info(f"   Reached --limit {limit} — stopping.")
            break

        time.sleep(DELAY)

    log.info(
        f"   {cat['name']} done: {total_scraped} products, "
        f"{total_inserted} new, {total_updated} updated, "
        f"{total_scored} with index score."
    )
    return {
        "scraped": total_scraped,
        "inserted": total_inserted,
        "updated": total_updated,
        "scored": total_scored,
    }


# ── single product test mode ──────────────────────────────────────────────────

def test_single_url(url: str, session):
    """Scrape a single product URL and print extracted data (for debugging)."""
    log.info(f"Testing single URL: {url}")
    extras = scrape_product_detail(url, session)
    print("\n── Detail page extraction result ──")
    for k, v in sorted(extras.items()):
        print(f"  {k:40s}: {v}")


# ── main entry point ──────────────────────────────────────────────────────────

def run_scraper(fetch_details: bool = True, limit: int = 0) -> dict:
    """Run the full Fnac scrape.  Called by scheduler.py."""
    log.info("=" * 60)
    log.info("QualityDB Fnac.fr Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}. Run load_data.py first.")
        return {"error": "database_not_found"}

    def _open_conn():
        c = sqlite3.connect(DB_PATH, timeout=30)
        c.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    conn = _open_conn()
    ensure_columns(conn)

    session = make_session()
    summary = {
        "categories_scraped": 0,
        "total_scraped":  0,
        "total_inserted": 0,
        "total_updated":  0,
        "total_scored":   0,
        "errors": [],
    }

    for cat in CATEGORIES:
        try:
            result = scrape_category(cat, session, conn,
                                     fetch_details=fetch_details, limit=limit)
            summary["categories_scraped"] += 1
            for key in ("scraped", "inserted", "updated", "scored"):
                summary[f"total_{key}"] += result.get(key, 0)
        except sqlite3.OperationalError as e:
            log.error(f"Error scraping {cat['name']}: {e}", exc_info=True)
            summary["errors"].append({"category": cat["name"], "error": str(e)})
            # Reconnect on disk I/O or locking errors so subsequent categories can proceed
            try:
                conn.close()
            except Exception:
                pass
            try:
                conn = _open_conn()
                log.info("Reconnected to database after error.")
            except Exception as re:
                log.error(f"Failed to reconnect: {re}")
                break
        except Exception as e:
            log.error(f"Error scraping {cat['name']}: {e}", exc_info=True)
            summary["errors"].append({"category": cat["name"], "error": str(e)})

    conn.close()

    log.info("=" * 60)
    log.info(f"Fnac scrape complete.")
    log.info(f"  Categories : {summary['categories_scraped']}")
    log.info(f"  Products   : {summary['total_scraped']}")
    log.info(f"  New rows   : {summary['total_inserted']}")
    log.info(f"  Updated    : {summary['total_updated']}")
    log.info(f"  With score : {summary['total_scored']}")
    if summary["errors"]:
        log.warning(f"  Errors     : {len(summary['errors'])}")
    log.info("=" * 60)

    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fnac.fr scraper with durability index")
    parser.add_argument("--no-details", action="store_true",
                        help="Skip product detail pages (faster, fewer fields)")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max products per category (0 = unlimited)")
    parser.add_argument("--test-url", type=str, default=None,
                        help="Scrape a single product URL for debugging")
    args = parser.parse_args()

    session = make_session()

    if args.test_url:
        test_single_url(args.test_url, session)
    else:
        run_scraper(
            fetch_details=not args.no_details,
            limit=args.limit,
        )
