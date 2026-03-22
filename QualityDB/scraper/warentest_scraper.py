#!/usr/bin/env python3
"""
warentest_scraper.py — Stiftung Warentest (test.de) scraper
─────────────────────────────────────────────────────────────
Scrapes product test results using premium account cookies.

Usage:
  python3 scraper/warentest_scraper.py              # full run
  python3 scraper/warentest_scraper.py --debug       # show HTML structure & exit
  python3 scraper/warentest_scraper.py --debug-full   # dump raw div content

Cookies:
  Place cookies_warentest.json in the QualityDB folder (next to products.db).
  Format: {"cookie_name": "value", ...}
"""

import os, sys, re, json, time, sqlite3, logging

try:
    from curl_cffi import requests as cffi_requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing: pip install curl_cffi beautifulsoup4")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from scraper.config import DB_PATH
except ImportError:
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "products.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# ── Test URLs — focus on expensive electronics & household appliances ─────────
TEST_URLS = [
    # ── Electronics ──────────────────────────────────────────────────────────
    ("https://www.test.de/Smartphones-im-Test-4222793-0/",                                           "Smartphones",           "Telefony a tablety"),
    ("https://www.test.de/Tablets-im-Test-4627215-0/",                                               "Tablets",               "Telefony a tablety"),
    ("https://www.test.de/Tablets-mit-Tastatur-Notebooks-Ultrabooks-Convertibles-im-Test-4734961-0/","Laptops & Notebooks",   "Počítače a notebooky"),
    ("https://www.test.de/Fernseher-im-Test-1629201-0/",                                             "Fernseher",             "Televize a video"),
    ("https://www.test.de/Bluetooth-Kopfhoerer-Test-4378783-0/",                                     "Kopfhörer",             "Zvuk a hudba"),
    ("https://www.test.de/Smartwatch-und-Fitnesstracker-im-Test-5254021-0/",                         "Smartwatches",          "Chytré zařízení"),
    ("https://www.test.de/tv-box-streaming-stick-vergleich-5114866-0/",                              "Streaming Devices",     "Televize a video"),
    ("https://www.test.de/Drucker-im-Test-4339831-0/",                                               "Drucker",               "Počítače a notebooky"),
    ("https://www.test.de/Dashcams-im-Test-Viele-schwaecheln-bei-Dunkelheit-6263647-0/",             "Dashcams",              "Auto a moto"),

    # ── Large household appliances ────────────────────────────────────────────
    ("https://www.test.de/Waschmaschinen-im-Test-4296800-0/",                                        "Waschmaschinen",        "Velké domácí spotřebiče"),
    ("https://www.test.de/Waeschetrockner-im-Test-4735809-0/",                                       "Wäschetrockner",        "Velké domácí spotřebiče"),
    ("https://www.test.de/Geschirrspueler-im-Test-4685888-0/",                                       "Geschirrspüler",        "Velké domácí spotřebiče"),
    ("https://www.test.de/Kuehlschraenke-im-Test-4735177-0/",                                        "Kühlschränke",          "Velké domácí spotřebiče"),
    ("https://www.test.de/Backoefen-im-Test-4434994-0/",                                             "Backöfen",              "Velké domácí spotřebiče"),
    ("https://www.test.de/Klimageraete-im-Test-4722766-0/",                                          "Klimageräte",           "Velké domácí spotřebiče"),

    # ── Small household appliances ────────────────────────────────────────────
    ("https://www.test.de/Staubsauger-im-Test-1838262-0/",                                           "Staubsauger",           "Vysavače a úklid"),
    ("https://www.test.de/Saugroboter-im-Test-4806685-0/",                                           "Saugroboter",           "Vysavače a úklid"),
    ("https://www.test.de/Dampfreiniger-im-Test-Kaercher-dampft-am-besten-1523412-0/",               "Dampfreiniger",         "Vysavače a úklid"),
    ("https://www.test.de/Kaffeevollautomaten-im-Test-4635644-0/",                                   "Kaffeemaschinen",       "Malé domácí spotřebiče"),
    ("https://www.test.de/Heissluftfritteusen-im-Test-5115675-0/",                                   "Heißluftfritteusen",    "Malé domácí spotřebiče"),
    ("https://www.test.de/Mixer-Standmixer-im-Test-5073614-0/",                                      "Standmixer",            "Malé domácí spotřebiče"),
    ("https://www.test.de/Luftreiniger-im-Test-5579439-0/",                                          "Luftreiniger",          "Malé domácí spotřebiče"),

    # ── Garden & Mobility ─────────────────────────────────────────────────────
    ("https://www.test.de/Maehroboter-im-Test-4698387-0/",                                           "Mähroboter",            "Zahrada a dílna"),
    ("https://www.test.de/E-Bike-Test-4733454-0/",                                                   "E-Bikes",               "Sport a kola"),

    # ── Health & Personal care ────────────────────────────────────────────────
    ("https://www.test.de/elektrische-Zahnbuersten-im-Test-4621863-0/",                              "Elektrozahnbürsten",    "Zdraví a hygiena"),
    ("https://www.test.de/Blutdruckmessgeraete-im-Test-5007166-0/",                                  "Blutdruckmessgeräte",   "Zdraví a hygiena"),
    ("https://www.test.de/Trockenrasur-Nassrasur-Elektrorasierer-Test-4633728-0/",                   "Elektrorasierer",       "Zdraví a hygiena"),
    ("https://www.test.de/Test-Sonnencreme-und-Sonnenspray-fuer-Erwachsene-4868984-0/",              "Sonnencreme",           "Zdraví a hygiena"),

    # ── Baby & Family ─────────────────────────────────────────────────────────
    ("https://www.test.de/Autokindersitze-im-Test-1806826-0/",                                       "Kindersitze",           "Dětské zboží"),
    ("https://www.test.de/Kinderwagen-im-Test-4805700-0/",                                           "Kinderwagen",           "Dětské zboží"),

    # ── Furniture & Sleep ─────────────────────────────────────────────────────
    ("https://www.test.de/Matratzen-im-Test-1830877-0/",                                             "Matratzen",             "Bytové vybavení"),
]

DELAY = 3.0  # seconds between requests


# ── Cookie loader ────────────────────────────────────────────────────────────

def load_cookies():
    """Load cookies from cookies_warentest.json in the QualityDB folder."""
    qdb = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # QualityDB/
    path = os.path.join(qdb, "cookies_warentest.json")
    if not os.path.exists(path):
        log.warning(f"Cookie file not found: {path}")
        return {}
    try:
        with open(path) as f:
            cookies = json.load(f)
        log.info(f"Loaded {len(cookies)} cookies from {path}")
        return cookies
    except Exception as e:
        log.error(f"Failed to load cookies: {e}")
        return {}


# ── HTTP fetch ───────────────────────────────────────────────────────────────

def fetch(url, cookies):
    """Fetch page with curl_cffi Chrome 131 impersonation."""
    try:
        r = cffi_requests.get(url, impersonate="chrome131", cookies=cookies,
                              headers={"Accept-Language": "de-DE,de;q=0.9"},
                              timeout=20)
        if r.status_code != 200:
            log.warning(f"HTTP {r.status_code} for {url}")
            return None
        return r.text
    except Exception as e:
        log.error(f"Request error: {e}")
        return None


# ── Grade conversion ─────────────────────────────────────────────────────────

def grade_to_stars(grade):
    """Warentest grade 1.0-5.0 (1=best) → 5-star scale (5=best)."""
    try:
        g = float(grade)
        return round(max(1.0, min(5.0, 6.0 - g)), 1)
    except (ValueError, TypeError):
        return None

def grade_to_recommend(grade):
    """Warentest grade → recommend percentage."""
    try:
        g = float(grade)
        if g <= 1.5: return 98
        if g <= 2.5: return 90
        if g <= 3.5: return 75
        if g <= 4.5: return 50
        return 25
    except (ValueError, TypeError):
        return None

def parse_grade(text):
    """Extract a Warentest grade from text like 'GUT (1,7)' or '2,3' or 'befriedigend (2,8)'."""
    if not text:
        return None
    # Look for number in parentheses first: "(1,7)"
    m = re.search(r'\((\d[.,]\d)\)', text)
    if m:
        try:
            return float(m.group(1).replace(',', '.'))
        except ValueError:
            pass
    # Look for standalone grade: "1,7" or "2.3"
    m = re.search(r'\b(\d[.,]\d)\b', text)
    if m:
        try:
            v = float(m.group(1).replace(',', '.'))
            if 0.5 <= v <= 5.9:
                return v
        except ValueError:
            pass
    return None

def parse_price_eur(text):
    """Extract EUR price from text like '349 €' or '1.299,00 €'."""
    if not text:
        return None
    m = re.search(r'([\d.]+,?\d*)\s*€', text)
    if not m:
        m = re.search(r'€\s*([\d.]+,?\d*)', text)
    if not m:
        return None
    s = m.group(1).replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


# ── HTML parsers ─────────────────────────────────────────────────────────────

# Grade pattern: "gut (1,7)" or "befriedigend (2,8)" etc.
GRADE_RE = re.compile(
    r'(sehr\s*gut|gut|befriedigend|ausreichend|mangelhaft)\s*\((\d[.,]\d)\)',
    re.IGNORECASE
)


def parse_detail_links(soup):
    """
    Primary strategy: find ALL product detail links on the page.
    Warentest product links contain '-detail/' in the URL.
    Returns list of (name, url) tuples.
    """
    results = []
    seen_urls = set()

    for link in soup.find_all('a', href=True):
        href = link['href']
        if '-detail/' not in href:
            continue

        name = link.get_text(strip=True)
        if not name or len(name) < 3:
            continue

        url = href if href.startswith('http') else "https://www.test.de" + href

        if url not in seen_urls:
            seen_urls.add(url)
            results.append((name, url))

    return results


def parse_comparison_teaser(soup):
    """
    Parse the product-comparison-teaser div.
    Products are detail links, grades follow in sequence as "gut (1,7)" etc.
    """
    products = []

    teaser = soup.find('div', class_='product-comparison-teaser')
    if not teaser:
        # Try broader search
        teaser = soup.find('div', class_=lambda c: c and 'comparison' in c.lower())
    if not teaser:
        return products

    # Get product detail links from the teaser
    detail_links = []
    for link in teaser.find_all('a', href=True):
        if '-detail/' in link['href']:
            name = link.get_text(strip=True)
            href = link['href']
            url = href if href.startswith('http') else "https://www.test.de" + href
            if name and len(name) > 3:
                detail_links.append((name, url))

    # Deduplicate while preserving order
    seen = set()
    unique_links = []
    for name, url in detail_links:
        if url not in seen:
            seen.add(url)
            unique_links.append((name, url))

    # Get ALL grade matches from the teaser text (in order)
    teaser_text = teaser.get_text(" ", strip=True)
    grade_matches = GRADE_RE.findall(teaser_text)

    # The first N grades correspond to the "Qualitätsurteil" row
    # Match them 1-to-1 with the product links
    for i, (name, url) in enumerate(unique_links):
        grade = None
        if i < len(grade_matches):
            try:
                grade = float(grade_matches[i][1].replace(',', '.'))
            except ValueError:
                pass

        products.append({
            "name": name,
            "grade": grade,
            "price": None,
            "url": url,
        })

    return products


def parse_product_cards(soup):
    """
    Parse individual product cards that may appear below the comparison table.
    These are often in a list/grid showing all tested products.
    """
    products = []

    # Look for product listing items (cards, tiles, list items)
    for el in soup.find_all(['div', 'li', 'a'], class_=lambda c: c and any(
        k in c.lower() for k in ['product-tile', 'product-card', 'product-list-item',
                                   'product-overview', 'test-result']
    ) if c else False):
        try:
            # Find product link
            link = el if el.name == 'a' else el.find('a', href=True)
            if not link or '-detail/' not in link.get('href', ''):
                continue

            name = link.get_text(strip=True)
            href = link['href']
            url = href if href.startswith('http') else "https://www.test.de" + href

            if not name or len(name) < 3:
                continue

            # Find grade near this element
            text = el.get_text(" ", strip=True)
            grade_match = GRADE_RE.search(text)
            grade = None
            if grade_match:
                try:
                    grade = float(grade_match.group(2).replace(',', '.'))
                except ValueError:
                    pass

            # Find price
            price = parse_price_eur(text)

            products.append({
                "name": name,
                "grade": grade,
                "price": price,
                "url": url,
            })
        except Exception:
            continue

    return products


def tabelle_url(listing_url):
    """Convert a listing URL (...-0/) to the full table URL (...-tabelle/).
    The tabelle page shows ALL tested products, not just the top comparison."""
    return re.sub(r'-0/$', '-tabelle/', listing_url)


def parse_tabelle_page(soup):
    """
    Parse the full comparison table page (-tabelle/ URL).
    test.de shows ALL products here (e.g. all 448 smartphones).
    Each product row has a -detail/ link and an overall grade nearby.
    Returns list of {name, grade, price, url} dicts.
    """
    products = []
    seen = set()

    # The tabelle is a transposed HTML table: rows = criteria, columns = products.
    # BUT on the tabelle page products are often listed as rows in a product list.
    # Strategy: find every -detail/ link, then look for a grade in its closest ancestor row/cell.
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '-detail/' not in href:
            continue

        name = link.get_text(strip=True)
        if not name or len(name) < 3:
            continue

        url = href if href.startswith('http') else "https://www.test.de" + href
        if url in seen:
            continue
        seen.add(url)

        # Look for an overall grade near this link by walking up the DOM
        grade = None
        node = link.parent
        for _ in range(8):
            if node is None:
                break
            cell_text = node.get_text(" ", strip=True)
            # Remove soft hyphens before matching
            cell_text = cell_text.replace('\u00ad', '').replace('\xad', '')
            m = GRADE_RE.search(cell_text)
            if m:
                try:
                    grade = float(m.group(2).replace(',', '.'))
                    if 0.5 <= grade <= 5.9:
                        break
                    grade = None
                except ValueError:
                    pass
            # Stop climbing when we hit a large container (many products)
            if node.name in ('body', 'main', 'section', 'article') and len(cell_text) > 2000:
                break
            node = node.parent

        products.append({"name": name, "grade": grade, "price": None, "url": url})

    return products


def fetch_all_tabelle_pages(listing_url, cookies):
    """
    Fetch the -tabelle/ page and handle pagination if present.
    Returns merged list of all products found across all pages.
    """
    base_tabelle = tabelle_url(listing_url)
    all_products = {}

    for page in range(1, 30):  # max 30 pages (safety limit)
        url = base_tabelle if page == 1 else f"{base_tabelle}?page={page}"
        log.info(f"  Tabelle page {page}: {url}")
        html = fetch(url, cookies)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        products = parse_tabelle_page(soup)

        if not products:
            log.info(f"  No products on page {page}, stopping pagination")
            break

        new_count = 0
        for p in products:
            if p["url"] not in all_products:
                all_products[p["url"]] = p
                new_count += 1

        log.info(f"  Tabelle page {page}: {len(products)} found, {new_count} new (total: {len(all_products)})")

        # Stop if this page had no new products (duplicate page = end of list)
        if new_count == 0:
            break

        # Check for a "next page" link
        has_next = bool(
            soup.find('a', rel='next') or
            soup.find('a', string=re.compile(r'Weiter|next|›|»', re.I)) or
            soup.find('link', rel='next')
        )
        if not has_next:
            break

        time.sleep(DELAY)

    return list(all_products.values())


def extract_products(html, base_url, cookies=None):
    """Combine all parsing strategies, deduplicate by URL."""
    soup = BeautifulSoup(html, "html.parser")

    # Strategy 1: Comparison teaser (top of page, has grades)
    teaser_products = parse_comparison_teaser(soup)
    log.info(f"  Teaser: {len(teaser_products)} products")

    # Strategy 2: Individual product cards (rest of page)
    card_products = parse_product_cards(soup)
    log.info(f"  Cards: {len(card_products)} products")

    # Strategy 3: All detail links on the page (fallback, no grades)
    all_links = parse_detail_links(soup)
    log.info(f"  Detail links: {len(all_links)} total on page")

    # Merge: teaser products first (have grades), then cards, then remaining links
    merged = {}

    for p in teaser_products:
        merged[p["url"]] = p

    for p in card_products:
        if p["url"] not in merged:
            merged[p["url"]] = p
        elif p.get("grade") and not merged[p["url"]].get("grade"):
            merged[p["url"]]["grade"] = p["grade"]

    for name, url in all_links:
        if url not in merged:
            merged[url] = {"name": name, "grade": None, "price": None, "url": url}

    products = list(merged.values())
    log.info(f"  Total unique: {len(products)} products")
    return products


# ── Detail page scraper ──────────────────────────────────────────────────────

# Sub-rating label → short English key. Covers both English and German
# (test.de serves English by default for many visitors).
SUB_RATING_LABELS = [
    # English labels (longest match first to avoid partial hits)
    ("quality judgment",                "overall"),
    ("protection against water damage", "water_protection"),
    ("environmental properties",        "environmental"),
    ("beverage preparation",            "beverage_prep"),
    ("temperature stability",           "temperature_stability"),
    ("energy efficiency",               "energy_efficiency"),
    ("power consumption",               "power_consumption"),
    ("phone calls",                     "phone"),
    ("endurance test",                  "endurance"),
    ("basic functions",                 "functions"),
    ("water protection",                "water_protection"),
    ("handling",                        "handling"),
    ("endurance",                       "endurance"),
    ("washing",                         "wash"),
    ("vacuuming",                       "vacuum"),
    ("vacuum",                          "vacuum"),
    ("battery",                         "battery"),
    ("display",                         "display"),
    ("camera",                          "camera"),
    ("picture",                         "picture"),
    ("sound",                           "sound"),
    ("noise",                           "noise"),
    ("durability",                      "durability"),
    ("pollutants",                      "pollutants"),
    ("stability",                       "stability"),
    ("baking",                          "baking"),
    ("grilling",                        "grilling"),
    ("cleaning",                        "cleaning"),
    ("safety",                          "safety"),
    ("rinsing",                         "wash_cycle"),
    ("rinse",                           "wash_cycle"),
    ("cooling",                         "cooling"),
    ("freezing",                        "freezing"),
    ("navigation",                      "navigation"),
    ("fitness",                         "fitness"),
    ("communication",                   "communication"),
    ("functions",                       "functions"),
    ("wash",                            "wash"),
    ("quality",                         "overall"),
    # German labels — general
    ("qualitätsurteil",                 "overall"),
    ("grundfunktionen",                 "functions"),
    ("waschen",                         "wash"),
    ("trocknen",                        "dry"),
    ("dauerprüfung",                    "endurance"),
    ("handhabung",                      "handling"),
    ("umwelteigenschaften",             "environmental"),
    ("schutz vor wasserschäden",        "water_protection"),
    ("saugen",                          "vacuum"),
    ("akku",                            "battery"),
    ("geräusch",                        "noise"),
    ("haltbarkeit",                     "durability"),
    ("schadstoffe",                     "pollutants"),
    ("kamera",                          "camera"),
    ("stabilität",                      "stability"),
    ("backen",                          "baking"),
    ("grillen",                         "grilling"),
    ("reinigung",                       "cleaning"),
    ("sicherheit",                      "safety"),
    ("spülen",                          "wash_cycle"),
    ("kühlen",                          "cooling"),
    ("gefrieren",                       "freezing"),
    ("energieeffizienz",                "energy_efficiency"),
    ("temperaturstabilität",            "temperature_stability"),
    ("stromverbrauch",                  "power_consumption"),
    ("getränkezubereitung",             "beverage_prep"),
    ("kommunikation",                   "communication"),
    # E-Bikes
    ("fahreigenschaften",               "ride_quality"),
    ("antrieb",                         "motor"),
    ("reichweite",                      "range"),
    ("bremsen",                         "brakes"),
    ("beleuchtung",                     "lighting"),
    ("motor",                           "motor"),
    ("fahrkomfort",                     "ride_quality"),
    ("lenkung",                         "steering"),
    ("ride quality",                    "ride_quality"),
    ("range",                           "range"),
    ("brakes",                          "brakes"),
    ("lighting",                        "lighting"),
    ("motor",                           "motor"),
    # Mattresses
    ("liegekomfort",                    "comfort"),
    ("hygiene",                         "hygiene"),
    ("verarbeitung",                    "build_quality"),
    ("schlafkomfort",                   "comfort"),
    ("comfort",                         "comfort"),
    ("build quality",                   "build_quality"),
    # Car seats / Baby
    ("crashtest",                       "crash_test"),
    ("bedienung",                       "ease_of_use"),
    ("ergonomie",                       "ergonomics"),
    ("crash test",                      "crash_test"),
    ("ease of use",                     "ease_of_use"),
    ("ergonomics",                      "ergonomics"),
    # Electric toothbrushes
    ("reinigungswirkung",               "cleaning_effect"),
    ("zahnfleisch",                     "gum_care"),
    ("cleaning effect",                 "cleaning_effect"),
    ("gum care",                        "gum_care"),
    # Blood pressure monitors
    ("messgenauigkeit",                 "accuracy"),
    ("measurement accuracy",            "accuracy"),
    ("accuracy",                        "accuracy"),
    # Air purifiers / Climate
    ("luftreinigung",                   "air_cleaning"),
    ("kühlung",                         "cooling"),
    ("air cleaning",                    "air_cleaning"),
    ("air quality",                     "air_cleaning"),
    # Robot lawnmowers
    ("mähen",                           "mowing"),
    ("mowing",                          "mowing"),
    ("navigation",                      "navigation"),
    ("rasenpflege",                     "mowing"),
    # Printers
    ("druckqualität",                   "print_quality"),
    ("druckkosten",                     "print_cost"),
    ("print quality",                   "print_quality"),
    ("print cost",                      "print_cost"),
    ("running costs",                   "print_cost"),
    # Shavers / Personal care
    ("rasierergebnis",                  "shave_quality"),
    ("hautverträglichkeit",             "skin_comfort"),
    ("shave quality",                   "shave_quality"),
    ("skin comfort",                    "skin_comfort"),
    # Sunscreen
    ("lichtschutz",                     "uv_protection"),
    ("uv-schutz",                       "uv_protection"),
    ("uv protection",                   "uv_protection"),
    ("verträglichkeit",                 "skin_tolerance"),
    ("skin tolerance",                  "skin_tolerance"),
    # Air fryers / cooking
    ("garergebnis",                     "cooking_quality"),
    ("cooking quality",                 "cooking_quality"),
    ("cooking",                         "cooking_quality"),
    ("frittierergebnis",                "cooking_quality"),
    # Mixers / Blenders
    ("mixen",                           "blending"),
    ("blending",                        "blending"),
    ("mixergebnis",                     "blending"),
    # Dashcams
    ("videoqualität",                   "video_quality"),
    ("aufnahme",                        "video_quality"),
    ("video quality",                   "video_quality"),
    ("night vision",                    "night_vision"),
    ("nachtsicht",                      "night_vision"),
]


def map_sub_rating_label(label_clean):
    """Map a cleaned label string to a short English key."""
    for de_label, en_key in SUB_RATING_LABELS:
        if de_label in label_clean:
            return en_key
    return label_clean  # fallback: use the label itself


def scrape_detail_page(url, cookies):
    """
    Fetch a product's detail page and extract sub-ratings, price, and metadata.
    Returns dict with: sub_ratings, price, test_program, similar_to
    """
    html = fetch(url, cookies)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    result = {"sub_ratings": {}, "price": None, "test_program": None, "similar_to": None}

    # Strip soft hyphens (\xad / &#173;) which test.de uses everywhere and break regexes
    raw_text = soup.get_text(" ", strip=True)
    text = raw_text.replace('\u00ad', '').replace('\xad', '').replace('\u202f', ' ')

    # ── Sub-ratings ──────────────────────────────────────────────────────────
    # test.de format (German): "Display 15 % befriedigend (3,4)"
    #   = <label> <weight>% <grade_word> (<grade_num>)
    # Also seen without weight, or with soft-hyphen in label (already stripped above).
    sub_pattern = re.compile(
        r'([\wäöüÄÖÜß]+(?:\s[\wäöüÄÖÜß]+){0,4}?)'   # label: 1-5 words
        r'\s*(?:\d+\s*%\s*)?'                          # optional weight "15 %"
        r'(sehr\s*gut|gut|befriedigend|ausreichend|mangelhaft'
        r'|very\s+good|good|satisfactory|sufficient|poor)\s*'
        r'\((\d[.,]\d)\)',                             # "(3,4)"
        re.IGNORECASE
    )

    for match in sub_pattern.finditer(text):
        label_clean = match.group(1).strip().lower()
        grade_word  = match.group(2).strip()
        grade_num_str = match.group(3).replace(',', '.')

        try:
            grade_num = float(grade_num_str)
        except ValueError:
            continue

        if not (0.5 <= grade_num <= 5.9):
            continue

        key = map_sub_rating_label(label_clean)
        result["sub_ratings"][key] = {
            "label": grade_word.lower(),
            "grade": grade_num,
            "stars": grade_to_stars(grade_num),
        }

    # ── Price ────────────────────────────────────────────────────────────────
    # German (after soft-hyphen removal): "Mittlerer Onlinepreis 135,00 Euro"
    # English: "Average online price 135.00 Euro"
    price_match = re.search(
        r'(?:Mittlerer\s+Onlinepreis|Average\s+[Oo]nline\s+[Pp]rice)\s+([\d.,]+)\s*Euro',
        text, re.IGNORECASE
    )
    if price_match:
        result["price"] = parse_price_eur(price_match.group(1) + " €")

    # ── Test program ─────────────────────────────────────────────────────────
    # German (after stripping): "Untersuchungsprogramm Fußnote: 3 Handys 06/2024 Online-Veröffentlichung"
    # We want the part after the keyword (and optional "Fußnote: N") up to "Online"
    prog_match = re.search(
        r'Untersuchungsprogramm\s+(?:Funote|Fu.note|Fußnote):\s*\d+\s+(.+?)\s+Online',
        text, re.IGNORECASE
    )
    if not prog_match:
        # Try without footnote
        prog_match = re.search(
            r'(?:Untersuchungsprogramm|Investigation\s+program)\s+(.{5,60}?)\s+(?:Online|Produkt|$)',
            text, re.IGNORECASE
        )
    if prog_match:
        result["test_program"] = prog_match.group(1).strip()

    # ── Similar / identical product ──────────────────────────────────────────
    sim_match = re.search(
        r'(?:Baugleich|Ähnlichkeit|Similarity)\s*:?\s*(.{3,150}?)\s*(?:Mittlerer|Average|Unter|$)',
        text, re.IGNORECASE
    )
    if sim_match:
        result["similar_to"] = sim_match.group(1).strip()[:200]

    return result


# ── Database ─────────────────────────────────────────────────────────────────

def ensure_details_column(conn):
    """Add details_json column if it doesn't exist."""
    try:
        conn.execute("SELECT details_json FROM products LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE products ADD COLUMN details_json TEXT")
        conn.commit()
        log.info("Added details_json column to products table")


def upsert(conn, products, category, main_category):
    """Insert/update products into the products table (same as all other scrapers)."""
    cur = conn.cursor()
    inserted = updated = 0

    for p in products:
        if not p.get("name") or not p.get("url"):
            continue
        try:
            stars = grade_to_stars(p["grade"]) if p.get("grade") else None
            recommend = grade_to_recommend(p["grade"]) if p.get("grade") else None
            details = json.dumps(p.get("details"), ensure_ascii=False) if p.get("details") else None

            cur.execute("""
                INSERT INTO products
                  (Name, Category, MainCategory, ProductURL,
                   Price_EUR, AvgStarRating, RecommendRate_pct,
                   source, country, currency, details_json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(ProductURL) DO UPDATE SET
                  Name             = excluded.Name,
                  Price_EUR        = COALESCE(excluded.Price_EUR, products.Price_EUR),
                  AvgStarRating    = excluded.AvgStarRating,
                  RecommendRate_pct= excluded.RecommendRate_pct,
                  details_json     = COALESCE(excluded.details_json, products.details_json)
            """, (
                p["name"], category, main_category, p["url"],
                p.get("price"), stars, recommend,
                "warentest", "DE", "EUR", details,
            ))
            if conn.execute("SELECT changes()").fetchone()[0] == 1:
                inserted += 1
            else:
                updated += 1
        except Exception as e:
            log.error(f"  DB error: {e}")

    conn.commit()
    return inserted, updated


# ── Debug ────────────────────────────────────────────────────────────────────

def debug_page(html):
    """Print detailed HTML structure for debugging."""
    soup = BeautifulSoup(html, "html.parser")
    print("\n" + "="*80)
    print("HTML STRUCTURE ANALYSIS")
    print("="*80)

    title = soup.find('title')
    print(f"\nPage Title: {title.get_text() if title else 'N/A'}")

    # Tables
    tables = soup.find_all('table')
    print(f"\nTables: {len(tables)}")
    for i, t in enumerate(tables[:3]):
        rows = t.find_all('tr')
        print(f"  Table {i+1}: {len(rows)} rows")
        if rows:
            cells = rows[0].find_all(['th', 'td'])
            print(f"    Header cells: {len(cells)}")
            for j, c in enumerate(cells[:5]):
                print(f"      [{j}] {c.get_text(strip=True)[:60]}")
            if len(rows) > 1:
                cells2 = rows[1].find_all(['th', 'td'])
                print(f"    Row 2 cells: {len(cells2)}")
                for j, c in enumerate(cells2[:5]):
                    print(f"      [{j}] {c.get_text(strip=True)[:60]}")

    # Product divs
    pdivs = soup.find_all('div', class_=lambda c: c and 'product' in c.lower())
    print(f"\nProduct divs: {len(pdivs)}")
    for i, d in enumerate(pdivs[:3]):
        print(f"  Div {i+1} class={d.get('class')}")
        text = d.get_text(" ", strip=True)[:200]
        print(f"    Text: {text}")
        links = d.find_all('a', href=True)
        for a in links[:2]:
            print(f"    Link: {a['href'][:80]} → {a.get_text(strip=True)[:40]}")

    # Articles
    articles = soup.find_all('article')
    print(f"\nArticle tags: {len(articles)}")
    for i, a in enumerate(articles[:3]):
        print(f"  Article {i+1} class={a.get('class')}")
        text = a.get_text(" ", strip=True)[:200]
        print(f"    Text: {text}")

    # Any elements with grade text
    grade_keywords = ['sehr gut', 'gut (', 'befriedigend', 'ausreichend', 'mangelhaft']
    body_text = soup.get_text()
    print(f"\nGrade keyword occurrences:")
    for kw in grade_keywords:
        count = body_text.lower().count(kw)
        if count:
            print(f"  '{kw}': {count} times")

    print("="*80 + "\n")


def debug_full(html):
    """Dump first few product divs' full HTML."""
    soup = BeautifulSoup(html, "html.parser")
    pdivs = soup.find_all('div', class_=lambda c: c and 'product' in c.lower())
    print(f"\n{'='*80}\nFULL PRODUCT DIV HTML (first 3)\n{'='*80}\n")
    for i, d in enumerate(pdivs[:3]):
        print(f"--- DIV {i+1} ---")
        print(str(d)[:2000])
        print()


# ── Entry point ──────────────────────────────────────────────────────────────

def scrape_warentest(db_path=None):
    if db_path is None:
        db_path = DB_PATH

    debug = "--debug" in sys.argv
    debug_f = "--debug-full" in sys.argv
    detail_debug = "--detail-debug" in sys.argv
    with_details = "--details" in sys.argv or "--with-details" in sys.argv

    cookies = load_cookies()
    if not cookies:
        log.warning("No cookies — premium content may be inaccessible")

    # Debug: show detail page structure for first product
    if detail_debug:
        url, cat, _ = TEST_URLS[0]
        log.info(f"Detail debug: fetching listing {url}")
        html = fetch(url, cookies)
        if not html:
            return
        products = extract_products(html, url)
        if not products:
            log.warning("No products on listing page")
            return
        # Pick first product with a detail URL
        for p in products:
            if '-detail/' in p['url']:
                log.info(f"Fetching detail page: {p['url']}")
                detail = scrape_detail_page(p['url'], cookies)
                print(f"\nProduct: {p['name']}")
                print(f"URL: {p['url']}")
                if detail:
                    print(f"Price: {detail['price']}")
                    print(f"Test program: {detail['test_program']}")
                    print(f"Similar to: {detail['similar_to']}")
                    print(f"Sub-ratings ({len(detail['sub_ratings'])}):")
                    for key, val in detail['sub_ratings'].items():
                        print(f"  {key:25s}  {val.get('label','?'):15s} ({val['grade']})  → {val['stars']}★")
                    # Search page text for relevant keywords to debug missing fields
                    from bs4 import BeautifulSoup as _BS
                    _html = fetch(p['url'], cookies)
                    if _html:
                        _text = _BS(_html, "html.parser").get_text(" ", strip=True)
                        print(f"\n--- Page total: {len(_text)} chars ---")
                        for kw in ["very good","good","gut","sehr gut","Euro","price","Preis",
                                   "program","Programm","similar","baugleich","satisfactory","(1,","(2,"]:
                            idx = _text.lower().find(kw.lower())
                            if idx >= 0:
                                s = max(0, idx-60); e = min(len(_text), idx+150)
                                print(f"  [{kw}@{idx}]: ...{repr(_text[s:e])}...")
                        print("---")
                else:
                    print("  (no detail data extracted)")
                print()
                break
        return

    if debug or debug_f:
        url, cat, _ = TEST_URLS[0]
        log.info(f"Debug: fetching {url}")
        html = fetch(url, cookies)
        if html:
            if debug_f:
                debug_full(html)
            else:
                debug_page(html)

            # Also try parsing to show results
            products = extract_products(html, url)
            print(f"\nParsed {len(products)} products:")
            for p in products[:10]:
                g = f"Grade {p['grade']}" if p.get('grade') else "No grade"
                pr = f"{p['price']:.0f}€" if p.get('price') else "No price"
                print(f"  {p['name'][:50]:50s}  {g:15s}  {pr}")
        return

    conn = sqlite3.connect(db_path)
    ensure_details_column(conn)
    total_ins = total_upd = 0
    all_products = []  # Collect for detail pass

    for url, category, main_cat in TEST_URLS:
        log.info(f"Warentest  [{category}]")

        # PRIMARY: tabelle URL → get ALL tested products (e.g. all 448 smartphones)
        products = fetch_all_tabelle_pages(url, cookies)

        if not products:
            # FALLBACK: listing page (only shows ~17 top products)
            log.warning(f"  Tabelle empty — falling back to listing page")
            html = fetch(url, cookies)
            if not html:
                continue
            products = extract_products(html, url)

        if not products:
            log.warning(f"  No products found for [{category}]")
            continue

        ins, upd = upsert(conn, products, category, main_cat)
        total_ins += ins
        total_upd += upd
        log.info(f"  [{category}] {len(products)} found → {ins} new, {upd} updated")

        if with_details:
            all_products.extend([(p, category, main_cat) for p in products])

        time.sleep(DELAY)

    log.info(f"\nListing pass done: {total_ins} inserted, {total_upd} updated")

    # Second pass: fetch detail pages for sub-ratings & prices
    if with_details and all_products:
        detail_count = 0
        detail_products = [(p, cat, mc) for p, cat, mc in all_products if '-detail/' in p.get('url', '')]
        log.info(f"\nDetail pass: {len(detail_products)} product detail pages to scrape...")

        for p, category, main_cat in detail_products:
            log.info(f"  Detail: {p['name'][:40]}")
            detail = scrape_detail_page(p['url'], cookies)
            if detail:
                p['details'] = detail
                # Update price if we got one from detail page
                if detail.get('price') and not p.get('price'):
                    p['price'] = detail['price']
                # Re-upsert with details
                upsert(conn, [p], category, main_cat)
                detail_count += 1
            time.sleep(DELAY)

        log.info(f"Detail pass done: {detail_count} products enriched with sub-ratings")

    conn.close()
    log.info(f"\nWarentest finished: {total_ins} inserted, {total_upd} updated")
    return total_ins, total_upd


if __name__ == "__main__":
    scrape_warentest()
