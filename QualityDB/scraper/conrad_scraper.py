"""
Conrad.de scraper — large German electronics & hobby retailer.

Conrad has a broad catalog: electronics, Arduino/Raspberry Pi, tools,
industrial components, and more — complements Amazon.de and Otto well.
Products stored with country='DE', source='conrad', Price_EUR.

Usage:
    python3 scraper/conrad_scraper.py
    python3 scraper/conrad_scraper.py --dry-run
"""

import re, time, logging, sqlite3, os, sys, argparse

try:
    from curl_cffi import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install curl_cffi beautifulsoup4\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH       = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")
MIN_STARS     = 4.0    # out of 5
MIN_REVIEWS   = 3      # Conrad often has fewer reviews per product
STOP_BELOW    = 3.5
REQUEST_DELAY = 2.5    # Conrad may rate-limit more aggressively
MAX_PAGES     = 12

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "scraper.log"), encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": '"Google Chrome";v="124", "Chromium";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

# ── Categories ────────────────────────────────────────────────────────────────
# Conrad category URLs: https://www.conrad.de/de/c/{slug}.html
# IMPORTANT: Conrad requires exact numeric IDs in every slug — wrong IDs = 404.
# IDs below are verified from live Conrad.de category pages.
CATEGORIES = [
    # Raspberry Pi & Arduino — Conrad's flagship hobby/maker section
    {"name": "Raspberry Pi & SBC",    "main": "Počítače a notebooky",  "slug": "raspberry-pi-2864444"},
    {"name": "Arduino",               "main": "Průmyslové zboží",      "slug": "arduino-2871550"},

    # Messtechnik / Měřicí přístroje
    {"name": "Měřicí přístroje",      "main": "Průmyslové zboží",      "slug": "messgeraete-37381"},

    # Nářadí a dílna
    {"name": "Pájení a elektrotechnika", "main": "Průmyslové zboží",   "slug": "loettechnik-17583"},
    {"name": "Napájecí zdroje",          "main": "Průmyslové zboží",   "slug": "labornetzteile-17452"},

    # Smart Home & IoT
    {"name": "Smart Home",            "main": "Elektro",               "slug": "alle-geraete-hersteller-17200"},

    # Audio
    {"name": "Reproduktory",          "main": "Elektro",               "slug": "lautsprecher-17483"},
    {"name": "Sluchátka",             "main": "Elektro",               "slug": "kopfhoerer-zubehoer-1688942"},

    # Modely & RC
    {"name": "Drony",                 "main": "Foto a video",          "slug": "drohnen-221790"},
]


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_stars(card):
    """Conrad shows ratings as star widgets. Try multiple selectors."""
    # JSON-LD / microdata
    el = card.select_one("[itemprop='ratingValue']")
    if el:
        try: return float((el.get("content") or el.get_text(strip=True)).replace(",", "."))
        except: pass

    # data attributes
    for attr in ["data-average-rating", "data-rating", "data-score"]:
        el = card.select_one(f"[{attr}]")
        if el:
            try: return float(el.get(attr).replace(",", "."))
            except: pass

    # aria-label "4,5 von 5 Sternen"
    for el in card.select("[aria-label]"):
        label = el.get("aria-label", "")
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:von|/)\s*5", label, re.IGNORECASE)
        if m:
            try: return float(m.group(1).replace(",", "."))
            except: pass

    # CSS star fill pattern (count filled)
    filled = card.select(".star--full, .icon--star-full, .rating__star--active")
    if filled: return float(len(filled))

    return None


def parse_reviews(card):
    for el in card.select("[itemprop='reviewCount'], [itemprop='ratingCount']"):
        try: return int(el.get("content") or el.get_text(strip=True))
        except: pass

    for el in card.find_all(["span", "a", "div"]):
        text = el.get_text(strip=True)
        # "23 Bewertungen" or "(12)"
        m = re.search(r"\((\d+)\)|(\d+)\s*Bewertung", text, re.IGNORECASE)
        if m:
            val = m.group(1) or m.group(2)
            try: return int(val)
            except: pass
    return 0


def parse_price(card):
    """Conrad prices in EUR."""
    for sel in ["[itemprop='price']", ".price__value", ".product-price",
                ".c-price__value", ".price-box", ".product__price"]:
        el = card.select_one(sel)
        if el:
            text = el.get("content") or el.get_text(strip=True)
            m = re.search(r"(\d+(?:[.,]\d+)?)", text.replace(".", "").replace(",", "."))
            if m:
                try: return float(m.group(1))
                except: pass
    return None


def parse_name(card):
    for sel in ["[itemprop='name']", "h2 a", "h3 a", ".product-tile__title",
                ".product__name a", ".c-product-card__title"]:
        el = card.select_one(sel)
        if el:
            name = el.get("content") or el.get_text(strip=True)
            if name: return name
    return None


def parse_url(card):
    for sel in ["a[itemprop='url']", "h2 a", "h3 a", ".product-tile__title a",
                ".product__name a", "a.product-link"]:
        el = card.select_one(sel)
        if el and el.get("href"):
            href = el["href"]
            return href if href.startswith("http") else "https://www.conrad.de" + href
    return ""


# ── Session & fetching ────────────────────────────────────────────────────────

def warm_up(session):
    try:
        resp = session.get("https://www.conrad.de/de/", headers=HEADERS, timeout=20)
        resp.raise_for_status()
        log.info(f"Session ready — status {resp.status_code}, {len(session.cookies)} cookies")
        time.sleep(2.5)
    except Exception as e:
        log.warning(f"Warmup failed: {e}")


def scrape_page(url, session):
    page_headers = {**HEADERS, "Referer": "https://www.conrad.de/de/", "Sec-Fetch-Site": "same-origin"}
    try:
        resp = session.get(url, headers=page_headers, timeout=25)
        if resp.status_code == 404:
            log.warning(f"  404 Not Found — check category slug/ID: {url}")
            return []
        if resp.status_code == 403:
            log.warning(f"  403 Forbidden — bot protection at {url}")
            return []
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # Conrad card selectors — try several across redesigns
    cards = (soup.select(".product-tile") or
             soup.select("article.product") or
             soup.select(".c-product-card") or
             soup.select("[data-product-id]") or
             soup.select(".search-result-item"))

    if not cards:
        log.debug(f"  No product cards at {url}")
        return []

    products = []
    for card in cards:
        name = parse_name(card)
        if not name: continue
        stars   = parse_stars(card)
        reviews = parse_reviews(card)
        price   = parse_price(card)
        purl    = parse_url(card)

        rec_pct = round((stars / 5.0) * 100, 1) if stars else None
        products.append({
            "Name": name, "ProductURL": purl,
            "AvgStarRating": stars, "ReviewsCount": reviews,
            "Price_EUR": price, "RecommendRate_pct": rec_pct,
        })
    return products


# ── Database ──────────────────────────────────────────────────────────────────

def load_existing(conn):
    return {r[0] for r in conn.execute("SELECT lower(Name) FROM products").fetchall()}


def insert(conn, products, cat_name, main_cat, dry_run=False):
    existing = load_existing(conn)
    added = 0
    for p in products:
        key = p["Name"].lower()
        if key in existing: continue
        if not dry_run:
            conn.execute(
                """INSERT OR IGNORE INTO products
                   (Name, Category, MainCategory, ProductURL,
                    Price_EUR, AvgStarRating, RecommendRate_pct,
                    ReviewsCount, source, country, currency)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (p["Name"], cat_name, main_cat, p.get("ProductURL",""),
                 p.get("Price_EUR"), p.get("AvgStarRating"),
                 p.get("RecommendRate_pct"), p.get("ReviewsCount", 0),
                 "conrad", "DE", "EUR")
            )
        existing.add(key)
        added += 1
    if not dry_run:
        conn.commit()
    return added


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat, session, conn, dry_run=False):
    total = 0
    base  = f"https://www.conrad.de/de/c/{cat['slug']}.html"
    log.info(f"── {cat['name']}  ({base})")

    for page in range(1, MAX_PAGES + 1):
        # Conrad pagination: ?page=N (or offset param)
        url = base if page == 1 else f"{base}?page={page}"
        log.info(f"   Page {page}: {url}")

        products = scrape_page(url, session)
        if not products:
            log.info("   No products — stopping.")
            break

        qualified = [p for p in products
                     if (p.get("AvgStarRating") or 0) >= MIN_STARS
                     and p["ReviewsCount"] >= MIN_REVIEWS]

        star_vals = [p["AvgStarRating"] for p in products if p.get("AvgStarRating")]
        lowest = min(star_vals) if star_vals else 5.0
        added = insert(conn, qualified, cat["name"], cat.get("main", cat["name"]), dry_run)
        total += added
        log.info(f"   Found {len(products)} | Qualified {len(qualified)} | Added {added} | Min ★ {lowest:.1f}")

        if lowest < STOP_BELOW:
            log.info(f"   Stars dropped to {lowest:.1f} — stopping.")
            break
        time.sleep(REQUEST_DELAY)
    return total


def run_scraper(dry_run=False):
    log.info("=" * 60)
    log.info("QualityDB — Conrad.de Scraper")
    log.info("=" * 60)
    session = requests.Session(impersonate="chrome124")
    warm_up(session)
    conn = sqlite3.connect(DB_PATH)
    summary = {"total_added": 0, "categories_scraped": 0, "errors": []}
    for cat in CATEGORIES:
        try:
            added = scrape_category(cat, session, conn, dry_run)
            summary["total_added"] += added
            summary["categories_scraped"] += 1
        except Exception as e:
            log.error(f"Error in {cat['name']}: {e}")
            summary["errors"].append(str(e))
        time.sleep(REQUEST_DELAY)
    conn.close()
    log.info(f"Done — {summary['total_added']} new DE products added.")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    r = run_scraper(dry_run=args.dry_run)
    print(f"\n✓  Done. {r['total_added']} products {'would be ' if args.dry_run else ''}added.")
