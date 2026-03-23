"""
Heureka.sk scraper — Slovak version of Heureka.cz.

Same platform, same HTML structure, just .sk domain + Slovak category URLs.
Adds products with country='SK', source='heureka_sk'.

Usage:
    python3 scraper/heureka_sk_scraper.py
"""

import re, time, logging, sqlite3, os, sys

try:
    from curl_cffi import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install curl_cffi beautifulsoup4\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH       = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")
MIN_RATING    = 88    # % — slightly lower than CZ since Slovak market is smaller
MIN_REVIEWS   = 5
STOP_BELOW    = 80
REQUEST_DELAY = 1.8
MAX_PAGES     = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "scraper.log"), encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

HEADERS = {"Accept-Language": "sk-SK,sk;q=0.9,cs;q=0.8,en;q=0.7"}

# ── Slovak Heureka categories ─────────────────────────────────────────────────
# Heureka.sk uses subdomain-based URLs just like .cz
CATEGORIES = [
    # Telefóny a GPS
    {"name": "Smartphones",        "main": "Telefony a tablety",     "url": "https://mobily.heureka.sk/mobilne-telefony/"},
    {"name": "Tablety",            "main": "Telefony a tablety",     "url": "https://mobily.heureka.sk/tablety/"},
    {"name": "Smartwatch",         "main": "Telefony a tablety",     "url": "https://chytre-hodinky.heureka.sk/"},

    # Audio / video
    {"name": "Sluchátka",          "main": "Elektro",                "url": "https://audio-video.heureka.sk/sluchadla/"},
    {"name": "Reproduktory",       "main": "Elektro",                "url": "https://audio-video.heureka.sk/reproduktory/"},
    {"name": "Televizory",         "main": "Elektro",                "url": "https://audio-video.heureka.sk/televizory/"},

    # Počítače
    {"name": "Notebooky",          "main": "Počítače a notebooky",   "url": "https://pc-notebooky.heureka.sk/notebooky/"},
    {"name": "SSD",                "main": "Počítače a notebooky",   "url": "https://pc-notebooky.heureka.sk/ssd-disky/"},
    {"name": "Myši",               "main": "Počítače a notebooky",   "url": "https://pc-notebooky.heureka.sk/mysi/"},
    {"name": "Klávesnice",         "main": "Počítače a notebooky",   "url": "https://pc-notebooky.heureka.sk/klavesnice/"},

    # Domácí spotřebiče
    {"name": "Kávovar",            "main": "Domácí spotřebiče",      "url": "https://spotrebice.heureka.sk/kavovary/"},
    {"name": "Vysavač",            "main": "Domácí spotřebiče",      "url": "https://spotrebice.heureka.sk/vysavace/"},
    {"name": "Malé spotřebiče",    "main": "Domácí spotřebiče",      "url": "https://spotrebice.heureka.sk/male-spotrebice/"},

    # Foto
    {"name": "Fotoaparáty",        "main": "Foto a video",           "url": "https://foto-video.heureka.sk/fotoaparaty/"},

    # Sport
    {"name": "Sport",              "main": "Sport a outdoor",        "url": "https://sport.heureka.sk/"},
]


# ── Parsers (identical to heureka.cz) ────────────────────────────────────────

def parse_rating(text):
    if not text: return None
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%", text)
    return float(m.group(1).replace(",", ".")) if m else None

def parse_reviews(text):
    if not text: return 0
    m = re.search(r"(\d[\d\s]*)", text)
    return int(re.sub(r"\s", "", m.group(1))) if m else 0

def parse_price(text):
    if not text: return None
    m = re.search(r"(\d[\d\s]*)", text)
    return float(re.sub(r"\s", "", m.group(1))) if m else None


def warm_up(session):
    try:
        resp = session.get("https://www.heureka.sk/", headers=HEADERS, timeout=20)
        resp.raise_for_status()
        log.info(f"Session ready — {len(session.cookies)} cookies")
        time.sleep(1.2)
    except Exception as e:
        log.warning(f"Warmup failed: {e}")


def scrape_page(url, session):
    try:
        resp = session.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    cards = soup.select(".c-product")
    if not cards:
        log.debug(f"  No .c-product cards at {url}")
        return []

    products = []
    for card in cards:
        name_el = card.select_one(".c-product__link")
        if not name_el: continue
        name = name_el.get_text(strip=True)
        overlay = card.select_one(".c-product__overlay-link")
        url_p = name_el.get("href") or (overlay.get("href") if overlay else "")

        rating_el = card.select_one(".c-rating-widget__value")
        rating = parse_rating(rating_el.get_text(strip=True) if rating_el else "")

        review_span = next(
            (s for s in card.find_all("span")
             if "recenz" in s.get_text().lower() or "hodnocen" in s.get_text().lower()),
            None
        )
        reviews = parse_reviews(review_span.get_text(strip=True) if review_span else "")

        price_el = card.select_one(".c-product__price--bold, .c-product__price")
        price = parse_price(price_el.get_text(strip=True) if price_el else "")

        products.append({
            "Name": name, "ProductURL": url_p,
            "RecommendRate_pct": rating, "ReviewsCount": reviews, "Price_CZK": price
        })
    return products


def load_existing(conn):
    return {r[0] for r in conn.execute("SELECT lower(Name) FROM products").fetchall()}

def insert(conn, products, cat_name, main_cat):
    existing = load_existing(conn)
    added = 0
    for p in products:
        key = p["Name"].lower()
        if key in existing: continue
        conn.execute(
            """INSERT OR IGNORE INTO products
               (Name, Category, MainCategory, ProductURL, Price_CZK,
                RecommendRate_pct, ReviewsCount, source, country)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (p["Name"], cat_name, main_cat, p.get("ProductURL",""),
             p.get("Price_CZK"), p.get("RecommendRate_pct"),
             p.get("ReviewsCount", 0), "heureka_sk", "SK")
        )
        existing.add(key)
        added += 1
    conn.commit()
    return added


def scrape_category(cat, session, conn):
    base = cat["url"].rstrip("/")
    total = 0
    log.info(f"── {cat['name']}  ({base})")
    for page in range(1, MAX_PAGES + 1):
        url = f"{base}/?sort=rating" if page == 1 else f"{base}/?sort=rating&f={page}"
        log.info(f"   Page {page}: {url}")
        products = scrape_page(url, session)
        if not products:
            log.info("   No products — stopping.")
            break

        qualified = [p for p in products
                     if (p["RecommendRate_pct"] or 0) >= MIN_RATING
                     and p["ReviewsCount"] >= MIN_REVIEWS]

        rated = [p["RecommendRate_pct"] for p in products if p["RecommendRate_pct"] is not None]
        lowest = min(rated) if rated else 100
        added = insert(conn, qualified, cat["name"], cat.get("main", cat["name"]))
        total += added
        log.info(f"   Found {len(products)} | Qualified {len(qualified)} | Added {added} | Min rating {lowest}%")

        if lowest < STOP_BELOW:
            log.info(f"   Rating dropped to {lowest}% — stopping.")
            break
        time.sleep(REQUEST_DELAY)
    return total


def run_scraper():
    log.info("=" * 60)
    log.info("QualityDB — Heureka.sk Scraper")
    log.info("=" * 60)
    session = requests.Session(impersonate="chrome120")
    warm_up(session)
    conn = sqlite3.connect(DB_PATH)
    summary = {"total_added": 0, "categories_scraped": 0, "errors": []}
    for cat in CATEGORIES:
        try:
            added = scrape_category(cat, session, conn)
            summary["total_added"] += added
            summary["categories_scraped"] += 1
        except Exception as e:
            log.error(f"Error in {cat['name']}: {e}")
            summary["errors"].append(str(e))
        time.sleep(REQUEST_DELAY)
    conn.close()
    log.info(f"Done — {summary['total_added']} new SK products added.")
    return summary


if __name__ == "__main__":
    r = run_scraper()
    print(f"\n✓  Done. {r['total_added']} new products added.")
