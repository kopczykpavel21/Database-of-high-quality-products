"""
CZC.cz scraper — Czech electronics retailer.

CZC.cz is one of the largest Czech e-shops specialising in electronics/IT.
Products have 1-5 star ratings and written reviews.

Usage:
    python3 scraper/czc_scraper.py
    python3 scraper/czc_scraper.py --dry-run   # print without saving
"""

import re, time, logging, sqlite3, os, sys, json, argparse

try:
    from curl_cffi import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install curl_cffi beautifulsoup4\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH       = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")
MIN_STARS     = 4.0    # out of 5 — equivalent to ~80% recommend
MIN_REVIEWS   = 5
STOP_BELOW    = 3.5    # stop page when avg stars drop this low
REQUEST_DELAY = 2.0
MAX_PAGES     = 15

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "scraper.log"), encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

# Full browser headers including Sec-Fetch-* to pass Cloudflare/bot protection
HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "cs-CZ,cs;q=0.9,sk;q=0.8,en-US;q=0.7,en;q=0.6",
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
# CZC URL pattern: https://www.czc.cz/{slug}/produkty
# Slugs are FLAT (no subdirectory paths) — all categories live at root level.
CATEGORIES = [
    # Mobily & tablety
    {"name": "Smartphones",        "main": "Telefony a tablety",     "slug": "mobilni-telefony"},
    {"name": "Tablety",            "main": "Telefony a tablety",     "slug": "tablety"},
    {"name": "Smartwatch",         "main": "Telefony a tablety",     "slug": "chytre-hodinky"},

    # Počítače & notebooky
    {"name": "Notebooky",          "main": "Počítače a notebooky",   "slug": "notebooky"},
    {"name": "Monitory",           "main": "Počítače a notebooky",   "slug": "graficke-monitory"},
    {"name": "Klávesnice",         "main": "Počítače a notebooky",   "slug": "klavesnice"},
    {"name": "Myši",               "main": "Počítače a notebooky",   "slug": "mysi"},
    {"name": "SSD",                "main": "Počítače a notebooky",   "slug": "ssd"},
    {"name": "Grafické karty",     "main": "Počítače a notebooky",   "slug": "graficke-karty"},

    # Elektronika
    {"name": "Sluchátka",          "main": "Elektro",                "slug": "sluchatka"},
    {"name": "Reproduktory",       "main": "Elektro",                "slug": "reproduktory"},
    {"name": "Televizory",         "main": "Elektro",                "slug": "televizory"},

    # Foto
    {"name": "Fotoaparáty",        "main": "Foto a video",           "slug": "fotoaparaty"},

    # Síťové prvky
    {"name": "Routery",            "main": "Počítače a notebooky",   "slug": "routery"},
]


# ── Parsers ───────────────────────────────────────────────────────────────────

def parse_stars(card):
    """
    CZC shows star rating in several possible ways:
    1. data-average-score attribute on a rating element
    2. aria-label like "Hodnocení: 4.5 z 5"
    3. width% on a filled-stars bar
    Returns float 0-5 or None.
    """
    # Try data attribute first
    el = card.select_one("[data-average-score], [data-score]")
    if el:
        val = el.get("data-average-score") or el.get("data-score")
        try: return float(val)
        except: pass

    # Try aria-label "X z 5" or "X/5"
    for el in card.select("[aria-label]"):
        label = el.get("aria-label", "")
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:z|/)\s*5", label)
        if m:
            try: return float(m.group(1).replace(",", "."))
            except: pass

    # Try itemprop ratingValue
    el = card.select_one("[itemprop='ratingValue']")
    if el:
        val = el.get("content") or el.get_text(strip=True)
        try: return float(val.replace(",", "."))
        except: pass

    # Try class-based star rating (count filled stars)
    filled = card.select(".star--full, .icon-star--full, .rating__star--full")
    if filled:
        return float(len(filled))

    return None


def parse_reviews(card):
    """Look for review/rating count near the stars."""
    for el in card.select("[itemprop='reviewCount'], [itemprop='ratingCount']"):
        try: return int(el.get("content") or el.get_text(strip=True))
        except: pass

    # Text like "12 recenzí" or "(45)"
    for el in card.find_all(["span", "a", "div"]):
        text = el.get_text(strip=True)
        m = re.search(r"\((\d+)\)|(\d+)\s*(?:recenz|hodnocen|review)", text, re.IGNORECASE)
        if m:
            val = m.group(1) or m.group(2)
            try: return int(val)
            except: pass
    return 0


def parse_price(card):
    """Return price as float CZK, or None."""
    for sel in [".price-box__price", ".price__value", ".c-price", "[itemprop='price']",
                ".normal-price", ".pd-price"]:
        el = card.select_one(sel)
        if el:
            text = el.get("content") or el.get_text(strip=True)
            m = re.search(r"(\d[\d\s]*(?:[.,]\d+)?)", text)
            if m:
                try: return float(re.sub(r"\s", "", m.group(1)).replace(",", "."))
                except: pass
    return None


def parse_name(card):
    for sel in [".pd-name a", ".product-tile__title a", "h2 a", "h3 a",
                ".pd-title a", ".c-product__link", "[itemprop='name']"]:
        el = card.select_one(sel)
        if el:
            return el.get_text(strip=True) or None
    return None


def parse_url(card):
    for sel in [".pd-name a", ".product-tile__title a", "h2 a", "h3 a",
                ".pd-title a", ".c-product__link"]:
        el = card.select_one(sel)
        if el and el.get("href"):
            href = el["href"]
            if href.startswith("http"):
                return href
            return "https://www.czc.cz" + href
    return ""


# ── Session & fetching ────────────────────────────────────────────────────────

def warm_up(session):
    try:
        resp = session.get("https://www.czc.cz/", headers=HEADERS, timeout=20)
        resp.raise_for_status()
        log.info(f"Session ready — status {resp.status_code}, {len(session.cookies)} cookies")
        time.sleep(2.0)
    except Exception as e:
        log.warning(f"Warmup failed: {e}")


def scrape_page(url, session):
    # Add Referer so requests look like they come from browsing the site
    page_headers = {**HEADERS, "Referer": "https://www.czc.cz/", "Sec-Fetch-Site": "same-origin"}
    try:
        resp = session.get(url, headers=page_headers, timeout=25)
        if resp.status_code == 403:
            log.warning(f"  403 Forbidden — bot protection active at {url}")
            return []
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  Request failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # CZC uses several possible card selectors across page redesigns
    cards = (soup.select(".pd-wrapper") or
             soup.select(".product-tile") or
             soup.select("article.product") or
             soup.select(".p-item") or
             soup.select("[data-product-id]"))

    if not cards:
        log.debug(f"  No product cards found at {url}")
        # Try to detect pagination end vs empty
        if "nenalezeny žádné produkty" in resp.text.lower():
            log.info("  End of results.")
        return []

    products = []
    for card in cards:
        name = parse_name(card)
        if not name: continue
        stars  = parse_stars(card)
        reviews = parse_reviews(card)
        price  = parse_price(card)
        purl   = parse_url(card)
        products.append({
            "Name": name, "ProductURL": purl,
            "AvgStarRating": stars, "ReviewsCount": reviews, "Price_CZK": price,
            "RecommendRate_pct": round((stars / 5.0) * 100, 1) if stars else None,
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
                   (Name, Category, MainCategory, ProductURL, Price_CZK,
                    AvgStarRating, RecommendRate_pct, ReviewsCount,
                    source, country, currency)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (p["Name"], cat_name, main_cat, p.get("ProductURL",""),
                 p.get("Price_CZK"), p.get("AvgStarRating"),
                 p.get("RecommendRate_pct"), p.get("ReviewsCount", 0),
                 "czc", "CZ", "CZK")
            )
        existing.add(key)
        added += 1
    if not dry_run:
        conn.commit()
    return added


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(cat, session, conn, dry_run=False):
    total = 0
    base  = f"https://www.czc.cz/{cat['slug']}/produkty"
    log.info(f"── {cat['name']}  ({base})")

    for page in range(1, MAX_PAGES + 1):
        # CZC pagination: ?offset=N (24 per page)
        offset = (page - 1) * 24
        url = base if page == 1 else f"{base}?offset={offset}"
        log.info(f"   Page {page} (offset {offset}): {url}")

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
    log.info("QualityDB — CZC.cz Scraper")
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
    log.info(f"Done — {summary['total_added']} new CZ products added.")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    r = run_scraper(dry_run=args.dry_run)
    print(f"\n✓  Done. {r['total_added']} products {'would be ' if args.dry_run else ''}added.")
