"""
CZC.cz scraper — Czech electronics retailer.

CZC.cz is one of the largest Czech e-shops specialising in electronics/IT.
Products have 1-5 star ratings and written reviews.

Uses Playwright (headless Chromium) to bypass Cloudflare JS challenge.
Install once:
    pip3 install playwright beautifulsoup4
    playwright install chromium

Usage:
    python3 scraper/czc_scraper.py
    python3 scraper/czc_scraper.py --dry-run   # print without saving
"""

import re, time, logging, sqlite3, os, sys, argparse

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    from bs4 import BeautifulSoup
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install playwright beautifulsoup4")
    print("    playwright install chromium\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH       = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")
MIN_STARS     = 0      # 0 = collect all ratings (full distribution)
MIN_REVIEWS   = 10    # keep at 10 for reliability
STOP_BELOW    = 0      # 0 = never stop early based on score
PAGE_DELAY    = 2.5    # seconds between page loads
MAX_PAGES     = 15
PAGE_SIZE     = 24     # CZC shows 24 products per page

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(os.path.dirname(__file__), "scraper.log"), encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

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
    1. data-average-score / data-score attribute
    2. aria-label "Hodnocení: 4.5 z 5"
    3. itemprop ratingValue
    Returns float 0-5 or None.
    """
    el = card.select_one("[data-average-score], [data-score]")
    if el:
        val = el.get("data-average-score") or el.get("data-score")
        try: return float(val)
        except: pass

    for el in card.select("[aria-label]"):
        label = el.get("aria-label", "")
        m = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:z|/)\s*5", label)
        if m:
            try: return float(m.group(1).replace(",", "."))
            except: pass

    el = card.select_one("[itemprop='ratingValue']")
    if el:
        val = el.get("content") or el.get_text(strip=True)
        try: return float(val.replace(",", "."))
        except: pass

    filled = card.select(".star--full, .icon-star--full, .rating__star--full")
    if filled:
        return float(len(filled))

    return None


def parse_reviews(card):
    for el in card.select("[itemprop='reviewCount'], [itemprop='ratingCount']"):
        try: return int(el.get("content") or el.get_text(strip=True))
        except: pass

    for el in card.find_all(["span", "a", "div"]):
        text = el.get_text(strip=True)
        m = re.search(r"\((\d+)\)|(\d+)\s*(?:recenz|hodnocen|review)", text, re.IGNORECASE)
        if m:
            val = m.group(1) or m.group(2)
            try: return int(val)
            except: pass
    return 0


def parse_price(card):
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
            return href if href.startswith("http") else "https://www.czc.cz" + href
    return ""


# ── Playwright fetching ───────────────────────────────────────────────────────

def fetch_page_html(page, url):
    """
    Load URL in Playwright and return full HTML after JS has run.
    Waits for the product grid to appear or times out gracefully.
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        # Wait for any product card selector to appear
        try:
            page.wait_for_selector(
                ".pd-wrapper, .product-tile, [data-product-id], article.product",
                timeout=10_000
            )
        except PWTimeout:
            pass  # Maybe no products — let BeautifulSoup decide
        time.sleep(1.0)  # small extra settle time for lazy-loaded content
        return page.content()
    except PWTimeout:
        log.warning(f"  Timeout loading {url}")
        return ""
    except Exception as e:
        log.warning(f"  Error loading {url}: {e}")
        return ""


def scrape_page(html):
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")

    cards = (soup.select(".pd-wrapper") or
             soup.select(".product-tile") or
             soup.select("article.product") or
             soup.select(".p-item") or
             soup.select("[data-product-id]"))

    if not cards:
        if "nenalezeny žádné produkty" in html.lower():
            log.info("  End of results.")
        return []

    products = []
    for card in cards:
        name = parse_name(card)
        if not name: continue
        stars   = parse_stars(card)
        reviews = parse_reviews(card)
        price   = parse_price(card)
        purl    = parse_url(card)
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

def scrape_category(cat, pw_page, conn, dry_run=False):
    total = 0
    base  = f"https://www.czc.cz/{cat['slug']}/produkty"
    log.info(f"── {cat['name']}  ({base})")

    for page_num in range(1, MAX_PAGES + 1):
        offset = (page_num - 1) * PAGE_SIZE
        url = base if page_num == 1 else f"{base}?offset={offset}"
        log.info(f"   Page {page_num} (offset {offset}): {url}")

        html = fetch_page_html(pw_page, url)
        products = scrape_page(html)
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
        time.sleep(PAGE_DELAY)
    return total


def run_scraper(dry_run=False):
    log.info("=" * 60)
    log.info("QualityDB — CZC.cz Scraper (Playwright)")
    log.info("=" * 60)

    conn = sqlite3.connect(DB_PATH)
    summary = {"total_added": 0, "categories_scraped": 0, "errors": []}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            locale="cs-CZ",
            timezone_id="Europe/Prague",
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        pw_page = context.new_page()

        # Warm up — visit homepage so cookies are set
        log.info("Warming up session…")
        try:
            pw_page.goto("https://www.czc.cz/", wait_until="domcontentloaded", timeout=30_000)
            time.sleep(3.0)
            log.info("Homepage loaded — session ready")
        except Exception as e:
            log.warning(f"Warmup failed: {e}")

        for cat in CATEGORIES:
            try:
                added = scrape_category(cat, pw_page, conn, dry_run)
                summary["total_added"] += added
                summary["categories_scraped"] += 1
            except Exception as e:
                log.error(f"Error in {cat['name']}: {e}")
                summary["errors"].append(str(e))
            time.sleep(PAGE_DELAY)

        browser.close()

    conn.close()
    log.info(f"Done — {summary['total_added']} new CZ products added.")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    r = run_scraper(dry_run=args.dry_run)
    print(f"\n✓  Done. {r['total_added']} products {'would be ' if args.dry_run else ''}added.")
