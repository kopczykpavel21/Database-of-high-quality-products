"""
Ceneo.pl scraper — finds top-rated products and adds them to QualityDB.

Two-phase approach:
  Phase 1: Scrape category listing pages → product ID, name, score, review
           count, price (PLN)
  Phase 2: Scrape each product's detail page → per-feature satisfaction
           scores (wygląd %, funkcjonalność %, bateria %, …) + spec table

Dependencies (install once):
    pip3 install playwright beautifulsoup4
    playwright install chromium

Usage:
    python3 scraper/ceneo_scraper.py              # full run (Phase 1 + 2)
    python3 scraper/ceneo_scraper.py --no-details # Phase 1 only (fast)
"""

import re
import json
import time
import logging
import sqlite3
import os
import sys
import argparse

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("\n⚠  Missing dependencies. Please run:")
    print("    pip3 install playwright beautifulsoup4")
    print("    playwright install chromium\n")
    sys.exit(1)

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("\n⚠  Playwright not found. Please run:")
    print("    pip3 install playwright")
    print("    playwright install chromium\n")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

BASE_URL        = "https://www.ceneo.pl"
REQUEST_DELAY   = 1.8   # seconds between requests (be polite)
MAX_PAGES       = 8     # max listing pages per category per run
MIN_REVIEWS     = 10    # skip products with fewer reviews
MIN_SCORE_PCT   = 70    # skip products below this % (= 3.5 / 5.0 * 100)
STOP_BELOW_PCT  = 65    # stop paginating when page's lowest score drops here

EXTRA_HEADERS = {
    "Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8",
}

# (url_path, display_name, main_category)
# Paths confirmed against live site — add/remove as needed.
CENEO_CATEGORIES = [
    ("/Smartfony",               "Smartfony",             "Telefony i tablety"),
    ("/Telefony_komorkowe",      "Telefony komórkowe",    "Telefony i tablety"),
    ("/Tablety",                 "Tablety",               "Telefony i tablety"),
    ("/Laptopy",                 "Laptopy",               "Komputery"),
    ("/Telewizory",              "Telewizory",            "TV i foto"),
    ("/Sluchawki",               "Słuchawki",             "Audio"),
    ("/Glosniki_przenosne",      "Głośniki przenośne",    "Audio"),
    ("/Smartwatche_i_opaski_fitness", "Smartwatche",      "Wearables"),
    ("/Lodowki",                 "Lodówki",               "AGD"),
    ("/Pralki",                  "Pralki",                "AGD"),
    ("/Odkurzacze",              "Odkurzacze",            "AGD"),
    ("/Ekspresy_do_kawy",        "Ekspresy do kawy",      "AGD"),
    ("/Roboty_sprzatajace",      "Roboty sprzątające",    "AGD"),
    ("/Aparaty_fotograficzne",   "Aparaty fotograficzne", "TV i foto"),
]


# ── Parsing helpers ───────────────────────────────────────────────────────────

def _int(text):
    """Extract first integer from text.  '1 688 opinii' → 1688."""
    if not text:
        return 0
    m = re.search(r"\d[\d\s]*", text)
    return int(re.sub(r"\s", "", m.group())) if m else 0


def _float(text):
    """'4,8' or '4.8' → 4.8.  None on failure."""
    if not text:
        return None
    m = re.search(r"(\d+)[,.](\d+)", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    m = re.search(r"(\d+)", text)
    return float(m.group(1)) if m else None


def score_to_pct(score_out_of_5):
    """Convert 0–5 score to 0–100 percentage."""
    if score_out_of_5 is None:
        return None
    return round(score_out_of_5 / 5.0 * 100, 1)


def parse_price_pln(text):
    """'od 4 034,00 zł' → 4034.0  (lower bound)."""
    if not text:
        return None
    # Remove non-numeric except comma/dot then parse
    m = re.search(r"(\d[\d\s]*)[,.](\d{2})", text)
    if m:
        integer_part = re.sub(r"\s", "", m.group(1))
        return float(f"{integer_part}.{m.group(2)}")
    m = re.search(r"(\d[\d\s]+)", text)
    return float(re.sub(r"\s", "", m.group(1))) if m else None


# ── Phase 1: listing pages ────────────────────────────────────────────────────

def page_url(path, page):
    """
    Ceneo pagination:
      page 1 → /Category
      page 2 → /Category;0020-30-0-0-1.htm
      page 3 → /Category;0020-30-0-0-2.htm  …
    """
    if page == 1:
        return BASE_URL + path
    return BASE_URL + path + f";0020-30-0-0-{page - 1}.htm"


def fetch_html(url, page):
    """
    Fetch a URL using a Playwright page object.
    Waits for the F-detection JS challenge to complete (networkidle),
    then returns the fully-rendered HTML.
    """
    try:
        page.goto(url, wait_until="load", timeout=45000)
        # Extra pause so Ceneo's F-detection JS can run and redirect,
        # and so any lazy-loaded price elements have time to render.
        time.sleep(3)
        return page.content()
    except Exception as e:
        log.warning(f"  Playwright fetch failed ({url}): {e}")
        return ""


def scrape_listing_page(url, page):
    """
    Fetch one Ceneo listing page.
    Returns list of dicts with keys: product_id, name, score_pct,
    review_count, price_pln, product_url.
    """
    html = fetch_html(url, page)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".cat-prod-row")
    if not cards:
        log.debug(f"  No .cat-prod-row cards at {url}")
        return []

    products = []
    for card in cards:
        # ── Product ID & URL ──────────────────────────────────────────────
        pid = card.get("data-productid", "").strip()
        if not pid:
            continue
        product_url = f"{BASE_URL}/{pid}"

        # ── Name ──────────────────────────────────────────────────────────
        name_el = card.select_one("strong.cat-prod-row__name")
        name = name_el.get_text(" ", strip=True) if name_el else ""
        # Strip badge prefixes like "NOWOŚĆ", "HIT", "PROMOCJA"
        name = re.sub(r"^(NOWOŚĆ|HIT|PROMOCJA|SUPER CENA|BESTSELLER)\s*", "", name, flags=re.I).strip()
        if not name:
            continue

        # ── Score (out of 5) → % ─────────────────────────────────────────
        score_el = card.select_one(".product-score")
        raw_score = score_el.get_text(" ", strip=True) if score_el else ""
        score_val = _float(raw_score.split("/")[0].strip())
        score_pct = score_to_pct(score_val)

        # ── Review count ─────────────────────────────────────────────────
        # The span containing "opini" also holds the score text (e.g. "4,8 / 5  238 opinii"),
        # so we can't just grab the first number — we must find the count before "opini/ocen".
        review_span = next(
            (s for s in card.find_all("span")
             if re.search(r"opini|ocen", s.get_text(), re.I)),
            None,
        )
        review_count = 0
        if review_span:
            review_text = review_span.get_text()
            # Match the number immediately before "opini/ocen".
            # Allows thousand-separator spaces (e.g. "1 688") but not double
            # spaces, so "5  238 opinii" correctly yields 238 not 5238.
            m = re.search(r"(\d(?:\d| (?=\d))*) *(?:opini|ocen)", review_text, re.I)
            if m:
                review_count = int(re.sub(r"\s", "", m.group(1)))

        # ── Price ─────────────────────────────────────────────────────────
        # Try multiple selectors — Ceneo's markup varies by category/version
        price_pln = None
        for price_sel in [
            ".price-box__price",
            ".cat-prod-row__price",
            "[data-price]",
            ".product-price",
            ".js-price-value",
        ]:
            price_el = card.select_one(price_sel)
            if price_el:
                # data-price attribute holds raw numeric value (e.g. "4034.00")
                raw = price_el.get("data-price") or price_el.get_text()
                price_pln = parse_price_pln(raw)
                if price_pln:
                    break

        products.append({
            "product_id":   pid,
            "name":         name,
            "score_pct":    score_pct,
            "review_count": review_count,
            "price_pln":    price_pln,
            "product_url":  product_url,
        })

    return products


# ── Phase 2: product detail pages ────────────────────────────────────────────

def parse_feature_scores(soup):
    """
    Extract per-feature user satisfaction from .product-feature__item elements.
    Each item's text tokens: [positive_votes, negative_votes, "pct%", label]
    Returns: {"wygląd": 97, "funkcjonalność": 96, …}
    """
    scores = {}
    for item in soup.select(".product-feature__item"):
        tokens = [
            t.strip() for t in item.get_text("\n").split("\n")
            if t.strip() and t.strip() != "/"
        ]
        # Expected order: positive, negative, "XX%", label
        pct = None
        label = None
        for tok in tokens:
            if re.match(r"^\d+%$", tok):
                pct = int(tok.rstrip("%"))
            elif not re.match(r"^\d+$", tok):
                label = tok
        if label and pct is not None:
            scores[label] = pct
    return scores


def parse_star_distribution(soup):
    """
    Returns {"5": 94, "4": 4, "3": 1, "2": 0, "1": 1} from review histogram.
    """
    dist = {}
    stars_header = soup.select_one(".review-header, .score-extend")
    if not stars_header:
        return dist

    numbers = [el.get_text(strip=True) for el in stars_header.select(".score-extend__number")]
    percents = [el.get_text(strip=True) for el in stars_header.select(".score-extend__percent")]
    for num, pct in zip(numbers, percents):
        m = re.search(r"(\d+)", pct)
        if m:
            dist[num] = int(m.group(1))
    return dist


def parse_spec_table(soup):
    """
    Parse product-spec__group__attributes tables.
    Returns nested dict: {"Dane podstawowe": {"Marka": "Samsung", …}, …}
    """
    specs = {}
    current_section = "Ogólne"

    for row in soup.select("table.product-spec__group__attributes tr"):
        cells = row.find_all("td")
        if len(cells) == 1:
            # Section header row
            current_section = cells[0].get_text(strip=True)
            if current_section not in specs:
                specs[current_section] = {}
        elif len(cells) >= 2:
            # Key-value row — label cell may contain tooltip text after newline
            raw_label = cells[0].get_text("\n", strip=True)
            label = raw_label.split("\n")[0].strip()   # first line only
            # Strip the " ?" tooltip marker that Ceneo appends
            label = re.sub(r"\s*\?.*$", "", label).strip()
            value = cells[1].get_text(" ", strip=True)
            if label and value:
                if current_section not in specs:
                    specs[current_section] = {}
                specs[current_section][label] = value

    return specs


def scrape_detail_page(product_url, page):
    """
    Fetch a product detail page and return enrichment data:
      feature_scores, star_distribution, specs
    Returns empty dict on failure.
    """
    html = fetch_html(product_url, page)
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")

    return {
        "feature_scores":    parse_feature_scores(soup),
        "star_distribution": parse_star_distribution(soup),
        "specs":             parse_spec_table(soup),
    }


# ── Database helpers ──────────────────────────────────────────────────────────

SCHEMA_EXTRA = """
    CREATE TABLE IF NOT EXISTS products (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        Name              TEXT,
        Category          TEXT,
        MainCategory      TEXT,
        ProductURL        TEXT UNIQUE,
        Price_CZK         REAL,
        currency          TEXT DEFAULT 'PLN',
        RecommendRate_pct REAL,
        ReviewsCount      INTEGER,
        Brand             TEXT,
        country           TEXT DEFAULT 'PL',
        source            TEXT,
        details_json      TEXT
    )
"""


def ensure_schema(conn):
    conn.execute(SCHEMA_EXTRA)
    # Add columns that may be missing in older DBs
    for col, definition in [
        ("country",      "TEXT DEFAULT 'PL'"),
        ("currency",     "TEXT DEFAULT 'PLN'"),
        ("details_json", "TEXT"),
        ("MainCategory", "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {definition}")
        except sqlite3.OperationalError:
            pass  # column already exists
    conn.commit()


def load_existing_urls(conn):
    rows = conn.execute("SELECT ProductURL FROM products WHERE source='ceneo'").fetchall()
    return {r[0] for r in rows}


def upsert_product(conn, p):
    """Insert or update a Ceneo product row."""
    conn.execute(
        """INSERT INTO products
               (Name, Category, MainCategory, ProductURL, Price_CZK, currency,
                RecommendRate_pct, ReviewsCount, country, source, details_json)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(ProductURL) DO UPDATE SET
               RecommendRate_pct = COALESCE(excluded.RecommendRate_pct, RecommendRate_pct),
               ReviewsCount      = COALESCE(excluded.ReviewsCount, ReviewsCount),
               Price_CZK         = COALESCE(excluded.Price_CZK, Price_CZK),
               details_json      = COALESCE(excluded.details_json, details_json),
               source            = 'ceneo'
        """,
        (
            p["name"],
            p["category"],
            p["main_category"],
            p["product_url"],
            p.get("price_pln"),
            "PLN",
            p.get("score_pct"),
            p.get("review_count", 0),
            "PL",
            "ceneo",
            p.get("details_json"),
        ),
    )


# ── Main scrape logic ─────────────────────────────────────────────────────────

def scrape_category(path, cat_name, main_cat,
                    pw_page, conn,
                    scrape_details):
    log.info(f"── {cat_name}  ({BASE_URL + path})")
    stats = {"inserted": 0, "updated": 0, "skipped": 0}

    all_stubs = []

    # ── Phase 1: listing pages ────────────────────────────────────────────
    for page_num in range(1, MAX_PAGES + 1):
        url = page_url(path, page_num)
        log.info(f"   Page {page_num}: {url}")

        products = scrape_listing_page(url, pw_page)
        if not products:
            log.info("   Empty page — stopping.")
            break

        qualified = [
            p for p in products
            if (p["score_pct"] or 0) >= MIN_SCORE_PCT
            and p["review_count"] >= MIN_REVIEWS
        ]

        rated = [p["score_pct"] for p in products if p["score_pct"] is not None]
        lowest = min(rated) if rated else 100

        log.info(
            f"   Found {len(products)} | Qualified: {len(qualified)} | "
            f"Lowest score: {lowest:.0f}%"
        )

        all_stubs.extend(qualified)

        if lowest < STOP_BELOW_PCT:
            log.info(f"   Score dropped to {lowest:.0f}% — stopping early.")
            break

        time.sleep(REQUEST_DELAY)   # polite delay between listing pages

    if not all_stubs:
        return stats

    # ── Phase 2: detail pages ─────────────────────────────────────────────
    existing_urls = load_existing_urls(conn)

    for stub in all_stubs:
        stub["category"]      = cat_name
        stub["main_category"] = main_cat

        if scrape_details:
            log.info(f"   Detail: {stub['product_url']}")
            detail = scrape_detail_page(stub["product_url"], pw_page)
            stub["details_json"] = json.dumps(detail, ensure_ascii=False) if detail else None
            time.sleep(REQUEST_DELAY * 0.6)   # slightly faster for detail pages
        else:
            stub["details_json"] = None

        is_new = stub["product_url"] not in existing_urls
        upsert_product(conn, stub)
        conn.commit()

        if is_new:
            stats["inserted"] += 1
            existing_urls.add(stub["product_url"])
        else:
            stats["updated"] += 1

    return stats


def run_scraper(scrape_details=True):
    log.info("=" * 60)
    log.info("QualityDB Ceneo Scraper — starting run")
    log.info(f"  Detail scraping: {'ON' if scrape_details else 'OFF (--no-details)'}")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}. Run load_data.py first.")
        return {"error": "database_not_found"}

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    ensure_schema(conn)

    summary = {"categories_scraped": 0, "total_inserted": 0, "total_updated": 0, "errors": []}

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            locale="pl-PL",
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = context.new_page()

        # Warm up: visit homepage first so the F-detection cookie is set
        log.info("Warming up — visiting Ceneo homepage…")
        try:
            page.goto(BASE_URL + "/", wait_until="load", timeout=45000)
            time.sleep(3)
            log.info("Session ready.")
        except Exception as e:
            log.warning(f"Warm-up failed ({e}) — continuing anyway.")

        for path, cat_name, main_cat in CENEO_CATEGORIES:
            try:
                stats = scrape_category(path, cat_name, main_cat, page, conn, scrape_details)
                summary["total_inserted"]     += stats["inserted"]
                summary["total_updated"]      += stats["updated"]
                summary["categories_scraped"] += 1
            except Exception as e:
                log.error(f"Error scraping {cat_name}: {e}", exc_info=True)
                summary["errors"].append({"category": cat_name, "error": str(e)})
            time.sleep(REQUEST_DELAY)

        browser.close()

    conn.close()

    log.info("=" * 60)
    log.info(
        f"Run complete — {summary['total_inserted']} new products, "
        f"{summary['total_updated']} updated, "
        f"across {summary['categories_scraped']} categories."
    )
    log.info("=" * 60)
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ceneo.pl scraper for QualityDB")
    ap.add_argument(
        "--no-details",
        action="store_true",
        help="Skip Phase 2 (product detail pages) — runs faster but no feature scores",
    )
    args = ap.parse_args()

    result = run_scraper(scrape_details=not args.no_details)
    if result.get("errors"):
        print(f"\n⚠  {len(result['errors'])} category error(s) — check scraper/scraper.log")
    ins = result.get("total_inserted", 0)
    upd = result.get("total_updated", 0)
    print(f"\n✓  Done. {ins} new products inserted, {upd} updated.")
