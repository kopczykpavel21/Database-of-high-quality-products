"""
datart_scraper.py  —  Scrapes product listings from Datart.cz
Collects: Name, Price, RecommendRate, ReviewsCount, URL, Category, Description
Uses curl_cffi with Chrome impersonation to bypass bot detection.
"""

import re
import time
import random
import sqlite3
import logging
from datetime import datetime

try:
    from curl_cffi import requests as cffi_requests
    IMPERSONATE = "chrome120"
except ImportError:
    import requests as cffi_requests
    IMPERSONATE = None

log = logging.getLogger("datart_scraper")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xhtml,*/*;q=0.8",
    "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.datart.cz/",
}

# Categories to scrape: (display_name, datart_path)
CATEGORIES = [
    ("Televize",            "televize"),
    ("Mobilní telefony",    "mobilni-telefony"),
    ("Notebooky",           "notebooky"),
    ("Tablety",             "tablety"),
    ("Pračky",              "picky/pracky"),
    ("Myčky nádobí",        "bile-zbozi/mycky-nadob"),
    ("Ledničky",            "bile-zbozi/lednickozmrazaky"),
    ("Sušičky",             "bile-zbozi/susickypraden"),
    ("Vysavače",            "vysavace"),
    ("Kávovary",            "kavovary"),
    ("Trouby",              "vareni/trouby-a-sporaky"),
    ("Mikrovlnky",          "vareni/mikrovlnne-trouby"),
    ("Klimatizace",         "klimatizace"),
    ("Sluchátka",           "audio/sluchatka"),
    ("Fotoaparáty",         "foto/fotoaparaty"),
]

BASE_URL = "https://www.datart.cz"


def fetch(url):
    kwargs = {"headers": HEADERS, "timeout": 20}
    if IMPERSONATE:
        kwargs["impersonate"] = IMPERSONATE
    try:
        r = cffi_requests.get(url, **kwargs)
        return r.status_code, r.text
    except Exception as e:
        log.warning(f"fetch error {url}: {e}")
        return 0, ""


def parse_number(text):
    """Extract first number from text, return float or None."""
    if not text:
        return None
    text = text.strip().replace("\xa0", "").replace(" ", "")
    m = re.search(r"[\d]+[.,]?[\d]*", text)
    if m:
        return float(m.group().replace(",", "."))
    return None


def scrape_category_page(category_name, category_path, page=1):
    """Scrape one page of a category listing. Returns list of product dicts."""
    url = f"{BASE_URL}/{category_path}.html"
    if page > 1:
        # Datart uses ?pag=N or /strana-N/
        url = f"{BASE_URL}/{category_path}/strana-{page}.html"

    log.info(f"Scraping {category_name} page {page}: {url}")
    status, html = fetch(url)

    if status != 200:
        log.warning(f"  Got status {status} for {url}")
        return [], False

    products = []

    # ── Product boxes ──────────────────────────────────────────────
    # Datart uses <div class="product-box"> or <article class="product-box">
    # Each box has: product name, price, rating stars, review count, link
    product_blocks = re.findall(
        r'<(?:div|article)[^>]+class="[^"]*product-box[^"]*"[^>]*>(.*?)</(?:div|article)>',
        html, re.DOTALL | re.IGNORECASE
    )

    # Fallback: look for product list items
    if not product_blocks:
        product_blocks = re.findall(
            r'<li[^>]+class="[^"]*product[^"]*"[^>]*>(.*?)</li>',
            html, re.DOTALL | re.IGNORECASE
        )

    log.info(f"  Found {len(product_blocks)} product blocks")

    for block in product_blocks:
        try:
            product = _parse_product_block(block, category_name)
            if product and product.get("name"):
                products.append(product)
        except Exception as e:
            log.debug(f"  Block parse error: {e}")
            continue

    # Check if next page exists
    has_next = bool(re.search(r'strana-' + str(page + 1), html) or
                    re.search(r'pag=' + str(page + 1), html) or
                    re.search(r'class="[^"]*next[^"]*"', html, re.IGNORECASE))

    return products, has_next


def _parse_product_block(block, category_name):
    """Parse a single product HTML block into a dict."""
    # ── Name ────────────────────────────────────────────────
    name_match = (
        re.search(r'<(?:h2|h3)[^>]*class="[^"]*(?:product-name|name|title)[^"]*"[^>]*>\s*<a[^>]*>([^<]+)</a>', block, re.IGNORECASE) or
        re.search(r'<a[^>]+class="[^"]*(?:product-name|name|title)[^"]*"[^>]*>([^<]+)</a>', block, re.IGNORECASE) or
        re.search(r'<(?:h2|h3)[^>]*>.*?<a[^>]*>([^<]{10,})</a>', block, re.IGNORECASE)
    )
    name = name_match.group(1).strip() if name_match else None
    if not name:
        return None

    # ── URL ─────────────────────────────────────────────────
    url_match = re.search(r'<a[^>]+href="(/[^"]+\.htm[l]?)"', block, re.IGNORECASE)
    url = BASE_URL + url_match.group(1) if url_match else None

    # ── Price ────────────────────────────────────────────────
    price_match = re.search(
        r'class="[^"]*(?:price|cena)[^"]*"[^>]*>.*?([\d\s\xa0]+(?:Kč|,-|&nbsp;))',
        block, re.DOTALL | re.IGNORECASE
    )
    price = None
    if price_match:
        price = parse_number(price_match.group(1))

    # ── Rating ───────────────────────────────────────────────
    # Datart shows rating as a percentage fill or data-rating attribute
    rating_match = (
        re.search(r'data-rating=["\']([0-9.]+)["\']', block) or
        re.search(r'class="[^"]*rating[^"]*"[^>]*style="[^"]*width:\s*([0-9.]+)%', block) or
        re.search(r'rating["\s:]+([0-9.]+)', block, re.IGNORECASE)
    )
    recommend_rate = None
    if rating_match:
        val = float(rating_match.group(1))
        # If it's out of 5 stars, convert to percentage
        if val <= 5:
            recommend_rate = round(val / 5 * 100, 1)
        elif val <= 100:
            recommend_rate = val

    # ── Review count ─────────────────────────────────────────
    review_match = (
        re.search(r'(\d+)\s*(?:recenz|hodnocen|review)', block, re.IGNORECASE) or
        re.search(r'data-reviews-count=["\'](\d+)["\']', block)
    )
    reviews_count = int(review_match.group(1)) if review_match else None

    # ── Description (short) ─────────────────────────────────
    desc_match = re.search(
        r'class="[^"]*(?:perex|description|short-desc)[^"]*"[^>]*>(.*?)</(?:p|div)',
        block, re.DOTALL | re.IGNORECASE
    )
    description = None
    if desc_match:
        description = re.sub(r'<[^>]+>', '', desc_match.group(1)).strip()[:500]

    return {
        "name": name,
        "url": url,
        "price": price,
        "recommend_rate": recommend_rate,
        "reviews_count": reviews_count,
        "category": category_name,
        "description": description,
    }


def scrape_product_detail(url):
    """
    Fetch individual product page for richer data (description, better rating).
    Returns dict with fields to merge/update.
    """
    if not url:
        return {}
    status, html = fetch(url)
    if status != 200:
        return {}

    result = {}

    # Better description from product detail
    desc_match = re.search(
        r'class="[^"]*(?:product-description|description-text|perex)[^"]*"[^>]*>(.*?)</(?:div|section)',
        html, re.DOTALL | re.IGNORECASE
    )
    if desc_match:
        result["description"] = re.sub(r'<[^>]+>', '', desc_match.group(1)).strip()[:800]

    # Recommend rate (often more accurate on detail page)
    rec_match = re.search(r'(\d+)\s*%\s*(?:zákazníků\s*)?(?:doporučuje|recommends)', html, re.IGNORECASE)
    if rec_match:
        result["recommend_rate"] = float(rec_match.group(1))

    # Review count on detail page
    rev_match = re.search(r'(\d+)\s*(?:recenz[íe]|hodnocen)', html, re.IGNORECASE)
    if rev_match:
        result["reviews_count"] = int(rev_match.group(1))

    return result


def save_products(products, db_path="products.db"):
    """Upsert products into the database."""
    if not products:
        return 0

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    saved = 0
    for p in products:
        try:
            cur.execute("""
                INSERT INTO products
                    (Name, Category, Price_CZK, RecommendRate_pct, ReviewsCount,
                     ProductURL, Description, source, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'datart', ?)
                ON CONFLICT(ProductURL) DO UPDATE SET
                    Name              = excluded.Name,
                    Category          = excluded.Category,
                    Price_CZK         = excluded.Price_CZK,
                    RecommendRate_pct = excluded.RecommendRate_pct,
                    ReviewsCount      = excluded.ReviewsCount,
                    Description       = excluded.Description,
                    scraped_at        = excluded.scraped_at
            """, (
                p.get("name"),
                p.get("category"),
                p.get("price"),
                p.get("recommend_rate"),
                p.get("reviews_count"),
                p.get("url"),
                p.get("description"),
                datetime.utcnow().isoformat(),
            ))
            saved += 1
        except Exception as e:
            log.debug(f"DB insert error: {e}")

    conn.commit()
    conn.close()
    return saved


def run(db_path="products.db", max_pages=5, delay_range=(1.5, 3.5)):
    """Main entry point — scrape all categories."""
    log.info("=== Datart scraper starting ===")
    total = 0

    for cat_name, cat_path in CATEGORIES:
        log.info(f"\n── Category: {cat_name} ──")
        for page in range(1, max_pages + 1):
            products, has_next = scrape_category_page(cat_name, cat_path, page)

            if products:
                saved = save_products(products, db_path)
                total += saved
                log.info(f"  Page {page}: {len(products)} found, {saved} saved (total: {total})")

            if not has_next or not products:
                break

            time.sleep(random.uniform(*delay_range))

        time.sleep(random.uniform(2, 4))

    log.info(f"\n=== Datart scraper done. Total saved: {total} ===")
    return total


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    run()
