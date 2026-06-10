"""
Testberichte.de scraper — German test-result aggregator.

Testberichte.de aggregates professional product test results from major German
magazines (CHIP, Computer Bild, Stiftung Warentest, c't, Öko-Test, etc.) and
shows a consolidated grade per product.

Grades use the German school scale: 1.0 (Sehr gut / Excellent) to 5.0 (Mangelhaft / Fail).
We convert to a 0–100 score: score = (6 − grade) / 5 × 100.
  1.0 → 100   1.5 → 90   2.0 → 80   2.5 → 70   3.0 → 60

Only products with grade ≤ MAX_GRADE (i.e., score ≥ MIN_SCORE) are stored.
Pages stop when the average grade on a page exceeds STOP_GRADE.

API approach:
  GET https://www.testberichte.de/f/1/{category_id}/{page}.html?pw=false
  Returns paginated product cards, server-rendered, sortable by grade.

Category IDs verified May 2026 by crawling navigation + testing URLs:
  2651 → Fernseher (TVs)
  269  → Kopfhörer (Headphones)
  2573 → Notebooks (Laptops)
  2619 → Monitore (Monitors)
  2587 → Waschmaschinen (Washing Machines)
  2620 → Geschirrspüler (Dishwashers)
  2777 → Kühlschränke (Refrigerators)
  2669 → Kaffeevollautomaten (Coffee Machines)
  2680 → Lautsprecher (Speakers)
  2588 → Staubsauger (Vacuum Cleaners)

Dependencies:
    pip3 install beautifulsoup4 requests
    (uses stdlib urllib as fallback)

Usage:
    python3 scraper/testberichte_scraper.py
"""

import re
import time
import logging
import sqlite3
import os
import sys

try:
    import requests
    _HAS_REQUESTS = True
except ImportError:
    import urllib.request
    import urllib.error
    _HAS_REQUESTS = False

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Thresholds ────────────────────────────────────────────────────────────────
MAX_GRADE   = 2.5   # Max German school grade to store (lower = better, 1.0 = best)
STOP_GRADE  = 3.0   # Stop a category page when avg grade on the page exceeds this
MIN_REVIEWS = 1     # Min professional test count (each grade = at least 1 magazine test)
MAX_PAGES   = 4     # Max pages per category (20 products per page)
PAGE_DELAY  = 1.5   # Seconds between requests

BASE_URL = "https://www.testberichte.de"

# ── Categories ────────────────────────────────────────────────────────────────
# category_id → (label, canonical DB category)
CATEGORIES = [
    {"id": 2651, "name": "TVs",              "label": "televisions"},
    {"id": 269,  "name": "Headphones",       "label": "headphones"},
    {"id": 2573, "name": "Laptops",          "label": "laptops"},
    {"id": 2619, "name": "Monitors",         "label": "monitors"},
    {"id": 2587, "name": "Washing Machines", "label": "washing machines"},
    {"id": 2620, "name": "Dishwashers",      "label": "dishwashers"},
    {"id": 2777, "name": "Refrigerators",    "label": "refrigerators"},
    {"id": 2669, "name": "Coffee Machines",  "label": "coffee machines"},
    {"id": 2680, "name": "Speakers",         "label": "speakers"},
    {"id": 2588, "name": "Vacuum Cleaners",  "label": "vacuums"},
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            os.path.join(os.path.dirname(__file__), "testberichte_scraper.log"),
            encoding="utf-8",
        ),
    ],
)
log = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.testberichte.de/",
}


# ── HTTP ──────────────────────────────────────────────────────────────────────

def _fetch_html(url: str) -> str:
    if _HAS_REQUESTS:
        try:
            r = requests.get(url, headers=_HEADERS, timeout=20)
            if r.status_code == 200:
                return r.text
            log.warning(f"HTTP {r.status_code} for {url}")
            return ""
        except Exception as e:
            log.warning(f"Request error: {e}")
            return ""
    else:
        req = urllib.request.Request(url, headers=_HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except Exception as e:
            log.warning(f"Fetch error {url}: {e}")
            return ""


# ── Parsing ───────────────────────────────────────────────────────────────────

GRADE_MAP = {
    "Sehr gut":    (1.0, 1.5),
    "Gut":         (1.5, 2.5),
    "Befriedigend":(2.5, 3.5),
    "Ausreichend": (3.5, 4.5),
    "Mangelhaft":  (4.5, 5.5),
    "Ungenügend":  (5.5, 6.0),
}


def _parse_grade(text: str):
    """
    Extract German school grade from text.
    '(Sehr gut) 1,6' → 1.6
    'Gut 1.7' → 1.7
    Returns None if not found.
    """
    m = re.search(r"(Sehr gut|Gut|Befriedigend|Ausreichend|Mangelhaft|Ungenügend)\s+(\d)[,.](\d)", text)
    if m:
        return float(f"{m.group(2)}.{m.group(3)}")
    # Fallback: bare number
    m = re.search(r"\b([1-5])[,.](\d)\b", text)
    if m:
        return float(f"{m.group(1)}.{m.group(2)}")
    return None


def _grade_to_score(grade: float) -> float:
    """German school grade 1.0–5.0 → normalised score 0–100."""
    return round(max(0.0, (6.0 - grade) / 5.0 * 100.0), 1)


def _parse_page(html: str) -> list[dict]:
    """Parse one page of testberichte results. Returns list of product dicts."""
    soup = BeautifulSoup(html, "html.parser")
    products = []

    items = soup.select("li.card.uic-product-card-old, li[class*=product]")
    for item in items:
        # Product URL + name from first link
        links = item.select("a[href]")
        if not links:
            continue
        href = links[0].get("href", "")
        name = links[0].get_text(strip=True)
        # Normalise soft-hyphen and other unicode in names
        name = name.replace("­", "").strip()

        if not name or len(name) < 3:
            continue

        url_product = href if href.startswith("http") else (BASE_URL + href)

        # Grade
        full_text = item.get_text(" ", strip=True)
        grade = _parse_grade(full_text)
        if grade is None:
            continue

        score = _grade_to_score(grade)

        products.append({
            "Name":              name,
            "ProductURL":        url_product,
            "Grade":             grade,
            "RecommendRate_pct": score,
            "ReviewsCount":      1,  # at least 1 professional test
        })

    return products


# ── Database ──────────────────────────────────────────────────────────────────

def _load_existing(conn) -> set:
    rows = conn.execute("SELECT lower(Name) FROM products").fetchall()
    return {r[0] for r in rows}


def _insert(conn, products: list, category: str) -> int:
    from scraper.snapshots import ensure_snapshot_table, record_snapshot
    ensure_snapshot_table(conn)
    existing = _load_existing(conn)
    inserted = 0
    for p in products:
        record_snapshot(conn, p.get("ProductURL", ""), "testberichte", p, country="DE")
        key = p["Name"].lower()
        if key in existing:
            continue
        conn.execute(
            """INSERT INTO products
               (Name, Category, ProductURL, Price_CZK,
                RecommendRate_pct, ReviewsCount, source)
               VALUES (?,?,?,?,?,?,?)""",
            (
                p["Name"],
                category,
                p.get("ProductURL", ""),
                None,
                p.get("RecommendRate_pct"),
                p.get("ReviewsCount", 1),
                "testberichte",
            ),
        )
        existing.add(key)
        inserted += 1
    conn.commit()
    return inserted


# ── Category scraper ──────────────────────────────────────────────────────────

def scrape_category(cat: dict, conn) -> int:
    cat_id   = cat["id"]
    cat_name = cat["name"]
    total    = 0

    log.info(f"── {cat_name}  (id={cat_id})")

    for page_num in range(1, MAX_PAGES + 1):
        url = f"{BASE_URL}/f/1/{cat_id}/{page_num}.html?pw=false"
        log.info(f"   Page {page_num} — {url}")

        html = _fetch_html(url)
        if not html:
            log.warning("   Empty response — stopping.")
            break

        # Detect 404
        if "404" in (BeautifulSoup(html, "html.parser").title or {}).get_text(""):
            log.info("   404 — stopping.")
            break

        products = _parse_page(html)
        if not products:
            log.info("   No products parsed — stopping.")
            break

        # Filter by grade threshold
        qualified = [p for p in products if p["Grade"] <= MAX_GRADE]
        grades    = [p["Grade"] for p in products]
        avg_grade = sum(grades) / len(grades) if grades else 0

        added = _insert(conn, qualified, cat_name)
        total += added
        log.info(
            f"   Found {len(products)} | Qualified (≤{MAX_GRADE}): {len(qualified)} | "
            f"New: {added} | Avg grade: {avg_grade:.2f}"
        )

        if avg_grade > STOP_GRADE:
            log.info(f"   Avg grade {avg_grade:.2f} > {STOP_GRADE} — stopping early.")
            break

        time.sleep(PAGE_DELAY)

    return total


# ── Orchestrator ──────────────────────────────────────────────────────────────

def run_scraper() -> dict:
    log.info("=" * 60)
    log.info("QualityDB Testberichte.de Scraper — starting run")
    log.info("=" * 60)

    if not os.path.exists(DB_PATH):
        log.error(f"Database not found at {DB_PATH}.")
        return {"error": "database_not_found"}

    conn    = sqlite3.connect(DB_PATH)
    summary = {"categories_scraped": 0, "total_added": 0, "errors": []}

    for cat in CATEGORIES:
        try:
            added = scrape_category(cat, conn)
            summary["total_added"]        += added
            summary["categories_scraped"] += 1
        except Exception as e:
            log.error(f"Error scraping {cat['name']}: {e}")
            summary["errors"].append({"category": cat["name"], "error": str(e)})
        time.sleep(PAGE_DELAY)

    conn.close()

    log.info("=" * 60)
    log.info(
        f"Run complete — {summary['total_added']} new products added "
        f"across {summary['categories_scraped']} categories."
    )
    log.info("=" * 60)
    return summary


if __name__ == "__main__":
    result = run_scraper()
    if result.get("errors"):
        print(f"\n⚠  {len(result['errors'])} category error(s) — check testberichte_scraper.log")
    print(f"\n✓  Done. {result['total_added']} new products added to database.")
