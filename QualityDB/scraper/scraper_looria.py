"""
Looria scraper.

STATUS (2025-05): Looria has removed all product/category pages from their
public site. The build manifest only contains static marketing pages:
/, /about, /contact, /press, /privacy, /terms.
All former product URLs (/category/*, /search/) return 404.

This scraper returns 0 rows gracefully until Looria restores public product
data or an API endpoint becomes available.

Previously: product name, brand, score, category, Reddit mention count.
"""

from __future__ import annotations

import hashlib
import re
import time

import requests
from bs4 import BeautifulSoup

from scraper_competitors_config import REQUEST_DELAY, REQUEST_TIMEOUT, USER_AGENT, canonical_category
from scraper_competitors_db import count_records, get_checkpoint, init_table, save_checkpoint, upsert_record

SOURCE = "looria"
BASE_URL = "https://looria.com"

# Category landing pages to crawl
CATEGORY_PAGES = [
    {"url": f"{BASE_URL}/categories/laptops",          "category": "laptops"},
    {"url": f"{BASE_URL}/categories/smartphones",      "category": "smartphones"},
    {"url": f"{BASE_URL}/categories/headphones",       "category": "headphones"},
    {"url": f"{BASE_URL}/categories/tablets",          "category": "tablets"},
    {"url": f"{BASE_URL}/categories/washing-machines", "category": "washing machines"},
    {"url": f"{BASE_URL}/categories/dishwashers",      "category": "dishwashers"},
    {"url": f"{BASE_URL}/categories/refrigerators",    "category": "refrigerators"},
    {"url": f"{BASE_URL}/categories/televisions",      "category": "televisions"},
    {"url": f"{BASE_URL}/categories/vacuums",          "category": "vacuum"},
    {"url": f"{BASE_URL}/categories/coffee-machines",  "category": "coffee maker"},
    # Also try the root — may list all categories
    {"url": BASE_URL,                                  "category": ""},
]


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def _make_product_id(product_url: str) -> str:
    return hashlib.md5(product_url.encode()).hexdigest()[:16]


def _extract_brand(name: str) -> str:
    return name.strip().split()[0].title() if name.strip() else ""


def _parse_score(text: str) -> float | None:
    m = re.search(r'(\d+(?:\.\d+)?)\s*/\s*10', text)
    if m:
        return float(m.group(1))
    m = re.search(r'(\d+(?:\.\d+)?)(?:\s*pts?)?$', text.strip())
    if m:
        val = float(m.group(1))
        if 0 <= val <= 10:
            return val
        if 0 <= val <= 100:
            return val / 10
    return None


def _try_playwright(url: str) -> str | None:
    """Try to fetch page with Playwright (headless Chromium)."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("    Playwright not installed. Install with: pip install playwright && playwright install chromium")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.set_extra_http_headers({"User-Agent": USER_AGENT})
            page.goto(url, wait_until="networkidle", timeout=30000)
            # Wait for product cards to load
            try:
                page.wait_for_selector("[class*='product'], [class*='item'], [class*='card']", timeout=8000)
            except Exception:
                pass
            html = page.content()
            browser.close()
            return html
    except Exception as e:
        print(f"    Playwright error: {e}")
        return None


def _get_page_html(session: requests.Session, url: str, use_playwright_fallback: bool = True) -> str | None:
    """Fetch page HTML. Tries plain requests first, then Playwright."""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 200:
            # Check if we got meaningful content (not just a skeleton)
            if len(resp.text) > 3000 and "product" in resp.text.lower():
                return resp.text
            elif use_playwright_fallback:
                print(f"    Response too sparse ({len(resp.text)} chars), trying Playwright...")
                return _try_playwright(url)
            return resp.text
        elif resp.status_code == 403 and use_playwright_fallback:
            print(f"    Got 403, trying Playwright...")
            return _try_playwright(url)
    except Exception as e:
        print(f"    requests error: {e}")
        if use_playwright_fallback:
            return _try_playwright(url)
    return None


def _parse_category_page(html: str, category: str, base_url: str) -> list[dict]:
    """Extract product records from a category listing page."""
    soup = BeautifulSoup(html, "html.parser")
    records = []
    canon = canonical_category(category)

    # Looria product cards — try multiple selectors
    selectors = [
        "[class*='ProductCard']",
        "[class*='product-card']",
        "[class*='product_card']",
        "article[class*='product']",
        "li[class*='product']",
        "[data-product]",
        "[class*='item'][class*='card']",
    ]

    cards = []
    for sel in selectors:
        cards = soup.select(sel)
        if cards:
            break

    if not cards:
        # Fallback: any anchor with a score-like child
        cards = soup.find_all("a", href=True)

    for card in cards:
        # Product name
        name_el = card.find(["h2", "h3", "h4", "[class*='title']", "[class*='name']"])
        if not name_el and card.name == "a":
            name_el = card
        product_name = name_el.get_text(strip=True) if name_el else ""
        if not product_name or len(product_name) < 3:
            continue

        # Product URL
        if card.name == "a":
            href = card.get("href", "")
        else:
            a = card.find("a")
            href = a.get("href", "") if a else ""
        if href.startswith("/"):
            href = BASE_URL + href
        if not href:
            href = base_url

        # Score
        score_el = card.find(attrs={"class": re.compile(r'score|rating|grade', re.I)})
        raw_score = None
        if score_el:
            raw_score = _parse_score(score_el.get_text(strip=True))

        if raw_score is None:
            # Look for "X/10" patterns in card text
            card_text = card.get_text()
            raw_score = _parse_score(card_text)

        if raw_score is None:
            continue  # Skip items without scores

        # Reddit mentions or longevity data
        meta = {}
        reddit_el = card.find(string=re.compile(r'reddit|mention', re.I))
        if reddit_el:
            m = re.search(r'(\d+)', str(reddit_el))
            if m:
                meta["reddit_mentions"] = int(m.group(1))

        longevity_el = card.find(string=re.compile(r'year|lifespan|longevity', re.I))
        if longevity_el:
            m = re.search(r'(\d+(?:\.\d+)?)\s*year', str(longevity_el), re.I)
            if m:
                meta["estimated_lifespan_years"] = float(m.group(1))

        brand = _extract_brand(product_name)
        source_id = _make_product_id(href)

        records.append({
            "source":             SOURCE,
            "source_url":         href,
            "product_name":       product_name,
            "brand":              brand,
            "model":              product_name,
            "product_category":   category,
            "canonical_category": canon,
            "raw_score":          raw_score,
            "raw_score_min":      0.0,
            "raw_score_max":      10.0,
            "raw_score_label":    f"{raw_score}/10",
            "score_normalized":   round(raw_score * 10, 1) if raw_score <= 10 else raw_score,
            "sub_scores_json":    None,
            "meta_json":          meta if meta else None,
            "source_product_id":  source_id,
        })

    return records


def scrape() -> int:
    """Scrape Looria product scores. Returns total rows inserted.

    Currently returns 0 — Looria removed all product/category pages from
    their public site as of 2025. Only static marketing pages remain.
    """
    init_table()
    print(
        "\n[looria] Skipping — Looria has removed all product pages from their "
        "public site (all /category/* and /search/ URLs return 404). "
        "0 rows inserted."
    )
    return 0


if __name__ == "__main__":
    scrape()
