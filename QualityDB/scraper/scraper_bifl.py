"""
BuyItForLifeProducts.com scraper.

Scrapes BIFL Score (0–10) and sub-scores from static HTML.
Sub-scores are stored in <div class="score-field" data-score="X"> elements
preceded by headings like "Durability Score", "Repairability Score", etc.

BIFL Score formula: (Durability×0.3) + (Social×0.3) + (Warranty×0.2) + (Repairability×0.2)
Normalisation: score_normalized = raw_score × 10  (0–10 → 0–100)

Product URLs are discovered from the /products listing page
(48 products as of 2025). Only /products/SLUG URLs are scraped — no
category/badge/filter pages.

Note: site redirects http→https and no-www→www. Use www.buyitforlifeproducts.com.
"""

from __future__ import annotations

import hashlib
import re
import time

import requests
from bs4 import BeautifulSoup

from scraper_competitors_config import REQUEST_DELAY, REQUEST_TIMEOUT, USER_AGENT, canonical_category
from scraper_competitors_db import count_records, get_checkpoint, init_table, save_checkpoint, upsert_record

SOURCE = "bifl"
BASE_URL = "https://www.buyitforlifeproducts.com"
DELAY = max(REQUEST_DELAY, 2.0)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.headers["Accept"] = "text/html,application/xhtml+xml,*/*"
    s.headers["Accept-Language"] = "en-US,en;q=0.9"
    return s


def _make_product_id(product_url: str) -> str:
    return hashlib.md5(product_url.encode()).hexdigest()[:16]


def _extract_brand(product_name: str) -> str:
    return product_name.strip().split()[0].title() if product_name.strip() else ""


def _get_all_product_urls(session: requests.Session) -> list[str]:
    """Fetch the /products listing page and extract only /products/SLUG URLs."""
    try:
        resp = session.get(f"{BASE_URL}/products", timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        print(f"  [!] Cannot reach {BASE_URL}/products: {e}")
        return []

    # Extract href="/products/SLUG" — no query params, no trailing slashes
    slugs = re.findall(r'href="(/products/[a-z0-9][a-z0-9-]+)"', resp.text)
    urls = [f"{BASE_URL}{slug}" for slug in dict.fromkeys(slugs)]
    return urls


def _scrape_product_page(session: requests.Session, url: str) -> dict | None:
    """Scrape one BIFL product page. Returns a record dict or None."""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            return None
    except Exception as e:
        print(f"    [!] Error fetching {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Product name ──────────────────────────────────────────────────────────
    name_el = soup.find("h1")
    product_name = name_el.get_text(strip=True) if name_el else ""
    if not product_name:
        title = soup.find("title")
        if title:
            # "E3-16C Hammer Review 9.0/10 | Buy It For Life" → "E3-16C Hammer"
            product_name = re.sub(r'\s*Review.*', '', title.get_text(strip=True)).strip()
    if not product_name or len(product_name) < 3:
        return None

    # ── Brand ─────────────────────────────────────────────────────────────────
    # Try to find brand name — often in a subtitle or h2 below the h1
    brand = ""
    if name_el:
        nxt = name_el.find_next_sibling()
        if nxt:
            brand = nxt.get_text(strip=True)
        if not brand or len(brand) > 40:
            brand = _extract_brand(product_name)
    else:
        brand = _extract_brand(product_name)

    # ── Scores via data-score attributes ─────────────────────────────────────
    # Pattern: prev_sibling text = "Durability Score" / "Durability Notes" / "BIFL Score:"
    #          <div class="score-field" data-score="8.5">
    score_sections = {}
    for div in soup.find_all("div", class_=re.compile(r"score-field")):
        raw = div.get("data-score", "")
        try:
            val = float(raw)
        except (ValueError, TypeError):
            continue
        if not (0 <= val <= 10):
            continue
        # Label is the previous sibling's text
        prev = div.find_previous_sibling()
        label = prev.get_text(strip=True).lower() if prev else ""
        if not label:
            label = div.parent.get_text(strip=True).lower() if div.parent else ""
        if "durability" in label:
            score_sections["durability"] = val
        elif "repairability" in label or "repair" in label:
            score_sections["repairability"] = val
        elif "warranty" in label:
            score_sections["warranty"] = val
        elif "social" in label:
            score_sections["social"] = val
        elif "bifl" in label:
            score_sections["bifl_overall"] = val

    # ── Overall BIFL score ────────────────────────────────────────────────────
    raw_score = score_sections.get("bifl_overall")

    # Fallback: meta description "BIFL Score: 9.0/10"
    if raw_score is None:
        meta = soup.find("meta", {"name": "description"})
        if meta:
            m = re.search(r'BIFL Score:\s*(\d+(?:\.\d+)?)/10', meta.get("content", ""))
            if m:
                raw_score = float(m.group(1))

    # Fallback: page text "9.0/10"
    if raw_score is None:
        page_text = soup.get_text()
        m = re.search(r'(\d+(?:\.\d+)?)\s*/\s*10', page_text)
        if m:
            val = float(m.group(1))
            if 0 <= val <= 10:
                raw_score = val

    if raw_score is None:
        return None

    # ── Category ──────────────────────────────────────────────────────────────
    category = ""
    breadcrumb = soup.select_one("[class*='breadcrumb'] li:nth-last-child(2)")
    if breadcrumb:
        category = breadcrumb.get_text(strip=True)
    if not category:
        # /products/estwing-hammer → slug as category hint
        path = url.replace(BASE_URL, "").strip("/")
        parts = [p for p in path.split("/") if p]
        if len(parts) >= 2:
            category = parts[-2].replace("-", " ").title()

    sub_scores = {k: v for k, v in score_sections.items() if k != "bifl_overall"}

    return {
        "source":             SOURCE,
        "source_url":         url,
        "product_name":       product_name,
        "brand":              brand,
        "model":              product_name,
        "product_category":   category,
        "canonical_category": canonical_category(category),
        "raw_score":          raw_score,
        "raw_score_min":      0.0,
        "raw_score_max":      10.0,
        "raw_score_label":    f"{raw_score}/10",
        "score_normalized":   round(raw_score * 10, 1),
        "sub_scores_json":    sub_scores if sub_scores else None,
        "meta_json":          None,
        "source_product_id":  _make_product_id(url),
    }


def scrape() -> int:
    """Scrape BuyItForLifeProducts.com. Returns total rows inserted."""
    init_table()
    session = _session()
    checkpoint = get_checkpoint(SOURCE)
    total = 0

    print(f"\n[bifl] Starting scrape of {BASE_URL}")
    product_urls = _get_all_product_urls(session)
    print(f"  Found {len(product_urls)} product URLs")

    for i, url in enumerate(product_urls):
        url_key = _make_product_id(url)

        if checkpoint.get(url_key) in ("done", "no_score"):
            continue

        record = _scrape_product_page(session, url)

        if record is None:
            checkpoint[url_key] = "no_score"
            print(f"  [{i+1}/{len(product_urls)}] (no score) {url}")
        else:
            upsert_record(record)
            checkpoint[url_key] = "done"
            total += 1
            subs = record.get("sub_scores_json") or {}
            sub_str = "  ".join(f"{k}={v}" for k, v in subs.items()) if isinstance(subs, dict) else ""
            print(f"  [{i+1}/{len(product_urls)}] {record['product_name']} — {record['raw_score']}/10  {sub_str}")

        if (i + 1) % 10 == 0:
            save_checkpoint(SOURCE, checkpoint)
        time.sleep(DELAY)

    save_checkpoint(SOURCE, checkpoint)
    print(f"[bifl] Done — {total} rows. DB now has {count_records(SOURCE)} {SOURCE} records.")
    return total


if __name__ == "__main__":
    scrape()
