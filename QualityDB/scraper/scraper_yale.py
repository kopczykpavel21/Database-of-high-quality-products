"""
Yale Appliance Reliability Report scraper.

Yale Appliance publishes annual reliability reports as blog posts.
The site returns 403 to plain requests, so we use curl_cffi for
TLS fingerprint spoofing (already in requirements.txt).

Data extracted: brand, appliance category, first-year service rate %.
Normalization: score_normalized = max(0, 100 - service_rate_pct * 5)
  → 0% failures = 100, 5% = 75, 10% = 50, 20% = 0

Known report URLs (update annually):
"""

from __future__ import annotations

import hashlib
import re
import time

from bs4 import BeautifulSoup

from scraper_competitors_config import REQUEST_DELAY, REQUEST_TIMEOUT, USER_AGENT, canonical_category
from scraper_competitors_db import count_records, get_checkpoint, init_table, save_checkpoint, upsert_record

SOURCE = "yale"

# Yale reliability report blog posts — add new ones each year
REPORT_URLS = [
    "https://blog.yaleappliance.com/most-reliable-washers",
    "https://blog.yaleappliance.com/most-reliable-dishwashers",
    "https://blog.yaleappliance.com/most-reliable-clothes-dryers",
    "https://blog.yaleappliance.com/most-reliable-induction-ranges",
    "https://blog.yaleappliance.com/most-reliable-gas-ranges",
    "https://blog.yaleappliance.com/most-reliable-electric-ranges",
    "https://blog.yaleappliance.com/most-reliable-wall-ovens",
    "https://blog.yaleappliance.com/the-most-reliable-induction-cooktops",
    "https://blog.yaleappliance.com/most-reliable-counter-depth-french-door-refrigerators",
    "https://blog.yaleappliance.com/the-least-serviced-most-reliable-appliance-brands",
    "https://blog.yaleappliance.com/most-reliable-compact-washers",
    "https://blog.yaleappliance.com/most-reliable-compact-dryers",
    "https://blog.yaleappliance.com/the-most-reliable-combo-washer-and-dryer-brands",
    "https://blog.yaleappliance.com/youtube-appliance-rankings-vs-service-data",
]

# Appliance keywords to identify category from headings/context
CATEGORY_KEYWORDS = {
    "washer":        "Pračky",
    "washing":       "Pračky",
    "dryer":         "Sušičky",
    "dishwasher":    "Myčky",
    "refrigerator":  "Ledničky",
    "fridge":        "Ledničky",
    "range":         "Sporáky",
    "oven":          "Trouby",
    "microwave":     "Mikrovlnné trouby",
    "vacuum":        "Vysavače",
    "cooktop":       "Varné desky",
}


def _try_get(url: str) -> str | None:
    """Attempt GET with curl_cffi (Chrome TLS fingerprint), fall back to requests."""
    # Try curl_cffi first
    try:
        from curl_cffi import requests as cffi_requests
        resp = cffi_requests.get(
            url,
            impersonate="chrome120",
            timeout=REQUEST_TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        )
        if resp.status_code == 200:
            return resp.text
        print(f"    curl_cffi got HTTP {resp.status_code} for {url}")
    except ImportError:
        print("    curl_cffi not available, trying plain requests")
    except Exception as e:
        print(f"    curl_cffi error: {e}")

    # Fallback to requests
    try:
        import requests
        resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
        if resp.status_code == 200:
            return resp.text
        print(f"    requests got HTTP {resp.status_code} for {url}")
    except Exception as e:
        print(f"    requests error: {e}")

    return None


def _make_product_id(brand: str, category: str, year: str) -> str:
    key = f"{SOURCE}|{brand}|{category}|{year}".lower()
    return hashlib.md5(key.encode()).hexdigest()[:16]


def _detect_category(text: str) -> str:
    """Detect appliance category from surrounding text."""
    text_lower = text.lower()
    for keyword, cat in CATEGORY_KEYWORDS.items():
        if keyword in text_lower:
            return cat
    return "Spotřebiče"


def _extract_year_from_url(url: str) -> str:
    m = re.search(r'(20\d{2})', url)
    return m.group(1) if m else "2024"


def _parse_service_rate(text: str) -> float | None:
    """Extract a percentage like '4.5%' or '4.5' from text."""
    m = re.search(r'(\d+(?:\.\d+)?)\s*%', text)
    if m:
        return float(m.group(1))
    m = re.search(r'(\d+(?:\.\d+)?)', text)
    if m:
        val = float(m.group(1))
        if 0 <= val <= 100:
            return val
    return None


def _score_from_service_rate(rate_pct: float) -> float:
    """Convert first-year service rate % to 0–100 score (lower failure = higher score)."""
    return max(0.0, round(100.0 - rate_pct * 5, 1))


def _parse_table(table, category: str, year: str) -> list[dict]:
    """Parse an HTML table that contains brand reliability data."""
    records = []
    rows = table.find_all("tr")

    # Find header row to identify column indices
    header_row = rows[0] if rows else None
    if not header_row:
        return records

    headers = [th.get_text(strip=True).lower() for th in header_row.find_all(["th", "td"])]

    # Identify brand and service-rate column indices
    brand_col = None
    rate_col = None
    for i, h in enumerate(headers):
        if any(k in h for k in ["brand", "make", "manufacturer"]):
            brand_col = i
        if any(k in h for k in ["service", "rate", "repair", "%", "fail"]):
            rate_col = i

    if brand_col is None:
        brand_col = 0  # default: first column is brand
    if rate_col is None and len(headers) >= 2:
        rate_col = -1  # default: last column

    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if len(cells) < 2:
            continue

        brand = cells[brand_col].get_text(strip=True)
        rate_text = cells[rate_col].get_text(strip=True)

        if not brand or brand.lower() in ("brand", "make", "manufacturer"):
            continue

        rate = _parse_service_rate(rate_text)
        if rate is None:
            continue

        score = _score_from_service_rate(rate)

        records.append({
            "source":             SOURCE,
            "source_url":         "",  # set by caller
            "product_name":       f"{brand} {category} ({year})",
            "brand":              brand,
            "model":              None,
            "product_category":   category,
            "canonical_category": canonical_category(category.lower()),
            "raw_score":          rate,
            "raw_score_min":      0.0,
            "raw_score_max":      100.0,
            "raw_score_label":    f"{rate}% first-year service rate",
            "score_normalized":   score,
            "sub_scores_json":    None,
            "meta_json":          {"year": year, "metric": "first_year_service_rate_pct"},
            "source_product_id":  _make_product_id(brand, category, year),
        })

    return records


def _scrape_report_page(url: str) -> list[dict]:
    """Scrape one Yale reliability report page. Returns list of record dicts."""
    print(f"  Fetching {url} ...")
    html = _try_get(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    year = _extract_year_from_url(url)
    all_records = []

    # Find all tables — Yale reports are table-heavy
    tables = soup.find_all("table")
    for table in tables:
        # Determine category from the nearest preceding heading
        heading = table.find_previous(["h2", "h3", "h4"])
        heading_text = heading.get_text(strip=True) if heading else ""
        category = _detect_category(heading_text) if heading_text else "Spotřebiče"

        records = _parse_table(table, category, year)
        for r in records:
            r["source_url"] = url
        all_records.extend(records)
        print(f"    Found {len(records)} brand rows in table (category: {category})")

    # Also try to extract from text patterns if no tables found
    if not tables:
        text = soup.get_text()
        # Pattern: "Brand: X% service rate" or "Brand X.X%"
        pattern = re.compile(
            r'([A-Z][a-zA-Z& ]+?)\s*[:\-]\s*(\d+(?:\.\d+)?)\s*%',
        )
        # Find section context
        current_category = "Spotřebiče"
        for line in text.split("\n"):
            line = line.strip()
            # Check if line is a category heading
            for kw, cat in CATEGORY_KEYWORDS.items():
                if kw in line.lower() and len(line) < 60:
                    current_category = cat
                    break
            m = pattern.search(line)
            if m:
                brand = m.group(1).strip()
                rate = float(m.group(2))
                if 0 < rate < 100 and len(brand) > 2:
                    score = _score_from_service_rate(rate)
                    all_records.append({
                        "source":             SOURCE,
                        "source_url":         url,
                        "product_name":       f"{brand} {current_category} ({year})",
                        "brand":              brand,
                        "model":              None,
                        "product_category":   current_category,
                        "canonical_category": canonical_category(current_category.lower()),
                        "raw_score":          rate,
                        "raw_score_min":      0.0,
                        "raw_score_max":      100.0,
                        "raw_score_label":    f"{rate}% first-year service rate",
                        "score_normalized":   score,
                        "sub_scores_json":    None,
                        "meta_json":          {"year": year, "metric": "first_year_service_rate_pct"},
                        "source_product_id":  _make_product_id(brand, current_category, year),
                    })

    return all_records


def scrape() -> int:
    """Scrape Yale Appliance reliability reports. Returns total rows inserted."""
    init_table()
    checkpoint = get_checkpoint(SOURCE)
    total = 0

    print(f"\n[yale] Starting scrape — {len(REPORT_URLS)} report URLs")

    for url in REPORT_URLS:
        url_key = hashlib.md5(url.encode()).hexdigest()[:12]

        if checkpoint.get(url_key) == "done":
            print(f"  [skip] {url}")
            continue

        records = _scrape_report_page(url)
        inserted = 0
        for rec in records:
            upsert_record(rec)
            inserted += 1
            total += 1

        checkpoint[url_key] = "done"
        save_checkpoint(SOURCE, checkpoint)
        print(f"  Inserted {inserted} records from {url}")
        time.sleep(REQUEST_DELAY)

    print(f"[yale] Done — {total} rows total. DB now has {count_records(SOURCE)} {SOURCE} records.")
    return total


if __name__ == "__main__":
    scrape()
