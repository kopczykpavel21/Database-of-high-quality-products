"""
alza_live_scraper.py — Weekly live refresh of Alza.cz product data.

Fetches individual Alza product pages (using already-known URLs in products.db),
extracts current Price_CZK, RecommendRate_pct, ReviewsCount, AvgStarRating
from JSON-LD and inline JS, and updates the products table.

Strategy:
  1. Read all alza product URLs from products.db
  2. Prioritise products not updated in the last 6 days (avoids hammering)
  3. Fetch each product page with curl_cffi (Chrome TLS fingerprint)
  4. Extract data from JSON-LD → inline JS → HTML fallbacks
  5. UPDATE products SET ..., scraped_at = 'now' WHERE ...

Runtime estimate:
  ~12 000 products × 0.35 s = ~70 minutes (weekly, overnight)
  ~3 700 priority products (ReviewsCount >= 10) × 0.35 s = ~22 minutes

Usage:
  python3 -m scraper.alza_live_scraper              # all products
  python3 -m scraper.alza_live_scraper --priority   # ≥10 reviews only
  python3 -m scraper.alza_live_scraper --limit 500  # first 500
"""
from __future__ import annotations   # allow `dict | None` hints on Python 3.9

import os
import re
import sys
import json
import time
import sqlite3
import logging

log = logging.getLogger(__name__)

DELAY       = 0.35   # seconds between requests — stay polite
COMMIT_EVERY = 50    # write progress every N products


def _open_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _extract_from_jsonld(html: str) -> dict:
    """Parse JSON-LD blocks for Schema.org Product data."""
    result = {}
    for block in re.findall(
        r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, re.S | re.I
    ):
        try:
            d = json.loads(block)
            # Handle list or @graph wrappers
            candidates = d if isinstance(d, list) else [d]
            flat = []
            for c in candidates:
                if isinstance(c, dict):
                    flat.append(c)
                    flat.extend(c.get("@graph", []))
            for item in flat:
                if not isinstance(item, dict):
                    continue
                if item.get("@type") != "Product":
                    continue
                # Price
                offers = item.get("offers", {})
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                price = offers.get("price") or offers.get("lowPrice")
                if price:
                    try:
                        result["Price_CZK"] = float(str(price).replace(",", ".").replace(" ", ""))
                    except ValueError:
                        pass
                # Aggregate rating
                ar = item.get("aggregateRating", {})
                if ar.get("ratingValue"):
                    try:
                        result["AvgStarRating"] = float(str(ar["ratingValue"]).replace(",", "."))
                    except ValueError:
                        pass
                if ar.get("reviewCount"):
                    try:
                        result["ReviewsCount"] = int(ar["reviewCount"])
                    except ValueError:
                        pass
                if result.get("Price_CZK") or result.get("ReviewsCount"):
                    return result
        except Exception:
            continue
    return result


def _extract_from_inline_js(html: str) -> dict:
    """Fallback: regex patterns on Alza's embedded JS objects."""
    result = {}

    # Price: various Alza JS patterns
    for pattern in [
        r'"priceWithVat"\s*:\s*"?([0-9][0-9 ]*)"?',
        r'"price"\s*:\s*"?([0-9][0-9 .,]*)"?',
        r'data-price="([0-9 ]+)"',
        r'<span[^>]*class="[^"]*price-box__price[^"]*"[^>]*>\s*([\d\s]+)',
    ]:
        m = re.search(pattern, html, re.I)
        if m:
            try:
                val = float(re.sub(r'[\s]', '', m.group(1).replace(',', '.')))
                if 1 < val < 2_000_000:
                    result["Price_CZK"] = val
                    break
            except ValueError:
                pass

    # Review count
    for pattern in [
        r'"recomCount"\s*:\s*(\d+)',
        r'"reviewCount"\s*:\s*(\d+)',
        r'"ratingsCount"\s*:\s*(\d+)',
        r'data-review-count="(\d+)"',
        r'<span[^>]*class="[^"]*review-count[^"]*"[^>]*>\s*(\d+)',
    ]:
        m = re.search(pattern, html, re.I)
        if m:
            try:
                result["ReviewsCount"] = int(m.group(1))
                break
            except ValueError:
                pass

    # Star rating (0–5 scale)
    for pattern in [
        r'"ratingValue"\s*:\s*"?([0-9.]+)"?',
        r'"averageRating"\s*:\s*"?([0-9.]+)"?',
        r'data-rating="([0-9.]+)"',
    ]:
        m = re.search(pattern, html, re.I)
        if m:
            try:
                val = float(m.group(1))
                if 0 < val <= 5:
                    result["AvgStarRating"] = val
                    break
            except ValueError:
                pass

    # Recommend percentage ("92 % zákazníků doporučuje")
    m = re.search(r'(\d{1,3})\s*%\s*zákazníků\s*doporučuje', html, re.I)
    if m:
        try:
            result["RecommendRate_pct"] = float(m.group(1))
        except ValueError:
            pass

    return result


def _scrape_product_page(url: str, session) -> dict | None:
    """Fetch one Alza product page; return extracted dict or None on failure."""
    try:
        r = session.get(
            url, impersonate="chrome120", timeout=15,
            headers={
                "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.8",
                "Referer": "https://www.alza.cz/",
            },
        )
        if r.status_code == 404:
            return {"_deleted": True}    # product removed; mark it
        if r.status_code != 200:
            return None
        html = r.text
    except Exception as e:
        log.debug(f"[alza-live] fetch error for {url}: {e}")
        return None

    data = _extract_from_jsonld(html)
    if not data.get("Price_CZK") and not data.get("ReviewsCount"):
        data.update(_extract_from_inline_js(html))
    return data if data else None


def run_alza_live_scraper(
    db_path: str | None = None,
    priority_only: bool = False,
    max_products: int | None = None,
    categories: list | None = None,
) -> int:
    """
    Refresh Alza product data in products.db.

    Args:
        db_path:       Path to products.db. Defaults to $DB_PATH or /data/products.db.
        priority_only: If True, only update products with ReviewsCount >= 10.
        max_products:  Cap on number of products to update in this run.
        categories:    List of NormalizedCategory values to restrict to (optional).

    Returns:
        Number of products successfully updated.
    """
    if db_path is None:
        db_path = os.environ.get("DB_PATH", "/data/products.db")

    if not os.path.exists(db_path):
        log.error(f"[alza-live] products.db not found at {db_path}")
        return 0

    try:
        from curl_cffi import requests as cffi_requests
        session = cffi_requests.Session()
        log.info("[alza-live] using curl_cffi (Chrome fingerprint)")
    except ImportError:
        log.error("[alza-live] curl_cffi not installed — cannot scrape Alza")
        return 0

    conn = _open_db(db_path)

    # Select products to refresh.
    # Skip products freshly updated in the last 6 days so reruns don't repeat work.
    # source can be 'alza' (old import) or 'alza.cz' (live DB) — match both.
    where_clauses = [
        "source IN ('alza', 'alza.cz')",
        "ProductURL IS NOT NULL AND ProductURL != ''",
        "(scraped_at IS NULL OR scraped_at < datetime('now', '-6 days'))",
    ]
    if priority_only:
        where_clauses.append("ReviewsCount >= 10")
    if categories:
        placeholders = ",".join("?" * len(categories))
        where_clauses.append(f"NormalizedCategory IN ({placeholders})")

    limit_sql = f" LIMIT {max_products}" if max_products else ""
    params = categories if categories else []
    rows = conn.execute(
        f"SELECT rowid, ProductURL FROM products WHERE {' AND '.join(where_clauses)}"
        f" ORDER BY CASE WHEN ReviewsCount IS NULL THEN 1 ELSE 0 END, ReviewsCount DESC"
        f"{limit_sql}",
        params,
    ).fetchall()
    conn.close()

    total   = len(rows)
    updated = 0
    skipped = 0
    deleted = 0
    pending = []    # (set_clause, values, rowid) — batch-committed

    log.info(f"[alza-live] Starting refresh of {total} Alza products "
             f"({'priority' if priority_only else 'all'})")

    def _flush(batch):
        if not batch:
            return
        c = _open_db(db_path)
        for set_clause, vals, rid in batch:
            c.execute(f"UPDATE products SET {set_clause} WHERE rowid = ?", vals + [rid])
        c.commit()
        c.close()

    consecutive_fail = 0   # early-abort guard for datacenter IP blocking

    for i, row in enumerate(rows):
        url = row["ProductURL"]
        data = _scrape_product_page(url, session)

        if data is None:
            skipped += 1
            consecutive_fail += 1
            # Alza blocks datacenter IPs (HTTP 403). If the first 12 requests all
            # fail, we're blocked — abort instead of wasting ~70 min on 12k 403s.
            if consecutive_fail >= 12 and updated == 0:
                log.warning(
                    f"[alza-live] Aborting after {consecutive_fail} consecutive failures "
                    f"with 0 successes — Alza is blocking this IP (403). "
                    f"Use the IKOR Skener bookmarklet (runs in user browsers) instead."
                )
                break
        elif data.get("_deleted"):
            consecutive_fail = 0
            log.debug(f"[alza-live] 404: {url}")
            deleted += 1
        else:
            consecutive_fail = 0
            # Build UPDATE
            fields = []
            vals   = []
            for col in ("Price_CZK", "RecommendRate_pct", "ReviewsCount", "AvgStarRating"):
                if col in data:
                    fields.append(f"{col} = ?")
                    vals.append(data[col])
            fields.append("scraped_at = datetime('now')")

            if fields:
                pending.append((", ".join(fields), vals, row["rowid"]))
                updated += 1

        if len(pending) >= COMMIT_EVERY:
            _flush(pending)
            pending = []
            log.info(
                f"[alza-live] Progress {i+1}/{total}: "
                f"updated={updated} skipped={skipped} deleted={deleted}"
            )

        time.sleep(DELAY)

    _flush(pending)
    log.info(
        f"[alza-live] Done — {updated} updated, {skipped} failed, "
        f"{deleted} removed, {total} total"
    )
    return updated


if __name__ == "__main__":
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Alza.cz live product data refresh")
    parser.add_argument("--priority", action="store_true",
                        help="Only update products with ReviewsCount >= 10")
    parser.add_argument("--limit", type=int, default=None,
                        help="Cap number of products to process in this run")
    parser.add_argument("--db", default=None, help="Path to products.db")
    args = parser.parse_args()

    n = run_alza_live_scraper(
        db_path=args.db,
        priority_only=args.priority,
        max_products=args.limit,
    )
    print(f"Done — {n} products updated.")
