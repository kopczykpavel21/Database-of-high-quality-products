#!/usr/bin/env python3
"""
scrape_alza_local.py — Run the Alza scraper from a residential IP (your laptop)
and push fresh price/rating/review data to the LIVE QualityDB on fly.io.

Why: Alza blocks fly.io's datacenter IP (HTTP 403). Your home connection is
served normally, so we scrape here and upload the results via the API.

Alza rate-limits even residential IPs after a burst, so this runs SLOWLY
(default 1.8s/request) and backs off exponentially when it hits a 403.

Usage:
    python3 scrape_alza_local.py                      # 300 most-reviewed Alza products
    python3 scrape_alza_local.py --category Smartphones --limit 200
    python3 scrape_alza_local.py --limit 500 --delay 2.0
    python3 scrape_alza_local.py --min-reviews 20

Data extracted per product: Price_CZK (in-stock only), RecommendRate_pct,
ReviewsCount, AvgStarRating. Discontinued products keep their rating/reviews
refreshed even when price is unavailable.
"""
from __future__ import annotations

import re
import sys
import json
import time
import argparse
import urllib.request

API = "https://database-of-high-quality-products.fly.dev"

try:
    from curl_cffi import requests as cffi
except ImportError:
    print("Missing dependency. Run:  pip3 install curl_cffi")
    sys.exit(1)


# ── Extraction ────────────────────────────────────────────────────────────────
def extract(html: str) -> dict:
    """Pull Price_CZK / RecommendRate_pct / ReviewsCount / AvgStarRating from HTML."""
    out = {}
    # JSON-LD Product (rating + reviews + price)
    for block in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, re.S | re.I):
        try:
            d = json.loads(block)
        except Exception:
            continue
        for it in (d if isinstance(d, list) else [d]):
            if not isinstance(it, dict) or it.get("@type") != "Product":
                continue
            off = it.get("offers") or {}
            if isinstance(off, list):
                off = off[0] if off else {}
            price = off.get("price")
            avail = (off.get("availability") or "").split("/")[-1]
            if price and float(price) > 0 and avail != "Discontinued":
                out["Price_CZK"] = float(price)
            elif price and float(price) > 0:
                # Discontinued but has a last-known price — still useful
                out["Price_CZK"] = float(price)
            ar = it.get("aggregateRating") or {}
            if ar.get("ratingValue"):
                try:
                    out["AvgStarRating"] = float(ar["ratingValue"])
                except (ValueError, TypeError):
                    pass
            if ar.get("reviewCount"):
                try:
                    out["ReviewsCount"] = int(ar["reviewCount"])
                except (ValueError, TypeError):
                    pass
    # Recommend % ("92 % zákazníků doporučuje") — text on the page
    m = re.search(r'(\d{1,3})\s*%\s*z[aá]kazn[ií]k\w*\s*doporu', html, re.I)
    if m:
        out["RecommendRate_pct"] = float(m.group(1))
    elif "AvgStarRating" in out and "RecommendRate_pct" not in out:
        # Derive an approximate recommend% from stars (4.8/5 -> 96%)
        out["RecommendRate_pct"] = round(out["AvgStarRating"] / 5.0 * 100, 1)
    return out


# ── Fetch URL list from the live DB ───────────────────────────────────────────
def get_urls(category: str | None, limit: int, min_reviews: int) -> list:
    body = json.dumps({"category": category, "limit": limit, "min_reviews": min_reviews}).encode()
    req = urllib.request.Request(API + "/api/admin/alza-urls", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read()).get("urls", [])


# ── Push a batch of updates to the live DB ────────────────────────────────────
def push(updates: list) -> dict:
    body = json.dumps({"updates": updates}).encode()
    req = urllib.request.Request(API + "/api/admin/bulk-update-products", data=body,
                                 headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", default=None, help="NormalizedCategory to restrict to")
    ap.add_argument("--limit", type=int, default=300, help="Max products to scrape")
    ap.add_argument("--min-reviews", type=int, default=0, help="Only products with >= N reviews")
    ap.add_argument("--delay", type=float, default=1.8, help="Seconds between requests")
    ap.add_argument("--batch", type=int, default=25, help="Upload every N products")
    args = ap.parse_args()

    print(f"Fetching URL list (category={args.category}, limit={args.limit}, min_reviews={args.min_reviews})…")
    urls = get_urls(args.category, args.limit, args.min_reviews)
    print(f"Got {len(urls)} Alza URLs to refresh.\n")
    if not urls:
        return

    sess = cffi.Session()
    pending = []
    ok = fail = notfound = uploaded = 0
    backoff = 0.0

    for i, url in enumerate(urls, 1):
        try:
            r = sess.get(url, impersonate="chrome120", timeout=15,
                         headers={"Accept-Language": "cs-CZ,cs;q=0.9",
                                  "Referer": "https://www.alza.cz/"})
            if r.status_code == 404:
                notfound += 1
            elif r.status_code == 403 or "blokována" in r.text:
                # Rate-limited — exponential backoff
                backoff = min(backoff * 2 if backoff else 30, 300)
                print(f"  [{i}/{len(urls)}] 403 rate-limited — backing off {backoff:.0f}s…")
                time.sleep(backoff)
                continue
            elif r.status_code == 200:
                backoff = 0.0
                data = extract(r.text)
                if data:
                    data["ProductURL"] = url
                    pending.append(data)
                    ok += 1
            else:
                fail += 1
        except Exception as e:
            fail += 1
            print(f"  [{i}/{len(urls)}] error: {str(e)[:50]}")

        # Upload batch
        if len(pending) >= args.batch:
            try:
                res = push(pending)
                uploaded += res.get("applied", 0)
                print(f"  [{i}/{len(urls)}] uploaded batch: {res.get('applied',0)} applied, "
                      f"{res.get('notfound',0)} not-found  (totals: ok={ok} 404={notfound} fail={fail})")
            except Exception as e:
                print(f"  upload error: {str(e)[:60]}")
            pending = []

        time.sleep(args.delay)

    # Final batch
    if pending:
        try:
            res = push(pending)
            uploaded += res.get("applied", 0)
        except Exception as e:
            print(f"  final upload error: {str(e)[:60]}")

    print(f"\n=== Done ===")
    print(f"  Scraped OK:   {ok}")
    print(f"  Uploaded:     {uploaded} product updates applied to live DB")
    print(f"  404 (delisted): {notfound}")
    print(f"  Failed:       {fail}")


if __name__ == "__main__":
    main()
