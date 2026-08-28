#!/usr/bin/env python3
"""
scrape_alza_local.py — Refresh Alza product data from a residential IP (your
laptop) and push it to the LIVE QualityDB on fly.io.

Why: Alza blocks fly.io's datacenter IP (HTTP 403). Your home connection is
served normally, so we scrape here and upload via the API.

Data source: Alza's lightweight reviewStats JSON API
  https://webapi.alza.cz/api/catalog/v2/commodities/{ID}/reviewStats?country=CZ&pgrik=...&ucik=...
This returns, per product:
  - ratingAverage      -> AvgStarRating
  - recommendationRate -> RecommendRate_pct  (×100)
  - complaint.rate     -> ReturnRate_pct (reklamovanost!)  (×100)
  - ratingCount        -> ReviewsCount
The pgrik/ucik session tokens are harvested once from a product page and reused.

Optionally (--with-price) also fetches the full product page for the current
Price_CZK from JSON-LD (slower, more likely to hit rate-limits).

Alza rate-limits even residential IPs, so this runs slowly with exponential
backoff on 403.

Usage:
    python3 scrape_alza_local.py                          # 300 most-reviewed
    python3 scrape_alza_local.py --category Smartphones --limit 200
    python3 scrape_alza_local.py --with-price --limit 100
    python3 scrape_alza_local.py --min-reviews 20 --delay 1.5
"""
from __future__ import annotations

import re
import sys
import json
import time
import argparse
import os
import urllib.request

API = "https://database-of-high-quality-products.fly.dev"
SCRAPER_KEY = os.environ.get("SCRAPER_KEY", "")
ALZA_STATS = "https://webapi.alza.cz/api/catalog/v2/commodities/{pid}/reviewStats?country=CZ&pgrik={pgrik}&ucik={ucik}"

try:
    from curl_cffi import requests as cffi
except ImportError:
    print("Missing dependency. Run:  pip3 install curl_cffi")
    sys.exit(1)

import html as _html


# ── Token harvesting ──────────────────────────────────────────────────────────
def harvest_tokens(sess) -> tuple[str, str] | None:
    """Fetch one product page and extract reusable pgrik/ucik session tokens."""
    probe = "https://www.alza.cz/bosch-kgn392laf-d8133109.htm"
    for _ in range(6):
        r = sess.get(probe, impersonate="chrome120", timeout=15,
                     headers={"Accept-Language": "cs-CZ,cs;q=0.9", "Referer": "https://www.alza.cz/"})
        if r.status_code == 200 and "blokována" not in r.text:
            txt = _html.unescape(r.text)
            m = re.search(r'reviewStats\?country=CZ&pgrik=([^&"\s]+)&ucik=([^&"\s]+)', txt)
            if m:
                return m.group(1), m.group(2)
            # 200 but token markup missing (intermittent anti-bot page) — retry
        time.sleep(20)
    return None


# ── Product ID from URL ───────────────────────────────────────────────────────
def product_id(url: str) -> str | None:
    m = re.search(r'-d(\d+)\.htm', url) or re.search(r'[?&]dq=(\d+)', url)
    return m.group(1) if m else None


# ── reviewStats API call ──────────────────────────────────────────────────────
def get_stats(sess, pid: str, pgrik: str, ucik: str) -> dict | None:
    """Returns dict with AvgStarRating/RecommendRate_pct/ReturnRate_pct/ReviewsCount, or None."""
    url = ALZA_STATS.format(pid=pid, pgrik=pgrik, ucik=ucik)
    r = sess.get(url, impersonate="chrome120", timeout=15,
                 headers={"Accept": "application/json", "Referer": "https://www.alza.cz/",
                          "Origin": "https://www.alza.cz"})
    if r.status_code != 200:
        return {"_status": r.status_code}
    try:
        d = r.json()
    except Exception:
        return None
    out = {}
    if d.get("ratingAverage") is not None:
        out["AvgStarRating"] = round(float(d["ratingAverage"]), 2)
    if d.get("recommendationRate") is not None:
        out["RecommendRate_pct"] = round(float(d["recommendationRate"]) * 100, 1)
    if d.get("ratingCount") is not None:
        out["ReviewsCount"] = int(d["ratingCount"])
    comp = d.get("complaint") or {}
    if comp.get("rate") is not None:
        out["ReturnRate_pct"] = round(float(comp["rate"]) * 100, 2)
    return out


# ── Optional: current price from product page JSON-LD ─────────────────────────
def get_price(sess, url: str) -> float | None:
    r = sess.get(url, impersonate="chrome120", timeout=15,
                 headers={"Accept-Language": "cs-CZ,cs;q=0.9", "Referer": "https://www.alza.cz/"})
    if r.status_code != 200 or "blokována" in r.text:
        return None
    for block in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', r.text, re.S | re.I):
        try:
            d = json.loads(block)
        except Exception:
            continue
        for it in (d if isinstance(d, list) else [d]):
            if isinstance(it, dict) and it.get("@type") == "Product":
                off = it.get("offers") or {}
                if isinstance(off, list):
                    off = off[0] if off else {}
                p = off.get("price")
                if p and float(p) > 0:
                    return float(p)
    return None


# ── DB plumbing ───────────────────────────────────────────────────────────────
def get_urls(category, limit, min_reviews):
    body = json.dumps({"category": category, "limit": limit, "min_reviews": min_reviews}).encode()
    req = urllib.request.Request(API + "/api/admin/alza-urls", data=body,
                                 headers={"Content-Type": "application/json",
                                          "X-Scraper-Key": SCRAPER_KEY}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read()).get("urls", [])


def push(updates):
    body = json.dumps({"updates": updates}).encode()
    req = urllib.request.Request(API + "/api/admin/bulk-update-products", data=body,
                                 headers={"Content-Type": "application/json",
                                          "X-Scraper-Key": SCRAPER_KEY}, method="POST")
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", default=None)
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--min-reviews", type=int, default=0)
    ap.add_argument("--delay", type=float, default=1.2)
    ap.add_argument("--batch", type=int, default=30)
    ap.add_argument("--with-price", action="store_true",
                    help="Also fetch the full product page for current price (slower)")
    args = ap.parse_args()

    if not SCRAPER_KEY:
        print("SCRAPER_KEY is required. Export the same secret configured on Fly before running.")
        sys.exit(2)

    sess = cffi.Session()
    print("Harvesting Alza session tokens…")
    tok = harvest_tokens(sess)
    if not tok:
        print("Could not harvest tokens (Alza blocking). Try again in a few minutes.")
        return
    pgrik, ucik = tok
    print(f"  tokens: pgrik={pgrik} ucik={ucik}")

    print(f"Fetching URL list (category={args.category}, limit={args.limit}, min_reviews={args.min_reviews})…")
    urls = get_urls(args.category, args.limit, args.min_reviews)
    # Deduplicate by product id (many URLs are colour variants of the same commodity)
    seen = {}
    for u in urls:
        pid = product_id(u)
        if pid and pid not in seen:
            seen[pid] = u
    items = list(seen.items())
    print(f"Got {len(urls)} URLs → {len(items)} unique products.\n")
    if not items:
        return

    pending = []
    ok = fail = uploaded = with_return = 0
    backoff = 0.0

    for i, (pid, url) in enumerate(items, 1):
        stats = get_stats(sess, pid, pgrik, ucik)
        if stats and stats.get("_status") in (403,):
            backoff = min(backoff * 2 if backoff else 30, 300)
            print(f"  [{i}/{len(items)}] 403 — backing off {backoff:.0f}s")
            time.sleep(backoff)
            # Re-harvest tokens in case they expired
            t2 = harvest_tokens(sess)
            if t2:
                pgrik, ucik = t2
            continue
        backoff = 0.0
        if not stats or not any(k in stats for k in ("AvgStarRating", "RecommendRate_pct", "ReturnRate_pct")):
            fail += 1
        else:
            rec = {"ProductURL": url, **{k: v for k, v in stats.items() if not k.startswith("_")}}
            if args.with_price:
                pr = get_price(sess, url)
                if pr:
                    rec["Price_CZK"] = pr
                time.sleep(args.delay)
            if "ReturnRate_pct" in rec:
                with_return += 1
            pending.append(rec)
            ok += 1

        if len(pending) >= args.batch:
            try:
                res = push(pending)
                uploaded += res.get("applied", 0)
                print(f"  [{i}/{len(items)}] uploaded {res.get('applied',0)} "
                      f"(ok={ok} fail={fail} w/return={with_return})")
            except Exception as e:
                print(f"  upload error: {str(e)[:60]}")
            pending = []

        time.sleep(args.delay)

    if pending:
        try:
            res = push(pending)
            uploaded += res.get("applied", 0)
        except Exception as e:
            print(f"  final upload error: {str(e)[:60]}")

    print(f"\n=== Done ===")
    print(f"  Scraped OK:        {ok}")
    print(f"  With return rate:  {with_return}")
    print(f"  Uploaded:          {uploaded} updates to live DB")
    print(f"  Failed/no data:    {fail}")


if __name__ == "__main__":
    main()
