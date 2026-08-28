#!/usr/bin/env python3
"""
Institut Kvality — Contributor Scraper
=======================================
Run this script on your own laptop to contribute product quality data to the
shared Institut Kvality database.  No data is collected without your consent;
you choose which source to scrape and nothing is submitted until you confirm.

Usage:
    python3 contrib_scraper.py [--server URL] [--scanner-token TOKEN]

Requirements:
    pip install requests beautifulsoup4

The script will:
  1. Ask you to log in (or register) with your Institut Kvality account.
  2. Show the sources available for your country.
  3. Scrape product data from your chosen source (runs locally on your machine).
  4. Show you a summary and ask for confirmation before submitting.
  5. Upload the data to the central server, where it is staged.
     Products are only added to the public database once at least 3 distinct
     contributors submit consistent data for the same product.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from typing import Optional

# ── Config ─────────────────────────────────────────────────────────────────────
DEFAULT_SERVER = "https://database-of-high-quality-products.fly.dev"


# ── HTTP helpers ───────────────────────────────────────────────────────────────

def _post(server: str, path: str, payload: dict, token: Optional[str] = None,
          token_scheme: str = "Bearer") -> dict:
    url  = server.rstrip("/") + path
    data = json.dumps(payload).encode()
    req  = urllib.request.Request(url, data=data,
                                  headers={"Content-Type": "application/json"})
    if token:
        req.add_header("Authorization", f"{token_scheme} {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return json.loads(body)
        except Exception:
            return {"ok": False, "error": f"HTTP {e.code}: {body.decode(errors='replace')}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _get(server: str, path: str, token: Optional[str] = None,
         token_scheme: str = "Bearer") -> dict:
    url = server.rstrip("/") + path
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"{token_scheme} {token}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return json.loads(body)
        except Exception:
            return {"ok": False, "error": f"HTTP {e.code}: {body.decode(errors='replace')}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Auth flow ──────────────────────────────────────────────────────────────────

def login_or_register(server: str) -> tuple[str, dict]:
    """Interactive login/register.  Returns (token, user_dict)."""
    print("\n─── Institut Kvality — Contributor Login ───\n")
    choice = input("Do you have an account? [y/N] ").strip().lower()
    if choice == "y":
        return _do_login(server)
    else:
        return _do_register(server)


def _do_login(server: str) -> tuple[str, dict]:
    email    = input("Email: ").strip()
    password = _read_password("Password: ")
    result   = _post(server, "/api/login", {"email": email, "password": password})
    if not result.get("ok"):
        print(f"\n✗ Login failed: {result.get('error')}")
        sys.exit(1)
    print(f"\n✓ Logged in as {result['user']['email']}")
    return result["token"], result["user"]


def _do_register(server: str) -> tuple[str, dict]:
    print("\nCreate your Institut Kvality research account.\n")

    email   = input("Email: ").strip()
    pw      = _read_password("Password (min 8 chars): ")
    pw2     = _read_password("Confirm password: ")
    if pw != pw2:
        print("✗ Passwords do not match.")
        sys.exit(1)

    print("\nSelect your country (determines which sources you can scrape):")
    COUNTRIES = ["CZ", "SK", "DE", "AT", "CH", "FR", "PL", "NL", "SE", "DK", "US", "GB"]
    for i, c in enumerate(COUNTRIES, 1):
        print(f"  {i}. {c}")
    idx = int(input("Country number: ").strip()) - 1
    if idx < 0 or idx >= len(COUNTRIES):
        print("✗ Invalid selection.")
        sys.exit(1)
    country = COUNTRIES[idx]

    print("\n─── Research Questions ─────────────────────────────────────────────────")
    print("These short answers help us understand the research community")
    print("and guide the development of Institut Kvality.\n")
    q1 = input("1. What is your primary reason for researching product quality?\n   → ").strip()
    q2 = input("\n2. Which product categories do you think are most affected by planned obsolescence?\n   → ").strip()
    q3 = input("\n3. How do you currently discover high-quality products?\n   → ").strip()

    result = _post(server, "/api/register", {
        "email": email, "password": pw, "country": country,
        "q1": q1, "q2": q2, "q3": q3,
    })
    if not result.get("ok"):
        print(f"\n✗ Registration failed: {result.get('error')}")
        sys.exit(1)
    print(f"\n✓ Account created!  Welcome, {result['user']['email']}")
    return result["token"], result["user"]


def _read_password(prompt: str) -> str:
    try:
        import getpass
        return getpass.getpass(prompt)
    except Exception:
        return input(prompt)


# ── Source selection ───────────────────────────────────────────────────────────

def choose_source(server: str, token: str, user: dict, token_scheme: str = "Bearer") -> dict:
    """Fetch available sources for the user's country and let them pick one."""
    path = "/api/scanner-context" if token_scheme == "Scanner" else "/api/my-sources"
    result = _get(server, path, token=token, token_scheme=token_scheme)
    if not result.get("ok"):
        print(f"✗ Could not fetch sources: {result.get('error')}")
        sys.exit(1)

    sources = result["sources"]
    country = result["country"]
    print(f"\n─── Sources available for {country} ────────────────────────────────────")
    for i, s in enumerate(sources, 1):
        print(f"  {i}. {s['name']}  (key: {s['key']})")
    print()
    idx = int(input("Select source number: ").strip()) - 1
    if idx < 0 or idx >= len(sources):
        print("✗ Invalid selection.")
        sys.exit(1)
    return sources[idx]


# ── Scrapers (one per source key) ─────────────────────────────────────────────
# Each scraper returns a list of product dicts.  They use only stdlib so no
# extra dependencies are needed beyond `requests` + `beautifulsoup4` for the
# more complex ones.

def _scrape_heureka(base_url: str, country_tld: str = "cz",
                    max_products: int = 200) -> list[dict]:
    """Generic Heureka scraper (works for heureka.cz and heureka.sk)."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("  BeautifulSoup4 not installed — run: pip install beautifulsoup4")
        return []

    import urllib.request as _req
    products = []
    page = 1
    while len(products) < max_products:
        url = f"{base_url}?p={page}"
        try:
            req = _req.Request(url, headers={"User-Agent": "Mozilla/5.0 (QualityDB Contrib)"})
            with _req.urlopen(req, timeout=15) as r:
                html = r.read()
        except Exception as e:
            print(f"  Fetch error at page {page}: {e}")
            break

        soup = BeautifulSoup(html, "html.parser")
        items = soup.select(".c-product-item, .product-item")
        if not items:
            break

        for item in items:
            name_el = item.select_one(".c-product-item__title, .product-title a")
            url_el  = item.select_one("a[href]")
            stars_el = item.select_one(".star-rating, .c-rating__label")
            rec_el   = item.select_one(".c-product-item__opinions-count, .opinions-count")

            name     = name_el.get_text(strip=True)   if name_el  else None
            prod_url = url_el["href"]                  if url_el   else None
            if not name or not prod_url:
                continue
            if not prod_url.startswith("http"):
                prod_url = f"https://www.heureka.{country_tld}{prod_url}"

            products.append({
                "Name":        name,
                "ProductURL":  prod_url,
                "source":      f"heureka_{country_tld}" if country_tld != "cz" else "heureka",
                "country":     country_tld.upper(),
            })
            if len(products) >= max_products:
                break

        page += 1
        time.sleep(0.5)

    return products


def _scrape_alza(max_products: int = 200) -> list[dict]:
    """Scrape Alza.cz top-rated products via their public listing."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("  BeautifulSoup4 not installed — run: pip install beautifulsoup4")
        return []

    import urllib.request as _req
    products = []
    urls_to_try = [
        "https://www.alza.cz/nejlepsi-hodnoceni/",
        "https://www.alza.cz/mobilni-telefony/",
        "https://www.alza.cz/notebooky/",
    ]
    for listing_url in urls_to_try:
        if len(products) >= max_products:
            break
        try:
            req = _req.Request(listing_url,
                               headers={"User-Agent": "Mozilla/5.0 (QualityDB Contrib)"})
            with _req.urlopen(req, timeout=15) as r:
                html = r.read()
        except Exception as e:
            print(f"  Fetch error for {listing_url}: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        for item in soup.select(".browsingitem"):
            name_el  = item.select_one(".top .name")
            url_el   = item.select_one("a[href]")
            price_el = item.select_one(".price-box__price")
            star_el  = item.select_one(".stars-rating")

            name = name_el.get_text(strip=True)   if name_el  else None
            href = url_el["href"]                  if url_el   else None
            if not name or not href:
                continue
            if not href.startswith("http"):
                href = "https://www.alza.cz" + href

            price_czk = None
            if price_el:
                raw = price_el.get_text(strip=True).replace("\xa0", "").replace(" ", "")
                try:
                    price_czk = float("".join(c for c in raw if c.isdigit() or c == "."))
                except Exception:
                    pass

            products.append({
                "Name":       name,
                "ProductURL": href,
                "Price_CZK":  price_czk,
                "source":     "alza",
                "country":    "CZ",
                "currency":   "CZK",
            })
            if len(products) >= max_products:
                break
        time.sleep(0.5)
    return products


def _scrape_ceneo(max_products: int = 200) -> list[dict]:
    """Scrape Ceneo.pl via their category listings."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("  BeautifulSoup4 not installed — run: pip install beautifulsoup4")
        return []

    import urllib.request as _req
    products = []
    urls = [
        "https://www.ceneo.pl/Telefony_i_smartfony",
        "https://www.ceneo.pl/Laptopy_i_notebooki",
    ]
    for listing in urls:
        if len(products) >= max_products:
            break
        try:
            req = _req.Request(listing,
                               headers={"User-Agent": "Mozilla/5.0 (QualityDB Contrib)"})
            with _req.urlopen(req, timeout=15) as r:
                html = r.read()
        except Exception as e:
            print(f"  Fetch error: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        for item in soup.select(".cat-prod-row, .product-item"):
            name_el  = item.select_one(".cat-prod-row__name, .product-name")
            link_el  = item.select_one("a[href]")
            score_el = item.select_one(".product-score, .score-count")

            name = name_el.get_text(strip=True) if name_el else None
            href = link_el["href"]              if link_el else None
            if not name or not href:
                continue
            if not href.startswith("http"):
                href = "https://www.ceneo.pl" + href

            rec_pct = None
            if score_el:
                raw = score_el.get_text(strip=True).replace("%", "").replace(",", ".")
                try:
                    rec_pct = float(raw)
                except Exception:
                    pass

            products.append({
                "Name":              name,
                "ProductURL":        href,
                "RecommendRate_pct": rec_pct,
                "source":            "ceneo",
                "country":           "PL",
                "currency":          "PLN",
            })
            if len(products) >= max_products:
                break
        time.sleep(0.5)
    return products


# ── Dispatch table ─────────────────────────────────────────────────────────────
_SCRAPERS = {
    "heureka":    lambda: _scrape_heureka("https://www.heureka.cz/mobilni-telefony/", "cz"),
    "heureka_sk": lambda: _scrape_heureka("https://www.heureka.sk/mobilne-telefony/",  "sk"),
    "alza":       _scrape_alza,
    "ceneo":      _scrape_ceneo,
}


def run_scraper(source_key: str) -> list[dict]:
    """Run the local scraper for `source_key` and return product list."""
    fn = _SCRAPERS.get(source_key)
    if fn is None:
        print(f"\n  No local scraper implemented yet for '{source_key}'.")
        print("  You can contribute by adding one in contrib_scraper.py → _SCRAPERS.")
        return []
    print(f"\n  Scraping {source_key} locally — this may take a minute…")
    products = fn()
    return products


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Institut Kvality contributor scraper")
    parser.add_argument("--server", default=DEFAULT_SERVER,
                        help=f"Server URL (default: {DEFAULT_SERVER})")
    parser.add_argument("--token",  default=None,
                        help="Skip login — use a saved API token directly")
    parser.add_argument("--scanner-token", default=None,
                        help="Use a contribution-only IKOR scanner token")
    args = parser.parse_args()

    if args.token and args.scanner_token:
        parser.error("use either --token or --scanner-token, not both")

    server = args.server.rstrip("/")
    print(f"  Server: {server}")

    # ── 1. Auth ──────────────────────────────────────────────────────────────
    token_scheme = "Scanner" if args.scanner_token else "Bearer"
    if args.scanner_token:
        token = args.scanner_token
        result = _get(server, "/api/scanner-context", token=token, token_scheme=token_scheme)
        if not result.get("ok"):
            print(f"✗ Scanner token invalid: {result.get('error')}")
            sys.exit(1)
        user = {"country": result["country"]}
        print(f"✓ Using contribution-only scanner token for {result['country']}")
    elif args.token:
        account_token = args.token
        result = _get(server, "/api/me", token=account_token)
        if not result.get("ok"):
            print(f"✗ Token invalid: {result.get('error')}")
            sys.exit(1)
        user = result["user"]
        print(f"✓ Using saved token — logged in as {user['email']}")
    else:
        account_token, user = login_or_register(server)

    if token_scheme == "Bearer":
        scanner = _post(server, "/api/scanner-token", {}, token=account_token)
        if not scanner.get("ok"):
            print(f"✗ Could not create a contribution token: {scanner.get('error')}")
            sys.exit(1)
        token = scanner["scanner_token"]
        token_scheme = "Scanner"

    # ── 2. Pick source ────────────────────────────────────────────────────────
    source = choose_source(server, token, user, token_scheme=token_scheme)
    print(f"\n  Selected: {source['name']} ({source['key']})")

    # ── 3. Scrape locally ─────────────────────────────────────────────────────
    products = run_scraper(source["key"])
    if not products:
        print("\n  No products scraped — nothing to submit.")
        sys.exit(0)

    print(f"\n  Scraped {len(products)} products.")
    print("  Sample (first 3):")
    for p in products[:3]:
        print(f"    • {p.get('Name', '?')} — {p.get('ProductURL', '?')[:80]}")

    # ── 4. Confirm before upload ──────────────────────────────────────────────
    print()
    confirm = input(f"Submit {len(products)} products to Institut Kvality? [y/N] ").strip().lower()
    if confirm != "y":
        print("  Cancelled — nothing was submitted.")
        sys.exit(0)

    # ── 5. Upload in batches of 200 ───────────────────────────────────────────
    BATCH = 200
    total_queued = 0
    total_dup    = 0
    for i in range(0, len(products), BATCH):
        batch  = products[i:i + BATCH]
        result = _post(server, "/api/contribute",
                       {"source": source["key"], "products": batch}, token=token,
                       token_scheme=token_scheme)
        if not result.get("ok"):
            print(f"  ✗ Upload error: {result.get('error')}")
            sys.exit(1)
        total_queued += result.get("queued", 0)
        total_dup    += result.get("duplicate", 0)
        print(f"  Uploaded batch {i // BATCH + 1}: "
              f"+{result.get('queued', 0)} new, {result.get('duplicate', 0)} duplicates")
        time.sleep(0.2)

    # ── 6. Summary ────────────────────────────────────────────────────────────
    print(f"\n✓ Done!  {total_queued} new products staged, {total_dup} already in queue.")
    print("  Products are published once at least 3 contributors submit the same URL")
    print("  with consistent quality data.  Thank you for contributing!\n")

    # Show updated leaderboard stats
    stats = _get(server, "/api/contrib-stats")
    if "total_contributors" in stats:
        print(f"  Global stats: {stats['total_contributors']} contributors, "
              f"{stats['total_staged']} staged, {stats['total_merged']} merged.")


if __name__ == "__main__":
    main()
