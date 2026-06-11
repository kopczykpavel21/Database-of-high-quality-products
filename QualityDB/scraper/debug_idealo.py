#!/usr/bin/env python3
"""
debug_idealo.py — run this to diagnose what Idealo actually returns.
Usage: python3 debug_idealo.py
"""
import sys
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup

BASE = "https://www.idealo.de"

# Test several URL formats — we'll see which one works
TEST_URLS = [
    BASE + "/preisvergleich/ProductCategory/3513I16-705.html",   # smartphones v1
    BASE + "/preisvergleich/SubProductCategory/16516.html",       # smartphones v2
    BASE + "/preisvergleich/Typ/16516-100.html",                  # smartphones v3
    BASE + "/preisvergleich/ProductCategory/703.html",            # laptops
    BASE + "/",                                                    # homepage
]

s = cffi_requests.Session()
s.headers.update({
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "de-DE,de;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
})

print("=== Testing homepage first (warm-up) ===")
r = s.get(BASE + "/", impersonate="chrome131", timeout=20)
print(f"  Homepage: {r.status_code}  ({len(r.text)} chars)\n")

import time; time.sleep(2)

for url in TEST_URLS[:-1]:
    print(f"Testing: {url}")
    try:
        r = s.get(url, impersonate="chrome131", timeout=20)
        print(f"  Status : {r.status_code}")
        print(f"  Content-Type: {r.headers.get('content-type','')}")
        print(f"  Body length : {len(r.text)} chars")
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, "html.parser")
            print(f"  Title  : {soup.title.string if soup.title else 'none'}")
            # Count product-like elements
            cards = (soup.select("div.sr-resultItem") or
                     soup.select("article[class*='productCard']") or
                     soup.select("[data-testid*='product']"))
            print(f"  Product cards found: {len(cards)}")
            # Check for any JSON with product data
            scripts = soup.find_all("script")
            for sc in scripts:
                t = sc.string or ""
                if "\"name\"" in t and "\"price\"" in t and len(t) > 500:
                    print(f"  Found script with product-like JSON ({len(t)} chars)")
                    print(f"  First 200 chars: {t[:200]}")
                    break
        elif r.status_code in (301, 302, 303, 307, 308):
            print(f"  Redirect to: {r.headers.get('location','?')}")
        print()
    except Exception as e:
        print(f"  ERROR: {e}\n")
    time.sleep(3)
