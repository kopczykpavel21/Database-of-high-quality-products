#!/usr/bin/env python3
"""
debug_mediamarkt.py
───────────────────
Run this to see exactly what MediaMarkt returns so we can fix the parser.
Usage:  python3 debug_mediamarkt.py
"""

import sys, json, re
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.mediamarkt.de"
TEST_URL = BASE_URL + "/de/search.html?query=smartphone&sortBy=topRated&pageSize=24"

s = cffi_requests.Session()
s.headers.update({
    "Accept":          "text/html,application/xhtml+xml,*/*;q=0.9",
    "Accept-Language": "de-DE,de;q=0.9",
    "Referer":         BASE_URL + "/",
})

print(f"Fetching: {TEST_URL}\n")
resp = s.get(TEST_URL, impersonate="chrome131", timeout=30)
print(f"Status      : {resp.status_code}")
print(f"Content-Type: {resp.headers.get('content-type','')}")
print(f"Body length : {len(resp.text)} chars\n")

# Check 1: Is it JSON?
try:
    data = resp.json()
    print("=== RESPONSE IS JSON ===")
    print("Top-level keys:", list(data.keys())[:10])
    sys.exit(0)
except Exception:
    print("Not JSON — checking HTML...\n")

soup = BeautifulSoup(resp.text, "html.parser")

# Check 2: __NEXT_DATA__
nd = soup.find("script", id="__NEXT_DATA__")
if nd:
    print("=== __NEXT_DATA__ FOUND ===")
    raw = nd.string or ""
    print(f"Size: {len(raw)} chars")
    try:
        data = json.loads(raw)
        def show_keys(d, depth=0, max_depth=4):
            if depth > max_depth: return
            if isinstance(d, dict):
                for k, v in list(d.items())[:8]:
                    print("  " * depth + f"[{k}]  ({type(v).__name__})")
                    show_keys(v, depth+1, max_depth)
            elif isinstance(d, list) and d:
                print("  " * depth + f"  list[{len(d)}], first item type: {type(d[0]).__name__}")
                if isinstance(d[0], dict):
                    show_keys(d[0], depth+1, max_depth)
        show_keys(data)
    except Exception as e:
        print(f"Could not parse __NEXT_DATA__: {e}")
else:
    print("No __NEXT_DATA__ found.")

# Check 3: JSON-LD
ld_tags = soup.find_all("script", type="application/ld+json")
print(f"\n=== JSON-LD tags found: {len(ld_tags)} ===")
for i, tag in enumerate(ld_tags[:3]):
    try:
        d = json.loads(tag.string or "")
        t = d.get("@type") if isinstance(d, dict) else type(d).__name__
        print(f"  [{i}] @type={t}, keys={list(d.keys())[:6] if isinstance(d, dict) else '—'}")
    except Exception:
        print(f"  [{i}] could not parse")

# Check 4: Any product-like data anywhere
print("\n=== Inline JSON blobs containing 'product' ===")
scripts = soup.find_all("script")
for tag in scripts:
    txt = tag.string or ""
    if "product" in txt.lower() and len(txt) > 200:
        # Try to find JSON objects
        for m in re.finditer(r'\{[^{}]{50,}\}', txt[:3000]):
            snippet = m.group(0)
            if any(k in snippet.lower() for k in ["name", "price", "rating"]):
                print("  Found blob:", snippet[:200])
                break

# Check 5: CAPTCHA / bot block?
body_lower = resp.text.lower()
if "captcha" in body_lower or "robot" in body_lower or "blocked" in body_lower:
    print("\n⚠️  POSSIBLE BOT BLOCK / CAPTCHA DETECTED in response!")
else:
    print("\nNo obvious bot block detected.")

print("\n=== First 500 chars of HTML body ===")
print(resp.text[:500])
