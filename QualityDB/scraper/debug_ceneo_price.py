"""
Debug Ceneo HTML structure — identifies current card + price selectors.
Run from QualityDB dir:  python3 scraper/debug_ceneo_price.py
"""
import re, sys
sys.path.insert(0, ".")
from curl_cffi.requests import Session
from bs4 import BeautifulSoup

URL = "https://www.ceneo.pl/Smartfony"
HEADERS = {"Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8"}

print(f"Fetching {URL} …")
s = Session(impersonate="chrome120")
r = s.get(URL, headers=HEADERS, timeout=20)
soup = BeautifulSoup(r.text, "html.parser")

print(f"Page title: {soup.title.string if soup.title else '(none)'}")
print(f"HTTP status: {r.status_code}\n")

# ── 1. Try known card selectors ───────────────────────────────────────────────
card_selectors = [
    ".cat-prod-row[data-productid]",
    "[data-productid]",
    ".cat-prod-row",
    ".js_category-list-item",
    "article[data-pid]",
    "li[data-pid]",
    ".product-item",
    ".offer-item",
]
found_cards = []
for sel in card_selectors:
    els = soup.select(sel)
    print(f"  Card selector '{sel}': {len(els)} found")
    if els and not found_cards:
        found_cards = els

print()

if not found_cards:
    # Dump a snippet of the raw HTML to help identify structure
    print("No cards found with known selectors. Dumping first 3000 chars of <body>:\n")
    body = soup.find("body")
    print(str(body)[:3000] if body else r.text[:3000])
    sys.exit(0)

print(f"Using {len(found_cards)} cards from first matching selector.\n")

# ── 2. For first 3 cards, find name + any price-like element ─────────────────
for i, card in enumerate(found_cards[:3], 1):
    # Name
    for ns in ["strong.cat-prod-row__name", ".product-name", "h2", "h3", ".name", "strong"]:
        el = card.select_one(ns)
        if el:
            name = el.get_text(strip=True)[:50]
            break
    else:
        name = card.get_text(strip=True)[:50]

    print(f"  Card {i}: {name}")

    # All class names in this card that contain "price"
    price_candidates = []
    for el in card.find_all(True):
        classes = " ".join(el.get("class", []))
        if "price" in classes.lower():
            txt = el.get_text(strip=True)
            if txt and re.search(r"\d", txt):
                price_candidates.append((classes.strip(), txt[:40]))

    if price_candidates:
        print(f"    Price candidates:")
        seen = set()
        for cls, txt in price_candidates:
            if cls not in seen:
                print(f"      class='{cls}'  →  '{txt}'")
                seen.add(cls)
    else:
        print("    No price-like elements found in this card.")

    # Also show data attributes on the card element itself
    attrs = {k: v for k, v in card.attrs.items() if k != "class"}
    if attrs:
        print(f"    Card attrs: {attrs}")
    print()

print("Paste this output back so the scraper selectors can be updated.")
