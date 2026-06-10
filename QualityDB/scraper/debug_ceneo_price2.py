"""
Run this on your Mac to find the correct selectors on Ceneo listing pages.
Usage: python3 scraper/debug_ceneo_price2.py
"""
import time, re
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

TEST_URL = "https://www.ceneo.pl/Smartfony"

with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)
    context = browser.new_context(locale="pl-PL")
    page = context.new_page()
    page.goto(TEST_URL, wait_until="load", timeout=45000)
    time.sleep(5)

    html = page.content()
    soup = BeautifulSoup(html, "html.parser")

    # ── 1. Find all elements whose class contains product-like keywords ──
    print("=== Elements with 'prod', 'item', 'card', 'offer' in class ===")
    seen = set()
    for el in soup.find_all(True):
        cls = " ".join(el.get("class", []))
        if any(x in cls.lower() for x in ["prod", "item", "card", "offer", "listing"]):
            sig = f"{el.name}::{cls[:80]}"
            if sig not in seen:
                seen.add(sig)
                print(f"  <{el.name} class='{cls[:80]}'>")

    # ── 2. Look for price-related elements anywhere on the page ──
    print("\n=== Elements containing 'zł' or price-like text ===")
    seen2 = set()
    for el in soup.find_all(True):
        cls = " ".join(el.get("class", []))
        text = el.get_text(" ", strip=True)
        if re.search(r"\d[\d\s]{1,6}[,\.]\d{2}\s*zł", text) and len(text) < 40:
            sig = f"{el.name}::{cls[:60]}"
            if sig not in seen2:
                seen2.add(sig)
                print(f"  <{el.name} class='{cls[:60]}'> → '{text[:60]}'")

    # ── 3. Dump first 8000 chars of body for manual inspection ──
    print("\n=== Raw body HTML (first 8000 chars) ===")
    body = soup.find("body")
    print(str(body)[:8000] if body else html[:8000])

    browser.close()
