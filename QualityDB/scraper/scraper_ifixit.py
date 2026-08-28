"""
iFixit Repairability Score scraper.

Strategy: Use the iFixit guides API to fetch all teardown guides,
then scan each guide's last few steps for "Repairability Score: X out of 10"
text (the standard iFixit teardown format since ~2012).

API: GET /api/2.0/guides?filter=teardown&limit=50
     GET /api/2.0/guides/{guideid} (to get steps with text)

Reference: https://github.com/xiongchiamiov/ifixit-repairability-scores
Score format in guide steps: "Repairability Score: '''7 out of 10'''"
or: "Repairability: 7 out of 10"
"""

from __future__ import annotations

import hashlib
import re
import time

import requests

from scraper_competitors_config import REQUEST_DELAY, REQUEST_TIMEOUT, USER_AGENT, canonical_category
from scraper_competitors_db import count_records, get_checkpoint, init_table, save_checkpoint, upsert_record

SOURCE = "ifixit"
API_BASE = "https://www.ifixit.com/api/2.0"

# Score patterns in iFixit guide step text
SCORE_PATTERNS = [
    # Standard format: "Repairability Score: '''7 out of 10'''"
    re.compile(r"[Rr]epairability\s+[Ss]core\s*:\s*['\*]*\s*(\d+(?:\.\d+)?)\s+out\s+of\s+10", re.I),
    # Short format: "Repairability: 7 out of 10"
    re.compile(r"[Rr]epairability\s*:\s*(\d+(?:\.\d+)?)\s+out\s+of\s+10", re.I),
    # Numeric format: "Repairability Score: 7/10"
    re.compile(r"[Rr]epairability\s+[Ss]core\s*:\s*(\d+(?:\.\d+)?)\s*/\s*10", re.I),
    # Just score: "7 out of 10 repairability"
    re.compile(r"(\d+(?:\.\d+)?)\s+out\s+of\s+10\s+[Rr]epairability", re.I),
]

# Category mapping from iFixit device categories
CATEGORY_DEVICE_MAP = {
    "iPhone":           ("Smartphones", "Smartphony"),
    "iPad":             ("Tablets", "Tablety"),
    "MacBook":          ("Laptops", "Notebooky"),
    "Samsung":          ("Smartphones", "Smartphony"),
    "Google Pixel":     ("Smartphones", "Smartphony"),
    "Surface":          ("Laptops", "Notebooky"),
    "Laptop":           ("Laptops", "Notebooky"),
    "Notebook":         ("Laptops", "Notebooky"),
    "Headphones":       ("Headphones", "Sluchátka"),
    "AirPods":          ("Wireless Earbuds", "Sluchátka"),
    "Galaxy":           ("Smartphones", "Smartphony"),
    "Pixel":            ("Smartphones", "Smartphony"),
    "Nexus":            ("Smartphones", "Smartphony"),
    "Kindle":           ("E-readers", "E-readery"),
    "Apple Watch":      ("Smartwatches", "Chytré hodinky"),
    "Nintendo Switch":  ("Game Consoles", "Herní konzole"),
    "Steam Deck":       ("Game Consoles", "Herní konzole"),
    "PlayStation":      ("Game Consoles", "Herní konzole"),
    "Xbox":             ("Game Consoles", "Herní konzole"),
    "Washing Machine":  ("Washing Machines", "Pračky"),
    "Washer":           ("Washing Machines", "Pračky"),
    "Television":       ("Televisions", "Televize"),
}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.headers["Accept"] = "application/json"
    return s


def _make_product_id(guideid: int) -> str:
    return f"ifixit_guide_{guideid}"


def _extract_score_from_steps(steps: list) -> float | None:
    """Scan guide steps (last 3) for a repairability score."""
    for step in reversed(steps[-3:]):
        # Check all line texts in the step
        for line in step.get("lines", []):
            text = line.get("text", "")
            if not text:
                continue
            for pattern in SCORE_PATTERNS:
                m = pattern.search(text)
                if m:
                    try:
                        score = float(m.group(1))
                        if 0 <= score <= 10:
                            return score
                    except (ValueError, IndexError):
                        pass
        # Also check step title
        title = step.get("title", "")
        if title:
            for pattern in SCORE_PATTERNS:
                m = pattern.search(title)
                if m:
                    try:
                        return float(m.group(1))
                    except (ValueError, IndexError):
                        pass
    return None


def _guess_category(title: str, device_category: str) -> tuple[str, str]:
    """Returns (iFixit category, IKOR canonical) tuple."""
    for key, (cat, canon) in CATEGORY_DEVICE_MAP.items():
        if key.lower() in title.lower() or key.lower() in device_category.lower():
            return cat, canon
    return device_category, canonical_category(device_category.lower())


def _extract_brand(title: str) -> str:
    """Extract brand from teardown title like 'iPhone 16 Pro Teardown'."""
    # Known brands at start of title
    known_brands = [
        "Apple", "Samsung", "Google", "Microsoft", "Lenovo", "HP", "Dell",
        "ASUS", "Acer", "LG", "Sony", "Motorola", "OnePlus", "Xiaomi",
        "Fairphone", "Framework", "Huawei", "Nokia", "Amazon", "Nintendo",
        "Valve", "Meta", "Bose", "JBL", "Sennheiser",
    ]
    title_clean = title.replace("Teardown", "").strip()
    for brand in known_brands:
        if title_clean.startswith(brand):
            return brand
    # Check device family → brand
    family_brand = {
        "iPhone": "Apple", "iPad": "Apple", "MacBook": "Apple",
        "Mac Mini": "Apple", "Apple Watch": "Apple", "AirPods": "Apple",
        "Galaxy": "Samsung", "Pixel": "Google",
        "Surface": "Microsoft", "ThinkPad": "Lenovo",
    }
    for fam, brand in family_brand.items():
        if fam in title_clean:
            return brand
    # First word
    parts = title_clean.split()
    return parts[0].title() if parts else ""


def scrape() -> int:
    """
    Scrape iFixit teardown guides for repairability scores.
    Iterates through all teardowns, fetches each guide's steps, extracts score.
    Returns total rows inserted.
    """
    init_table()
    session = _session()
    checkpoint = get_checkpoint(SOURCE)
    total = 0

    # Get the list of all teardown guides
    # We store the list in checkpoint to avoid re-fetching
    if "teardown_index" not in checkpoint:
        print(f"\n[ifixit] Building teardown guide index...")
        guide_index = []
        limit = 50
        offset = 0
        max_guides = 50000  # safety cap

        while offset < max_guides:
            try:
                resp = session.get(
                    f"{API_BASE}/guides",
                    params={"filter": "teardown", "limit": limit, "offset": offset},
                    timeout=REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                guides = resp.json()
            except Exception as e:
                print(f"  [!] Index fetch error at offset {offset}: {e}")
                break

            if not guides:
                break

            for g in guides:
                guide_index.append({
                    "guideid": g.get("guideid"),
                    "title": g.get("title", ""),
                    "category": g.get("category", ""),
                    "url": g.get("url", ""),
                })

            print(f"  Indexed {len(guide_index)} teardowns (offset {offset})...")
            if len(guides) < limit:
                break
            offset += limit
            time.sleep(0.5)

        checkpoint["teardown_index"] = guide_index
        save_checkpoint(SOURCE, checkpoint)
        print(f"  Index complete: {len(guide_index)} teardown guides")
    else:
        guide_index = checkpoint["teardown_index"]
        print(f"\n[ifixit] Using cached index: {len(guide_index)} teardown guides")

    # Process each guide
    print(f"[ifixit] Scanning guides for repairability scores...")
    done_set = set(checkpoint.get("done_guides", []))

    for i, guide_info in enumerate(guide_index):
        guideid = guide_info.get("guideid")
        if not guideid:
            continue

        prod_id = _make_product_id(guideid)

        if guideid in done_set or checkpoint.get(prod_id) in ("done", "no_score"):
            continue

        # Fetch full guide with steps
        try:
            resp = session.get(
                f"{API_BASE}/guides/{guideid}",
                params={"excludePrerequisiteSteps": "false"},
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 404:
                checkpoint[prod_id] = "no_score"
                done_set.add(guideid)
                continue
            resp.raise_for_status()
            guide = resp.json()
        except Exception as e:
            print(f"  [!] Guide {guideid} fetch error: {e}")
            time.sleep(1)
            continue

        title = guide.get("title", "") or guide_info.get("title", "")
        steps = guide.get("steps", [])

        score = _extract_score_from_steps(steps) if steps else None

        if score is None:
            checkpoint[prod_id] = "no_score"
            done_set.add(guideid)
        else:
            device_cat = guide.get("category", "") or guide_info.get("category", "")
            category, canon = _guess_category(title, device_cat)
            brand = _extract_brand(title)

            upsert_record({
                "source":             SOURCE,
                "source_url":         guide_info.get("url") or f"https://www.ifixit.com/Teardown/{guideid}",
                "product_name":       title.replace("Teardown", "").strip(),
                "brand":              brand,
                "model":              title.replace("Teardown", "").strip(),
                "product_category":   category,
                "canonical_category": canon,
                "raw_score":          score,
                "raw_score_min":      1.0,
                "raw_score_max":      10.0,
                "raw_score_label":    f"{int(score)}/10",
                "score_normalized":   round(score * 10, 1),
                "sub_scores_json":    None,
                "meta_json":          {"guideid": guideid, "device_category": device_cat},
                "source_product_id":  prod_id,
            })

            checkpoint[prod_id] = "done"
            done_set.add(guideid)
            total += 1
            print(f"  [{i+1}] {title[:60]}: {int(score)}/10 ({category})")

        # Periodic checkpoint save
        if (i + 1) % 100 == 0:
            checkpoint["done_guides"] = list(done_set)
            save_checkpoint(SOURCE, checkpoint)

        time.sleep(REQUEST_DELAY)

    checkpoint["done_guides"] = list(done_set)
    save_checkpoint(SOURCE, checkpoint)
    print(f"\n[ifixit] Done — {total} rows total. DB now has {count_records(SOURCE)} {SOURCE} records.")
    return total


if __name__ == "__main__":
    scrape()
