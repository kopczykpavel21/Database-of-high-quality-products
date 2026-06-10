"""
Cross-Market Product Matcher for QualityDB
==========================================
Identifies the same physical product appearing in multiple markets by extracting
model-number tokens from product names and clustering on shared tokens.

Usage (standalone):
    python3 scraper/cross_market.py                 # prints summary table
    python3 scraper/cross_market.py --json          # JSON output
    python3 scraper/cross_market.py --min-markets 3 # only 3+ market matches
    python3 scraper/cross_market.py --include-amazon # include amazon_us

Public API (imported by server.py):
    from scraper.cross_market import find_cross_market_matches
    groups = find_cross_market_matches(conn, min_markets=2)
"""

import re
import os
import sys
import json
import sqlite3
import argparse
from collections import defaultdict

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "products.db")

# ── Model token extraction ────────────────────────────────────────────────────

_STOPWORDS = {
    # Connectivity
    "USB", "USB-C", "USB3", "USB2", "USB-A", "USB-B", "HDMI", "WIFI", "WI-FI",
    "WLAN", "BLE", "NFC", "GPS", "LTE", "5G", "4G", "TYPE-C", "TYPE-A", "TYPE-B",
    # Display tech
    "OLED", "QLED", "AMOLED", "UHD", "FHD", "RGB", "LCD", "LED", "TFT", "IPS",
    "QXGA", "WQHD", "SRGB",
    # Storage interfaces
    "DDR4", "DDR5", "NVME", "SATA", "PCIE", "M2",
    # Apple chips (too common to be unique identifiers)
    "M1", "M2", "M3", "M4",
    # Generic generation/version codes
    "A1", "A2", "A3", "A4", "A5",
    "B1", "B2", "B3",
    "C1", "C2", "C3",
    "V1", "V2", "V3", "V4", "V5",
    "G1", "G2", "G3", "G4", "G5", "G6", "G7", "G8", "G9", "G10",
    "GEN10", "GEN11", "GEN12", "GEN13", "GEN14",
    # Misc abbreviations
    "MHZ", "GHZ", "FLAC", "HEVC", "HEIF", "DLNA", "DLCI",
}


def extract_model_tokens(text: str) -> list[str]:
    """
    Extract model-number tokens from a product name.

    A model token:
    - Mixes BOTH letters and digits  (rules out pure words and pure numbers)
    - Is at least 3 characters
    - Is not a generic technical abbreviation (USB, HDMI, DDR4 …)
    - Is not a capacity/watt/version spec (256GB, 65W, V2 …)

    Returns a deduplicated list sorted by token length descending
    (longer = more specific, prefer as primary key).

    Examples:
        "Sony WH-1000XM5"            → ['WH-1000XM5', '1000XM5']
        "Bosch WUU28T18FR"           → ['WUU28T18FR']
        "Brother MFC-L3770CDW"       → ['MFC-L3770CDW']
        "Samsung Galaxy S24 Ultra"   → ['S24']
        "Hisense 55U6KQ"             → ['55U6KQ']
    """
    if not text:
        return []
    t = text.upper()
    raw: set[str] = set()

    # Pattern A: letter-led alphanumeric codes, min 4 chars total
    # Captures: WH-1000XM5, MFC-L3770CDW, Q2790PQE, WUU28T18FR, SX610
    for tok in re.findall(r"\b[A-Z][A-Z0-9\-\.]{2,}[A-Z0-9]\b", t):
        if re.search(r"[A-Z]", tok) and re.search(r"[0-9]", tok):
            raw.add(tok)

    # Pattern B: digit-led codes that include letters, min 5 chars
    # Captures: 1000XM5 (sub-token of WH-1000XM5), 55U6KQ
    for tok in re.findall(r"\b\d[A-Z0-9]{4,}\b", t):
        if re.search(r"[A-Z]", tok):
            raw.add(tok)

    # Pattern C: short "suffix" model codes — single letter + 2 digits
    # e.g. S24, T20, X1, T2 — useful when paired with brand context
    for tok in re.findall(r"\b([A-Z]\d{1,2})\b", t):
        raw.add(tok)

    # Filter
    result = []
    for tok in raw:
        if tok in _STOPWORDS:
            continue
        # Skip pure storage: 256GB, 16GB, 512MB, 2TB
        if re.fullmatch(r"\d+G[B]?|\d+TB|\d+MB", tok):
            continue
        # Skip watt/volt/amp specs: 65W, 12V, 3A
        if re.fullmatch(r"\d+[WVA]", tok):
            continue
        # Skip resolution/framerate patterns: 1080P, 144Hz
        if re.fullmatch(r"\d{3,4}[PH][Z]?", tok):
            continue
        # Skip plain version/revision: V2, R3
        if re.fullmatch(r"[VR]\d+", tok):
            continue
        result.append(tok)

    # Sort: longest (most specific) first
    return sorted(set(result), key=lambda x: -len(x))


# ── Cross-market matching ─────────────────────────────────────────────────────

def _extract_brand(name: str) -> str:
    """
    Extract the most likely brand from the first word(s) of a product name.
    Returns uppercase normalised brand token.
    """
    if not name:
        return ""
    # Brand is usually the first ALL-CAPS or Title-Case word(s) before a model number.
    # We take the first 1–2 words and normalise.
    parts = name.strip().split()
    if not parts:
        return ""
    brand = parts[0].upper().rstrip(".,:")
    # Some brands are two words (e.g. "De Longhi")
    if len(parts) > 1 and parts[1][0].isupper() and not re.search(r'\d', parts[1]):
        two = (parts[0] + parts[1]).upper()
        # If 2nd word is short it's likely part of brand name, not a model
        if len(parts[1]) <= 6:
            brand = two
    return brand


# Category compatibility map — groups of categories that represent the same physical
# product class.  Products whose MainCategory falls in DIFFERENT groups are
# considered incompatible and will NOT be clustered together even if they share a
# model token.  Add more groups as your data grows.
_CATEGORY_GROUPS: list[frozenset] = [
    frozenset(["Mobily", "Smartphones", "Mobilní telefony", "Handy", "Smartphones & Handys"]),
    frozenset(["Notebooky", "Laptops", "Notebooks"]),
    frozenset(["Tablety", "Tablets"]),
    frozenset(["Sluchátka", "Kopfhörer", "Headphones", "Casques audio", "Écouteurs"]),
    frozenset(["Televize", "Televizory", "Fernseher", "TV", "Téléviseurs"]),
    frozenset(["Pračky", "Waschmaschinen", "Lave-linge"]),
    frozenset(["Myčky nádobí", "Geschirrspüler", "Lave-vaisselle"]),
    frozenset(["Sušičky", "Wäschetrockner", "Sèche-linge"]),
    frozenset(["Ledničky", "Kühlschränke", "Réfrigérateurs"]),
    frozenset(["Fotoaparáty", "Kameras", "Appareils photo"]),
    frozenset(["Tiskárny", "Drucker", "Imprimantes"]),
    frozenset(["Vysavače", "Staubsauger", "Aspirateurs"]),
    frozenset(["Reproduktory", "Lautsprecher", "Enceintes"]),
]

def _category_group(cat: str | None) -> int | None:
    """Return group index for a category, or None if not in any group."""
    if not cat:
        return None
    cat_up = cat.upper()
    for i, grp in enumerate(_CATEGORY_GROUPS):
        if any(g.upper() == cat_up or g.upper() in cat_up for g in grp):
            return i
    return None


def find_cross_market_matches(
    conn: sqlite3.Connection,
    min_markets: int = 2,
    include_amazon_us: bool = False,
    min_token_len: int = 4,
) -> list[dict]:
    """
    Cluster products from different countries that share a model-number token.

    False-positive reduction (v2):
    1. Brand-aware: within each token bucket, only cluster products sharing the
       same brand name.  A "WH-1000XM5" made by Sony must not be confused with a
       hypothetical WH-1000XM5 from another brand.
    2. Category-gated: products are only grouped if their categories are
       compatible (fall in the same _CATEGORY_GROUPS bucket, or both have unknown
       categories).  A printer and a lamp that happen to share a short model code
       will not be grouped.
    3. Minimum token length scales with ambiguity: short tokens (len < 6) require
       the brand to match exactly; longer tokens (≥ 6 chars) allow brand mismatch
       since the model number itself is highly specific.

    Returns a list of group dicts sorted by number of matched countries (desc).
    """
    source_filter = "" if include_amazon_us else "AND source != 'amazon_us'"
    rows = conn.execute(
        f"""SELECT id, Name, source, COALESCE(country,'CZ') as country,
                   RecommendRate_pct, ReviewsCount, Price_CZK,
                   COALESCE(currency,'CZK') as currency,
                   COALESCE(MainCategory, Category, '') as main_cat
            FROM products
            WHERE Name IS NOT NULL {source_filter}"""
    ).fetchall()

    # Build token → products index (keyed by (token, brand) for brand awareness)
    # For long tokens (≥ 6 chars) we use key = (token, "") so brand is irrelevant.
    token_index: dict[tuple, list[dict]] = defaultdict(list)
    for id_, name, source, country, rate, reviews, price, currency, main_cat in rows:
        brand = _extract_brand(name)
        for tok in extract_model_tokens(name):
            if len(tok) < min_token_len:
                continue
            # Short tokens require brand match to avoid false positives
            brand_key = brand if len(tok) < 6 else ""
            token_index[(tok, brand_key)].append(
                dict(id=id_, name=name, source=source, country=country,
                     rate=rate, reviews=reviews, price=price, currency=currency,
                     main_cat=main_cat, brand=brand)
            )

    # Find token+brand combos with entries in 2+ countries
    seen_product_sets: set[frozenset] = set()  # de-duplicate equivalent groups
    groups = []

    for (tok, brand_key), prods in sorted(
        token_index.items(), key=lambda kv: -len({p["country"] for p in kv[1]})
    ):
        # ── Category gating ────────────────────────────────────────────────
        # Group products by their category-group index.  If there are products
        # in more than one KNOWN category group, split and only keep the largest
        # compatible sub-cluster.
        by_cat_group: dict[int | None, list] = defaultdict(list)
        for p in prods:
            by_cat_group[_category_group(p["main_cat"])].append(p)

        # Try each category-group sub-cluster independently
        candidates = []
        for cat_grp_idx, sub_prods in by_cat_group.items():
            # If cat_grp_idx is None (unknown category), allow it to merge with
            # any group but only if it's the sole group.
            if cat_grp_idx is None and len(by_cat_group) > 1:
                # There are known-category products too — skip unknowns to
                # avoid pulling in accessories/cases/etc.
                continue
            candidates.append((cat_grp_idx, sub_prods))

        # If all products have unknown categories, use them all together
        if not candidates:
            candidates = [(None, prods)]

        for cat_grp_idx, sub_prods in candidates:
            countries = {p["country"] for p in sub_prods}
            if len(countries) < min_markets:
                continue

            # De-duplicate: if we already have a group with the same set of product IDs
            pid_set = frozenset(p["id"] for p in sub_prods)
            if pid_set in seen_product_sets:
                continue
            seen_product_sets.add(pid_set)

            # Compute price/rating ranges for the group
            prices_czk = [p["price"] for p in sub_prods if p["price"] is not None]
            rates = [p["rate"] for p in sub_prods if p["rate"] is not None]

            groups.append({
                "token":         tok,
                "brand":         brand_key or (sub_prods[0]["brand"] if sub_prods else ""),
                "countries":     sorted(countries),
                "n_markets":     len(countries),
                "products":      sub_prods,
                "n_products":    len(sub_prods),
                "price_min_czk": min(prices_czk) if prices_czk else None,
                "price_max_czk": max(prices_czk) if prices_czk else None,
                "rate_min":      round(min(rates), 1) if rates else None,
                "rate_max":      round(max(rates), 1) if rates else None,
            })

    # Sort: most markets first, then by number of products
    groups.sort(key=lambda g: (-g["n_markets"], -g["n_products"]))
    return groups


# ── Standalone script ─────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Cross-market product matcher")
    ap.add_argument("--min-markets", type=int, default=2,
                    help="Minimum number of markets a product must appear in (default: 2)")
    ap.add_argument("--include-amazon", action="store_true",
                    help="Include Amazon.com US products (very large, slow)")
    ap.add_argument("--json", action="store_true", help="Output JSON instead of table")
    ap.add_argument("--top", type=int, default=50,
                    help="Show top N groups (default: 50)")
    args = ap.parse_args()

    if not os.path.exists(DB_PATH):
        print(f"ERROR: Database not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    groups = find_cross_market_matches(
        conn,
        min_markets=args.min_markets,
        include_amazon_us=args.include_amazon,
    )
    conn.close()

    if args.json:
        print(json.dumps(groups[:args.top], ensure_ascii=False, indent=2))
        return

    # Pretty-print table
    print(f"\n{'═' * 72}")
    print(f"  Cross-Market Product Matches  (min {args.min_markets} markets)")
    print(f"  {len(groups)} groups found")
    print(f"{'═' * 72}")
    for g in groups[:args.top]:
        markets = "/".join(g["countries"])
        price_str = (
            f"CZK {g['price_min_czk']:.0f}–{g['price_max_czk']:.0f}"
            if g["price_min_czk"] is not None else "n/a"
        )
        rate_str = (
            f"{g['rate_min']}–{g['rate_max']}%"
            if g["rate_min"] is not None else "n/a"
        )
        print(f"\n  [{markets}]  {g['token']}  ({g['n_products']} products)")
        print(f"  Price: {price_str}  ·  Rating: {rate_str}")
        # Show up to 4 representative product names
        seen_names: set[str] = set()
        for p in sorted(g["products"], key=lambda x: -(x["reviews"] or 0))[:4]:
            short = p["name"][:60]
            if short not in seen_names:
                seen_names.add(short)
                rate = f"{p['rate']:.0f}%" if p["rate"] else "?"
                print(f"    [{p['country']}] {short} — {rate}  ({p['source']})")

    print(f"\n{'═' * 72}\n")


if __name__ == "__main__":
    main()
