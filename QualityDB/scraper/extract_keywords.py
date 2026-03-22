"""
Extract quality signal keywords from product names for ALL sources.

Scans the Name column and adds recognised signals to the keywords column.
Preserves any keywords already set (e.g. Alza's scraped tags) — only adds new ones.

Signals extracted:
  - IP/water ratings (IP67, IPX4, Waterproof, …)
  - Display tech (OLED, AMOLED, QLED, Mini LED, 4K, 8K, …)
  - Materials (Aluminium, Titanium, Ceramic, Sapphire glass, …)
  - Connectivity (5G, WiFi 6, USB-C, Thunderbolt, …)
  - Audio (Noise Cancelling, Hi-Res Audio, Dolby Atmos, …)
  - Features (Wireless Charging, MFi Certified, ENERGY STAR, …)
  - Warranty patterns (5Y warranty, 10Y warranty, Lifetime warranty)

Run:
  python3 scraper/extract_keywords.py
  python3 scraper/extract_keywords.py --dry-run
  python3 scraper/extract_keywords.py --source amazon_us
  python3 scraper/extract_keywords.py --source alza --dry-run
"""

import argparse
import json
import logging
import os
import re
import sqlite3

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "products.db")

# ── Signal definitions ─────────────────────────────────────────────────────────
# Each entry: (signal_label, [patterns_to_match])
# Patterns are case-insensitive. First match wins within each category group.

SIGNALS = [

    # ── Water / dust protection ───────────────────────────────────────────────
    ("IP69K",        [r"\bIP69K\b"]),
    ("IP68",         [r"\bIP68\b"]),
    ("IP67",         [r"\bIP67\b"]),
    ("IP65",         [r"\bIP65\b"]),
    ("IP55",         [r"\bIP55\b"]),
    ("IP54",         [r"\bIP54\b"]),
    ("IP52",         [r"\bIP52\b"]),
    ("IP20",         [r"\bIP20\b"]),
    ("IPX8",         [r"\bIPX8\b"]),
    ("IPX7",         [r"\bIPX7\b"]),
    ("IPX6",         [r"\bIPX6\b"]),
    ("IPX5",         [r"\bIPX5\b"]),
    ("IPX4",         [r"\bIPX4\b"]),
    ("IPX3",         [r"\bIPX3\b"]),
    ("IPX2",         [r"\bIPX2\b"]),
    ("Waterproof",   [r"\bwaterproof\b", r"\bwater.proof\b", r"\bwater resistant\b",
                      r"\bwater.resistant\b", r"\bwater repellent\b"]),
    ("Dustproof",    [r"\bdustproof\b", r"\bdust.proof\b"]),
    ("MIL-STD-810",  [r"\bMIL.STD.810\b", r"\bmilitary.grade\b", r"\bmilitary grade\b"]),

    # ── Display technology ─────────────────────────────────────────────────────
    ("OLED",         [r"\bOLED\b"]),
    ("AMOLED",       [r"\bAMOLED\b", r"\bSuper AMOLED\b"]),
    ("QLED",         [r"\bQLED\b"]),
    ("Mini LED",     [r"\bMini.LED\b"]),
    ("MicroLED",     [r"\bMicroLED\b", r"\bMicro.LED\b"]),
    ("4K",           [r"\b4K\b", r"\b4K UHD\b", r"\bUltra HD\b", r"\b3840.?x.?2160\b"]),
    ("8K",           [r"\b8K\b", r"\b7680.?x.?4320\b"]),
    ("HDR",          [r"\bHDR10\b", r"\bHDR10\+\b", r"\bDolby Vision\b"]),
    ("120Hz",        [r"\b120\s*Hz\b", r"\b144\s*Hz\b", r"\b165\s*Hz\b", r"\b240\s*Hz\b"]),
    ("Retina",       [r"\bRetina display\b", r"\bRetina screen\b"]),

    # ── Materials ─────────────────────────────────────────────────────────────
    ("Aluminium",    [r"\balumini?um\b", r"\balumini?um alloy\b", r"\ball.alumini?um\b",
                      r"\banodized alumini?um\b"]),
    ("Titanium",     [r"\btitanium\b"]),
    ("Ceramic",      [r"\bceramic\b"]),
    ("Carbon fibre", [r"\bcarbon.fib(re|er)\b"]),
    ("Stainless steel",[r"\bstainless steel\b"]),
    ("Sapphire glass",[r"\bsapphire glass\b", r"\bsapphire crystal\b"]),
    ("Gorilla Glass",[r"\bGorilla Glass\b"]),
    ("Kevlar",       [r"\bKevlar\b"]),

    # ── Connectivity ──────────────────────────────────────────────────────────
    ("5G",           [r"\b5G\b"]),
    ("WiFi 6",       [r"\bWiFi\s*6\b", r"\bWi.Fi\s*6\b", r"\b802\.11ax\b"]),
    ("Bluetooth 5",  [r"\bBluetooth\s*5\b", r"\bBT\s*5\b"]),
    ("USB-C",        [r"\bUSB.C\b", r"\bUSB Type.C\b"]),
    ("Thunderbolt",  [r"\bThunderbolt\b"]),
    ("USB4",         [r"\bUSB4\b", r"\bUSB 4\b"]),

    # ── Audio ─────────────────────────────────────────────────────────────────
    ("Noise Cancelling",[r"\bnoise.cancell?ing\b", r"\bnoise.cancell?ation\b",
                         r"\bANC\b", r"\bactive noise\b"]),
    ("Hi-Res Audio", [r"\bhi.res audio\b", r"\bhigh.res audio\b", r"\bhigh resolution audio\b"]),
    ("Dolby Atmos",  [r"\bDolby Atmos\b"]),
    ("Spatial Audio",[r"\bspatial audio\b"]),

    # ── Features ──────────────────────────────────────────────────────────────
    ("Wireless Charging",[r"\bwireless charging\b", r"\bwireless charge\b",
                          r"\bQi charging\b", r"\bMagSafe\b"]),
    ("Fast Charging",[r"\bfast charging\b", r"\brapid charging\b", r"\bquick charge\b",
                      r"\bPD charging\b", r"\b65W\b", r"\b90W\b", r"\b120W\b"]),
    ("ENERGY STAR",  [r"\bENERGY STAR\b"]),
    ("Child lock",   [r"\bchild lock\b", r"\bchildproof\b", r"\bchild.safe\b"]),
    ("MFi Certified",[r"\bMFi\b", r"\bMFi Certified\b"]),
    ("Organic",      [r"\borganic\b"]),
    ("Foldable",     [r"\bfoldable\b", r"\bfold\b.*\bphone\b", r"\bflip\b.*\bphone\b"]),
    ("Self-cleaning",[r"\bself.cleaning\b"]),

    # ── Warranty (extract "N year warranty" patterns) ─────────────────────────
    ("Lifetime warranty",[r"\blifetime warranty\b", r"\blifetime guarantee\b"]),
    ("20Y warranty", [r"\b20.year warranty\b", r"\b20yr warranty\b"]),
    ("15Y warranty", [r"\b15.year warranty\b", r"\b15yr warranty\b"]),
    ("12Y warranty", [r"\b12.year warranty\b", r"\b12yr warranty\b"]),
    ("10Y warranty", [r"\b10.year warranty\b", r"\b10yr warranty\b"]),
    ("5Y warranty",  [r"\b5.year warranty\b",  r"\b5yr warranty\b"]),
    ("3Y warranty",  [r"\b3.year warranty\b",  r"\b3yr warranty\b"]),
    ("2Y warranty",  [r"\b2.year warranty\b",  r"\b2yr warranty\b"]),
    ("1Y warranty",  [r"\b1.year warranty\b",  r"\b1yr warranty\b"]),
]

# Pre-compile patterns for speed
COMPILED = [
    (label, [re.compile(p, re.IGNORECASE) for p in patterns])
    for label, patterns in SIGNALS
]


def extract_signals(name: str) -> list:
    """Return list of signal labels found in name."""
    found = []
    for label, patterns in COMPILED:
        for pat in patterns:
            if pat.search(name):
                found.append(label)
                break   # one match per signal is enough
    return found


def merge_keywords(existing_json, new_signals: list) :
    """Merge new signals into existing keywords JSON. Returns new JSON or None if unchanged."""
    try:
        existing = json.loads(existing_json) if existing_json else []
    except Exception:
        existing = []

    existing_set = set(existing)
    to_add = [s for s in new_signals if s not in existing_set]
    if not to_add:
        return None  # nothing changed

    merged = existing + to_add
    return json.dumps(merged, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="Extract quality signal keywords from product names")
    parser.add_argument("--dry-run", action="store_true", help="Show counts without writing")
    parser.add_argument("--source",  help="Only process this source (default: all)")
    parser.add_argument("--list-signals", action="store_true", help="Print all defined signals and exit")
    args = parser.parse_args()

    if args.list_signals:
        print(f"{len(SIGNALS)} signals defined:")
        for label, patterns in SIGNALS:
            print(f"  {label:25} patterns: {len(patterns)}")
        return

    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")

    where = "WHERE source = ?" if args.source else ""
    params = [args.source] if args.source else []

    log.info(f"Loading products{' for source: ' + args.source if args.source else ' (all sources)'}...")
    rows = conn.execute(
        f"SELECT rowid, Name, source, keywords FROM products {where}", params
    ).fetchall()
    log.info(f"  {len(rows):,} products to scan")

    updates = []       # (new_keywords_json, id)
    signal_counts = {} # label → count of products tagged
    source_counts = {} # source → {added, unchanged}

    for row_id, name, source, kw_json in rows:
        if not name:
            continue

        signals = extract_signals(name)
        if not signals:
            continue

        new_json = merge_keywords(kw_json, signals)
        if new_json is None:
            source_counts.setdefault(source, {"added": 0, "unchanged": 0})["unchanged"] += 1
            continue

        updates.append((new_json, row_id))
        source_counts.setdefault(source, {"added": 0, "unchanged": 0})["added"] += 1
        for s in signals:
            signal_counts[s] = signal_counts.get(s, 0) + 1

    log.info(f"\n{'DRY RUN — ' if args.dry_run else ''}Results:")
    log.info(f"  Products that would get new/updated keywords: {len(updates):,}")

    log.info("\n  By source:")
    for src, counts in sorted(source_counts.items(), key=lambda x: -x[1]["added"]):
        log.info(f"    {src:20}  added={counts['added']:>7,}  already_had={counts['unchanged']:>7,}")

    log.info("\n  Top signals extracted:")
    for label, n in sorted(signal_counts.items(), key=lambda x: -x[1])[:30]:
        log.info(f"    {label:25} {n:>7,}")

    if args.dry_run or not updates:
        if not updates:
            log.info("Nothing to update.")
        conn.close()
        return

    log.info(f"\nWriting {len(updates):,} updates...")
    CHUNK = 500
    for i in range(0, len(updates), CHUNK):
        chunk = updates[i:i + CHUNK]
        conn.executemany("UPDATE products SET keywords=? WHERE rowid=?", chunk)
        if i % 50000 == 0 and i > 0:
            conn.commit()
            log.info(f"  …{i:,} written")

    conn.commit()
    log.info("Done.")
    conn.close()


if __name__ == "__main__":
    main()
