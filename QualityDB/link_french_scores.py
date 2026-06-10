#!/usr/bin/env python3
"""
link_french_scores.py
──────────────────────
Links French durability / repairability index scores (stored on country='FR'
product rows scraped from Fnac.fr) onto matching CZ and DE product records.

Why it matters for the dissertation
  The French index scores are the only publicly mandated, standardised
  product-level durability/repairability data in the EU.  Linking them onto
  the wider CZ/DE product database creates a cross-national dataset that lets
  you correlate the index score with:
    • consumer star ratings (is a durable product better-rated?)
    • price stratification across markets
    • category-level obsolescence patterns
    • review volume over time (snapshot data)

Three-stage matching strategy
  Stage 1 — EAN exact match         (perfect, if EAN is available)
  Stage 2 — model_number exact match (OEM part number, very reliable)
  Stage 3 — Fuzzy name match        (brand + normalised model, ~0.85 threshold)

  For each CZ/DE product, the best FR match above the confidence threshold is
  written back via:
    fr_matched_product_id  → products.id of the FR row
    fr_match_confidence    → 0.0–1.0
    fr_match_method        → 'ean' | 'model_exact' | 'fuzzy'
    fr_match_date          → ISO date of this run
    repairability_score_fr → copied from FR row (if CZ/DE row lacks it)
    durability_score_fr    → copied from FR row

Usage
  python3 link_french_scores.py                        # run on ./products.db
  python3 link_french_scores.py /path/to/products.db
  python3 link_french_scores.py --dry-run              # show matches without writing
  python3 link_french_scores.py --min-confidence 0.80  # override threshold (default 0.82)
  python3 link_french_scores.py --country CZ           # only update CZ rows
  python3 link_french_scores.py --report               # write match report CSV

Dependencies
  pip install rapidfuzz       ← fast fuzzy string matching
  (Falls back to difflib.SequenceMatcher if rapidfuzz is not installed.)
"""

import sqlite3
import sys
import os
import re
import json
import csv
import datetime
import argparse
import logging
from dataclasses import dataclass, field
from typing import Optional, List, Dict

# ── optional fast fuzzy lib ───────────────────────────────────────────────────
try:
    from rapidfuzz import fuzz, process as rf_process
    RAPIDFUZZ = True
except ImportError:
    import difflib
    RAPIDFUZZ = False
    print("⚠  rapidfuzz not found — falling back to difflib (slower).")
    print("   For best performance:  pip install rapidfuzz\n")

DB_DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")
DEFAULT_MIN_CONFIDENCE = 0.82   # 0–1.0;  0.82 ≈ good balance precision/recall

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class FRProduct:
    id:                     int
    name:                   str
    brand:                  str
    model_normalized:       str
    model_number:           str
    ean:                    str
    category:               str
    repairability_score_fr: float
    durability_score_fr:    float
    repairability_date:     str
    durability_date:        str

    # Computed matching key (brand + model, lowercased, stripped)
    match_key: str = field(init=False)

    def __post_init__(self):
        self.match_key = _make_match_key(self.brand, self.model_normalized or self.name)


@dataclass
class MatchResult:
    target_id:      int
    fr_id:          int
    confidence:     float
    method:         str    # 'ean' | 'model_exact' | 'fuzzy'
    fr_repair:      float
    fr_durability:  float
    fr_repair_date: str
    fr_dur_date:    str


# ── text normalisation ────────────────────────────────────────────────────────

# Known brand aliases (FR ↔ CZ/DE name variants)
BRAND_ALIASES = {
    "apple":         ["apple", "iphone", "ipad", "macbook"],
    "samsung":       ["samsung", "galaxy"],
    "lg":            ["lg", "lg electronics"],
    "sony":          ["sony"],
    "huawei":        ["huawei"],
    "xiaomi":        ["xiaomi", "redmi", "poco"],
    "lenovo":        ["lenovo", "thinkpad", "ideapad", "yoga"],
    "hp":            ["hp", "hewlett", "hewlett-packard"],
    "dell":          ["dell", "xps", "inspiron", "latitude"],
    "acer":          ["acer", "aspire", "swift", "nitro"],
    "asus":          ["asus", "zenbook", "vivobook", "rog"],
    "microsoft":     ["microsoft", "surface"],
    "bosch":         ["bosch"],
    "siemens":       ["siemens"],
    "electrolux":    ["electrolux", "aeg"],
    "whirlpool":     ["whirlpool"],
    "miele":         ["miele"],
    "dyson":         ["dyson"],
    "philips":       ["philips"],
    "panasonic":     ["panasonic"],
    "beko":          ["beko"],
    "hisense":       ["hisense"],
    "tcl":           ["tcl"],
}

# Reverse lookup alias → canonical brand
_ALIAS_TO_BRAND = {
    alias: canonical
    for canonical, aliases in BRAND_ALIASES.items()
    for alias in aliases
}

_NOISE = re.compile(
    r"\b(reconditionn[eé]|neuf|occasion|grade\s*\w|certifi[eé]|"
    r"smartphone|téléphone|tablette|ordinateur|portable|lave-linge|"
    r"aspirateur|télévision|t[eé]l[eé]viseur|notebook|laptop|neuf|"
    r"noir|blanc|gris|argent|bleu|rouge|vert|violet|rose|gold|"
    r"black|white|grey|silver|blue|red|green|purple|"
    r"\d+\s*go|\d+\s*gb|\d+\s*to|\d+\s*tb|\d+\s*mp|\d+\s*hz|\d+"
    r")\b",
    re.IGNORECASE | re.UNICODE
)
_WS = re.compile(r"\s+")


def _normalize(text: str) -> str:
    """Lowercase, remove noise words and punctuation, collapse whitespace."""
    if not text:
        return ""
    text = text.lower()
    text = _NOISE.sub(" ", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = _WS.sub(" ", text).strip()
    return text


def _canonical_brand(brand: str) -> str:
    """Map a brand string to its canonical lowercase form."""
    if not brand:
        return ""
    b = brand.lower().strip()
    return _ALIAS_TO_BRAND.get(b, b)


def _make_match_key(brand: str, model: str) -> str:
    """Build a single normalised key used for fuzzy comparison."""
    cb = _canonical_brand(brand or "")
    cm = _normalize(model or "")
    # Remove brand from model if it's already prepended
    if cb and cm.startswith(cb):
        cm = cm[len(cb):].strip()
    return f"{cb} {cm}".strip()


def _fuzzy_score(a: str, b: str) -> float:
    """Return similarity score 0.0–1.0 between two strings."""
    if not a or not b:
        return 0.0
    if RAPIDFUZZ:
        # token_sort_ratio handles word-order differences well for product names
        return fuzz.token_sort_ratio(a, b) / 100.0
    else:
        return difflib.SequenceMatcher(None, a, b).ratio()


# ── database helpers ──────────────────────────────────────────────────────────

def ensure_match_columns(conn: sqlite3.Connection):
    """Add matching columns to products table if missing (idempotent)."""
    cur = conn.execute("PRAGMA table_info(products)")
    existing = {row[1] for row in cur.fetchall()}
    needed = [
        ("fr_matched_product_id", "INTEGER"),
        ("fr_match_confidence",   "REAL"),
        ("fr_match_method",       "TEXT"),
        ("fr_match_date",         "TEXT"),
        ("repairability_score_fr","REAL"),
        ("repairability_score_date","TEXT"),
        ("durability_score_fr",   "REAL"),
        ("durability_score_date", "TEXT"),
        ("brand",                 "TEXT"),
        ("model_normalized",      "TEXT"),
        ("model_number",          "TEXT"),
        ("ean",                   "TEXT"),
    ]
    for col, col_type in needed:
        if col not in existing:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
    conn.commit()


def load_fr_products(conn: sqlite3.Connection) -> List[FRProduct]:
    """Load all French products that have at least one index score."""
    rows = conn.execute("""
        SELECT id, Name, brand, model_normalized, model_number, ean,
               Category,
               repairability_score_fr, durability_score_fr,
               repairability_score_date, durability_score_date
        FROM products
        WHERE country = 'FR'
          AND (repairability_score_fr IS NOT NULL OR durability_score_fr IS NOT NULL)
    """).fetchall()

    results = []
    for r in rows:
        results.append(FRProduct(
            id=r[0], name=r[1] or "",
            brand=r[2] or "",
            model_normalized=r[3] or "",
            model_number=r[4] or "",
            ean=r[5] or "",
            category=r[6] or "",
            repairability_score_fr=r[7],
            durability_score_fr=r[8],
            repairability_date=r[9] or "",
            durability_date=r[10] or "",
        ))
    return results


def load_target_products(conn: sqlite3.Connection,
                         countries: List[str]) -> List[dict]:
    """Load CZ/DE products that don't yet have a French score."""
    placeholders = ",".join("?" * len(countries))
    rows = conn.execute(f"""
        SELECT id, Name, brand, model_normalized, model_number, ean,
               Category, country,
               repairability_score_fr, durability_score_fr
        FROM products
        WHERE country IN ({placeholders})
          AND (repairability_score_fr IS NULL AND durability_score_fr IS NULL)
    """, countries).fetchall()

    return [
        {
            "id":               r[0],
            "name":             r[1] or "",
            "brand":            r[2] or "",
            "model_normalized": r[3] or "",
            "model_number":     r[4] or "",
            "ean":              r[5] or "",
            "category":         r[6] or "",
            "country":          r[7] or "",
            "match_key":        _make_match_key(r[2] or "", r[3] or r[1] or ""),
        }
        for r in rows
    ]


# ── matching logic ────────────────────────────────────────────────────────────

def build_fr_indexes(fr_products: List[FRProduct]) -> dict:
    """
    Build lookup indexes for fast matching:
      ean_index:        ean → FRProduct
      model_index:      model_number → FRProduct
      category_buckets: category → [FRProduct]  (for fuzzy matching)
    """
    ean_index   = {}
    model_index = {}
    cat_buckets: Dict[str, List[FRProduct]] = {}

    for fr in fr_products:
        if fr.ean:
            ean_index[fr.ean.strip()] = fr
        if fr.model_number:
            mn = fr.model_number.strip().upper()
            model_index[mn] = fr
        cat_key = fr.category.lower()
        cat_buckets.setdefault(cat_key, []).append(fr)

    return {
        "ean":     ean_index,
        "model":   model_index,
        "buckets": cat_buckets,
    }


# Category similarity map — used to limit fuzzy search to relevant FR products
_CAT_GROUPS = {
    "smartphones":      ["smartphones", "mobile phones", "telefony"],
    "laptops":          ["laptops", "notebooky", "notebooks"],
    "tablets":          ["tablets", "tablety"],
    "tvs":              ["tvs", "televisions", "televizory", "fernseher"],
    "headphones":       ["headphones", "sluchatka", "kopfhörer"],
    "washing machines": ["washing machines", "pracky", "waschmaschinen"],
    "vacuum cleaners":  ["vacuum cleaners", "vysavace", "staubsauger"],
}

def _category_bucket_keys(target_cat: str) -> List[str]:
    """Return FR category bucket keys relevant to a target category."""
    tc = target_cat.lower()
    for canonical, aliases in _CAT_GROUPS.items():
        if any(a in tc for a in aliases) or tc in aliases:
            return aliases
    return [tc]  # fallback: exact category name


def match_product(target: dict, indexes: dict,
                  min_confidence: float) -> Optional[MatchResult]:
    """
    Attempt to find the best FR match for a single CZ/DE product.
    Returns MatchResult or None if no match above threshold.
    """
    ean_idx   = indexes["ean"]
    model_idx = indexes["model"]
    buckets   = indexes["buckets"]

    def _build_result(fr: FRProduct, conf: float, method: str) -> MatchResult:
        return MatchResult(
            target_id=target["id"],
            fr_id=fr.id,
            confidence=conf,
            method=method,
            fr_repair=fr.repairability_score_fr,
            fr_durability=fr.durability_score_fr,
            fr_repair_date=fr.repairability_date,
            fr_dur_date=fr.durability_date,
        )

    # ── Stage 1: EAN exact match ──────────────────────────────────────────────
    if target["ean"]:
        fr = ean_idx.get(target["ean"].strip())
        if fr:
            log.debug(f"EAN match: {target['name'][:50]} ↔ {fr.name[:50]}")
            return _build_result(fr, 1.0, "ean")

    # ── Stage 2: Model number exact match ─────────────────────────────────────
    if target["model_number"]:
        mn = target["model_number"].strip().upper()
        fr = model_idx.get(mn)
        if fr:
            log.debug(f"Model match: {target['name'][:50]} ↔ {fr.name[:50]} [{mn}]")
            return _build_result(fr, 0.97, "model_exact")

    # ── Stage 3: Fuzzy match within category buckets ──────────────────────────
    # Limit candidates to the same category group to reduce false positives
    bucket_keys = _category_bucket_keys(target.get("category", ""))
    candidates: List[FRProduct] = []
    for bk in bucket_keys:
        candidates.extend(buckets.get(bk, []))

    # Also try generic brand-bucket search if we have a brand
    if target["brand"]:
        cb = _canonical_brand(target["brand"])
        for fr_list in buckets.values():
            for fr in fr_list:
                if _canonical_brand(fr.brand) == cb and fr not in candidates:
                    candidates.append(fr)

    if not candidates:
        return None

    target_key = target["match_key"] or _make_match_key(
        target["brand"], target["model_normalized"] or target["name"]
    )

    best_fr   = None
    best_conf = 0.0

    for fr in candidates:
        conf = _fuzzy_score(target_key, fr.match_key)
        if conf > best_conf:
            best_conf = conf
            best_fr   = fr

    if best_conf >= min_confidence and best_fr is not None:
        log.debug(
            f"Fuzzy match ({best_conf:.2f}): "
            f"'{target['name'][:40]}' ↔ '{best_fr.name[:40]}'"
        )
        return _build_result(best_fr, best_conf, "fuzzy")

    return None


# ── database write-back ───────────────────────────────────────────────────────

def apply_match(conn: sqlite3.Connection, m: MatchResult, today: str,
                dry_run: bool = False) -> None:
    """Write match results back to the target product row."""
    if dry_run:
        return
    conn.execute("""
        UPDATE products
        SET
            fr_matched_product_id   = ?,
            fr_match_confidence     = ?,
            fr_match_method         = ?,
            fr_match_date           = ?,
            repairability_score_fr  = COALESCE(repairability_score_fr, ?),
            repairability_score_date= COALESCE(repairability_score_date, ?),
            durability_score_fr     = COALESCE(durability_score_fr, ?),
            durability_score_date   = COALESCE(durability_score_date, ?)
        WHERE id = ?
    """, (
        m.fr_id,
        round(m.confidence, 4),
        m.method,
        today,
        m.fr_repair,    m.fr_repair_date,
        m.fr_durability, m.fr_dur_date,
        m.target_id,
    ))


# ── report writer ─────────────────────────────────────────────────────────────

def write_report(matches: List[MatchResult], conn: sqlite3.Connection,
                 output_path: str):
    """Write a CSV report of all matches for manual validation."""
    id_to_name = {}
    for row in conn.execute("SELECT id, Name, country, Category FROM products"):
        id_to_name[row[0]] = {"name": row[1], "country": row[2], "cat": row[3]}

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "target_id", "target_name", "target_country", "target_category",
            "fr_id", "fr_name", "fr_category",
            "confidence", "method",
            "repairability_score_fr", "durability_score_fr",
        ])
        for m in matches:
            t = id_to_name.get(m.target_id, {})
            fr = id_to_name.get(m.fr_id, {})
            writer.writerow([
                m.target_id, t.get("name",""), t.get("country",""), t.get("cat",""),
                m.fr_id, fr.get("name",""), fr.get("cat",""),
                round(m.confidence, 4), m.method,
                m.fr_repair, m.fr_durability,
            ])
    log.info(f"Match report written to: {output_path}")


# ── confidence diagnostics ────────────────────────────────────────────────────

def print_confidence_histogram(matches: List[MatchResult]):
    """Print a simple ASCII histogram of match confidence scores."""
    buckets = {
        "1.00 (EAN/model)":   0,
        "0.95–0.99":          0,
        "0.90–0.94":          0,
        "0.85–0.89":          0,
        "0.82–0.84":          0,
    }
    for m in matches:
        if m.confidence >= 1.0:
            buckets["1.00 (EAN/model)"] += 1
        elif m.confidence >= 0.95:
            buckets["0.95–0.99"] += 1
        elif m.confidence >= 0.90:
            buckets["0.90–0.94"] += 1
        elif m.confidence >= 0.85:
            buckets["0.85–0.89"] += 1
        else:
            buckets["0.82–0.84"] += 1

    print("\n── Match confidence distribution ──")
    for label, count in buckets.items():
        bar = "█" * min(count, 60)
        print(f"  {label}: {count:5d}  {bar}")
    print()


# ── main ──────────────────────────────────────────────────────────────────────

def run(db_path: str, dry_run: bool = False,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        countries: List[str] = None,
        report: bool = False):

    if countries is None:
        countries = ["CZ", "DE"]

    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute(f"PRAGMA journal_mode={os.environ.get('JOURNAL_MODE', 'wal').upper()}")
    ensure_match_columns(conn)

    # ── Load data ─────────────────────────────────────────────────────────────
    log.info("Loading French products with index scores…")
    fr_products = load_fr_products(conn)
    log.info(f"  {len(fr_products)} FR products with scores found.")

    if not fr_products:
        log.warning(
            "No French products with scores found.\n"
            "Run:  python3 scraper/fnac_scraper.py  first."
        )
        conn.close()
        return

    indexes = build_fr_indexes(fr_products)
    log.info(
        f"  EAN index: {len(indexes['ean'])} entries | "
        f"Model index: {len(indexes['model'])} entries | "
        f"Category buckets: {len(indexes['buckets'])}"
    )

    log.info(f"Loading {'/'.join(countries)} products without FR scores…")
    targets = load_target_products(conn, countries)
    log.info(f"  {len(targets)} target products to match.")

    # ── Run matching ──────────────────────────────────────────────────────────
    today   = datetime.date.today().isoformat()
    matches: List[MatchResult] = []
    no_match = 0

    log.info(f"Matching (min_confidence={min_confidence}, dry_run={dry_run})…")

    for i, target in enumerate(targets, 1):
        if i % 500 == 0:
            log.info(f"  Progress: {i}/{len(targets)}…")

        result = match_product(target, indexes, min_confidence)
        if result:
            matches.append(result)
            apply_match(conn, result, today, dry_run=dry_run)
        else:
            no_match += 1

    if not dry_run:
        conn.commit()

    # ── Summary ───────────────────────────────────────────────────────────────
    method_counts = {}
    for m in matches:
        method_counts[m.method] = method_counts.get(m.method, 0) + 1

    log.info("=" * 60)
    log.info("Matching complete.")
    log.info(f"  Targets processed : {len(targets):,}")
    log.info(f"  Matched           : {len(matches):,}  "
             f"({100*len(matches)/max(len(targets),1):.1f}%)")
    log.info(f"  No match found    : {no_match:,}")
    log.info(f"  By method: EAN={method_counts.get('ean',0)} | "
             f"model_exact={method_counts.get('model_exact',0)} | "
             f"fuzzy={method_counts.get('fuzzy',0)}")
    if dry_run:
        log.info("  DRY RUN — no changes written to DB.")
    log.info("=" * 60)

    if matches:
        print_confidence_histogram(matches)

    if report and matches:
        report_path = os.path.join(
            os.path.dirname(db_path),
            f"french_match_report_{today}.csv"
        )
        write_report(matches, conn, report_path)

    # ── Verification query ────────────────────────────────────────────────────
    if not dry_run:
        scored = conn.execute(
            "SELECT COUNT(*) FROM products WHERE repairability_score_fr IS NOT NULL "
            "OR durability_score_fr IS NOT NULL"
        ).fetchone()[0]
        log.info(f"Products now with FR score in DB: {scored:,}")

    conn.close()
    return {"matched": len(matches), "unmatched": no_match, "methods": method_counts}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Link French durability index scores to CZ/DE products"
    )
    parser.add_argument("db", nargs="?", default=DB_DEFAULT,
                        help="Path to products.db (default: ./products.db)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show matches without writing to DB")
    parser.add_argument("--min-confidence", type=float, default=DEFAULT_MIN_CONFIDENCE,
                        help=f"Minimum fuzzy match confidence (default {DEFAULT_MIN_CONFIDENCE})")
    parser.add_argument("--country", type=str, default=None,
                        help="Only match products from this country (e.g. CZ or DE)")
    parser.add_argument("--report", action="store_true",
                        help="Write CSV match report for manual validation")
    args = parser.parse_args()

    countries = [args.country.upper()] if args.country else ["CZ", "DE"]

    run(
        db_path=args.db,
        dry_run=args.dry_run,
        min_confidence=args.min_confidence,
        countries=countries,
        report=args.report,
    )
