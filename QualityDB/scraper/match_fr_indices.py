#!/usr/bin/env python3
"""
match_fr_indices.py — link the French official indices to the product catalogue
══════════════════════════════════════════════════════════════════════════════

PROBLEM
───────
`fr_repairability_index` (4,385 models) and `fr_durability_index` (2,769
models) are the only legally-mandated durability evidence in the DB, and almost
none of it reaches `products`: EAN linking covers ~19 rows because
`products.ean` is populated for 412 of 76,312 rows (only Fnac/Darty capture
gtin13; Geizhals, Idealo, Heureka, Alza, Amazon.de and CZC discard it).

So the join has to run on the model designation, which for appliances is a
highly distinctive alphanumeric token ("EW6FI5922PA", "WU14UT28") that retailers
put verbatim in the listing title.

WHY BRAND IS ADVISORY, NOT REQUIRED
───────────────────────────────────
The CSV has no `marque` column -- only `nom_metteur_sur_le_marche`, the legal
entity that placed the product on the market.  That is often NOT the consumer
brand: "Groupe SEB Retailing" ships Rowenta/Moulinex/Tefal, "ELECTRO DEPOT"
ships Valberg, "VESTEL France" ships Continental Edison.  Requiring brand
agreement would throw away correct matches, so brand agreement RAISES
confidence and brand disagreement only blocks the short-token tier.

CONFIDENCE TIERS
────────────────
  1.00  ean_exact            EAN-13 identical
  0.95  model_brand          model token >= 8 chars AND brand agrees
  0.85  model_long           model token >= 8 chars, brand unknown/unchecked
  0.80  model_brand_short    model token 5-7 chars AND brand agrees
  ----  rejected             token < 5 chars, or 5-7 chars without brand support

A token shorter than 5 characters is not evidence -- "W1" matches half the
catalogue.  Ambiguous tokens (one model matching several products) are written
to every match; ambiguity is recorded in `fr_match_method` as a "+ambig" suffix
so it can be filtered out of any analysis that needs one-to-one links.

Usage:
  python3 match_fr_indices.py --db ~/QualityData/qualitydb/products.db [--dry-run]

NOTE: the default `config.DB_PATH` points at the in-repo QualityDB/products.db
(13k rows), NOT the canonical ~/QualityData/qualitydb/products.db (76k rows).
Always pass --db explicitly.
"""

import argparse
import json
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

MIN_TOKEN = 5
LONG_TOKEN = 8

# Legal-entity noise stripped before comparing a market-placer to a brand.
_ENTITY_NOISE = re.compile(
    r"\b(s\.?a\.?s\.?|s\.?a\.?r\.?l\.?|gmbh|kg|ltd|limited|b\.?v\.?|n\.?v\.?|"
    r"inc|corp|co|cie|spa|sp\s*z\s*o\s*o|plc|ag|se|srl|"
    r"france|europe|deutschland|italia|iberia|group|groupe|holding|holdings|"
    r"international|services|retailing|marketing|participations|et|"
    r"succursale|de|du|des|la|le|les)\b", re.I)

# Market-placer → consumer brands it actually ships.  Only entries that are
# unambiguous in the data; a distributor that ships everything (TD SYNNEX) is
# deliberately absent so it never lends false confidence.
PLACER_BRANDS = {
    "groupe seb":            {"rowenta", "moulinex", "tefal", "calor", "krups", "seb"},
    "seb":                   {"rowenta", "moulinex", "tefal", "calor", "krups", "seb"},
    "haier":                 {"haier", "candy", "hoover"},
    "electro depot":         {"valberg"},
    "boulanger":             {"essentielb", "essentiel b"},
    "cdiscount":             {"continental edison"},
    "vestel":                {"vestel", "continental edison", "telefunken"},
    "fnac darty":            {"proline", "listo", "essentielb"},
    "whirlpool":             {"whirlpool", "indesit", "hotpoint", "bauknecht", "ignis"},
    "bsh":                   {"bosch", "siemens", "neff", "gaggenau", "constructa"},
}


def norm_token(s):
    """Collapse to bare alphanumerics, uppercase — 'EW6FI-5922 PA' -> 'EW6FI5922PA'."""
    return re.sub(r"[^A-Za-z0-9]", "", (s or "")).upper()


def norm_brand(s):
    s = (s or "").lower()
    s = re.sub(r"[.,]", " ", s)
    s = _ENTITY_NOISE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def placer_brand_set(placer):
    """Consumer brands a market-placer plausibly ships."""
    n = norm_brand(placer)
    if not n:
        return set()
    out = {n}
    for key, brands in PLACER_BRANDS.items():
        if key in n or n in key:
            out |= brands
    return out


def load_catalogue(conn):
    """Catalogue rows as (rowid, normalised name, brand_canonical, source).

    Keyed on rowid, NOT on `products.id`: that column is a plain INTEGER, not a
    primary key, and is NULL for 66,122 of 76,312 rows (the table was rebuilt
    at some point and lost its PK).  Matching on `id` silently skips 87% of the
    catalogue and collapses every NULL-id row onto one key.
    """
    rows = conn.execute("""
        SELECT rowid, Name, COALESCE(brand_canonical, brand, ''), COALESCE(source,'')
        FROM products WHERE Name IS NOT NULL AND Name <> ''
    """).fetchall()
    return [(pid, norm_token(name), (bc or "").lower(), src) for pid, name, bc, src in rows]


def match_index(conn, table, score_col, date_col, json_col, id_col, conf_col,
                method_col, dry_run):
    """Match one FR index table into products; return per-tier counts."""
    is_dur = table == "fr_durability_index"
    score_field = "note_id" if is_dur else "note_ir"
    url_field = "url_tableau_detail"

    fr_rows = conn.execute(f"""
        SELECT id, nom_modele, nom_metteur_sur_le_marche, ean,
               {score_field}, sub_scores_json, {url_field}, categorie_produit
        FROM {table}
        WHERE {score_field} IS NOT NULL
    """).fetchall()

    catalogue = load_catalogue(conn)
    # EAN index (only 412 rows carry one, but they are the surest matches)
    ean_idx = defaultdict(list)
    for pid, ean in conn.execute(
            "SELECT rowid, ean FROM products WHERE ean IS NOT NULL AND ean <> ''"):
        ean_idx[str(ean).strip()].append(pid)

    today = date.today().isoformat()
    tiers = defaultdict(int)
    updates = []           # (pid, score, json, url, fr_id, conf, method, toklen)

    for fr_id, model, placer, ean, score, sub_json, detail_url, cat in fr_rows:
        # ── tier 1: EAN ──────────────────────────────────────────────────────
        if ean and str(ean).strip() in ean_idx:
            for pid in ean_idx[str(ean).strip()]:
                updates.append((pid, score, sub_json, detail_url, fr_id,
                                1.00, "ean_exact", 999))
                tiers["ean_exact"] += 1
            continue

        tok = norm_token(model)
        if len(tok) < MIN_TOKEN:
            tiers["rejected_short"] += 1
            continue

        brands = placer_brand_set(placer)
        hits = [(pid, bc, src) for pid, nname, bc, src in catalogue if tok in nname]
        if not hits:
            tiers["no_match"] += 1
            continue

        ambig = "+ambig" if len(hits) > 1 else ""
        for pid, bc, src in hits:
            brand_ok = bool(bc) and any(b and (b in bc or bc in b) for b in brands)
            if len(tok) >= LONG_TOKEN:
                conf, method = (0.95, "model_brand") if brand_ok else (0.85, "model_long")
            elif brand_ok:
                conf, method = 0.80, "model_brand_short"
            else:
                tiers["rejected_short_nobrand"] += 1
                continue
            updates.append((pid, score, sub_json, detail_url, fr_id,
                            conf, method + ambig, len(tok)))
            tiers[method] += 1

    # ── resolve variant bleed ────────────────────────────────────────────────
    # A base model token is a prefix of its variants: "55U7Q" matches the listing
    # for "55U7Q PRO" and "55U7QF" as well as its own.  Those are DIFFERENT models
    # with their own FR records and their own scores, so keep, per product, the
    # match with the highest confidence and then the LONGEST model token -- the
    # most specific designation that fits the title.
    best = {}
    for u in updates:
        pid, conf, toklen = u[0], u[5], u[7]
        cur_best = best.get(pid)
        if cur_best is None or (conf, toklen) > (cur_best[5], cur_best[7]):
            best[pid] = u
    dropped = len(updates) - len(best)
    if dropped:
        tiers["variant_bleed_dropped"] = dropped
    updates = list(best.values())

    if not dry_run and updates:
        cur = conn.cursor()
        for pid, score, sub_json, detail_url, fr_id, conf, method, _tok in updates:
            # Per-index provenance ({id_col}) is the authoritative pointer.
            # fr_matched_product_id is kept only for backward compatibility and
            # is AMBIGUOUS: fr_durability_index.id and fr_repairability_index.id
            # are independent AUTOINCREMENT sequences that collide on all 2,769
            # durability rows, so a bare pointer cannot say which table it means.
            # Joining it blindly to fr_durability_index turns a correctly matched
            # laptop (repairability index) into a Haier washing machine.
            cur.execute(f"""
                UPDATE products
                   SET {score_col}   = ?,
                       {date_col}    = ?,
                       {json_col}    = COALESCE(?, {json_col}),
                       {id_col}      = ?,
                       {conf_col}    = ?,
                       {method_col}  = ?,
                       fr_source_url = COALESCE(?, fr_source_url),
                       fr_matched_product_id = ?,
                       fr_match_confidence   = ?,
                       fr_match_method       = ?,
                       fr_match_date         = ?
                 WHERE rowid = ?
                   AND ({conf_col} IS NULL OR {conf_col} <= ?)
            """, (score, today, sub_json, fr_id, conf, method, detail_url,
                  fr_id, conf, method, today, pid, conf))
        conn.commit()

    return tiers, len(fr_rows)


def main():
    ap = argparse.ArgumentParser(description="Link FR indices to products")
    ap.add_argument("--db", required=True, help="path to products.db (pass the canonical one)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)

    jobs = [
        ("fr_durability_index", "durability_score_fr",
         "durability_score_date", "durability_sub_scores_json",
         "fr_durability_id", "fr_durability_conf", "fr_durability_method"),
        ("fr_repairability_index", "repairability_score_fr",
         "repairability_score_date", "repairability_sub_scores_json",
         "fr_repairability_id", "fr_repairability_conf", "fr_repairability_method"),
    ]

    for table, score_col, date_col, json_col, id_col, conf_col, method_col in jobs:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not exists:
            print(f"── {table}: table absent, skipped")
            continue
        tiers, n = match_index(conn, table, score_col, date_col, json_col,
                               id_col, conf_col, method_col, args.dry_run)
        print(f"── {table}  ({n:,} scored models)")
        for k in ("ean_exact", "model_brand", "model_long", "model_brand_short",
                  "variant_bleed_dropped", "no_match", "rejected_short",
                  "rejected_short_nobrand"):
            if tiers.get(k):
                print(f"     {k:<24} {tiers[k]:,}")

    print()
    for col, label in (("durability_score_fr", "durability"),
                       ("repairability_score_fr", "repairability")):
        n = conn.execute(
            f"SELECT COUNT(*) FROM products WHERE {col} IS NOT NULL").fetchone()[0]
        print(f"products with {label:<14}: {n:,}")
    rows = conn.execute("""
        SELECT fr_match_method, COUNT(*) FROM products
        WHERE fr_match_method IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""").fetchall()
    if rows:
        print("\nby match method:")
        for m, c in rows:
            print(f"  {m:<26} {c:,}")
    conn.close()


if __name__ == "__main__":
    main()
