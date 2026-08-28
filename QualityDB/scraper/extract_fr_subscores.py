#!/usr/bin/env python3
"""
extract_fr_subscores.py — normalise the French durability index sub-criteria
════════════════════════════════════════════════════════════════════════════

`fr_durability_index.sub_scores_json` holds 24 values per model, but they are
NOT all on the same scale, and manufacturers do not all use the same convention.
This flattens them into one long table where every score is comparable.

Two scales in the source
────────────────────────
  * sub-criteria (`note_X_cN.M`)  — always RAW, 0–10. Comparable as declared.
  * criteria     (`note_X_cN`)    — normally the *weighted contribution* of that
    criterion to its block, i.e. raw × weight, so it maxes out at the weight
    (B1 ≤ 5.0, B3 ≤ 1.0) rather than at 10.

One manufacturer breaks the convention
──────────────────────────────────────
ELECTROLUX (366 washing-machine models) declares the criterion level on the RAW
0–10 scale instead. Its `note_b_c1` values run to 9.5 where every other maker's
run to 5.0. Pooling `note_b_c1` across makers without correcting this compares
two different units and inflates ELECTROLUX by ~2×. Detected per row, not
hardcoded to the brand, so a future offender is caught automatically.

Criterion weights (recovered empirically, exact to 3dp; see docstring of
WEIGHTS): A1–A4 = 0.25 each; B1 = 0.50, B2 = 0.40, B3 = 0.10 — in both
categories. Sub-criterion weights are stored as metadata only; A3's are
unstable under collinearity and are NOT used for any conversion.

Usage:
    python3 extract_fr_subscores.py --db ~/QualityData/qualitydb/products.db
    python3 extract_fr_subscores.py --db ... --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone

log = logging.getLogger("fr_subscores")

WASHERS = "Velké domácí spotřebiče"
TVS = "Televize a video"

# ── Criterion weights ─────────────────────────────────────────────────────────
# Recovered two independent ways that agree to 3dp:
#   (a) max observed contribution / 10 over contribution-convention rows
#   (b) least squares of block score on raw criteria over ELECTROLUX rows
#       -> B [0.5002, 0.4014, 0.0969], residual < 0.05 (i.e. 1dp rounding)
# Identical in both categories.
CRIT_WEIGHT: dict[str, float] = {
    "A1": 0.25, "A2": 0.25, "A3": 0.25, "A4": 0.25,
    "B1": 0.50, "B2": 0.40, "B3": 0.10,
}

# ── Sub-criterion weights (metadata only — never used to convert) ─────────────
# Fitted on contribution-convention rows. `stable` marks fits that reproduce the
# criterion to rms < 0.05 AND agree across category subsamples. A3 fails both:
# its four sub-criteria are pairwise collinear (list-1 vs list-2 parts usually
# carry identical declarations), so its split is not identified by this data.
SUB_WEIGHT: dict[str, dict[str, tuple[list[float], bool]]] = {
    WASHERS: {
        "A1": ([0.80, 0.20], True),
        "A2": ([0.53, 0.24, 0.23], True),
        "A3": ([0.45, 0.31, 0.07, 0.17], False),
        "B1": ([0.90, 0.10], True),
        "B2": ([0.70, 0.30], True),
        "B3": ([0.75, 0.25], True),
    },
    TVS: {
        "A1": ([0.81, 0.19], True),
        "A2": ([0.51, 0.25, 0.25], True),
        "A3": ([0.48, 0.27, 0.18, 0.07], False),
        "B1": ([0.10, 0.90], True),   # inverted vs washers — per-category arrêté
        "B2": ([0.60, 0.40], True),
        "B3": ([0.75, 0.25], True),
    },
}

CHILDREN: dict[str, list[str]] = {
    "A1": ["A1.1", "A1.2"],
    "A2": ["A2.1", "A2.2", "A2.3"],
    "A3": ["A3.1", "A3.2", "A3.3", "A3.4"],
    "A4": [],
    "B1": ["B1.1", "B1.2"],
    "B2": ["B2.1", "B2.2"],
    "B3": ["B3.1", "B3.2"],
}

A_CRITS = ["A1", "A2", "A3", "A4"]
B_CRITS = ["B1", "B2", "B3"]

# Source values are published rounded to 1dp, so a sum of k rounded terms can
# drift by k*0.05. 0.35 covers the 4-term A block with margin; the raw/contrib
# gap it must distinguish is ~12–27 points, so there is no ambiguity.
ROUNDING_TOL = 0.35

DDL = """
CREATE TABLE IF NOT EXISTS fr_durability_subscores (
    fr_id                     INTEGER NOT NULL,
    id_unique                 TEXT,
    main_category             TEXT,
    categorie_produit         TEXT,
    nom_metteur_sur_le_marche TEXT,
    nom_modele                TEXT,
    ean                       TEXT,
    block                     TEXT NOT NULL,   -- 'A' | 'B'
    code                      TEXT NOT NULL,   -- 'B1', 'B1.2', ...
    level                     TEXT NOT NULL,   -- 'criterion' | 'sub'
    label_en                  TEXT,
    score_raw                 REAL,            -- 0–10, comparable across makers
    contribution              REAL,            -- points into its block (criterion only)
    weight                    REAL,            -- weight within its parent
    weight_is_stable          INTEGER,         -- 0 for A3 sub-weights
    convention                TEXT,            -- declared c-level scale: contribution|raw
    is_suspect                INTEGER NOT NULL DEFAULT 0,
    suspect_reason            TEXT,
    extracted_at              TEXT NOT NULL,
    PRIMARY KEY (fr_id, code)
);
CREATE INDEX IF NOT EXISTS ix_fr_sub_code     ON fr_durability_subscores(code);
CREATE INDEX IF NOT EXISTS ix_fr_sub_maker    ON fr_durability_subscores(nom_metteur_sur_le_marche);
CREATE INDEX IF NOT EXISTS ix_fr_sub_cat_code ON fr_durability_subscores(main_category, code);
CREATE INDEX IF NOT EXISTS ix_fr_sub_unique   ON fr_durability_subscores(id_unique);
"""

# Placeholder rows in the official dataset: random lowercase strings in both the
# market-placer and model fields, with internally inconsistent scores.
DUMMY_RE = re.compile(r"^[a-z]{8,12}$")


def key(code: str) -> str:
    """'B1.2' -> 'note_B_c1.2' (the sub_scores_json key)."""
    blk, rest = code[0], code[1:]
    return f"note_{blk}_c{rest}"


BLOCK_SPEC = {"A": (A_CRITS, "note_reparabilite"), "B": (B_CRITS, "note_fiabilite")}


def detect_convention(d: dict, block: str) -> tuple[str, float]:
    """Return (convention, residual) for ONE block's criterion level.

    contribution: sum(criteria) == block score
    raw:          sum(criteria * weight) == block score

    Detected per block, not per model: a manufacturer can get one block right
    and the other wrong (Panasonic TV-55W85BEZ declares B exactly and A 2.5
    points off; Metz 40MQF7030Z is the mirror image). Judging the model as a
    whole would discard the good block with the bad one.
    """
    crits, total = BLOCK_SPEC[block]
    if total not in d:
        return "unknown", float("inf")
    best = None
    for conv in ("contribution", "raw"):
        s = sum(
            d[key(c)]["score"] * (CRIT_WEIGHT[c] if conv == "raw" else 1.0)
            for c in crits
            if key(c) in d
        )
        resid = abs(s - d[total]["score"])
        if best is None or resid < best[1]:
            best = (conv, resid)
    return best if best[1] <= ROUNDING_TOL else ("inconsistent", best[1])


def extract(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = conn.execute(
        """SELECT id, id_unique, main_category, categorie_produit,
                  nom_metteur_sur_le_marche, nom_modele, ean, sub_scores_json
           FROM fr_durability_index
           WHERE sub_scores_json IS NOT NULL AND sub_scores_json <> ''"""
    ).fetchall()

    out: list[tuple] = []
    stats: dict = {
        "models": 0, "rows": 0,
        "convention": defaultdict(int), "suspect": defaultdict(int),
        "converted": 0,
    }

    for fr_id, uniq, cat, cat_fr, maker, model, ean, js in rows:
        try:
            d = json.loads(js)
        except (TypeError, ValueError):
            stats["suspect"]["unparseable_json"] += 1
            continue
        stats["models"] += 1

        is_dummy = bool(DUMMY_RE.match(maker or "") and DUMMY_RE.match(model or ""))
        if is_dummy:
            stats["suspect"]["placeholder row (random maker+model in source)"] += 1

        subw = SUB_WEIGHT.get(cat, {})
        for block in ("A", "B"):
            conv, resid = detect_convention(d, block)
            stats["convention"][f"{block}:{conv}"] += 1

            suspect, reason = 0, None
            if is_dummy:
                suspect, reason = 1, "placeholder row (random maker+model in source)"
            elif conv in ("inconsistent", "unknown"):
                suspect = 1
                reason = f"block {block} reconciles to neither scale (residual {resid:.2f})"
                stats["suspect"][f"block {block} reconciles to neither scale"] += 1
            if conv == "raw" and not suspect:
                stats["converted"] += 1

            for crit in BLOCK_SPEC[block][0]:
                k = key(crit)
                if k not in d:
                    continue
                declared = d[k]["score"]
                w = CRIT_WEIGHT[crit]
                # Normalise onto BOTH scales regardless of how it was declared.
                # An inconsistent block is stored on the documented (contribution)
                # reading and flagged, rather than dropped.
                if conv == "raw":
                    raw, contrib = declared, declared * w
                else:
                    raw, contrib = (declared / w if w else None), declared

                out.append((fr_id, uniq, cat, cat_fr, maker, model, ean, block, crit,
                            "criterion", d[k].get("label"), raw, contrib, w, 1,
                            conv, suspect, reason, now))
                stats["rows"] += 1

                ws, stable = subw.get(crit, ([], True))
                for i, child in enumerate(CHILDREN[crit]):
                    ck = key(child)
                    if ck not in d:
                        continue
                    out.append((fr_id, uniq, cat, cat_fr, maker, model, ean, block, child,
                                "sub", d[ck].get("label"), d[ck]["score"], None,
                                ws[i] if i < len(ws) else None, 1 if stable else 0,
                                conv, suspect, reason, now))
                    stats["rows"] += 1

    if dry_run:
        return stats

    conn.executescript(DDL)
    conn.executemany(
        """INSERT INTO fr_durability_subscores
             (fr_id,id_unique,main_category,categorie_produit,nom_metteur_sur_le_marche,
              nom_modele,ean,block,code,level,label_en,score_raw,contribution,weight,
              weight_is_stable,convention,is_suspect,suspect_reason,extracted_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(fr_id, code) DO UPDATE SET
             score_raw=excluded.score_raw, contribution=excluded.contribution,
             weight=excluded.weight, weight_is_stable=excluded.weight_is_stable,
             convention=excluded.convention, is_suspect=excluded.is_suspect,
             suspect_reason=excluded.suspect_reason, label_en=excluded.label_en,
             extracted_at=excluded.extracted_at""",
        out,
    )
    conn.commit()
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.environ.get("DB_PATH"),
                    help="path to products.db (REQUIRED — config.DB_PATH points at a stale copy)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(message)s")
    if not args.db:
        log.error("--db is required (or set DB_PATH)")
        return 2
    db = os.path.expanduser(args.db)
    if not os.path.exists(db):
        log.error("no such database: %s", db)
        return 2

    conn = sqlite3.connect(db)
    try:
        stats = extract(conn, dry_run=args.dry_run)
    finally:
        conn.close()

    log.info("models processed : %d", stats["models"])
    log.info("subscore rows    : %d%s", stats["rows"], "  (dry run, nothing written)" if args.dry_run else "")
    log.info("declaration convention:")
    for k, v in sorted(stats["convention"].items(), key=lambda x: -x[1]):
        log.info("    %-14s %d", k, v)
    log.info("blocks rescaled from raw to contribution: %d", stats["converted"])
    if stats["suspect"]:
        log.info("suspect rows flagged:")
        for k, v in stats["suspect"].items():
            log.info("    %-40s %d", k, v)
    return 0


if __name__ == "__main__":
    sys.exit(main())
