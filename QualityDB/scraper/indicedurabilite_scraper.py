#!/usr/bin/env python3
"""
indicedurabilite_scraper.py — French official Durability Index importer
══════════════════════════════════════════════════════════════════════════════

Downloads the consolidated *Indice de Durabilité* datasets from the French
government open-data portal (data.gouv.fr) and imports them into QualityDB.

WHY THIS SOURCE (and not Darty/Fnac scraping)
─────────────────────────────────────────────
Darty and Fnac sit behind Datadome bot protection (persistent HTTP 403 —
see darty_scraper.log / fnac_scraper.log).  But the retailers' displayed
durability scores all originate from data manufacturers are LEGALLY required
to publish (loi AGEC, art. 16, law n° 2020-105).  data.gouv.fr consolidates
every manufacturer's published file into one CSV per category, updated daily,
under Licence Ouverte v2.0 (free reuse incl. academic).  Same data, richer
(all sub-criteria!), zero bot walls.

Data sources (schema etalab/schema-indice-durabilite)
  • Téléviseur   (mandatory display since 2025-01-05)  ~1,200 models
  • Lave-linge   (mandatory display since 2025-04-05)  ~1,180 models
  Further categories (lave-vaisselle, aspirateurs, smartphones…) will appear
  as the law extends; run with --discover to auto-detect new consolidated
  datasets on data.gouv.fr.

What each row contains
──────────────────────
  note_id            (0–10) overall durability index (what shops display)
  note_reparabilite  (0–10) repairability block (A)
  note_fiabilite     (0–10) reliability block (B)

  Block A — réparabilité criteria:
    A_c1  documentation            (A_c1.1 availability duration,
                                    A_c1.2 self-repair support)
    A_c2  disassembly              (A_c2.1 steps, A_c2.2 tools, A_c2.3 fasteners)
    A_c3  spare-parts availability (A_c3.1–.4 durations & delivery delays)
    A_c4  spare-parts price
  Block B — fiabilité criteria:
    B_c1  resistance to stress/wear (B_c1.1 external stress, B_c1.2 wear)
    B_c2  maintenance & servicing   (B_c2.1 maintenance, B_c2.2 care)
    B_c3  durability guarantee & QA (B_c3.1 commercial guarantee duration,
                                     B_c3.2 continuous improvement process)
  Plus: EAN (GTIN), brand, model, date of calculation, link to the
  manufacturer's detailed scoring table (PDF), usage-counter accessibility.

Linking strategy
────────────────
1. All records → fr_durability_index table (full academic archive, all
   sub-criteria as columns + JSON).
2. Rows with referentiel_id_modele = 'GTIN_EAN' → matched on products.ean →
   durability_score_fr / durability_score_date / durability_sub_scores_json.
3. Same EAN match → french_durability_scores lookup table (durability_score,
   durability_reliability, durability_repairability).

Usage
─────
  python3 scraper/indicedurabilite_scraper.py            # import TV + washer
  python3 scraper/indicedurabilite_scraper.py --stats    # print DB stats only
  python3 scraper/indicedurabilite_scraper.py --limit 50 # first N rows/dataset
  python3 scraper/indicedurabilite_scraper.py --discover # also look for new
                                                         # category datasets
"""

from __future__ import annotations

import os
import sys
import csv
import io
import json
import time
import sqlite3
import logging
import datetime
import argparse
import urllib.request as urlrequest
import urllib.error as urlerror

# ── Path setup ────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from scraper.config import DB_PATH, JOURNAL_MODE
except ImportError:
    DB_PATH = os.path.join(BASE_DIR, "products.db")
    JOURNAL_MODE = "WAL"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("indice_durabilite")

# ── Data sources ──────────────────────────────────────────────────────────────
# Stable redirect URLs — /api/1/datasets/r/<resource-id> always resolves to the
# latest consolidated CSV ("Consolidation de la dernière version à date").
DATASETS: dict[str, dict] = {
    "Téléviseur": {
        "resource": "3df7e799-cba8-4f74-b33c-6d9db9b6361e",
        "dataset_page": (
            "https://www.data.gouv.fr/fr/datasets/fichiers-consolides-des-"
            "donnees-respectant-le-schema-indice-de-durabilite-televiseur/"
        ),
    },
    "Lave-linge": {
        "resource": "53b4d56e-5e23-4e52-9527-9921ef1934d0",
        "dataset_page": (
            "https://www.data.gouv.fr/fr/datasets/fichiers-consolides-des-"
            "donnees-respectant-le-schema-indice-de-durabilite-lave-linge/"
        ),
    },
}

RESOURCE_URL_TPL = "https://www.data.gouv.fr/api/1/datasets/r/{rid}"
DISCOVER_URL = (
    "https://www.data.gouv.fr/api/2/datasets/search/"
    "?q=fichiers+consolides+indice+de+durabilite&page_size=20"
)

# Category mapping: French → QualityDB main_category (mirrors the
# repairability importer so both indices land in the same buckets)
CATEGORY_MAP: dict[str, str] = {
    "Téléviseur":           "Televize a video",
    "Télévision":           "Televize a video",
    "Lave-linge":           "Velké domácí spotřebiče",
    "Lave-vaisselle":       "Velké domácí spotřebiče",
    "Sèche-linge":          "Velké domácí spotřebiče",
    "Réfrigérateur":        "Velké domácí spotřebiče",
    "Aspirateur":           "Vysavače a úklid",
    "Smartphone":           "Telefony a tablety",
    "Téléphone portable":   "Telefony a tablety",
    "Ordinateur portable":  "Počítače a notebooky",
}

# Human-readable labels for the regulatory criteria (schema
# etalab/schema-indice-durabilite, v0.1.1) — stored alongside scores in the
# sub-scores JSON so downstream consumers don't need this file.
CRITERIA_LABELS: dict[str, str] = {
    "note_reparabilite": "Repairability block (A)",
    "note_fiabilite":    "Reliability block (B)",
    "note_A_c1":   "A1 Documentation",
    "note_A_c1.1": "A1.1 Documentation availability duration",
    "note_A_c1.2": "A1.2 Diagnosis & self-repair support",
    "note_A_c2":   "A2 Disassembly",
    "note_A_c2.1": "A2.1 Number of disassembly steps",
    "note_A_c2.2": "A2.2 Type of tools required",
    "note_A_c2.3": "A2.3 Type of fasteners",
    "note_A_c3":   "A3 Spare parts availability",
    "note_A_c3.1": "A3.1 Availability duration (list-2 parts)",
    "note_A_c3.2": "A3.2 Delivery delay (list-2 parts)",
    "note_A_c3.3": "A3.3 Availability duration (list-1 parts)",
    "note_A_c3.4": "A3.4 Delivery delay (list-1 parts)",
    "note_A_c4":   "A4 Spare parts price",
    "note_B_c1":   "B1 Resistance to stress & wear",
    "note_B_c1.1": "B1.1 Resistance to external stress",
    "note_B_c1.2": "B1.2 Resistance to wear",
    "note_B_c2":   "B2 Maintenance & servicing",
    "note_B_c2.1": "B2.1 Maintenance",
    "note_B_c2.2": "B2.2 Servicing / care",
    "note_B_c3":   "B3 Durability guarantee & quality process",
    "note_B_c3.1": "B3.1 Commercial durability guarantee duration",
    "note_B_c3.2": "B3.2 Continuous improvement process",
}

SUB_SCORE_KEYS = [k for k in CRITERIA_LABELS if k.startswith("note_")]


# ══════════════════════════════════════════════════════════════════════════════
#  Database setup
# ══════════════════════════════════════════════════════════════════════════════

DDL_ID_TABLE = """
CREATE TABLE IF NOT EXISTS fr_durability_index (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    id_unique                   TEXT    UNIQUE,
    id_modele                   TEXT,
    referentiel_id_modele       TEXT,
    ean                         TEXT,
    nom_modele                  TEXT,
    categorie_produit           TEXT,
    main_category               TEXT,
    id_metteur_sur_le_marche    TEXT,
    nom_metteur_sur_le_marche   TEXT,
    note_id                     REAL,
    note_reparabilite           REAL,
    note_fiabilite              REAL,
    note_a_c1                   REAL,
    note_a_c2                   REAL,
    note_a_c3                   REAL,
    note_a_c4                   REAL,
    note_b_c1                   REAL,
    note_b_c2                   REAL,
    note_b_c3                   REAL,
    accessibilite_compteur_usage REAL,
    sub_scores_json             TEXT,
    date_calcul                 TEXT,
    url_tableau_detail          TEXT,
    lien_documentation          TEXT,
    imported_at                 TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_id_ean
    ON fr_durability_index(ean)
    WHERE ean IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_id_category
    ON fr_durability_index(categorie_produit);
CREATE INDEX IF NOT EXISTS idx_id_brand
    ON fr_durability_index(nom_metteur_sur_le_marche);
"""

# Columns expected on the products table (added by
# migrate_add_french_durability.py; ensured here defensively)
PRODUCTS_EXTRA_COLS = [
    ("durability_score_fr",        "REAL"),
    ("durability_score_date",      "TEXT"),
    ("durability_sub_scores_json", "TEXT"),
]


def setup_db(conn: sqlite3.Connection) -> None:
    conn.executescript(DDL_ID_TABLE)
    conn.commit()
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(products)").fetchall()
    }
    for col, col_type in PRODUCTS_EXTRA_COLS:
        if col not in existing:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
            log.info(f"  Added column products.{col}")
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
#  Download
# ══════════════════════════════════════════════════════════════════════════════

def _http_get(url: str, timeout: int = 90) -> bytes:
    req = urlrequest.Request(
        url,
        headers={
            "User-Agent": (
                "QualityDB-Research/1.0 "
                "(dissertation research on product obsolescence; "
                "contact: academic use only)"
            ),
            "Accept": "text/csv,application/json,text/plain,*/*",
        },
    )
    for attempt in range(1, 4):
        try:
            with urlrequest.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urlerror.URLError as exc:
            log.warning(f"  Attempt {attempt}/3 failed: {exc}")
            if attempt < 3:
                time.sleep(15 * attempt)
            else:
                raise RuntimeError(f"Download failed after 3 attempts: {url}") from exc
    return b""  # unreachable


def download_csv(resource_id: str) -> str:
    url = RESOURCE_URL_TPL.format(rid=resource_id)
    log.info(f"  Downloading {url}")
    raw = _http_get(url)
    log.info(f"  Downloaded {len(raw):,} bytes ({len(raw)/1024/1024:.2f} MB)")
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("latin-1")


def discover_datasets() -> dict[str, dict]:
    """
    Query data.gouv.fr for consolidated 'indice de durabilité' datasets and
    return {category: {resource, dataset_page}} for every one found.
    Lets the importer pick up new categories (lave-vaisselle, smartphones…)
    as the AGEC law extends, without a code change.
    """
    found: dict[str, dict] = {}
    try:
        data = json.loads(_http_get(DISCOVER_URL))
    except Exception as exc:
        log.warning(f"  Discovery failed ({exc}); using built-in dataset list.")
        return found
    for ds in data.get("data", []):
        title = ds.get("title", "")
        if "ndice de durabilité" not in title or "consolidés" not in title:
            continue
        # Category trails the title after a SPACED hyphen:
        #   … schéma "Indice de durabilité - Lave-linge"
        # Splitting on a bare "-" tears hyphenated categories apart and yielded
        # a phantom "linge" dataset that re-imported Lave-linge a second time.
        cat = title.split(" - ")[-1].strip().strip('"» ').strip()
        try:
            full = json.loads(_http_get(
                f"https://www.data.gouv.fr/api/1/datasets/{ds['id']}/"
            ))
        except Exception:
            continue
        for r in full.get("resources", []):
            if "dernière version" in (r.get("title") or ""):
                found[cat] = {
                    "resource": r["id"],
                    "dataset_page": full.get("page", ""),
                }
                break
    if found:
        log.info(f"  Discovery: found {len(found)} consolidated dataset(s): "
                 f"{', '.join(found)}")
    return found


# ══════════════════════════════════════════════════════════════════════════════
#  Parsing + import
# ══════════════════════════════════════════════════════════════════════════════

def _safe_float(value, lo: float | None = None, hi: float | None = None):
    v = (value or "").strip().replace(",", ".")
    if not v:
        return None
    try:
        f = float(v)
    except ValueError:
        return None
    if lo is not None and f < lo:
        return None
    if hi is not None and f > hi:
        return None
    return f


def _sub_scores_json(row: dict) -> str | None:
    """
    Serialise every regulatory criterion & sub-criterion score to JSON, with
    the official label embedded:  {"note_A_c2.1": {"score": 9.2,
    "label": "A2.1 Number of disassembly steps"}, …}
    """
    sub: dict[str, dict] = {}
    for key in SUB_SCORE_KEYS:
        val = _safe_float(row.get(key, ""), lo=0.0, hi=10.0)
        if val is not None:
            sub[key] = {"score": round(val, 2), "label": CRITERIA_LABELS[key]}
    return json.dumps(sub, ensure_ascii=False) if sub else None


def import_csv(conn: sqlite3.Connection, csv_text: str, limit: int = 0) -> dict:
    now = datetime.datetime.now().isoformat()
    today = datetime.date.today().isoformat()

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    log.info(f"  CSV parsed: {len(rows):,} rows")

    if limit and limit < len(rows):
        log.info(f"  Limiting to first {limit:,} rows (--limit flag)")
        rows = rows[:limit]

    added = updated = linked = linked_fds = 0

    for row in rows:
        id_unique = (row.get("id_unique") or "").strip()
        if not id_unique:
            continue

        referentiel = (row.get("referentiel_id_modele") or "").strip()
        id_modele = (row.get("id_modele") or "").strip()
        ean = id_modele if referentiel == "GTIN_EAN" else None

        cat_fr = (row.get("categorie_produit") or "").strip()
        main_cat = None
        for fr_key, cz_val in CATEGORY_MAP.items():
            if cat_fr.lower().startswith(fr_key.lower()):
                main_cat = cz_val
                break

        note_id = _safe_float(row.get("note_id", ""), lo=0.0, hi=10.0)
        sub_json = _sub_scores_json(row)
        docs = (row.get("lien_documentation_particuliers")
                or row.get("lien_documentation_professionnels") or "").strip()

        record = (
            id_unique,
            id_modele,
            referentiel,
            ean,
            (row.get("nom_modele") or "").strip(),
            cat_fr,
            main_cat,
            (row.get("id_metteur_sur_le_marche") or "").strip(),
            (row.get("nom_metteur_sur_le_marche") or "").strip(),
            note_id,
            _safe_float(row.get("note_reparabilite", ""), 0.0, 10.0),
            _safe_float(row.get("note_fiabilite", ""), 0.0, 10.0),
            _safe_float(row.get("note_A_c1", ""), 0.0, 10.0),
            _safe_float(row.get("note_A_c2", ""), 0.0, 10.0),
            _safe_float(row.get("note_A_c3", ""), 0.0, 10.0),
            _safe_float(row.get("note_A_c4", ""), 0.0, 10.0),
            _safe_float(row.get("note_B_c1", ""), 0.0, 10.0),
            _safe_float(row.get("note_B_c2", ""), 0.0, 10.0),
            _safe_float(row.get("note_B_c3", ""), 0.0, 10.0),
            _safe_float(row.get("accessibilite_compteur_usage", "")),
            sub_json,
            (row.get("date_calcul") or "").strip(),
            (row.get("url_tableau_detail_notation") or "").strip(),
            docs,
            now,
        )

        existing = conn.execute(
            "SELECT id FROM fr_durability_index WHERE id_unique = ?",
            (id_unique,),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE fr_durability_index SET
                       id_modele=?, referentiel_id_modele=?, ean=?,
                       nom_modele=?, categorie_produit=?, main_category=?,
                       id_metteur_sur_le_marche=?, nom_metteur_sur_le_marche=?,
                       note_id=?, note_reparabilite=?, note_fiabilite=?,
                       note_a_c1=?, note_a_c2=?, note_a_c3=?, note_a_c4=?,
                       note_b_c1=?, note_b_c2=?, note_b_c3=?,
                       accessibilite_compteur_usage=?, sub_scores_json=?,
                       date_calcul=?, url_tableau_detail=?,
                       lien_documentation=?, imported_at=?
                   WHERE id_unique=?""",
                record[1:] + (id_unique,),
            )
            updated += 1
        else:
            conn.execute(
                """INSERT INTO fr_durability_index (
                       id_unique, id_modele, referentiel_id_modele, ean,
                       nom_modele, categorie_produit, main_category,
                       id_metteur_sur_le_marche, nom_metteur_sur_le_marche,
                       note_id, note_reparabilite, note_fiabilite,
                       note_a_c1, note_a_c2, note_a_c3, note_a_c4,
                       note_b_c1, note_b_c2, note_b_c3,
                       accessibilite_compteur_usage, sub_scores_json,
                       date_calcul, url_tableau_detail, lien_documentation,
                       imported_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                record,
            )
            added += 1

        # ── Cross-link 1: products table by EAN ──────────────────────────────
        if ean and note_id is not None:
            cur = conn.execute(
                """UPDATE products
                   SET durability_score_fr        = ?,
                       durability_score_date      = ?,
                       durability_sub_scores_json = ?
                   WHERE ean = ?
                     AND (durability_score_fr IS NULL
                          OR durability_score_fr != ?)""",
                (note_id, today, sub_json, ean, note_id),
            )
            linked += cur.rowcount

            # ── Cross-link 2: french_durability_scores lookup table ──────────
            cur = conn.execute(
                """UPDATE french_durability_scores
                   SET durability_score          = ?,
                       durability_score_date     = ?,
                       durability_reliability    = ?,
                       durability_repairability  = ?
                   WHERE ean = ?""",
                (note_id, today,
                 _safe_float(row.get("note_fiabilite", ""), 0.0, 10.0),
                 _safe_float(row.get("note_reparabilite", ""), 0.0, 10.0),
                 ean),
            )
            linked_fds += cur.rowcount

        if (added + updated) % 1000 == 0:
            conn.commit()

    conn.commit()
    return {"added": added, "updated": updated,
            "linked_products": linked, "linked_fds": linked_fds}


# ══════════════════════════════════════════════════════════════════════════════
#  Stats
# ══════════════════════════════════════════════════════════════════════════════

def print_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM fr_durability_index").fetchone()[0]
    with_ean = conn.execute(
        "SELECT COUNT(*) FROM fr_durability_index WHERE ean IS NOT NULL"
    ).fetchone()[0]
    print(f"\nfr_durability_index: {total:,} rows  ({with_ean:,} with EAN)")
    print("\nBy category:")
    for cat, n, avg in conn.execute(
        """SELECT categorie_produit, COUNT(*), ROUND(AVG(note_id),2)
           FROM fr_durability_index GROUP BY 1 ORDER BY 2 DESC"""
    ):
        print(f"  {cat:<25} {n:>6,}   avg note_id {avg}")
    print("\nTop brands:")
    for brand, n, avg in conn.execute(
        """SELECT nom_metteur_sur_le_marche, COUNT(*), ROUND(AVG(note_id),2)
           FROM fr_durability_index GROUP BY 1 ORDER BY 2 DESC LIMIT 12"""
    ):
        print(f"  {brand:<35} {n:>6,}   avg {avg}")
    n_linked = conn.execute(
        "SELECT COUNT(*) FROM products WHERE durability_score_fr IS NOT NULL"
    ).fetchone()[0]
    print(f"\nproducts with durability_score_fr: {n_linked:,}")


# ══════════════════════════════════════════════════════════════════════════════
#  Main
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(description="Import French durability index")
    ap.add_argument("--limit", type=int, default=0, help="max rows per dataset")
    ap.add_argument("--stats", action="store_true", help="print stats and exit")
    ap.add_argument("--discover", action="store_true",
                    help="query data.gouv.fr for new category datasets too")
    ap.add_argument("--db", default=DB_PATH, help="path to products.db")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        conn.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")
    except sqlite3.OperationalError:
        pass

    if args.stats:
        print_stats(conn)
        conn.close()
        return

    setup_db(conn)

    datasets = dict(DATASETS)
    if args.discover:
        # Dedupe on the RESOURCE id: two titles can name the same consolidated
        # file, and importing it twice silently doubles the update count.
        known = {d["resource"] for d in datasets.values()}
        for cat, info in discover_datasets().items():
            if info["resource"] in known or cat in datasets:
                log.info(f"  Discovery: skipping '{cat}' (already covered)")
                continue
            datasets[cat] = info
            known.add(info["resource"])

    log.info("=" * 60)
    log.info(f"Importing French durability index — {len(datasets)} dataset(s)")
    totals = {"added": 0, "updated": 0, "linked_products": 0, "linked_fds": 0}

    for cat, info in datasets.items():
        log.info(f"── {cat}")
        try:
            csv_text = download_csv(info["resource"])
            res = import_csv(conn, csv_text, limit=args.limit)
        except Exception as exc:
            log.error(f"  FAILED: {exc}")
            continue
        log.info(f"  {cat}: +{res['added']} new, {res['updated']} updated, "
                 f"{res['linked_products']} products linked by EAN, "
                 f"{res['linked_fds']} french_durability_scores rows enriched")
        for k in totals:
            totals[k] += res[k]

    log.info("=" * 60)
    log.info(f"Done.  new={totals['added']}  updated={totals['updated']}  "
             f"products_linked={totals['linked_products']}  "
             f"fds_enriched={totals['linked_fds']}")
    print_stats(conn)
    conn.close()


if __name__ == "__main__":
    main()
