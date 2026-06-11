#!/usr/bin/env python3
"""
indicereparabilite_scraper.py — French official Repairability Index importer
══════════════════════════════════════════════════════════════════════════════

Downloads the consolidated Indice de Réparabilité dataset from the French
government open-data portal (data.gouv.fr) and imports it into QualityDB.

Data source  : Fichiers consolidés — schema etalab/schema-indice-reparabilite
Licence      : Licence Ouverte / Open Licence v2.0 (free reuse, incl. academic)
Dataset URL  : https://www.data.gouv.fr/fr/datasets/
               fichiers-consolides-des-donnees-respectant-le-schema-indice-de-reparabilite
Direct CSV   : https://www.data.gouv.fr/api/1/datasets/r/b0fddb1d-7032-4e30-a055-9d923e94ce59
Updated      : daily by data.gouv.fr (~2.2 MB, ~thousands of product models)

What the data contains
──────────────────────
Each row = one product model with a legally-mandated repairability score:
  note_ir     (0–10) — overall repairability index
  note_c1..c5 (0–10) — five regulatory criteria:
    C1: documentation availability
    C2: disassembly / tooling
    C3: spare parts availability & pricing
    C4: spare parts ordering delay
    C5: product-specific criteria (varies by category)
  Sub-criteria note_c2.1…c5.3 for granular academic analysis.
  nom_metteur_sur_le_marche — brand/manufacturer name
  categorie_produit          — product category (French)
  id_modele / referentiel    — EAN-13 or internal reference

Academic value for Pillar I / Pillar II (Ds v2.0)
──────────────────────────────────────────────────
This is the ONLY market where manufacturers are LEGALLY REQUIRED to publish
a standardised repairability score (since 2021, law n° 2020-105).  The data
allows cross-market BCI comparison (CZ/SK/PL/DE have no equivalent mandate).

Linking strategy
────────────────
1. All records → stored in fr_repairability_index table (full academic archive)
2. Records where referentiel_id_modele = 'GTIN_EAN' → EAN matched against
   products.ean → repairability_score_fr updated on matched products
3. Sub-criterion scores serialised as JSON → repairability_sub_scores_json

Usage
─────
  python3 scraper/indicereparabilite_scraper.py          # full import
  python3 scraper/indicereparabilite_scraper.py --stats  # print DB table stats
  python3 scraper/indicereparabilite_scraper.py --limit 500  # import first N rows
"""

from __future__ import annotations

import os
import sys
import csv
import json
import time
import sqlite3
import logging
import datetime
import argparse
import io

try:
    import urllib.request as urlrequest
    import urllib.error as urlerror
except ImportError:
    pass  # Python 3 always has these

# ── Path setup ────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from scraper.config import DB_PATH, JOURNAL_MODE
except ImportError:
    DB_PATH = os.path.join(BASE_DIR, "products.db")

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("indice_reparabilite")

# ── Data source ───────────────────────────────────────────────────────────────
# Stable redirect URL — always resolves to the latest consolidated CSV.
CSV_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/"
    "b0fddb1d-7032-4e30-a055-9d923e94ce59"
)
DATASET_PAGE = (
    "https://www.data.gouv.fr/fr/datasets/"
    "fichiers-consolides-des-donnees-respectant-le-schema-indice-de-reparabilite"
)

# Category mapping: French → QualityDB main_category
CATEGORY_MAP: dict[str, str] = {
    "Smartphone":                       "Telefony a tablety",
    "Téléphone portable":               "Telefony a tablety",
    "Tablette":                         "Telefony a tablety",
    "Ordinateur portable":              "Počítače a notebooky",
    "Ordinateur":                       "Počítače a notebooky",
    "Télévision":                       "Televize a video",
    "Téléviseur":                       "Televize a video",
    "Lave-linge hublot":                "Velké domácí spotřebiče",
    "Lave-linge top":                   "Velké domácí spotřebiče",
    "Lave-linge":                       "Velké domácí spotřebiče",
    "Lave-vaisselle":                   "Velké domácí spotřebiče",
    "Réfrigérateur":                    "Velké domácí spotřebiče",
    "Congélateur":                      "Velké domácí spotřebiče",
    "Four":                             "Velké domácí spotřebiče",
    "Cuisinière":                       "Velké domácí spotřebiče",
    "Aspirateur filaire":               "Vysavače a úklid",
    "Aspirateur sans fil":              "Vysavače a úklid",
    "Aspirateur robot":                 "Vysavače a úklid",
    "Aspirateur non filaire":           "Vysavače a úklid",
    "Aspirateur":                       "Vysavače a úklid",
    "Tondeuse à gazon":                 "Zahrada a dílna",
    "Trottinette électrique":           "Doprava",
    "Vélo électrique":                  "Doprava",
    "Appareil photo numérique":         "Foto a kamery",
    "Imprimante":                       "Periférie a příslušenství",
    "Perceuse":                         "Zahrada a dílna",
    "Perceuse-visseuse":                "Zahrada a dílna",
    # Additional categories present in the dataset (added 2025)
    "Nettoyeur haute pression":         "Zahrada a dílna",
    "Karcher":                          "Zahrada a dílna",
    "Tronçonneuse":                     "Zahrada a dílna",
    "Débroussailleuse":                 "Zahrada a dílna",
    "Casque":                           "Zvuk a hudba",
    "Écouteurs":                        "Zvuk a hudba",
    "Enceinte":                         "Zvuk a hudba",
    "Climatiseur":                      "Velké domácí spotřebiče",
    "Pompe à chaleur":                  "Velké domácí spotřebiče",
    "Chauffe-eau":                      "Velké domácí spotřebiče",
    "Sèche-linge":                      "Velké domácí spotřebiče",
    "Appareil photo":                   "Foto a kamery",
    "Caméra":                           "Foto a kamery",
    "Console de jeux":                  "Hry a konzole",
    "Manette":                          "Hry a konzole",
    "Montre connectée":                 "Chytré zařízení",
}


# ══════════════════════════════════════════════════════════════════════════════
#  Database setup
# ══════════════════════════════════════════════════════════════════════════════

DDL_IR_TABLE = """
CREATE TABLE IF NOT EXISTS fr_repairability_index (
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
    note_ir                     REAL,
    note_c1                     REAL,
    note_c2                     REAL,
    note_c3                     REAL,
    note_c4                     REAL,
    note_c5                     REAL,
    sub_scores_json             TEXT,
    date_calcul                 TEXT,
    url_tableau_detail          TEXT,
    imported_at                 TEXT    NOT NULL,
    last_modified               TEXT
);
CREATE INDEX IF NOT EXISTS idx_ir_ean
    ON fr_repairability_index(ean)
    WHERE ean IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ir_category
    ON fr_repairability_index(categorie_produit);
CREATE INDEX IF NOT EXISTS idx_ir_brand
    ON fr_repairability_index(nom_metteur_sur_le_marche);
"""

# Columns to add to the main products table (if not already present)
PRODUCTS_EXTRA_COLS = [
    ("repairability_score_fr",       "REAL"),
    ("repairability_score_date",     "TEXT"),
    ("repairability_sub_scores_json","TEXT"),
]


def setup_db(conn: sqlite3.Connection) -> None:
    """Create the fr_repairability_index table and ensure products columns exist."""
    conn.executescript(DDL_IR_TABLE)
    conn.commit()

    # Ensure the extra columns exist on the products table
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(products)").fetchall()
    }
    for col, col_type in PRODUCTS_EXTRA_COLS:
        if col not in existing:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
            log.info(f"  Added column products.{col}")
    conn.commit()


# ══════════════════════════════════════════════════════════════════════════════
#  CSV download
# ══════════════════════════════════════════════════════════════════════════════

def download_csv(url: str = CSV_URL, timeout: int = 60) -> str:
    """
    Download the CSV and return its contents as a string.
    Follows HTTP redirects (data.gouv.fr uses a redirect to the actual file).
    """
    log.info(f"Downloading repairability CSV from data.gouv.fr …")
    log.info(f"  URL: {url}")

    req = urlrequest.Request(
        url,
        headers={
            "User-Agent": (
                "QualityDB-Research/1.0 "
                "(dissertation research on product obsolescence; "
                "contact: academic use only)"
            ),
            "Accept": "text/csv,text/plain,*/*",
        },
    )

    for attempt in range(1, 4):
        try:
            with urlrequest.urlopen(req, timeout=timeout) as resp:
                content_length = resp.headers.get("Content-Length", "?")
                log.info(f"  HTTP {resp.status}  size={content_length} bytes")
                raw = resp.read()
                # data.gouv.fr serves UTF-8; some older files may be Latin-1
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    text = raw.decode("latin-1")
                log.info(f"  Downloaded {len(raw):,} bytes ({len(raw)/1024/1024:.2f} MB)")
                return text

        except urlerror.URLError as exc:
            log.warning(f"  Attempt {attempt}/3 failed: {exc}")
            if attempt < 3:
                time.sleep(15 * attempt)
            else:
                raise RuntimeError(
                    f"Failed to download repairability CSV after 3 attempts: {exc}"
                ) from exc

    return ""  # unreachable


# ══════════════════════════════════════════════════════════════════════════════
#  CSV parsing + import
# ══════════════════════════════════════════════════════════════════════════════

def _safe_float(value: str, lo: float | None = None, hi: float | None = None) -> float | None:
    """
    Parse a float from a CSV cell; return None on empty/invalid.
    If lo/hi are provided, values outside that range are rejected as data errors.
    """
    v = value.strip().replace(",", ".")
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
    Serialise all sub-criterion scores to JSON.
    Returns None (not '{}') when no sub-scores are present, so the DB column
    stays NULL rather than storing an empty JSON object.
    """
    sub: dict[str, float] = {}
    for key in ("note_c2.1", "note_c2.2", "note_c2.3",
                "note_c3.1", "note_c3.2", "note_c3.3", "note_c3.4",
                "note_c5.1", "note_c5.2", "note_c5.3"):
        val = _safe_float(row.get(key, ""), lo=0.0, hi=10.0)
        if val is not None:
            sub[key] = val
    return json.dumps(sub, ensure_ascii=False) if sub else None


def import_csv(
    conn: sqlite3.Connection,
    csv_text: str,
    limit: int = 0,
) -> dict:
    """
    Parse the CSV and upsert rows into fr_repairability_index.
    Then cross-link with products by EAN.
    Returns {"added": N, "updated": N, "linked": N}.
    """
    now = datetime.datetime.now().isoformat()
    today = datetime.date.today().isoformat()

    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)
    log.info(f"  CSV parsed: {len(rows):,} rows, "
             f"columns: {', '.join(list(rows[0].keys())[:8] if rows else [])}")

    if limit and limit < len(rows):
        log.info(f"  Limiting to first {limit:,} rows (--limit flag)")
        rows = rows[:limit]

    added = updated = linked = 0

    for row in rows:
        id_unique = row.get("id_unique", "").strip()
        if not id_unique:
            continue

        referentiel = row.get("referentiel_id_modele", "").strip()
        id_modele   = row.get("id_modele", "").strip()

        # EAN is only valid when the reference type is GTIN_EAN
        ean = id_modele if referentiel == "GTIN_EAN" else None

        # Map to QualityDB main_category
        cat_fr = row.get("categorie_produit", "").strip()
        main_cat = None
        for fr_key, cz_val in CATEGORY_MAP.items():
            if cat_fr.lower().startswith(fr_key.lower()):
                main_cat = cz_val
                break
        if main_cat is None and cat_fr:
            log.debug(f"  Unmapped category: '{cat_fr}' — stored without main_category")

        note_ir = _safe_float(row.get("note_ir", ""), lo=0.0, hi=10.0)
        sub_json = _sub_scores_json(row)

        record = (
            id_unique,
            id_modele,
            referentiel,
            ean,
            row.get("nom_modele", "").strip(),
            cat_fr,
            main_cat,
            row.get("id_metteur_sur_le_marche", "").strip(),
            row.get("nom_metteur_sur_le_marche", "").strip(),
            note_ir,
            _safe_float(row.get("note_c1", ""), lo=0.0, hi=10.0),
            _safe_float(row.get("note_c2", ""), lo=0.0, hi=10.0),
            _safe_float(row.get("note_c3", ""), lo=0.0, hi=10.0),
            _safe_float(row.get("note_c4", ""), lo=0.0, hi=10.0),
            _safe_float(row.get("note_c5", ""), lo=0.0, hi=10.0),
            sub_json,
            row.get("date_calcul", "").strip(),
            row.get("url_tableau_detail_notation", "").strip(),
            now,
            row.get("last_modified", "").strip() or None,
        )

        # Check if record already exists
        existing = conn.execute(
            "SELECT id FROM fr_repairability_index WHERE id_unique = ?",
            (id_unique,),
        ).fetchone()

        if existing:
            conn.execute(
                """UPDATE fr_repairability_index SET
                       id_modele=?, referentiel_id_modele=?, ean=?,
                       nom_modele=?, categorie_produit=?, main_category=?,
                       id_metteur_sur_le_marche=?, nom_metteur_sur_le_marche=?,
                       note_ir=?, note_c1=?, note_c2=?, note_c3=?, note_c4=?,
                       note_c5=?, sub_scores_json=?, date_calcul=?,
                       url_tableau_detail=?, imported_at=?, last_modified=?
                   WHERE id_unique=?""",
                record[1:] + (id_unique,),
            )
            updated += 1
        else:
            conn.execute(
                """INSERT INTO fr_repairability_index (
                       id_unique, id_modele, referentiel_id_modele, ean,
                       nom_modele, categorie_produit, main_category,
                       id_metteur_sur_le_marche, nom_metteur_sur_le_marche,
                       note_ir, note_c1, note_c2, note_c3, note_c4, note_c5,
                       sub_scores_json, date_calcul, url_tableau_detail,
                       imported_at, last_modified)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                record,
            )
            added += 1

        # Cross-link to products table by EAN
        if ean and note_ir is not None:
            cursor = conn.execute(
                """UPDATE products
                   SET repairability_score_fr       = ?,
                       repairability_score_date     = ?,
                       repairability_sub_scores_json = ?
                   WHERE ean = ?
                     AND (repairability_score_fr IS NULL
                          OR repairability_score_fr != ?)""",
                (note_ir, today, sub_json, ean, note_ir),
            )
            linked += cursor.rowcount

        if (added + updated) % 1000 == 0:
            conn.commit()

    conn.commit()
    return {"added": added, "updated": updated, "linked": linked}


# ══════════════════════════════════════════════════════════════════════════════
#  Stats
# ══════════════════════════════════════════════════════════════════════════════

def print_stats(conn: sqlite3.Connection) -> None:
    """Print summary statistics about the imported data."""
    total = conn.execute(
        "SELECT COUNT(*) FROM fr_repairability_index"
    ).fetchone()[0]
    with_ean = conn.execute(
        "SELECT COUNT(*) FROM fr_repairability_index WHERE ean IS NOT NULL"
    ).fetchone()[0]
    avg_ir = conn.execute(
        "SELECT ROUND(AVG(note_ir),2) FROM fr_repairability_index WHERE note_ir IS NOT NULL"
    ).fetchone()[0]
    categories = conn.execute(
        "SELECT categorie_produit, COUNT(*) c FROM fr_repairability_index "
        "GROUP BY categorie_produit ORDER BY c DESC LIMIT 10"
    ).fetchall()
    linked = conn.execute(
        "SELECT COUNT(*) FROM products WHERE repairability_score_fr IS NOT NULL"
    ).fetchone()[0]
    unmapped = conn.execute(
        "SELECT COUNT(*) FROM fr_repairability_index WHERE main_category IS NULL"
    ).fetchone()[0]

    print(f"\n{'═'*55}")
    print(f"  Indice de Réparabilité — Database Statistics")
    print(f"{'═'*55}")
    print(f"  Total records in fr_repairability_index : {total:>8,}")
    print(f"  Records with EAN (GTIN_EAN)              : {with_ean:>8,}")
    print(f"  Records without mapped main_category     : {unmapped:>8,}")
    print(f"  Average note_ir (0–10)                   : {avg_ir:>8}")
    print(f"  Products linked in products table        : {linked:>8,}")
    print(f"\n  Top categories:")
    for cat, cnt in categories:
        mapped = "✓" if any(cat.lower().startswith(k.lower()) for k in CATEGORY_MAP) else "?"
        print(f"    {mapped} {cat:<38} {cnt:>6,}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

def run_scraper() -> dict:
    """
    Scheduler entry point. Downloads the CSV, imports it, returns stats.
    Called by scheduler.py as: add("Indice Réparabilité FR", "FR", "monthly", run_scraper)
    """
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║  Indice de Réparabilité — data.gouv.fr import        ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    log.info(f"  Source  : {DATASET_PAGE}")
    log.info(f"  Licence : Licence Ouverte / Open Licence v2.0")

    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")

    try:
        setup_db(conn)
        csv_text = download_csv()
        stats = import_csv(conn, csv_text)

        log.info(
            f"✓  Import complete — "
            f"+{stats['added']:,} new  "
            f"~{stats['updated']:,} updated  "
            f"{stats['linked']:,} products linked by EAN"
        )
        return {
            "added":   stats["added"],
            "updated": stats["updated"],
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    global DB_PATH  # may be overridden by --db flag

    ap = argparse.ArgumentParser(
        description="Import French official Repairability Index from data.gouv.fr"
    )
    ap.add_argument("--stats",  action="store_true",
                    help="Print database statistics and exit")
    ap.add_argument("--limit",  type=int, default=0, metavar="N",
                    help="Import only the first N rows (for testing)")
    ap.add_argument("--db",     default=DB_PATH, metavar="PATH",
                    help=f"Database path (default: {DB_PATH})")
    args = ap.parse_args()

    DB_PATH = args.db

    conn = sqlite3.connect(DB_PATH)
    conn.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")

    if args.stats:
        try:
            print_stats(conn)
        finally:
            conn.close()
        return

    try:
        setup_db(conn)

        csv_text = download_csv()
        stats = import_csv(conn, csv_text, limit=args.limit)

        log.info(
            f"✓  Import complete — "
            f"+{stats['added']:,} new  "
            f"~{stats['updated']:,} updated  "
            f"{stats['linked']:,} products linked by EAN"
        )
        print_stats(conn)

    except Exception as exc:
        conn.rollback()
        log.error(f"Import failed: {exc}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
