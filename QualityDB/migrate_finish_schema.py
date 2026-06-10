#!/usr/bin/env python3
"""
migrate_finish_schema.py
─────────────────────────
Run ONCE (after migrate_add_german_support.py and migrate_add_french_durability.py)
to add the remaining columns/tables that server.py's query_products() and the
scheduler/scraper modules expect, but which are normally only created lazily by
individual scrapers (details_json, test_date) or the scheduler (scraper_runs) /
the French repairability scraper (fr_repairability_index) on their first run.

Safe to re-run — every change is guarded with IF NOT EXISTS / column checks.

Usage
    python3 migrate_finish_schema.py                    # uses ./products.db
    python3 migrate_finish_schema.py /path/to/products.db
"""

import sqlite3
import sys
import os

DB_DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")


def column_exists(conn, table, column):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def table_exists(conn, table):
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return cur.fetchone() is not None


def migrate(db_path):
    if not os.path.exists(db_path):
        print(f"ERROR: database not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)

    # ── products: details_json / test_date ───────────────────────────────────
    for col, col_type in [("details_json", "TEXT"), ("test_date", "TEXT")]:
        if column_exists(conn, "products", col):
            print(f"  Column 'products.{col}' already exists — skipping.")
        else:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {col_type}")
            print(f"  Added column 'products.{col}' ({col_type}).")

    # ── scraper_runs (normally created by scraper/scheduler.py) ──────────────
    if table_exists(conn, "scraper_runs"):
        print("  Table 'scraper_runs' already exists — skipping.")
    else:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS scraper_runs (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                scraper_name     TEXT    NOT NULL,
                market           TEXT    NOT NULL,
                started_at       TEXT    NOT NULL,
                finished_at      TEXT,
                status           TEXT    NOT NULL DEFAULT 'running',
                products_added   INTEGER DEFAULT 0,
                products_updated INTEGER DEFAULT 0,
                error_msg        TEXT,
                duration_sec     REAL
            );
            CREATE INDEX IF NOT EXISTS idx_runs_name_date
                ON scraper_runs(scraper_name, started_at);
        """)
        print("  Created table 'scraper_runs'.")

    # ── fr_repairability_index (normally created by indicereparabilite_scraper.py)
    if table_exists(conn, "fr_repairability_index"):
        print("  Table 'fr_repairability_index' already exists — skipping.")
    else:
        conn.executescript("""
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
        """)
        print("  Created table 'fr_repairability_index'.")

    conn.commit()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DB_DEFAULT
    migrate(db_path)
