#!/usr/bin/env python3
"""
migrate_add_french_durability.py
─────────────────────────────────
Run ONCE on an existing products.db to add French durability/repairability
index columns plus broader product lifecycle & obsolescence research fields.

Background
  France's loi AGEC introduced the *indice de réparabilité* (repairability
  index, 0–10) in Jan 2021 and expanded it to the *indice de durabilité*
  (durability index, 0–10) from Jan 2024.  Both scores are legally required
  on product pages for covered categories: smartphones, laptops, TVs,
  washing machines, dishwashers, vacuum cleaners, lawnmowers.

  This migration also adds:
    • normalised brand / model columns for cross-country matching
    • energy efficiency label (EU ecodesign label)
    • product release year and discontinuation flag
    • warranty and spare-parts availability (from detail pages)
    • cross-match fields (French score linked onto CZ/DE records)
    • a separate `french_durability_scores` lookup table that acts as the
      canonical source for scores (many-to-one: multiple CZ/DE products can
      map to one French scored product)

Usage
    python3 migrate_add_french_durability.py                     # ./products.db
    python3 migrate_add_french_durability.py /path/to/products.db
"""

import sqlite3
import sys
import os

DB_DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")


# ── helpers ───────────────────────────────────────────────────────────────────

def column_exists(conn, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def table_exists(conn, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    )
    return cur.fetchone() is not None


def add_column(conn, table: str, col: str, col_type: str, default=None):
    """Add a column if it doesn't already exist.  Returns True if added."""
    if column_exists(conn, table, col):
        print(f"  ✓  '{col}' already exists — skipping.")
        return False
    default_clause = f" DEFAULT {default}" if default is not None else ""
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}{default_clause}")
    print(f"  +  Added '{col}' ({col_type}{default_clause}).")
    return True


# ── main migration ────────────────────────────────────────────────────────────

def migrate(db_path: str):
    if not os.path.exists(db_path):
        print(f"ERROR: database not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    conn.execute(f"PRAGMA journal_mode={os.environ.get('JOURNAL_MODE', 'wal').upper()}")

    total_before = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    print(f"\nProducts before migration : {total_before:,}")
    print("─" * 60)

    # ══════════════════════════════════════════════════════════════════════════
    # A.  French durability / repairability index — scores stored on the product
    #     row that was scraped from Fnac/Darty (country='FR').
    #     For CZ/DE rows, these are populated by link_french_scores.py.
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[A] French durability / repairability index columns")

    add_column(conn, "products", "repairability_score_fr",      "REAL")          # 0–10
    add_column(conn, "products", "repairability_score_date",    "TEXT")          # ISO date scraped
    add_column(conn, "products", "repairability_sub_scores_json","TEXT")         # JSON sub-criteria
    add_column(conn, "products", "durability_score_fr",         "REAL")          # 0–10 (2024+)
    add_column(conn, "products", "durability_score_date",       "TEXT")
    add_column(conn, "products", "durability_sub_scores_json",  "TEXT")          # JSON sub-criteria
    add_column(conn, "products", "fr_source_url",               "TEXT")          # canonical FR URL

    # ══════════════════════════════════════════════════════════════════════════
    # B.  Brand / model normalisation — extracted from Name by link_french_scores.py
    #     or by a simple regex pass.  Used as the primary matching key.
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[B] Brand / model normalisation columns")

    add_column(conn, "products", "brand",            "TEXT")   # e.g. "Samsung", "Apple"
    add_column(conn, "products", "model_normalized", "TEXT")   # e.g. "Galaxy S24 Ultra"
    add_column(conn, "products", "model_number",     "TEXT")   # OEM model code, e.g. "SM-S928B"
    add_column(conn, "products", "ean",              "TEXT")   # EAN-13 barcode (if available)

    # ══════════════════════════════════════════════════════════════════════════
    # C.  Product lifecycle & planned obsolescence research fields
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[C] Product lifecycle & obsolescence fields")

    add_column(conn, "products", "release_year",            "INTEGER")    # year product launched
    add_column(conn, "products", "release_date",            "TEXT")       # ISO date if known
    add_column(conn, "products", "discontinued",            "INTEGER", 0) # 1 = no longer sold
    add_column(conn, "products", "discontinued_date",       "TEXT")       # when pulled from sale
    add_column(conn, "products", "software_support_end",    "TEXT")       # ISO date (phones/laptops)
    add_column(conn, "products", "spare_parts_available",   "INTEGER")    # 1/0/NULL
    add_column(conn, "products", "spare_parts_years",       "REAL")       # manufacturer commitment
    add_column(conn, "products", "warranty_years",          "REAL")       # standard warranty
    add_column(conn, "products", "warranty_extended_years", "REAL")       # optional extended
    add_column(conn, "products", "repairability_notes",     "TEXT")       # free-text from index page

    # ══════════════════════════════════════════════════════════════════════════
    # D.  EU ecodesign / energy label fields
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[D] EU energy label / ecodesign fields")

    add_column(conn, "products", "energy_class",         "TEXT")   # e.g. "A", "B", "A+++"
    add_column(conn, "products", "energy_kwh_per_year",  "REAL")   # annual consumption
    add_column(conn, "products", "noise_db",             "REAL")   # dB (appliances)
    add_column(conn, "products", "weight_kg",            "REAL")

    # ══════════════════════════════════════════════════════════════════════════
    # E.  Cross-country matching metadata (populated by link_french_scores.py)
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[E] Cross-country match fields")

    add_column(conn, "products", "fr_matched_product_id", "INTEGER")  # FK → products.id (FR row)
    add_column(conn, "products", "fr_match_confidence",   "REAL")     # 0.0–1.0
    add_column(conn, "products", "fr_match_method",       "TEXT")     # 'ean'|'model_exact'|'fuzzy'
    add_column(conn, "products", "fr_match_date",         "TEXT")     # ISO date match was run

    # ══════════════════════════════════════════════════════════════════════════
    # F.  Separate canonical French scores lookup table
    #     Allows many CZ/DE products to reference one scored FR record without
    #     duplicating score data across thousands of rows.
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[F] Creating french_durability_scores lookup table")

    if not table_exists(conn, "french_durability_scores"):
        conn.execute("""
            CREATE TABLE french_durability_scores (
                id                          INTEGER PRIMARY KEY AUTOINCREMENT,
                brand                       TEXT,
                model_normalized            TEXT,
                model_number                TEXT,
                ean                         TEXT,

                -- Indice de réparabilité (since 2021)
                repairability_score         REAL,           -- 0–10
                repairability_score_date    TEXT,
                -- Sub-criteria scores (stored individually for analysis)
                repair_documentation        REAL,           -- 0–10
                repair_spare_parts          REAL,
                repair_price_spare_parts    REAL,
                repair_software             REAL,
                repair_ease                 REAL,

                -- Indice de durabilité (since 2024, replaces repairability for some categories)
                durability_score            REAL,           -- 0–10
                durability_score_date       TEXT,
                -- Sub-criteria
                durability_reliability      REAL,
                durability_spare_parts_avail REAL,
                durability_spare_parts_years REAL,
                durability_software_updates  REAL,
                durability_repairability    REAL,           -- 0–10 (sub-component)

                -- Source metadata
                fr_product_url              TEXT,
                fr_retailer                 TEXT,           -- 'fnac'|'darty'|'boulanger'
                category                    TEXT,
                main_category               TEXT,
                scraped_at                  TEXT,

                -- Unique constraint: one row per product+retailer snapshot
                UNIQUE(ean, fr_retailer),
                UNIQUE(model_number, brand, fr_retailer)
            )
        """)
        print("  +  Created table 'french_durability_scores'.")
    else:
        print("  ✓  Table 'french_durability_scores' already exists — skipping.")

    # ══════════════════════════════════════════════════════════════════════════
    # G.  New indexes for research queries
    # ══════════════════════════════════════════════════════════════════════════
    print("\n[G] Adding indexes for research queries")

    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_brand          ON products(brand);
        CREATE INDEX IF NOT EXISTS idx_model_norm     ON products(model_normalized);
        CREATE INDEX IF NOT EXISTS idx_ean            ON products(ean);
        CREATE INDEX IF NOT EXISTS idx_release_year   ON products(release_year);
        CREATE INDEX IF NOT EXISTS idx_repair_score   ON products(repairability_score_fr);
        CREATE INDEX IF NOT EXISTS idx_durability_fr  ON products(durability_score_fr);
        CREATE INDEX IF NOT EXISTS idx_fr_matched     ON products(fr_matched_product_id);
        CREATE INDEX IF NOT EXISTS idx_fds_brand      ON french_durability_scores(brand);
        CREATE INDEX IF NOT EXISTS idx_fds_ean        ON french_durability_scores(ean);
        CREATE INDEX IF NOT EXISTS idx_fds_model      ON french_durability_scores(model_number);
    """)
    print("  +  Indexes created / verified.")

    conn.commit()

    # ── summary ────────────────────────────────────────────────────────────────
    total_after = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    fr_count    = conn.execute("SELECT COUNT(*) FROM products WHERE country='FR'").fetchone()[0]
    scored      = conn.execute(
        "SELECT COUNT(*) FROM products WHERE repairability_score_fr IS NOT NULL"
    ).fetchone()[0]

    print("\n" + "─" * 60)
    print("Migration complete.")
    print(f"  Total products         : {total_after:,}")
    print(f"  FR products            : {fr_count:,}  (0 until fnac_scraper.py runs)")
    print(f"  Products with FR score : {scored:,}  (0 until link_french_scores.py runs)")
    print("\nNext steps:")
    print("  1. python3 scraper/fnac_scraper.py        # scrape Fnac.fr for FR products + scores")
    print("  2. python3 link_french_scores.py          # match FR scores onto CZ/DE records")

    conn.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DB_DEFAULT
    migrate(db_path)
