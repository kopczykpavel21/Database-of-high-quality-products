#!/usr/bin/env python3
"""
migrate_add_german_support.py
─────────────────────────────
Run ONCE on an existing products.db to add multi-country / multi-currency support.

What it does
  1. Adds  country   TEXT  DEFAULT 'CZ'
  2. Adds  currency  TEXT  DEFAULT 'CZK'
  3. Adds  Price_EUR REAL  (NULL for Czech products)
  4. Back-fills country='CZ' and currency='CZK' on all existing rows.
  5. Reports counts before/after for sanity.

Usage
  python3 migrate_add_german_support.py                    # uses ./products.db
  python3 migrate_add_german_support.py /path/to/products.db
"""

import sqlite3
import sys
import os

DB_DEFAULT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")


def column_exists(conn, table, column):
    cur = conn.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def migrate(db_path):
    if not os.path.exists(db_path):
        print(f"ERROR: database not found at {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # ── count before ──────────────────────────────────────────────────────────
    total_before = cur.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    print(f"Products before migration : {total_before:,}")

    # ── add columns (idempotent) ───────────────────────────────────────────────
    migrations = [
        ("country",   "TEXT",  "'CZ'"),
        ("currency",  "TEXT",  "'CZK'"),
        ("Price_EUR", "REAL",  "NULL"),
    ]

    for col, typ, default in migrations:
        if column_exists(conn, "products", col):
            print(f"  Column '{col}' already exists — skipping.")
        else:
            conn.execute(
                f"ALTER TABLE products ADD COLUMN {col} {typ} DEFAULT {default}"
            )
            print(f"  Added column '{col}' ({typ}, default {default}).")

    # ── back-fill existing Czech rows ─────────────────────────────────────────
    updated = conn.execute(
        "UPDATE products SET country='CZ', currency='CZK' WHERE country IS NULL"
    ).rowcount
    print(f"  Back-filled {updated:,} existing rows → country='CZ', currency='CZK'.")

    conn.commit()

    # ── verify ────────────────────────────────────────────────────────────────
    total_after = cur.execute("SELECT COUNT(*) FROM products").fetchone()[0]
    cz_count    = cur.execute("SELECT COUNT(*) FROM products WHERE country='CZ'").fetchone()[0]
    de_count    = cur.execute("SELECT COUNT(*) FROM products WHERE country='DE'").fetchone()[0]

    print(f"\nMigration complete.")
    print(f"  Total products : {total_after:,}")
    print(f"  CZ products    : {cz_count:,}")
    print(f"  DE products    : {de_count:,}  (0 expected on first run)")

    conn.close()


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DB_DEFAULT
    migrate(db_path)
