"""
fix_product_url_unique.py
=========================
One-time migration: adds a UNIQUE index on products.ProductURL.

The Otto/Amazon/Heureka/Zbozi scrapers all use:
    INSERT INTO products ... ON CONFLICT(ProductURL) DO UPDATE SET ...
This syntax requires ProductURL to have a UNIQUE constraint in SQLite.
Without it every insert throws:
    "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"

Steps:
  1. Find rows with duplicate ProductURL (keep row with highest id = most recent)
  2. Delete the older duplicates
  3. CREATE UNIQUE INDEX on ProductURL

Run with the server STOPPED to avoid write conflicts:
    python3 fix_product_url_unique.py
"""

import sqlite3, os, sys

DB = os.path.join(os.path.dirname(__file__), "products.db")

def main():
    if not os.path.exists(DB):
        print(f"ERROR: {DB} not found"); sys.exit(1)

    conn = sqlite3.connect(DB, timeout=60)
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=NORMAL")

    # --- Step 1: count duplicates
    dups = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT ProductURL FROM products
            WHERE ProductURL IS NOT NULL
            GROUP BY ProductURL HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    print(f"Duplicate ProductURL groups: {dups}")

    if dups > 0:
        # --- Step 2: delete older duplicates (keep highest id per URL)
        print("Removing older duplicates (keeping highest id per URL)…")
        conn.execute("""
            DELETE FROM products
            WHERE id NOT IN (
                SELECT MAX(id) FROM products
                WHERE ProductURL IS NOT NULL
                GROUP BY ProductURL
            )
            AND ProductURL IS NOT NULL
        """)
        removed = conn.total_changes
        conn.commit()
        print(f"  Removed {removed} duplicate rows")

    # Verify no duplicates remain
    remaining = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT ProductURL FROM products
            WHERE ProductURL IS NOT NULL
            GROUP BY ProductURL HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    if remaining > 0:
        print(f"ERROR: {remaining} duplicate groups still present — aborting")
        conn.close(); sys.exit(1)

    # --- Step 3: create UNIQUE index
    print("Creating UNIQUE INDEX on ProductURL…")
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_product_url
        ON products(ProductURL)
        WHERE ProductURL IS NOT NULL
    """)
    conn.commit()
    print("Done — UNIQUE index created successfully.")
    print()
    print("All scrapers that use ON CONFLICT(ProductURL) will now work correctly.")
    conn.close()

if __name__ == "__main__":
    main()
