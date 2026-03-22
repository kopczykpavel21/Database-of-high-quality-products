"""
One-time migration: update source field for all existing 'scraper' products.

Run once from the QualityDB folder:
    python3 migrate_sources.py
"""
import sqlite3, os

DB_PATH = os.path.join(os.path.dirname(__file__), "products.db")

conn = sqlite3.connect(DB_PATH)
total = 0

# 1. Full amazon.de URL
cur = conn.execute(
    "UPDATE products SET source='amazon' WHERE source='scraper' AND ProductURL LIKE '%amazon.de%'"
)
print(f"  amazon.de URL          → 'amazon':  {cur.rowcount} rows")
total += cur.rowcount

# 2. Full heureka.cz URL
cur = conn.execute(
    "UPDATE products SET source='heureka' WHERE source='scraper' AND ProductURL LIKE '%heureka.cz%'"
)
print(f"  heureka.cz URL         → 'heureka': {cur.rowcount} rows")
total += cur.rowcount

# 3. Full zbozi.cz URL
cur = conn.execute(
    "UPDATE products SET source='zbozi' WHERE source='scraper' AND ProductURL LIKE '%zbozi.cz%'"
)
print(f"  zbozi.cz URL           → 'zbozi':   {cur.rowcount} rows")
total += cur.rowcount

# 4. Zbozi relative click-tracking URLs (/exit-click-web)
cur = conn.execute(
    "UPDATE products SET source='zbozi' WHERE source='scraper' AND ProductURL LIKE '/exit-click-web%'"
)
print(f"  zbozi /exit-click-web  → 'zbozi':   {cur.rowcount} rows")
total += cur.rowcount

# 5. Amazon products with empty URL (German-language product names, no URL stored)
cur = conn.execute(
    "UPDATE products SET source='amazon' WHERE source='scraper' AND (ProductURL = '' OR ProductURL IS NULL)"
)
print(f"  empty URL (Amazon)     → 'amazon':  {cur.rowcount} rows")
total += cur.rowcount

conn.commit()

remaining = conn.execute("SELECT COUNT(*) FROM products WHERE source='scraper'").fetchone()[0]
print(f"\nDone. {total} rows updated. {remaining} rows still have source='scraper'.")
print("\nFull source breakdown:")
for src, cnt in conn.execute("SELECT source, COUNT(*) FROM products GROUP BY source ORDER BY COUNT(*) DESC"):
    print(f"  {src or '(null)':<12} {cnt:>6}")
conn.close()
