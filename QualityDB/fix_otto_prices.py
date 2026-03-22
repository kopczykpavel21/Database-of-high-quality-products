#!/usr/bin/env python3
"""
fix_otto_prices.py — One-time fix for Otto.de prices stored as cents.

Run from the QualityDB/ folder (stop the server first):
  python3 fix_otto_prices.py
"""
import os, sys, sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")

if not os.path.exists(DB_PATH):
    print(f"ERROR: DB not found at {DB_PATH}")
    sys.exit(1)

conn = sqlite3.connect(DB_PATH)

# Check current state
before = conn.execute(
    "SELECT COUNT(*), ROUND(AVG(Price_EUR),2) FROM products WHERE source IN ('otto','otto_de')"
).fetchone()
print(f"Before: {before[0]} Otto products, avg price = {before[1]}")

# Fix prices: all otto prices were stored as integer cents (69900 = €699.00)
conn.execute("""
    UPDATE products
    SET Price_EUR = ROUND(Price_EUR / 100.0, 2)
    WHERE source IN ('otto', 'otto_de')
      AND Price_EUR IS NOT NULL
""")
print(f"  Price rows updated: {conn.total_changes}")

# Normalise source name: otto_de → otto
conn.execute("UPDATE products SET source = 'otto' WHERE source = 'otto_de'")

conn.commit()

after = conn.execute(
    "SELECT COUNT(*), ROUND(AVG(Price_EUR),2), MIN(Price_EUR), MAX(Price_EUR) FROM products WHERE source='otto'"
).fetchone()
print(f"After:  {after[0]} Otto products, avg = €{after[1]}, min = €{after[2]}, max = €{after[3]}")

# Spot check
print("\nSample products:")
for row in conn.execute("SELECT Name, Price_EUR FROM products WHERE source='otto' LIMIT 5").fetchall():
    print(f"  €{row[1]:>8.2f}  {row[0][:60]}")

conn.close()
print("\nDone. Restart the server.")
