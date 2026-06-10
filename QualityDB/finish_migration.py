#!/usr/bin/env python3
"""Run with the server stopped:  python3 finish_migration.py"""
import sqlite3, os

HERE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(HERE, "products.db")

print(f"Opening {DB} ({os.path.getsize(DB)/1024/1024:.0f} MB) ...")
c = sqlite3.connect(DB, isolation_level=None)  # autocommit required for VACUUM
c.execute("PRAGMA journal_mode=DELETE")
c.execute("DROP TABLE IF EXISTS product_snapshots")
print("Table dropped.")
print("Vacuuming (takes 1-2 min) ...")
c.execute("VACUUM")
c.close()
print(f"Done. products.db is now {os.path.getsize(DB)/1024/1024:.0f} MB")
