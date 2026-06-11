#!/usr/bin/env python3
"""
saturn_scraper.py  (v2 — search-based, slug-independent)
─────────────────────────────────────────────────────────
Scrapes top-rated products from Saturn.de.

Saturn and MediaMarkt share the same platform (Ceconomy / MediaSaturn group).
This scraper reuses all parsing logic from mediamarkt_scraper.py and just
overrides the base URL and source tag.
"""

import os
import sys
import time
import sqlite3

sys.path.insert(0, os.path.dirname(__file__))

from mediamarkt_scraper import (
    make_session,
    fetch_query,
    upsert_products,
    SEARCH_QUERIES,
    DELAY_OK,
)
from config import DB_PATH

BASE_URL = "https://www.saturn.de"


def scrape_saturn(db_path=None):
    if db_path is None:
        db_path = DB_PATH

    conn    = sqlite3.connect(db_path)
    session = make_session()

    # Override headers for Saturn
    session.headers.update({
        "Referer": BASE_URL + "/",
        "Origin":  BASE_URL,
    })
    try:
        session.get(BASE_URL + "/", impersonate="chrome131", timeout=15)
        time.sleep(1.5)
    except Exception:
        pass

    total_ins = total_upd = 0

    for query, cat_label, main_cat in SEARCH_QUERIES:
        print(f"  Saturn.de  [{cat_label}]")
        products = fetch_query(session, query)
        ins, upd = upsert_products(
            conn, products, cat_label, main_cat, source="saturn_de"
        )
        total_ins += ins
        total_upd += upd
        print(f"    {len(products)} found → {ins} new, {upd} updated")
        time.sleep(DELAY_OK)

    conn.close()
    print(f"\nSaturn.de finished: {total_ins} inserted, {total_upd} updated")
    return total_ins, total_upd


if __name__ == "__main__":
    db_path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    scrape_saturn(db_path_arg)
