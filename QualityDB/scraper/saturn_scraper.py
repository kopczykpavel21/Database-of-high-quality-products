#!/usr/bin/env python3
"""
saturn_scraper.py  (v3 — genuinely targets saturn.de)
─────────────────────────────────────────────────────
Scrapes top-rated products from Saturn.de.

Saturn and MediaMarkt are one Ceconomy storefront under two brands, so all
parsing logic is reused from mediamarkt_scraper.py.  Only the base URL and the
source tag differ.

v2 BUG (fixed here): v2 overrode the session's Referer/Origin headers but left
`mediamarkt_scraper.SEARCH_URL` and `.BASE_URL` untouched, so every request
still went to mediamarkt.de and every row landed under source='saturn_de' with
a mediamarkt.de ProductURL -- 550 such rows are in products.db.  The base URL is
now passed explicitly and the results are host-checked (`enforce_host`), so the
same mistake cannot pass silently again.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from mediamarkt_scraper import SATURN_BASE_URL, scrape_ceconomy

BASE_URL = SATURN_BASE_URL


def scrape_saturn(db_path=None):
    return scrape_ceconomy(BASE_URL, "saturn_de", "Saturn.de", db_path)


if __name__ == "__main__":
    db_path_arg = sys.argv[1] if len(sys.argv) > 1 else None
    scrape_saturn(db_path_arg)
