import os
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "products.db")

# SQLite journal mode.
#   Locally (Mac / Linux dev):  leave unset → WAL mode.
#     WAL lets the server and scrapers run at the same time without "database is locked".
#   Fly.io production:  set env var JOURNAL_MODE=delete (Fly's FUSE mount blocks WAL files).
#     fly secrets set JOURNAL_MODE=delete
JOURNAL_MODE = os.environ.get("JOURNAL_MODE", "wal").upper()

"""
Scraper configuration — edit this file to tune behaviour.
"""

# ── Research mode ────────────────────────────────────────────────────────────
# Set RESEARCH_MODE = True to collect ALL products regardless of rating/reviews.
# This is used for dissertation research on planned/premature obsolescence —
# the full distribution of products (including low-rated ones) is needed to
# analyse quality stratification and lifecycle patterns across the market.
# When False, only products meeting MIN_RATING_PCT / MIN_REVIEWS are kept.
RESEARCH_MODE    = True

# ── Quality thresholds ───────────────────────────────────────────────────────
# These are only applied when RESEARCH_MODE = False.
MIN_RATING_PCT   = 0       # 0 = collect all ratings (RESEARCH_MODE=True bypasses this anyway)
MIN_REVIEWS      = 10      # Ignore products with fewer reviews
STOP_BELOW_PCT   = 0       # 0 = never stop early based on score
                           # (pages are sorted by rating, so everything after
                           #  is worse — saves many unnecessary requests)
                           # Set to 0 to disable the early-stop in research mode.

# ── Scheduling ──────────────────────────────────────────────────────────────
SCHEDULE_HOUR    = 3       # Hour (24 h) to run the daily scrape
SCHEDULE_MINUTE  = 0       # Minute

# ── Politeness ──────────────────────────────────────────────────────────────
REQUEST_DELAY    = 1.5     # Seconds between requests (be nice to the server)
MAX_PAGES        = 50      # Max pages per category per run (24 products/page)
                           # Increased from 10 → 50 for full-market collection.
                           # Set to 0 for truly unlimited (use with caution).

# ── Heureka category URLs to scrape ─────────────────────────────────────────
# These are subdomain-based.  Add or remove as needed.
# All categories are sorted by rating (?sort=rating)
# Focus: small electronics + household appliances

CATEGORIES = [
    # ── Televize ──────────────────────────────────────────────────────────
    {"name": "TVs",              "url": "https://televize.heureka.cz/"},

    # ── Audio ──────────────────────────────────────────────────────────────
    {"name": "Headphones",       "url": "https://sluchatka-reproduktory-handsfree.heureka.cz/sluchatka/"},
    {"name": "Speakers",         "url": "https://sluchatka-reproduktory-handsfree.heureka.cz/reproduktory/"},

    # ── Počítačové periferie ────────────────────────────────────────────────
    {"name": "Mice",             "url": "https://pocitace-notebooky.heureka.cz/mysi/"},
    {"name": "Keyboards",        "url": "https://pocitace-notebooky.heureka.cz/klavesnice/"},
    {"name": "Laptop Accessories","url": "https://pocitace-notebooky.heureka.cz/prislusenstvi-k-notebookum/"},

    # ── Úložiště ────────────────────────────────────────────────────────────
    {"name": "SSD",              "url": "https://pocitace-notebooky.heureka.cz/ssd-disky/"},
    {"name": "RAM",              "url": "https://pocitace-notebooky.heureka.cz/operacni-pameti/"},

    # ── Smartwatches ────────────────────────────────────────────────────────
    {"name": "Smartwatches",     "url": "https://chytre-hodinky-a-fitness-naramky.heureka.cz/"},

    # ── Domácí spotřebiče ───────────────────────────────────────────────────
    {"name": "Kitchen Appliances","url": "https://bile-zbozi.heureka.cz/male-spotrebice/"},
    {"name": "Vacuum Cleaners",  "url": "https://bile-zbozi.heureka.cz/vysavace/"},
    {"name": "Coffee Machines",  "url": "https://bile-zbozi.heureka.cz/kavovary/"},
    {"name": "Air Purifiers",    "url": "https://bile-zbozi.heureka.cz/cisticky-vzduchu/"},

    # ── Kabely & příslušenství ──────────────────────────────────────────────
    {"name": "Cables & Hubs",    "url": "https://pocitace-notebooky.heureka.cz/kabely-redukce/"},
]
from config_de_additions import *
