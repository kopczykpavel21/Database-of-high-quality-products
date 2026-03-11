"""
Scraper configuration — edit this file to tune behaviour.
"""

# ── Quality thresholds ──────────────────────────────────────────────────────
MIN_RATING_PCT   = 90      # Heureka recommendation % (0–100).  90 ≈ 4.5 stars
MIN_REVIEWS      = 10      # Ignore products with fewer reviews
STOP_BELOW_PCT   = 85      # Stop scraping a page when rating drops this low
                           # (pages are sorted by rating, so everything after
                           #  is worse — saves many unnecessary requests)

# ── Scheduling ──────────────────────────────────────────────────────────────
SCHEDULE_HOUR    = 3       # Hour (24 h) to run the daily scrape
SCHEDULE_MINUTE  = 0       # Minute

# ── Politeness ──────────────────────────────────────────────────────────────
REQUEST_DELAY    = 1.5     # Seconds between requests (be nice to the server)
MAX_PAGES        = 10      # Max pages per category per run (24 products/page)
                           # Set to 0 for unlimited

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
