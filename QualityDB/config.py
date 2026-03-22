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
    # ── Televize & Obraz ───────────────────────────────────────────────────
    {"name": "TVs",                 "url": "https://televize.heureka.cz/"},
    {"name": "Monitors",            "url": "https://pocitace-notebooky.heureka.cz/monitory/"},
    {"name": "Projectors",          "url": "https://televize.heureka.cz/projektory/"},

    # ── Audio ─────────────────────────────────────────────────────────────
    {"name": "Headphones",          "url": "https://sluchatka-reproduktory-handsfree.heureka.cz/sluchatka/"},
    {"name": "Speakers",            "url": "https://sluchatka-reproduktory-handsfree.heureka.cz/reproduktory/"},
    {"name": "Soundbars",           "url": "https://sluchatka-reproduktory-handsfree.heureka.cz/soundbary/"},

    # ── Mobily & Tablety ──────────────────────────────────────────────────
    {"name": "Mobile Phones",       "url": "https://mobily-tablety.heureka.cz/mobilni-telefony/"},
    {"name": "Tablets",             "url": "https://mobily-tablety.heureka.cz/tablety/"},
    {"name": "Phone Cases",         "url": "https://mobily-tablety.heureka.cz/kryty-na-mobilni-telefony/"},
    {"name": "Phone Chargers",      "url": "https://mobily-tablety.heureka.cz/nabijeni/"},

    # ── Počítače & Notebooky ──────────────────────────────────────────────
    {"name": "Laptops",             "url": "https://pocitace-notebooky.heureka.cz/notebooky/"},
    {"name": "Mice",                "url": "https://pocitace-notebooky.heureka.cz/mysi/"},
    {"name": "Keyboards",           "url": "https://pocitace-notebooky.heureka.cz/klavesnice/"},
    {"name": "Laptop Accessories",  "url": "https://pocitace-notebooky.heureka.cz/prislusenstvi-k-notebookum/"},
    {"name": "Webcams",             "url": "https://pocitace-notebooky.heureka.cz/webkamery/"},
    {"name": "Routers",             "url": "https://pocitace-notebooky.heureka.cz/routery/"},

    # ── Úložiště ──────────────────────────────────────────────────────────
    {"name": "SSD",                 "url": "https://pocitace-notebooky.heureka.cz/ssd-disky/"},
    {"name": "HDD",                 "url": "https://pocitace-notebooky.heureka.cz/pevne-disky/"},
    {"name": "External Drives",     "url": "https://pocitace-notebooky.heureka.cz/externi-disky/"},
    {"name": "RAM",                 "url": "https://pocitace-notebooky.heureka.cz/operacni-pameti/"},
    {"name": "USB Flash Drives",    "url": "https://pocitace-notebooky.heureka.cz/flash-disky/"},
    {"name": "Cables & Hubs",       "url": "https://pocitace-notebooky.heureka.cz/kabely-redukce/"},

    # ── Chytré hodinky & fitness ──────────────────────────────────────────
    {"name": "Smartwatches",        "url": "https://chytre-hodinky-a-fitness-naramky.heureka.cz/chytre-hodinky/"},
    {"name": "Fitness Trackers",    "url": "https://chytre-hodinky-a-fitness-naramky.heureka.cz/fitness-naramky/"},

    # ── Foto & Video ──────────────────────────────────────────────────────
    {"name": "Digital Cameras",     "url": "https://foto-kamery.heureka.cz/digitalni-fotoaparaty/"},
    {"name": "Action Cameras",      "url": "https://foto-kamery.heureka.cz/akcni-kamery/"},
    {"name": "Camera Accessories",  "url": "https://foto-kamery.heureka.cz/prislusenstvi-k-fotoaparatotum/"},

    # ── Domácí spotřebiče ─────────────────────────────────────────────────
    {"name": "Vacuum Cleaners",     "url": "https://bile-zbozi.heureka.cz/vysavace/"},
    {"name": "Robot Vacuums",       "url": "https://bile-zbozi.heureka.cz/roboticke-vysavace/"},
    {"name": "Coffee Machines",     "url": "https://bile-zbozi.heureka.cz/kavovary/"},
    {"name": "Air Purifiers",       "url": "https://bile-zbozi.heureka.cz/cisticky-vzduchu/"},
    {"name": "Air Fryers",          "url": "https://bile-zbozi.heureka.cz/friteze/"},
    {"name": "Blenders",            "url": "https://bile-zbozi.heureka.cz/mixery/"},
    {"name": "Kettles",             "url": "https://bile-zbozi.heureka.cz/varne-konvice/"},
    {"name": "Toasters",            "url": "https://bile-zbozi.heureka.cz/toustovace/"},
    {"name": "Kitchen Appliances",  "url": "https://bile-zbozi.heureka.cz/male-spotrebice/"},
    {"name": "Kitchen Robots",      "url": "https://bile-zbozi.heureka.cz/kuchynske-roboty/"},
    {"name": "Microwaves",          "url": "https://bile-zbozi.heureka.cz/mikrovlnne-trouby/"},
    {"name": "Hair Dryers",         "url": "https://bile-zbozi.heureka.cz/feny-a-kulmy/"},
    {"name": "Electric Shavers",    "url": "https://bile-zbozi.heureka.cz/holiace-strojceky/"},
    {"name": "Irons",               "url": "https://bile-zbozi.heureka.cz/zehlicky/"},

    # ── Hry & Gaming ──────────────────────────────────────────────────────
    {"name": "Game Controllers",    "url": "https://herni-konzole.heureka.cz/ovladace/"},
    {"name": "Gaming Headsets",     "url": "https://herni-konzole.heureka.cz/herni-sluchatka/"},
    {"name": "Gaming Mice",         "url": "https://herni-konzole.heureka.cz/herni-mysi/"},
    {"name": "Gaming Keyboards",    "url": "https://herni-konzole.heureka.cz/herni-klavesnice/"},

    # ── Zabezpečení & Chytrá domácnost ────────────────────────────────────
    {"name": "IP Cameras",          "url": "https://zabezpeceni.heureka.cz/ip-kamery/"},
    {"name": "Smart Home",          "url": "https://chytra-domacnost.heureka.cz/"},
]
from config_de_additions import *
