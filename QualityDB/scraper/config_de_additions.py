#!/usr/bin/env python3
"""
config_de_additions.py
──────────────────────
German-market additions for config.py.

HOW TO INTEGRATE
  Option A (recommended) — append the block below directly to your existing
  scraper/config.py file.

  Option B — import at the bottom of config.py:
      from config_de_additions import *

──────────────────────────────────────────────────────────────────────────────
PASTE THIS INTO scraper/config.py
──────────────────────────────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════════════════
# GERMAN MARKET SETTINGS (added for QualityDB DE expansion)
# ═══════════════════════════════════════════════════════════════════════════════

# Supported countries
COUNTRIES = ["CZ", "DE"]

# Currency codes per country
COUNTRY_CURRENCY = {
    "CZ": "CZK",
    "DE": "EUR",
}

# Sources registered for the German market
DE_SOURCES = ["amazon_de", "otto_de", "mediamarkt_de", "saturn_de"]

# Minimum review count threshold for DE products (same logic as CZ)
# Override per-scraper if needed
MIN_REVIEWS_THRESHOLD_DE = 10        # same default as CZ threshold

# DE scraper enable flags  — set False to skip a source in the scheduler
ENABLE_AMAZON_DE    = True
ENABLE_OTTO_DE      = True
ENABLE_MEDIAMARKT   = True
ENABLE_SATURN       = True

# Polite delay (seconds) between category requests for DE scrapers
# (individual scrapers also have their own DELAY_OK constants)
DE_SCRAPE_DELAY = 2.5

# ─── API / URL roots ────────────────────────────────────────────────────────
AMAZON_DE_BASE      = "https://www.amazon.de"
OTTO_DE_BASE        = "https://www.otto.de"
MEDIAMARKT_DE_BASE  = "https://www.mediamarkt.de"
SATURN_DE_BASE      = "https://www.saturn.de"

# ─── MainCategory mapping extension ─────────────────────────────────────────
# German category labels are stored in the Category column (subcategory).
# MainCategory reuses the existing 16 Czech groups so the two-dropdown UI
# continues to work without code changes.
#
# This mapping is used by restructure_categories.py if you ever need to
# re-classify DE rows.
DE_CATEGORY_TO_MAIN = {
    # Electronics / Phones
    "Smartphones":                    "Telefony a tablety",
    "Handys & Zubehör":               "Telefony a tablety",
    "Tablets":                        "Telefony a tablety",
    # Computers
    "Laptops & Notebooks":            "Počítače a notebooky",
    "Computer & Zubehör":             "Počítače a notebooky",
    # PC Components
    "Grafikkarten":                   "PC komponenty",
    "PC-Hardware":                    "PC komponenty",
    "Software":                       "PC komponenty",
    # Peripherals
    "Drucker & Multifunktionsgeräte": "Periferie a příslušenství",
    "Bürobedarf & Schreibwaren":      "Periferie a příslušenství",
    # Networking
    "Netzwerk & Router":              "Sítě a konektivita",
    "WLAN-Router":                    "Sítě a konektivita",
    # Audio
    "Kopfhörer":                      "Zvuk a hudba",
    "Lautsprecher":                   "Zvuk a hudba",
    "Bluetooth-Lautsprecher":         "Zvuk a hudba",
    "Musikinstrumente":               "Zvuk a hudba",
    # Games
    "Spielkonsolen":                  "Herní technika",
    "Videospiele":                    "Herní technika",
    "Gaming-Headsets":                "Herní technika",
    "Gaming":                         "Herní technika",
    # TV
    "Fernseher":                      "Televize a video",
    "TV & Video":                     "Televize a video",
    # Photo
    "Kameras & Objektive":            "Foto a kamery",
    "Kamera & Foto":                  "Foto a kamery",
    "Digitalkameras":                 "Foto a kamery",
    # Storage
    "Externe Festplatten & SSDs":     "Datová úložiště",
    "Datenspeicher":                  "Datová úložiště",
    # Large appliances
    "Waschmaschinen":                 "Velké domácí spotřebiče",
    "Große Haushaltsgeräte":          "Velké domácí spotřebiče",
    "Kühlschränke":                   "Velké domácí spotřebiče",
    # Small appliances
    "Kaffeevollautomaten":            "Malé domácí spotřebiče",
    "Kaffeemaschinen":                "Malé domácí spotřebiče",
    "Küche & Haushalt":               "Malé domácí spotřebiče",
    # Vacuums
    "Staubsauger":                    "Vysavače a úklid",
    "Staubsauger-Roboter":            "Vysavače a úklid",
    # Smart devices
    "Smart Home":                     "Chytré zařízení",
    "Smartwatches":                   "Chytré zařízení",
    "Smartwatches & Fitness-Tracker": "Chytré zařízení",
}
"""

# ── Standalone constants (for import-based Option B) ─────────────────────────

COUNTRIES = ["CZ", "DE"]

COUNTRY_CURRENCY = {
    "CZ": "CZK",
    "DE": "EUR",
}

DE_SOURCES = ["amazon_de", "otto_de", "mediamarkt_de", "saturn_de"]

MIN_REVIEWS_THRESHOLD_DE = 10

ENABLE_AMAZON_DE   = True
ENABLE_OTTO_DE     = True
ENABLE_MEDIAMARKT  = True
ENABLE_SATURN      = True

DE_SCRAPE_DELAY = 2.5

AMAZON_DE_BASE      = "https://www.amazon.de"
OTTO_DE_BASE        = "https://www.otto.de"
MEDIAMARKT_DE_BASE  = "https://www.mediamarkt.de"
SATURN_DE_BASE      = "https://www.saturn.de"

DE_CATEGORY_TO_MAIN = {
    "Smartphones":                    "Telefony a tablety",
    "Handys & Zubehör":               "Telefony a tablety",
    "Tablets":                        "Telefony a tablety",
    "Laptops & Notebooks":            "Počítače a notebooky",
    "Computer & Zubehör":             "Počítače a notebooky",
    "Grafikkarten":                   "PC komponenty",
    "PC-Hardware":                    "PC komponenty",
    "Software":                       "PC komponenty",
    "Drucker & Multifunktionsgeräte": "Periferie a příslušenství",
    "Bürobedarf & Schreibwaren":      "Periferie a příslušenství",
    "Netzwerk & Router":              "Sítě a konektivita",
    "WLAN-Router":                    "Sítě a konektivita",
    "Kopfhörer":                      "Zvuk a hudba",
    "Lautsprecher":                   "Zvuk a hudba",
    "Bluetooth-Lautsprecher":         "Zvuk a hudba",
    "Musikinstrumente":               "Zvuk a hudba",
    "Spielkonsolen":                  "Herní technika",
    "Videospiele":                    "Herní technika",
    "Gaming-Headsets":                "Herní technika",
    "Gaming":                         "Herní technika",
    "Fernseher":                      "Televize a video",
    "TV & Video":                     "Televize a video",
    "Kameras & Objektive":            "Foto a kamery",
    "Kamera & Foto":                  "Foto a kamery",
    "Digitalkameras":                 "Foto a kamery",
    "Externe Festplatten & SSDs":     "Datová úložiště",
    "Datenspeicher":                  "Datová úložiště",
    "Waschmaschinen":                 "Velké domácí spotřebiče",
    "Große Haushaltsgeräte":          "Velké domácí spotřebiče",
    "Kühlschränke":                   "Velké domácí spotřebiče",
    "Kaffeevollautomaten":            "Malé domácí spotřebiče",
    "Kaffeemaschinen":                "Malé domácí spotřebiče",
    "Küche & Haushalt":               "Malé domácí spotřebiče",
    "Staubsauger":                    "Vysavače a úklid",
    "Staubsauger-Roboter":            "Vysavače a úklid",
    "Smart Home":                     "Chytré zařízení",
    "Smartwatches":                   "Chytré zařízení",
    "Smartwatches & Fitness-Tracker": "Chytré zařízení",
}
