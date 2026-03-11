=====================================================
 QualityDB — Phase 2: Heureka.cz Auto-Scraper
=====================================================

FIRST-TIME SETUP (one time only)
----------------------------------
Install the two required Python packages:

    pip install requests beautifulsoup4

(If you don't have pip: https://pip.pypa.io/en/stable/installation/)


HOW TO RUN A MANUAL SCRAPE
---------------------------
Open a terminal in the QualityDB folder and run:

    python3 scraper/heureka_scraper.py

This will scrape all configured categories, filter for quality,
and add new products to products.db.  The web app will show them
immediately (refresh the browser) — they'll be marked "Heureka"
in the source badge.


HOW TO RUN THE DAILY AUTO-SCRAPER
-----------------------------------
Keep this command running in a terminal (or set it as a startup item):

    python3 scraper/scheduler.py

It wakes up every day at 03:00 AM (configurable), runs the scraper,
and goes back to sleep.  You can press Ctrl+C to stop it.

To change the time, open scraper/config.py and edit:
    SCHEDULE_HOUR   = 3
    SCHEDULE_MINUTE = 0


WHAT COUNTS AS "HIGH QUALITY" (Heureka)
-----------------------------------------
Configured in scraper/config.py:

    MIN_RATING_PCT  = 90    ← Heureka recommendation % (≈ 4.5 stars)
    MIN_REVIEWS     = 10    ← Minimum number of reviews
    STOP_BELOW_PCT  = 85    ← Stop scraping a page when quality drops here

The scraper also:
  - Stops early per page when products drop below the threshold
    (pages are sorted by rating, so everything after is worse — this
    avoids scraping hundreds of low-quality pages unnecessarily)
  - Skips products already in the database (deduplication by name)
  - Respects a 1.5s delay between requests to be polite to the server


ADDING MORE CATEGORIES
-----------------------
Open scraper/config.py and add entries to the CATEGORIES list:

    {"name": "Robot Vacuums", "url": "https://bile-zbozi.heureka.cz/roboticke-vysavace/"},

Category URLs follow the pattern:  https://[category-name].heureka.cz/[subcategory]/
Browse heureka.cz to find the right URL for any category you want.


SCRAPE LOG
-----------
A log of every run (what was found, added, errors) is saved to:
    scraper/scraper.log


LIMITATIONS TO BE AWARE OF
----------------------------
1. Heureka uses JavaScript for some content — most products load in
   the initial HTML (which is what we scrape), but some edge cases
   may be missed.

2. If Heureka changes their HTML structure, selectors may need updating
   in heureka_scraper.py (the relevant section is clearly marked).

3. Heureka's ToS technically prohibits scraping. This is a very common
   and widely-practiced approach for personal/research use, but be aware
   of it. The scraper is polite (delays, no mass parallelism).

4. If you get blocked (HTTP 429 or 403), increase REQUEST_DELAY in
   config.py to 3.0 or higher.

=====================================================
