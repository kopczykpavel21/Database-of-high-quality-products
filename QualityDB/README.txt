=====================================
 QualityDB – Product Quality Database
=====================================

HOW TO RUN
----------
Option A (Mac): Double-click START.command
  (If blocked: right-click → Open → Open anyway)

Option B (any OS): Open a terminal in this folder and run:
  python3 server.py
  Then open http://localhost:5000 in your browser.

HOW TO REBUILD THE DATABASE
----------------------------
If you get a new version of the Excel file, run:
  python3 load_data.py
This re-filters the data and rebuilds products.db.

FILTER SETTINGS (in load_data.py)
-----------------------------------
  RETURN_RATE_MAX = 1.4   ← change to e.g. 0.5 for stricter filter
  REVIEWS_MIN     = 2     ← change to e.g. 10 for more reliable data

PHASE 2 – AUTOMATED SCRAPER
-----------------------------
Coming next: a scheduled scraper that automatically pulls top-rated
products from Heureka.cz and other sources and adds them to the DB
with source = 'scraper'.

FILES
-----
  server.py      Main web server (no external dependencies needed)
  load_data.py   Excel → SQLite loader
  products.db    The database (auto-generated, ~15 MB)
  templates/     HTML templates
  static/        CSS and JavaScript
=====================================
