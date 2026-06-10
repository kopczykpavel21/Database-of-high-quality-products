"""
QualityDB – Standalone HTTP server (no external dependencies).
Run: python3 server.py
Then open: http://localhost:8080

Scraping:
    Handled entirely by scraper/scheduler.py, which is launched as a
    background process when this server starts.  The scheduler runs each
    scraper on its configured day/time (see scheduler.py for the full
    schedule) and writes run history to the scraper_runs table.

    API endpoints:
        /api/scrape-status  — last N runs from scraper_runs table
        /api/run-scraper    — trigger today's due scrapers now (--now flag)
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import sqlite3, json, os, math, urllib.parse, mimetypes, subprocess, sys
import time, datetime, threading

# Support DB on a mounted volume (e.g. Fly.io) via env var, fallback to local
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "products.db"))
STATIC  = os.path.join(os.path.dirname(__file__), "static")
TMPL    = os.path.join(os.path.dirname(__file__), "templates", "index.html")
PAGE_SIZE = 24

# Sources never shown in public queries
HIDDEN_SOURCES = {"dtest", "warentest"}

# Journal mode selection:
#   • Locally (Mac/Linux dev): WAL mode — allows server + scrapers to run simultaneously.
#     Multiple readers + one writer can coexist; "database is locked" errors disappear.
#   • Fly.io production: set env var JOURNAL_MODE=delete because Fly's FUSE-mounted
#     persistent volume doesn't support WAL's extra -wal/-shm files.
#
# To run scrapers alongside a live local server, just leave JOURNAL_MODE unset (WAL).
# On Fly.io add a secret:  fly secrets set JOURNAL_MODE=delete
_JOURNAL_MODE = os.environ.get("JOURNAL_MODE", "wal").lower()

# ── In-memory caches ──────────────────────────────────────────────────────────
# build_html() is expensive (opens DB, fetches 2K+ rows, serialises to JSON).
# Cache the result for HTML_TTL seconds; invalidated automatically after that
# window or when a scraper run completes (call _invalidate_html_cache()).
_html_cache: dict = {"html": None, "ts": 0.0}
_html_lock = threading.Lock()
HTML_TTL   = int(os.environ.get("HTML_TTL", 300))   # default: 5 minutes

# /api/ir-data shares the same underlying data as the HTML injection.
# Cache it separately with a longer TTL (IR scores change at most monthly).
_ir_cache: dict = {"data": None, "ts": 0.0}
IR_TTL = int(os.environ.get("IR_TTL", 3600))         # default: 1 hour

# Version string appended to static asset URLs (?v=...) so browsers always
# fetch fresh JS/CSS after a server restart. Format: YYYYMMDD-HHMMSS.
_ASSET_VERSION = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

def _invalidate_html_cache():
    """Call this after a scraper run finishes so the next request rebuilds."""
    with _html_lock:
        _html_cache["ts"] = 0.0
    _ir_cache["ts"] = 0.0

def open_db():
    """Open DB connection.  WAL mode locally so scrapers and server coexist;
    DELETE mode on Fly.io (set JOURNAL_MODE=delete env var) due to FUSE limits."""
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA journal_mode={_JOURNAL_MODE}")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-32000")   # 32 MB page cache
    return conn


def ensure_indexes(conn):
    """Create missing indexes for common filter/sort queries. Safe to call repeatedly."""
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_source          ON products(source);
        CREATE INDEX IF NOT EXISTS idx_main_category   ON products(MainCategory);
        CREATE INDEX IF NOT EXISTS idx_country         ON products(country);
        CREATE INDEX IF NOT EXISTS idx_source_cat      ON products(source, Category);
        CREATE INDEX IF NOT EXISTS idx_rec_rate        ON products(RecommendRate_pct);
        CREATE INDEX IF NOT EXISTS idx_price_czk       ON products(Price_CZK);
        CREATE INDEX IF NOT EXISTS idx_price_eur       ON products(Price_EUR);
        CREATE INDEX IF NOT EXISTS idx_keywords        ON products(keywords);
    """)

# ── Scheduler subprocess ──────────────────────────────────────────────────────
_SCHEDULER_PY = os.path.join(os.path.dirname(__file__), "scraper", "scheduler.py")
_scheduler_proc = None   # subprocess.Popen or None


def _start_scheduler():
    """Launch scraper/scheduler.py as a background subprocess (daemon mode)."""
    global _scheduler_proc
    if _scheduler_proc and _scheduler_proc.poll() is None:
        return  # already running
    _scheduler_proc = subprocess.Popen(
        [sys.executable, _SCHEDULER_PY],
        stdout=open(os.path.join(os.path.dirname(__file__), "scraper", "logs", "scheduler.log"), "a"),
        stderr=subprocess.STDOUT,
    )
    print(f"[scheduler] Started as PID {_scheduler_proc.pid} — daily wake-up at 03:00.")


def _query_scrape_status(limit: int = 20) -> dict:
    """Read last N scraper runs from the scraper_runs table."""
    try:
        conn = open_db()
        rows = conn.execute(
            "SELECT scraper_name, market, started_at, finished_at, status, "
            "       products_added, products_updated, error_msg, duration_sec "
            "FROM scraper_runs ORDER BY started_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        running_row = conn = None
        # Check if any row has status='running'
        runs = [dict(r) for r in rows]
        currently_running = any(r["status"] == "running" for r in runs)
        scheduler_alive = _scheduler_proc is not None and _scheduler_proc.poll() is None
        return {
            "scheduler_pid":       _scheduler_proc.pid if scheduler_alive else None,
            "scheduler_running":   scheduler_alive,
            "scraper_running":     currently_running,
            "recent_runs":         runs,
        }
    except Exception as e:
        return {"error": str(e)}


def _query_health() -> dict:
    """Per-source freshness dashboard.
    Returns each source with: product count, last successful scrape date,
    days since last update, and a traffic-light status (ok/warn/stale/unknown).
    """
    try:
        conn = open_db()

        # Product counts per source
        counts = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT source, COUNT(*) FROM products GROUP BY source"
            ).fetchall()
        }

        # Last successful run per scraper from scraper_runs table
        run_rows = conn.execute(
            """SELECT scraper_name, market,
                      MAX(CASE WHEN status='ok' THEN started_at END) as last_ok,
                      MAX(started_at) as last_attempt,
                      SUM(CASE WHEN status='ok' THEN products_added   ELSE 0 END) as total_added,
                      SUM(CASE WHEN status='ok' THEN products_updated ELSE 0 END) as total_updated,
                      SUM(CASE WHEN status='error' AND started_at > datetime('now','-30 days') THEN 1 ELSE 0 END) as errors_30d
               FROM scraper_runs
               GROUP BY scraper_name, market"""
        ).fetchall()
        conn.close()

        now = datetime.datetime.utcnow()

        # Map scraper_name → source key(s) used in products table
        SCRAPER_SOURCE_MAP = {
            "Alza.cz":            ["alza"],
            "Heureka.cz":         ["heureka"],
            "Heureka CZ":         ["heureka"],
            "Heureka.sk":         ["heureka_sk"],
            "Heureka SK":         ["heureka_sk"],
            "Zbozi.cz":           ["zbozi"],
            "Datart.cz":          ["datart"],
            "CZC.cz":             ["czc"],
            "Amazon.de":          ["amazon", "amazon_de"],
            "Otto.de":            ["otto", "otto_de"],
            "Conrad.de":          ["conrad"],
            "Fnac.fr":            ["fnac"],
            "Darty.fr":           ["darty"],
            "Ceneo.pl":           ["ceneo"],
            # "Stiftung Warentest": ["warentest"],  # hidden
            # "D-test.cz":          ["dtest"],  # hidden
        }

        sources = []
        seen_scrapers = set()

        for row in run_rows:
            scraper_name, market, last_ok, last_attempt, total_added, total_updated, errors_30d = row
            seen_scrapers.add(scraper_name)

            # Compute product count (sum across all mapped source keys)
            source_keys = SCRAPER_SOURCE_MAP.get(scraper_name, [scraper_name.lower()])
            product_count = sum(counts.get(k, 0) for k in source_keys)

            # Days since last successful scrape
            days_since = None
            if last_ok:
                try:
                    last_dt = datetime.datetime.fromisoformat(last_ok.replace("Z", ""))
                    days_since = (now - last_dt).days
                except Exception:
                    pass

            # Traffic light
            if days_since is None:
                status_color = "unknown"
            elif days_since <= 7:
                status_color = "ok"
            elif days_since <= 14:
                status_color = "warn"
            else:
                status_color = "stale"

            sources.append({
                "scraper":       scraper_name,
                "market":        market,
                "product_count": product_count,
                "last_ok":       last_ok,
                "last_attempt":  last_attempt,
                "days_since_ok": days_since,
                "status":        status_color,
                "total_added":   total_added or 0,
                "total_updated": total_updated or 0,
                "errors_30d":    errors_30d or 0,
            })

        # Add any sources with products but no scraper_runs entry yet
        all_known = {k for keys in SCRAPER_SOURCE_MAP.values() for k in keys}
        for src_key, cnt in counts.items():
            if src_key not in all_known:
                sources.append({
                    "scraper":       src_key,
                    "market":        "??",
                    "product_count": cnt,
                    "last_ok":       None,
                    "last_attempt":  None,
                    "days_since_ok": None,
                    "status":        "unknown",
                    "total_added":   0,
                    "total_updated": 0,
                    "errors_30d":    0,
                })

        # Sort: stale first, then warn, then ok, then unknown
        order = {"stale": 0, "warn": 1, "unknown": 2, "ok": 3}
        sources.sort(key=lambda s: (order.get(s["status"], 9), s["scraper"]))

        scheduler_alive = _scheduler_proc is not None and _scheduler_proc.poll() is None
        total_products  = sum(counts.values())

        return {
            "scheduler_running": scheduler_alive,
            "total_products":    total_products,
            "sources":           sources,
            "generated_at":      now.isoformat() + "Z",
        }
    except Exception as e:
        return {"error": str(e), "sources": []}


def get_categories():
    conn = open_db()
    rows = conn.execute(
        "SELECT Category, COUNT(*) as cnt FROM products "
        "GROUP BY Category ORDER BY cnt DESC"
    ).fetchall()
    conn.close()
    return rows


# Master mapping: raw MainCategory value → clean super-category label
_SUPER_CAT_MAP = {
    # Technology
    "Telefony a tablety": "📱 Phones & Tablets",
    "Phones & Tablets": "📱 Phones & Tablets",
    "Počítače a notebooky": "💻 Computers & Tablets",
    "Computers": "💻 Computers & Tablets",
    "Počítače a hry": "💻 Computers & Tablets",
    "Herní technika": "💻 Computers & Tablets",
    "Elektro": "📺 TV, Audio & Electronics",
    "TV & Audio": "📺 TV, Audio & Electronics",
    "Televize a video": "📺 TV, Audio & Electronics",
    "Zvuk a hudba": "📺 TV, Audio & Electronics",
    "Zvuk": "📺 TV, Audio & Electronics",
    "Foto a video": "📺 TV, Audio & Electronics",
    "Sítě a konektivita": "📺 TV, Audio & Electronics",
    "PC komponenty": "💻 Computers & Tablets",
    "Datová úložiště": "💻 Computers & Tablets",
    "Periferie a příslušenství": "💻 Computers & Tablets",
    "Chytré zařízení": "📺 TV, Audio & Electronics",
    # Appliances
    "Velké domácí spotřebiče": "🏠 Large Appliances",
    "Large Appliances": "🏠 Large Appliances",
    "Malé domácí spotřebiče": "🍳 Small Appliances",
    "Small Appliances": "🍳 Small Appliances",
    "Vysavače a úklid": "🍳 Small Appliances",
    # Wearables
    "Wearables": "⌚ Wearables & Health",
    "Zdraví a sport": "⌚ Wearables & Health",
    "Zdraví a hygiena": "⌚ Wearables & Health",
    # Home, Garden & Sport
    "Dům a zahrada": "🏡 Home, Garden & Sport",
    "Zahrada a dílna": "🏡 Home, Garden & Sport",
    "Zahrada": "🏡 Home, Garden & Sport",
    "Sport a outdoor": "🏡 Home, Garden & Sport",
    "Sport a kola": "🏡 Home, Garden & Sport",
    "Sport": "🏡 Home, Garden & Sport",
    "Auto a moto": "🏡 Home, Garden & Sport",
    "Hobby": "🏡 Home, Garden & Sport",
    "Cestování": "🏡 Home, Garden & Sport",
    "Bytové vybavení": "🏡 Home, Garden & Sport",
    # Appliances (extra)
    "Domácí spotřebiče": "🍳 Small Appliances",
    # Technology (extra)
    "Foto a kamery": "📺 TV, Audio & Electronics",
    "Audio": "📺 TV, Audio & Electronics",
    "TV i foto": "📺 TV, Audio & Electronics",
    "Telefony i tablety": "📱 Phones & Tablets",
    "Příslušenství": "💻 Computers & Tablets",
    "Komputery": "💻 Computers & Tablets",
    # Family & Kids
    "Děti a hračky": "👶 Family & Kids",
    "Dětské zboží": "👶 Family & Kids",
    "Hry a hračky": "👶 Family & Kids",
    "Zvířata": "👶 Family & Kids",
    # Fashion, Beauty & Other
    "Móda a oblečení": "👗 Fashion & Beauty",
    "Kosmetika": "👗 Fashion & Beauty",
    "Knihy a média": "📦 Other",
    "Kancelář": "📦 Other",
    "Průmysl": "📦 Other",
    "Potraviny": "📦 Other",
    "Hudba": "📦 Other",
    "Ostatní": "📦 Other",
}
_SUPER_ORDER = [
    "📱 Phones & Tablets",
    "💻 Computers & Tablets",
    "📺 TV, Audio & Electronics",
    "🏠 Large Appliances",
    "🍳 Small Appliances",
    "⌚ Wearables & Health",
    "🏡 Home, Garden & Sport",
    "👶 Family & Kids",
    "👗 Fashion & Beauty",
    "📦 Other",
]

# Maps raw Category (sub-cat) strings → clean English label, or None to drop entirely.
_SUB_CLEAN_MAP = {
    # ── Phones ──────────────────────────────────────────────────────────────
    "Telefony":                         "Smartphones",
    "Chytré telefony":                  "Smartphones",
    "Mobilní telefony":                 "Smartphones",
    "Telefony komórkowe":               "Smartphones",
    "Handys und Smartphones im Test":   None,
    "Schnurlose Telefone im Test":      None,
    "Onlineshops fuer refurbished Smartphones im Test": None,
    "Kameras im Vergleich Smartphone Kameras gegen richtige Kameras": None,
    "Samsungs Falt Smartphones im":     None,
    "Wasserdichte handyhuelle":         None,
    # ── Tablets ─────────────────────────────────────────────────────────────
    "Tablety":                          "Tablets",
    "Tablety a čtečky":                 "Tablets",
    # ── Smartwatches ────────────────────────────────────────────────────────
    "Smartwatch":                       "Smartwatches",
    "Příslušenství Apple Watch":        "Smartwatches",
    # ── Headphones ──────────────────────────────────────────────────────────
    "Sluchátka":                        "Headphones",
    # ── Phone Accessories ───────────────────────────────────────────────────
    "Pouzdra a kryty":                  "Cases & Covers",
    "Ochranné fólie":                   "Screen Protectors",
    "Držáky a stojany":                 "Holders & Stands",
    "Nabíječky":                        "Chargers",
    "Kabely":                           "Cables",
    "Powerbanky":                       "Power Banks",
    # ── Laptops ─────────────────────────────────────────────────────────────
    "Notebooky":                        "Laptops",
    "Příslušenství k notebookům":       "Laptop Accessories",
    # ── Desktops ────────────────────────────────────────────────────────────
    "Počítače":                         "Desktop PCs",
    "PC skříně":                        "PC Cases",
    "Počítačové hry":                   "PC Games",
    # ── Storage ─────────────────────────────────────────────────────────────
    "Pevné disky a SSD":                "Hard Drives & SSDs",
    "Úložiště":                         "Storage",
    "SSD":                              "Hard Drives & SSDs",
    "Externí disky":                    "External Drives",
    "Flash disky":                      "USB Drives",
    "Úložiště a USB":                   "Storage",
    # ── Displays ────────────────────────────────────────────────────────────
    "Monitory":                         "Monitors",
    # ── Peripherals ─────────────────────────────────────────────────────────
    "Klávesnice":                       "Keyboards",
    "Myši":                             "Mice",
    "Webkamery":                        "Webcams",
    "Tiskárny":                         "Printers",
    "Dokovací stanice":                 "Docking Stations",
    "Herní příslušenství":              "Gaming Accessories",
    "Příslušenství":                    "Accessories",
    # ── Components ──────────────────────────────────────────────────────────
    "Komponenty":                       "Components",
    "Operační paměti":                  "RAM",
    "Grafické karty":                   "Graphics Cards",
    "RAM":                              "RAM",
    # ── Networking ──────────────────────────────────────────────────────────
    "Síťové prvky":                     "Networking",
    # ── TVs ─────────────────────────────────────────────────────────────────
    "Televizory":                       "Televisions",
    "TVs":                              "Televisions",
    # ── Audio ───────────────────────────────────────────────────────────────
    "Reproduktory":                     "Speakers",
    "Soundbary a reproduktory":         "Soundbars & Speakers",
    "Přenosný zvuk":                    "Portable Audio",
    "Domácí kino":                      "Home Cinema",
    "Audio":                            "Audio",
    # ── Cameras ─────────────────────────────────────────────────────────────
    "Fotoaparáty":                      "Cameras",
    "Foto a kamery":                    "Cameras",
    "Akční kamery":                     "Action Cameras",
    "Videokamery":                      "Camcorders",
    "Drony":                            "Drones",
    "Objektivy":                        "Lenses",
    "Stativy a stab.":                  "Tripods & Stabilisers",
    "Blesky":                           "Flash & Lighting",
    # ── Smart Home ──────────────────────────────────────────────────────────
    "Streaming zařízení":               "Streaming Devices",
    "Chytrá domácnost":                 "Smart Home",
    # ── Electronics Accessories ─────────────────────────────────────────────
    "Kabely a adaptéry":                "Cables & Adapters",
    "Kabely a rozbočovače":             "Cables & Hubs",
    "Baterie":                          "Batteries",
    "Dálkové ovladače":                 "Remote Controls",
    "Paměťová média":                   "Memory Cards",
    "Dalekohledce":                     "Binoculars",
    "Projektory":                       "Projectors",
    "Přehrávače":                       "Media Players",
    "Elektronika":                      None,   # too generic, drop
    # ── Large Appliances ────────────────────────────────────────────────────
    "Pračky a péče o prádlo":           "Washing Machines",
    "Washing Machines (top)":           "Washing Machines",
    "Washing Machines (hublot)":        "Washing Machines",
    "Waschmaschinen":                   "Washing Machines",
    "Pračky":                           "Washing Machines",
    "Wäschetrockner":                   "Tumble Dryers",
    "Chladničky a mrazničky":           "Fridges & Freezers",
    "Ledničky":                         "Fridges & Freezers",
    "Kühlschränke":                     "Fridges & Freezers",
    "Gefrierschränke":                  "Freezers",
    "Myčky nádobí":                     "Dishwashers",
    "Geschirrspüler":                   "Dishwashers",
    "Vaření a pečení":                  "Cookers & Ovens",
    "Sporáky":                          "Cookers & Ovens",
    "Backofenreiniger grillreiniger":   None,
    "Spotřebiče":                       None,   # too generic
    "Vytápění a klimatizace":           "Heating & Cooling",
    # ── Small Appliances ────────────────────────────────────────────────────
    "Kuchyňské spotřebiče":             "Kitchen Appliances",
    "Küche & Haushalt":                 "Kitchen Appliances",
    "Kávovary":                         "Coffee Machines",
    "Kaffeemaschinen":                  "Coffee Machines",
    "Filterkaffeemaschinen im Test Welche ist die beste": None,
    "Mixéry a roboty":                  "Blenders & Food Processors",
    "Varné konvice":                    "Kettles",
    "Toustovače":                       "Toasters",
    "Vysavač":                          "Vacuum Cleaners",
    "Vysavače":                         "Vacuum Cleaners",
    "Úklid (vysavače)":                 "Vacuum Cleaners",
    "Tyčové vysavače":                  "Stick Vacuums",
    "Robotické vysavače":               "Robot Vacuums",
    "Staubsauger":                      "Vacuum Cleaners",
    "Domácí spotřebiče":                "Home Appliances",
    "Fény a stylingové přístroje":      "Hair Styling",
    "Žehličky":                         "Irons",
    "Mikrovlnné trouby":                "Microwaves",
    "Microwaves":                       "Microwaves",
    "Sušičky prádla":                   "Tumble Dryers",
    "Trouby":                           "Ovens",
    "Kávovar":                          "Coffee Machines",
    "Blenders":                         "Blenders & Food Processors",
    # ── Wearables & Health ──────────────────────────────────────────────────
    "Zdraví":                           "Health & Wellness",
    "Péče o zdraví":                    "Health & Wellness",
    "Zdravotnické pomůcky":             "Medical Devices",
    "Dentální hygiena":                 "Dental Care",
    "Holicí strojky":                   "Shavers",
    "Opalovací krémy":                  "Sunscreen",
    "Sunscreen":                        "Sunscreen",
    "Chytré hodinky":                   "Smartwatches",
    "Fitness náramky":                  "Smartwatches",
    "Fény a stylingové přístroje":      "Hair Dryers",
    "Herren nassrasierer":              None,
    "Sonnencreme Kinder":               None,
    "Sonnencreme fuers gesicht":        None,
    "Sonnencreme":                      "Sunscreen",
    # ── Home, Garden & Sport ────────────────────────────────────────────────
    "Sport":                            "Sports & Outdoors",
    "Domácí potřeby":                   "Home Essentials",
    "Kuchyňské nádobí":                 "Kitchenware",
    "Organizace":                       "Organisation",
    "Koupelna":                         "Bathroom",
    "Úklid":                            "Cleaning",
    "Kreativní práce":                  "Arts & Crafts",
    "Malování a kreslení":              "Painting & Drawing",
    "Šití a pletení":                   "Sewing & Knitting",
    "Scrapbooking":                     "Scrapbooking",
    "Háčkování a haptika":              "Crochet & Crafts",
    "Dřevo a řemesla":                  "Woodwork & Crafts",
    "Tvoření s dětmi":                  "Kids' Crafts",
    "3D tisk a modelování":             "3D Printing",
    "Nástroje":                         "Tools",
    "Ruční nářadí":                     "Hand Tools",
    "Elektrické nářadí":                "Power Tools",
    "Šrouby a spojovací mat.":          "Fixings & Fasteners",
    "Lepidla a těsnicí látky":          "Adhesives & Sealants",
    "Zahrada a outdoor":                "Garden & Outdoor",
    "Zahrada a dílna":                  "Garden & Workshop",
    "Zahrada":                          "Garden",
    "Sport a outdoor":                  "Sports & Outdoors",
    "Sport a kola":                     "Sports & Cycling",
    "Vodní sporty":                     "Water Sports",
    "Cyklistika":                       "Cycling",
    "GPS a navigace":                   "GPS & Navigation",
    "Auto elektronika":                 "Car Electronics",
    "Dekorace":                         "Home Décor",
    "Bytové vybavení":                  "Home Furnishings",
    "Cestování":                        "Travel",
    # ── Family & Kids ───────────────────────────────────────────────────────
    "Hračky":                           "Toys",
    "Plyšové hračky":                   "Soft Toys",
    "Venkovní hračky":                  "Outdoor Toys",
    "Vzdělávací hračky":                "Educational Toys",
    "RC modely":                        "RC Models",
    "Figurky a sběratelství":           "Figures & Collectibles",
    "Panenky":                          "Dolls",
    "Kostýmy a party":                  "Costumes & Party",
    "Deskové a karetní hry":            "Board & Card Games",
    "LEGO a stavebnice":                "LEGO & Construction",
    "Puzzle":                           "Puzzles",
    "Tvoření a výtvarno":               "Kids' Art & Craft",
    "Dětské zboží":                     "Baby & Toddler",
    "Hry a hračky":                     "Games & Toys",
    "Zvířata":                          "Pet Supplies",
    "Dětské autosedačky":               "Child Car Seats",
    "Dětské kočárky":                   "Strollers",
    "Kindersitze":                      "Child Car Seats",
    "Kinderwagen":                      "Strollers",
    "Baby Formula (Pre)":               "Baby Food",
    "Fahrradhelme Kinder":              "Kids' Helmets",
    # ── Fashion & Beauty ────────────────────────────────────────────────────
    "Móda":                             "Clothing & Fashion",
    "Kosmetika":                        "Beauty & Cosmetics",
    "Parfumy":                          "Perfumes",
    "Boty":                             "Shoes",
    "Doplňky a šperky":                 "Accessories & Jewellery",
    "Tašky a batohy":                   "Bags & Backpacks",
    "Spodní prádlo a ponožky":          "Underwear & Socks",
    "Sportovní oblečení":               "Sportswear",
    "Líčení a make-up":                 "Make-up",
    "Vlasová kosmetika":                "Hair Care",
    "Péče o pleť":                      "Skin Care",
    "Prémiová kosmetika":               "Premium Beauty",
    "Manikúra a pedikúra":              "Nail Care",
    "Holení a depilace":                "Shaving & Hair Removal",
    "Ústní hygiena":                    "Oral Care",
    "Deodoranty a antiperspiranty":     "Deodorants",
    # ── Other ───────────────────────────────────────────────────────────────
    "Průmyslové zboží":                 "Industrial",
    "Kancelářské potřeby":              "Office Supplies",
    "Ostatní":                          "Other",
    "Ostatní spotřebiče":               "Other Appliances",
    "Hudební nástroje":                 "Musical Instruments",
    "Kytary":                           "Guitars",
    "Mikrofony":                        "Microphones",
    "Bicí":                             "Drums",
    "Dechové nástroje":                 "Wind Instruments",
    "Klávesy":                          "Keyboards (Music)",
    "Struny a příslušenství":           "Strings & Accessories",
    "Audio rozhraní":                   "Audio Interfaces",
    "Psací potřeby":                    "Stationery",
    "Papír a notesy":                   "Paper & Notebooks",
    "Organizace kanceláře":             "Office Organisation",
    "Měřicí přístroje":                 "Measuring Tools",
    "Elektroinstalace":                 "Electrical Installation",
    "Pájení a elektronika":             "Soldering & Electronics",
    "Automotive":                       "Automotive",
    "Sběratelství":                     "Collectibles",
    "Umění a sběratelství":             "Art & Collectibles",
    "Potraviny":                        "Food & Grocery",
    "Books":                            "Books",
    "None":                             None,
}

# Sub-group labels within each super-category — drives <optgroup> in the dropdown.
# Format: {super_cat: {clean_sub_name: group_label}}
_SUB_GROUP_MAP = {
    "📱 Phones & Tablets": {
        "Smartphones":          "📞 Phones",
        "Tablets":              "📟 Tablets",
        "Smartwatches":         "⌚ Wearables",
        "Headphones":           "🎧 Audio",
        "Cases & Covers":       "🔌 Accessories",
        "Screen Protectors":    "🔌 Accessories",
        "Holders & Stands":     "🔌 Accessories",
        "Chargers":             "🔌 Accessories",
        "Cables":               "🔌 Accessories",
        "Power Banks":          "🔌 Accessories",
    },
    "💻 Computers & Tablets": {
        "Laptops":              "💻 Laptops",
        "Laptop Accessories":   "💻 Laptops",
        "Desktop PCs":          "🖥️ Desktops",
        "PC Cases":             "🖥️ Desktops",
        "PC Games":             "🎮 Gaming",
        "Gaming Accessories":   "🎮 Gaming",
        "Monitors":             "🖥️ Displays & Peripherals",
        "Keyboards":            "🖥️ Displays & Peripherals",
        "Mice":                 "🖥️ Displays & Peripherals",
        "Webcams":              "🖥️ Displays & Peripherals",
        "Printers":             "🖥️ Displays & Peripherals",
        "Docking Stations":     "🖥️ Displays & Peripherals",
        "Hard Drives & SSDs":   "💾 Storage",
        "Storage":              "💾 Storage",
        "External Drives":      "💾 Storage",
        "USB Drives":           "💾 Storage",
        "Components":           "🔩 Components",
        "RAM":                  "🔩 Components",
        "Graphics Cards":       "🔩 Components",
        "Networking":           "🌐 Networking",
        "Tablets":              "📟 Tablets",
        "Accessories":          "🖥️ Displays & Peripherals",
    },
    "📺 TV, Audio & Electronics": {
        "Televisions":          "📺 TVs",
        "Headphones":           "🎧 Audio",
        "Speakers":             "🎧 Audio",
        "Soundbars & Speakers": "🎧 Audio",
        "Portable Audio":       "🎧 Audio",
        "Home Cinema":          "🎧 Audio",
        "Audio":                "🎧 Audio",
        "Cameras":              "📷 Cameras & Photo",
        "Action Cameras":       "📷 Cameras & Photo",
        "Camcorders":           "📷 Cameras & Photo",
        "Drones":               "📷 Cameras & Photo",
        "Lenses":               "📷 Cameras & Photo",
        "Tripods & Stabilisers":"📷 Cameras & Photo",
        "Flash & Lighting":     "📷 Cameras & Photo",
        "Streaming Devices":    "📡 Smart Home & Streaming",
        "Smart Home":           "📡 Smart Home & Streaming",
        "Projectors":           "📡 Smart Home & Streaming",
        "Cables & Adapters":    "🔌 Accessories",
        "Cables & Hubs":        "🔌 Accessories",
        "Batteries":            "🔌 Accessories",
        "Remote Controls":      "🔌 Accessories",
        "Memory Cards":         "🔌 Accessories",
        "Binoculars":           "🔌 Accessories",
        "Media Players":        "🔌 Accessories",
    },
    "🏠 Large Appliances": {
        "Washing Machines":     "🧺 Laundry",
        "Tumble Dryers":        "🧺 Laundry",
        "Fridges & Freezers":   "❄️ Refrigeration",
        "Refrigerators":        "❄️ Refrigeration",
        "Freezers":             "❄️ Refrigeration",
        "Dishwashers":          "🍽️ Dishwashers",
        "Cookers & Ovens":      "🔥 Cooking",
        "Ovens":                "🔥 Cooking",
        "Microwaves":           "🔥 Cooking",
        "Heating & Cooling":    "🌡️ Climate",
        "Air Conditioners":     "🌡️ Climate",
    },
    "🍳 Small Appliances": {
        "Coffee Machines":              "☕ Coffee & Kitchen",
        "Kitchen Appliances":           "☕ Coffee & Kitchen",
        "Blenders & Food Processors":   "☕ Coffee & Kitchen",
        "Kettles":                      "☕ Coffee & Kitchen",
        "Toasters":                     "☕ Coffee & Kitchen",
        "Microwaves":                   "☕ Coffee & Kitchen",
        "Air Fryers":                   "☕ Coffee & Kitchen",
        "Vacuum Cleaners":      "🧹 Vacuum Cleaners",
        "Stick Vacuums":        "🧹 Vacuum Cleaners",
        "Robot Vacuums":        "🧹 Vacuum Cleaners",
        "Air Purifiers":        "🌬️ Air & Personal Care",
        "Hair Styling":         "🌬️ Air & Personal Care",
        "Hair Dryers":          "🌬️ Air & Personal Care",
        "Irons":                "🌬️ Air & Personal Care",
        "Home Appliances":      "🏠 Home",
    },
    "⌚ Wearables & Health": {
        "Smartwatches":         "⌚ Smartwatches",
        "Fitness Trackers":     "⌚ Smartwatches",
        "Health & Wellness":    "💊 Health",
        "Medical Devices":      "💊 Health",
        "Blood Pressure Monitors": "💊 Health",
        "Dental Care":          "💊 Health",
        "Electric Toothbrushes":"💊 Health",
        "Hair Dryers":          "✂️ Personal Care",
        "Shavers":              "✂️ Personal Care",
        "Sunscreen":            "✂️ Personal Care",
    },
    "🏡 Home, Garden & Sport": {
        "Home Essentials":      "🏠 Home",
        "Kitchenware":          "🏠 Home",
        "Organisation":         "🏠 Home",
        "Bathroom":             "🏠 Home",
        "Cleaning":             "🏠 Home",
        "Home Décor":           "🏠 Home",
        "Home Furnishings":     "🏠 Home",
        "Arts & Crafts":        "🎨 Arts & Crafts",
        "Painting & Drawing":   "🎨 Arts & Crafts",
        "Sewing & Knitting":    "🎨 Arts & Crafts",
        "Scrapbooking":         "🎨 Arts & Crafts",
        "Crochet & Crafts":     "🎨 Arts & Crafts",
        "Woodwork & Crafts":    "🎨 Arts & Crafts",
        "Kids' Crafts":         "🎨 Arts & Crafts",
        "3D Printing":          "🎨 Arts & Crafts",
        "Tools":                "🔧 Tools & DIY",
        "Hand Tools":           "🔧 Tools & DIY",
        "Power Tools":          "🔧 Tools & DIY",
        "Fixings & Fasteners":  "🔧 Tools & DIY",
        "Adhesives & Sealants": "🔧 Tools & DIY",
        "Garden":               "🌿 Garden & Outdoors",
        "Garden & Outdoor":     "🌿 Garden & Outdoors",
        "Garden & Workshop":    "🌿 Garden & Outdoors",
        "Sports & Outdoors":    "⚽ Sports",
        "Sports & Cycling":     "⚽ Sports",
        "Water Sports":         "⚽ Sports",
        "Cycling":              "⚽ Sports",
        "GPS & Navigation":     "⚽ Sports",
        "Travel":               "⚽ Sports",
        "Car Electronics":      "🚗 Auto",
    },
    "👶 Family & Kids": {
        "Toys":                 "🧸 Toys",
        "Soft Toys":            "🧸 Toys",
        "Outdoor Toys":         "🧸 Toys",
        "Educational Toys":     "🧸 Toys",
        "RC Models":            "🧸 Toys",
        "Games & Toys":         "🧸 Toys",
        "Figures & Collectibles":"🎭 Figures & Games",
        "Dolls":                "🎭 Figures & Games",
        "Costumes & Party":     "🎭 Figures & Games",
        "Board & Card Games":   "🎭 Figures & Games",
        "LEGO & Construction":  "🎭 Figures & Games",
        "Puzzles":              "🎭 Figures & Games",
        "Kids' Art & Craft":    "🎭 Figures & Games",
        "Baby & Toddler":       "👶 Baby",
        "Child Car Seats":      "👶 Baby",
        "Strollers":            "👶 Baby",
        "Baby Food":            "👶 Baby",
        "Kids' Helmets":        "👶 Baby",
        "Pet Supplies":         "🐾 Pets",
    },
    "👗 Fashion & Beauty": {
        "Clothing & Fashion":   "👗 Clothing",
        "Accessories & Jewellery": "👗 Clothing",
        "Bags & Backpacks":     "👗 Clothing",
        "Underwear & Socks":    "👗 Clothing",
        "Sportswear":           "👗 Clothing",
        "Shoes":                "👗 Clothing",
        "Beauty & Cosmetics":   "💄 Beauty",
        "Make-up":              "💄 Beauty",
        "Hair Care":            "💄 Beauty",
        "Skin Care":            "💄 Beauty",
        "Nail Care":            "💄 Beauty",
        "Premium Beauty":       "💄 Beauty",
        "Perfumes":             "💄 Beauty",
        "Shaving & Hair Removal": "🪒 Grooming",
        "Oral Care":            "🪒 Grooming",
        "Deodorants":           "🪒 Grooming",
    },
    "📦 Other": {
        "Musical Instruments":  "🎵 Music",
        "Guitars":              "🎵 Music",
        "Microphones":          "🎵 Music",
        "Drums":                "🎵 Music",
        "Wind Instruments":     "🎵 Music",
        "Keyboards (Music)":    "🎵 Music",
        "Strings & Accessories":"🎵 Music",
        "Audio Interfaces":     "🎵 Music",
        "Office Supplies":      "📋 Office",
        "Stationery":           "📋 Office",
        "Paper & Notebooks":    "📋 Office",
        "Office Organisation":  "📋 Office",
        "Books":                "📚 Books & Media",
        "Industrial":           "🏭 Industrial",
        "Measuring Tools":      "🏭 Industrial",
        "Electrical Installation": "🏭 Industrial",
        "Soldering & Electronics": "🏭 Industrial",
        "Automotive":           "🚗 Automotive",
        "Food & Grocery":       "🛒 Grocery",
        "Collectibles":         "🎨 Art & Collectibles",
        "Art & Collectibles":   "🎨 Art & Collectibles",
    },
}

# ── Reverse look-up maps (built once at import time) ──────────────────────────
# _SUPER_CAT_REVERSE: clean super-label → [raw MainCategory value, ...]
_SUPER_CAT_REVERSE: dict[str, list[str]] = {}
for _raw_main, _super in _SUPER_CAT_MAP.items():
    _SUPER_CAT_REVERSE.setdefault(_super, []).append(_raw_main)

# _SUB_CLEAN_REVERSE: clean sub name → [raw Category value, ...]
_SUB_CLEAN_REVERSE: dict[str, list[str]] = {}
for _raw_sub, _clean in _SUB_CLEAN_MAP.items():
    if _clean is not None:
        _SUB_CLEAN_REVERSE.setdefault(_clean, []).append(_raw_sub)


def get_categories_hierarchical(country=None, source=None):
    """Return list of {main, subs: [{sub, count}]} grouped into clean super-categories.
    Pass country=None to get categories across all markets.
    Pass source=<source_name> to restrict to that source only."""
    conn = open_db()

    # Check whether MainCategory column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()]
    has_main = "MainCategory" in cols

    clauses = []
    params = []
    if country:
        clauses.append("COALESCE(country,'CZ') = ?")
        params.append(country)
    if source:
        clauses.append("source = ?")
        params.append(source)
    where_clause = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    if has_main:
        rows = conn.execute(
            f"SELECT COALESCE(MainCategory,'Ostatní') as main, Category, COUNT(*) as cnt "
            f"FROM products {where_clause} "
            f"GROUP BY main, Category ORDER BY main, cnt DESC",
            params
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT COALESCE(Category,'Ostatní') as main, Category, COUNT(*) as cnt "
            f"FROM products {where_clause} "
            f"GROUP BY Category ORDER BY cnt DESC",
            params
        ).fetchall()

    conn.close()

    # Map raw MainCategory values into super-categories, merging sub-categories
    from collections import defaultdict
    super_tree = defaultdict(dict)  # super_label -> {clean_sub: total_count}
    for main, sub, cnt in rows:
        super_label = _SUPER_CAT_MAP.get(main, "📦 Other")
        sub_raw = sub or "Nezařazeno"
        # Apply clean name mapping; skip if mapped to None (drop)
        if sub_raw in _SUB_CLEAN_MAP:
            clean_sub = _SUB_CLEAN_MAP[sub_raw]
            if clean_sub is None:
                continue
        else:
            clean_sub = sub_raw  # keep as-is if not in map
        super_tree[super_label][clean_sub] = super_tree[super_label].get(clean_sub, 0) + cnt

    result = []
    for super_label in _SUPER_ORDER:
        if super_label not in super_tree:
            continue
        subs_dict = super_tree[super_label]
        group_map = _SUB_GROUP_MAP.get(super_label, {})
        subs = sorted(
            [{"sub": s, "count": c, "group": group_map.get(s, "")}
             for s, c in subs_dict.items()],
            key=lambda x: x["count"],
            reverse=True
        )
        result.append({"main": super_label, "subs": subs})
    return result


def query_keywords():
    """Return all distinct keyword tags with their product counts, sorted by count."""
    import json as _json
    conn = open_db()
    rows = conn.execute(
        "SELECT keywords FROM products WHERE keywords IS NOT NULL"
    ).fetchall()
    conn.close()
    counts = {}
    for (kw_json,) in rows:
        try:
            for tag in _json.loads(kw_json):
                counts[tag] = counts.get(tag, 0) + 1
        except Exception:
            pass
    return sorted(counts.items(), key=lambda x: -x[1])


def build_html():
    with _html_lock:
        if _html_cache["html"] and (time.time() - _html_cache["ts"]) < HTML_TTL:
            return _html_cache["html"]

    with open(TMPL, encoding="utf-8") as f:
        html = f.read()
    # Inject repairability + FR gov data as inline JS (avoids extra round-trip on load).
    # Uses the shared query_ir_data() cache so no extra DB hit.
    ir = query_ir_data()
    inject = (
        f'<script>window.__IR_SCORES={json.dumps(ir["ir_scores"], separators=(",",":"))};</script>\n'
        f'<script>window.__FR_GOV={json.dumps(ir["fr_gov"], separators=(",",":"))};</script>\n'
    )
    html = html.replace("</head>", inject + "</head>", 1)
    # Fingerprint static asset URLs so browsers always reload JS/CSS after a server restart
    html = html.replace('href="/static/style.css"',  f'href="/static/style.css?v={_ASSET_VERSION}"')
    html = html.replace('src="/static/app.js"',      f'src="/static/app.js?v={_ASSET_VERSION}"')
    with _html_lock:
        _html_cache["html"] = html
        _html_cache["ts"]   = time.time()
    return html


def query_ir_data():
    """Return __IR_SCORES and __FR_GOV as a JSON-serialisable dict.
    Used both by build_html() (server-side injection) and the /api/ir-data
    endpoint (client-side fetch for the Cloudflare Pages static build)."""
    if _ir_cache["data"] and (time.time() - _ir_cache["ts"]) < IR_TTL:
        return _ir_cache["data"]

    conn = open_db()
    rows = conn.execute(
        "SELECT Name, repairability_score_fr, repairability_score_date, "
        "repairability_sub_scores_json FROM products WHERE repairability_score_fr IS NOT NULL"
    ).fetchall()
    scores = {r[0]: {"s": r[1], "d": r[2], "sub": r[3]} for r in rows}

    gov_rows = conn.execute(
        "SELECT nom_metteur_sur_le_marche, nom_modele, categorie_produit, "
        "COALESCE(main_category,''), note_ir, date_calcul, sub_scores_json, "
        "COALESCE(url_tableau_detail,'') FROM fr_repairability_index ORDER BY note_ir DESC"
    ).fetchall()
    conn.close()

    gov_products = []
    for r in gov_rows:
        brand, model, cat, main, score, date, sub, url = r
        display_name = f"{brand} {model}".strip() if brand else model
        scores[display_name] = {"s": score, "d": date, "sub": sub}
        gov_products.append({
            "n": display_name, "c": cat, "m": main,
            "s": score, "d": date, "sub": sub, "u": url,
        })

    result = {"ir_scores": scores, "fr_gov": gov_products}
    _ir_cache["data"] = result
    _ir_cache["ts"]   = time.time()
    return result


def query_products(params):
    q            = params.get("q", [""])[0].strip()
    main_category = params.get("main_category", [""])[0]
    category     = params.get("category", [""])[0]
    min_stars    = params.get("min_stars", [""])[0]
    max_return  = params.get("max_return", [""])[0]
    min_reviews = params.get("min_reviews", [""])[0]
    min_rec     = params.get("min_recommend", [""])[0]
    sort_by     = params.get("sort", ["RecommendRate_pct"])[0]
    order       = params.get("order", ["desc"])[0]
    page        = int(params.get("page", ["1"])[0])
    source      = params.get("source", [""])[0]
    keyword     = params.get("keyword", [""])[0]
    avoid       = params.get("avoid", [""])[0]  # "1" = show products to avoid

    # Country filter only applied when a specific source is selected.
    # When no source is chosen we show ALL countries so the default view
    # includes CZ + DE + PL products together.
    SOURCE_COUNTRY = {
        "otto": "DE", "otto_de": "DE",
        "amazon": "DE", "amazon_de": "DE",
        "warentest": "DE",
        "conrad": "DE",
        "dtest": "CZ",
        "alza": "CZ", "heureka": "CZ", "zbozi": "CZ", "datart": "CZ",
        "ceneo": "PL",
        "amazon_us": "US",
        "heureka_sk": "SK",
        "fnac": "FR",
    }
    country = SOURCE_COUNTRY.get(source, None) if source else None

    order_sql = "ASC" if order == "asc" else "DESC"

    # Never surface hidden sources (paid rating agencies) in the public UI.
    # If a user explicitly requests one (e.g. old bookmark), ignore it.
    if source in HIDDEN_SOURCES:
        source = ""

    # Always exclude hidden sources regardless of other filters
    conditions = ["source NOT IN ('dtest','warentest')"]
    plist = []
    if country:
        conditions.append("COALESCE(country,'CZ') = ?"); plist.append(country)
    if q:
        conditions.append("(Name LIKE ? OR Category LIKE ?)")
        plist += [f"%{q}%", f"%{q}%"]
    if main_category:
        # Expand clean super-label → all raw MainCategory values it covers.
        # Always include the filter value itself so English Coolblue names match directly.
        raw_mains = list(dict.fromkeys(
            _SUPER_CAT_REVERSE.get(main_category, []) + [main_category]
        ))
        placeholders = ",".join("?" * len(raw_mains))
        conditions.append(f"MainCategory IN ({placeholders})")
        plist.extend(raw_mains)
    if category:
        # Expand clean sub name → all raw Category values it covers.
        # Always include the filter value itself so English Coolblue names match directly.
        raw_cats = list(dict.fromkeys(
            _SUB_CLEAN_REVERSE.get(category, []) + [category]
        ))
        placeholders = ",".join("?" * len(raw_cats))
        conditions.append(f"Category IN ({placeholders})")
        plist.extend(raw_cats)
    if min_stars:
        conditions.append("AvgStarRating >= ?"); plist.append(float(min_stars))
    if max_return:
        conditions.append("(ReturnRate_pct <= ? OR ReturnRate_pct IS NULL)"); plist.append(float(max_return))
    if min_reviews:
        conditions.append("ReviewsCount >= ?"); plist.append(int(min_reviews))
    if min_rec:
        conditions.append("RecommendRate_pct >= ?"); plist.append(float(min_rec))
    if source:
        # Map UI filter values → actual source values stored in DB
        source_map = {
            "amazon": "source IN ('amazon','amazon_de')",
            "otto":   "source IN ('otto','otto_de')",
        }
        if source in source_map:
            conditions.append(source_map[source])
        else:
            conditions.append("source = ?"); plist.append(source)
    if keyword:
        conditions.append('keywords LIKE ?'); plist.append(f'%"{keyword}"%')
    if avoid == "1":
        # Products to avoid: low star rating with enough reviews to be meaningful
        # OR low recommendation rate with enough reviews
        # OR Stiftung Warentest / D-test flagged as poor (AvgStarRating < 2.5 = "ausreichend"/"mangelhaft")
        conditions.append("""(
            (AvgStarRating IS NOT NULL AND AvgStarRating < 3.5 AND ReviewsCount >= 100)
            OR
            (RecommendRate_pct IS NOT NULL AND RecommendRate_pct < 65 AND ReviewsCount >= 50)
            OR
            (1=0)  -- hidden sources placeholder (warentest/dtest removed)
        )""")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # For price sorting, use whichever price column is available
    ALLOWED_SORT = {
        "RecommendRate_pct", "AvgStarRating", "ReviewsCount",
        "Price_CZK", "Price_EUR", "ReturnRate_pct", "Name",
        "repairability_score_fr", "durability_score_fr",
    }
    if sort_by not in ALLOWED_SORT:
        sort_by = "RecommendRate_pct"

    sort_expr = sort_by
    if sort_by == "Price_CZK":
        sort_expr = "COALESCE(Price_CZK, Price_EUR)"
    elif sort_by == "Price_EUR":
        sort_expr = "COALESCE(Price_EUR, Price_CZK)"

    null_last = f"CASE WHEN {sort_expr} IS NULL THEN 1 ELSE 0 END"

    conn = open_db()
    total = conn.execute(f"SELECT COUNT(*) FROM products {where}", plist).fetchone()[0]
    offset = (page - 1) * PAGE_SIZE
    rows = conn.execute(
        f"""SELECT id, Name, MainCategory, Category, ProductURL, Price_CZK,
                   COALESCE(Price_EUR, NULL) as Price_EUR,
                   COALESCE(country, 'CZ') as country,
                   currency,
                   AvgStarRating, StarRatingsCount, ReviewsCount,
                   RecommendRate_pct, ReturnRate_pct,
                   Stars5_Count, Stars4_Count, Stars3_Count,
                   Stars2_Count, Stars1_Count, source,
                   COALESCE(cat_rank, 0) as source_rank,
                   COALESCE(cat_total, 0) as source_total,
                   keywords,
                   test_date,
                   brand,
                   CASE
                     WHEN repairability_score_fr IS NOT NULL
                          OR durability_score_fr IS NOT NULL
                     THEN json_patch(
                            COALESCE(details_json, '{{}}'),
                            json_object(
                              '_ir_score',    repairability_score_fr,
                              '_ir_date',     repairability_score_date,
                              '_ir_sub',      repairability_sub_scores_json,
                              '_dur_score',   durability_score_fr,
                              '_dur_sub',     durability_sub_scores_json,
                              '_warranty',    warranty_years,
                              '_energy',      energy_class,
                              '_brand',       brand
                            )
                          )
                     ELSE COALESCE(details_json, NULL)
                   END as details_json
            FROM products {where}
            ORDER BY {null_last}, {sort_expr} {order_sql}
            LIMIT ? OFFSET ?""",
        plist + [PAGE_SIZE, offset]
    ).fetchall()
    conn.close()
    return {
        "products": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "pages": math.ceil(total / PAGE_SIZE),
        "page_size": PAGE_SIZE,
        "_srv": "pid1021-v2"
    }


def query_repair_scores():
    """Return repairability scores keyed by rowid (works for products where id is NULL)."""
    conn = open_db()
    rows = conn.execute(
        """SELECT rowid, repairability_score_fr, repairability_score_date,
                  repairability_sub_scores_json
           FROM products
           WHERE repairability_score_fr IS NOT NULL"""
    ).fetchall()
    conn.close()
    result = {}
    for r in rows:
        result[r[0]] = {
            "s": r[1],
            "d": r[2],
            "sub": r[3],
        }
    return result


def get_fr_gov_categories():
    """Return hierarchical categories from fr_repairability_index."""
    from collections import OrderedDict
    conn = open_db()
    rows = conn.execute(
        """SELECT COALESCE(main_category,'Autres') as main,
                  categorie_produit, COUNT(*) as cnt
           FROM fr_repairability_index
           GROUP BY main, categorie_produit
           ORDER BY main, cnt DESC"""
    ).fetchall()
    conn.close()
    tree = OrderedDict()
    for main, sub, cnt in rows:
        tree.setdefault(main, []).append({"sub": sub, "count": cnt})
    ordered = sorted(tree.keys())
    return [{"main": m, "subs": tree[m]} for m in ordered]


def query_fr_gov_products(params):
    """Query fr_repairability_index — French government repairability database."""
    q         = params.get("q", [""])[0].strip()
    main_cat  = params.get("main_category", [""])[0]
    category  = params.get("category", [""])[0]
    sort_by   = params.get("sort", ["note_ir"])[0]
    order     = params.get("order", ["desc"])[0]
    page      = int(params.get("page", ["1"])[0])

    # Map UI sort keys to DB columns
    sort_map = {
        "RecommendRate_pct": "note_ir",
        "AvgStarRating":     "note_ir",
        "Name":              "nom_modele",
        "note_ir":           "note_ir",
    }
    db_sort   = sort_map.get(sort_by, "note_ir")
    order_sql = "ASC" if order == "asc" else "DESC"

    conditions, plist = [], []
    if q:
        conditions.append(
            "(nom_modele LIKE ? OR categorie_produit LIKE ? OR nom_metteur_sur_le_marche LIKE ?)"
        )
        plist += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if main_cat:
        conditions.append("COALESCE(main_category,'Autres') = ?"); plist.append(main_cat)
    if category:
        conditions.append("categorie_produit = ?"); plist.append(category)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    conn   = open_db()
    total  = conn.execute(
        f"SELECT COUNT(*) FROM fr_repairability_index {where}", plist
    ).fetchone()[0]
    offset = (page - 1) * PAGE_SIZE
    rows   = conn.execute(
        f"""SELECT nom_modele, nom_metteur_sur_le_marche, categorie_produit,
                   main_category, note_ir, date_calcul, sub_scores_json,
                   url_tableau_detail
            FROM fr_repairability_index {where}
            ORDER BY {db_sort} {order_sql}
            LIMIT ? OFFSET ?""",
        plist + [PAGE_SIZE, offset]
    ).fetchall()
    conn.close()

    products = []
    for r in rows:
        nom_modele, brand, cat_fr, main_cat_val, note_ir, date_calcul, sub_json, url = r
        display_name = f"{brand} {nom_modele}".strip() if brand else nom_modele
        products.append({
            "id":               None,
            "Name":             display_name,
            "MainCategory":     main_cat_val,
            "Category":         cat_fr,
            "ProductURL":       url,
            "Price_CZK":        None,
            "Price_EUR":        None,
            "country":          "FR",
            "currency":         "EUR",
            "AvgStarRating":    None,
            "StarRatingsCount": None,
            "ReviewsCount":     None,
            "RecommendRate_pct": None,
            "ReturnRate_pct":   None,
            "Stars5_Count":     None,
            "Stars4_Count":     None,
            "Stars3_Count":     None,
            "Stars2_Count":     None,
            "Stars1_Count":     None,
            "source":           "fr_ir",
            "source_rank":      0,
            "source_total":     0,
            "keywords":         None,
            "details_json":     None,
        })
    return {
        "products": products,
        "total":    total,
        "page":     page,
        "pages":    math.ceil(max(total, 1) / PAGE_SIZE),
        "page_size": PAGE_SIZE,
    }


def query_snapshot_movers(days: int = 7, limit: int = 40, metric: str = "recommend") -> dict:
    """
    Return biggest rating/price movers over the last `days` days.

    Compares the most recent snapshot for each product against a snapshot
    from ~`days` days ago (nearest available). Returns two lists:
      - risers: products whose metric improved the most
      - fallers: products whose metric dropped the most

    `metric` can be: "recommend", "stars", "price"
    """
    import os as _os
    _snaps_path = _os.environ.get(
        "SNAPSHOTS_DB_PATH",
        _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "snapshots.db"))
    )
    if not _os.path.exists(_snaps_path):
        return {"risers": [], "fallers": [], "error": "snapshots.db not found"}

    try:
        sc = sqlite3.connect(_snaps_path, timeout=20)
        sc.row_factory = sqlite3.Row

        if metric == "stars":
            col = "avg_star_rating"
        elif metric == "price":
            col = "price_czk"
        else:
            col = "recommend_pct"

        # Get latest and oldest-in-window snapshot per product
        rows = sc.execute(f"""
            WITH recent AS (
                SELECT product_url, source,
                       {col} AS new_val,
                       snapshot_date AS new_date
                FROM   product_snapshots
                WHERE  snapshot_date = (
                    SELECT MAX(snapshot_date) FROM product_snapshots s2
                    WHERE s2.product_url = product_snapshots.product_url
                      AND s2.source      = product_snapshots.source
                )
                AND    {col} IS NOT NULL
            ),
            older AS (
                SELECT product_url, source,
                       {col} AS old_val,
                       snapshot_date AS old_date
                FROM   product_snapshots
                WHERE  snapshot_date BETWEEN date('now', '-' || ? || ' days', '-7 days')
                                         AND date('now', '-' || ? || ' days', '+7 days')
                  AND  {col} IS NOT NULL
            ),
            -- pick one older row per product (closest to target)
            best_old AS (
                SELECT product_url, source, old_val, old_date,
                       ROW_NUMBER() OVER (
                           PARTITION BY product_url, source
                           ORDER BY ABS(julianday(old_date) - julianday(date('now', '-' || ? || ' days')))
                       ) AS rn
                FROM older
            )
            SELECT r.product_url, r.source, r.new_date, r.new_val,
                   b.old_date,   b.old_val,
                   ROUND(r.new_val - b.old_val, 2) AS delta
            FROM   recent r
            JOIN   best_old b ON b.product_url = r.product_url
                             AND b.source       = r.source
                             AND b.rn = 1
            WHERE  ABS(r.new_val - b.old_val) >= 0.5
            ORDER  BY delta DESC
        """, (days, days, days)).fetchall()
        sc.close()

        # Enrich with product names from products.db
        pconn = open_db()
        result = []
        for row in rows:
            prod = pconn.execute(
                "SELECT Name, Category, MainCategory FROM products WHERE ProductURL=? AND source=? LIMIT 1",
                (row["product_url"], row["source"])
            ).fetchone()
            result.append({
                "url":       row["product_url"],
                "source":    row["source"],
                "name":      prod["Name"]         if prod else row["product_url"],
                "category":  prod["Category"]     if prod else "",
                "main_cat":  (prod["MainCategory"] if prod and "MainCategory" in prod.keys() else "") or "",
                "new_val":   row["new_val"],
                "old_val":   row["old_val"],
                "delta":     row["delta"],
                "new_date":  row["new_date"],
                "old_date":  row["old_date"],
            })
        pconn.close()

        risers  = sorted([r for r in result if r["delta"] > 0],  key=lambda x: -x["delta"])[:limit]
        fallers = sorted([r for r in result if r["delta"] < 0],  key=lambda x:  x["delta"])[:limit]
        return {"risers": risers, "fallers": fallers, "metric": metric, "days": days}

    except Exception as e:
        import logging
        logging.error(f"query_snapshot_movers failed: {e}", exc_info=True)
        return {"risers": [], "fallers": [], "error": str(e)}


def query_product_history(product_url: str) -> list:
    """Return full snapshot history for one product URL, oldest first."""
    import os as _os
    _snaps_path = _os.environ.get(
        "SNAPSHOTS_DB_PATH",
        _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "snapshots.db"))
    )
    if not _os.path.exists(_snaps_path):
        return []
    try:
        sc = sqlite3.connect(_snaps_path, timeout=20)
        sc.row_factory = sqlite3.Row
        rows = sc.execute("""
            SELECT snapshot_date, recommend_pct, review_count,
                   avg_star_rating, price_czk, price_eur
            FROM   product_snapshots
            WHERE  product_url = ?
            ORDER  BY snapshot_date ASC
        """, (product_url,)).fetchall()
        sc.close()
        return [dict(r) for r in rows]
    except Exception as e:
        import logging
        logging.error(f"query_product_history failed: {e}")
        return []


def query_stats():
    conn = open_db()
    r = conn.execute("""
        SELECT COUNT(*) as total,
               ROUND(AVG(AvgStarRating),2) as avg_stars,
               ROUND(AVG(ReturnRate_pct),2) as avg_return,
               ROUND(AVG(RecommendRate_pct),2) as avg_recommend,
               COUNT(DISTINCT Category) as categories,
               SUM(CASE WHEN source='alza' THEN 1 ELSE 0 END) as from_alza,
               SUM(CASE WHEN source!='alza' THEN 1 ELSE 0 END) as from_scraper
        FROM products
    """).fetchone()
    conn.close()
    return dict(r)


def query_competitor_brands():
    """Return list of lowercase brand names that have any competitor scores."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT LOWER(brand) FROM competitor_scores WHERE brand IS NOT NULL AND brand != ''"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def query_competitor_scores(brand: str):
    """Return competitor scores for a given brand, grouped by source and category."""
    import json as _json
    if not brand or len(brand) < 2:
        return []
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT source, canonical_category, product_category,
               ROUND(AVG(score_normalized), 1) as score,
               COUNT(*) as n,
               MAX(raw_score_label) as label,
               MAX(source_url) as url,
               MAX(sub_scores_json) as sub_scores_json
        FROM competitor_scores
        WHERE LOWER(brand) = LOWER(?)
        GROUP BY source, canonical_category
        ORDER BY source, canonical_category
    """, (brand,)).fetchall()
    conn.close()
    result = []
    for r in rows:
        sub = None
        try:
            sub = _json.loads(r['sub_scores_json']) if r['sub_scores_json'] else None
        except Exception:
            pass
        result.append({
            "source": r["source"],
            "category": r["canonical_category"] or r["product_category"],
            "score": r["score"],
            "n": r["n"],
            "label": r["label"],
            "url": r["url"],
            "sub_scores": sub,
        })
    return result


# Czech/English common words and adjectives that appear as scraped "brands"
# but are not real brand names.
_NON_BRAND_WORDS = {
    # Czech adjectives/nouns scraped from product titles
    "prací", "mobilní", "bílé", "bílá", "černé", "černá", "velké", "malé",
    "nové", "nový", "domácí", "kuchyské", "elektrické", "digitální",
    "chytré", "chytrá", "herní", "výkonné", "přenosné", "bezdrátové",
    "slim", "ultra", "mini", "nano", "pro", "max", "plus", "air", "go",
    "lite", "neo", "one", "fit", "new", "top", "best", "cool", "smart",
    "eco", "turbo", "flex", "jet", "flow", "wave", "edge", "core",
    # Short fragments that pass the >=2 char filter
    "be", "by", "do", "na", "ze", "se", "to", "je", "od", "ve",
    # Generic category words sometimes scraped as brands
    "laptop", "notebook", "phone", "tablet", "watch", "camera", "speaker",
    "headphone", "keyboard", "mouse", "monitor", "router", "printer",
}


def query_brands(q="", category="", sort="avg_score"):
    """Return brands with their aggregated competitor scores."""
    import json as _json
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    conditions = []
    params = []
    if q:
        conditions.append("LOWER(brand) LIKE ?")
        params.append(f"%{q.lower()}%")
    if category:
        conditions.append("canonical_category = ?")
        params.append(category)

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    allowed_sort = {"avg_score", "brand", "num_sources", "french_score", "openrepair_score", "yale_score", "ifixit_score", "bifl_score"}
    if sort not in allowed_sort:
        sort = "avg_score"
    sort_sql = "brand ASC" if sort == "brand" else f"COALESCE({sort}, -1) DESC"

    rows = conn.execute(f"""
        SELECT
            brand,
            COUNT(DISTINCT source) as num_sources,
            COUNT(*) as total_records,
            ROUND(AVG(score_normalized), 1) as avg_score,
            ROUND(AVG(CASE WHEN source='french_index' THEN score_normalized END), 1) as french_score,
            ROUND(AVG(CASE WHEN source='openrepair'   THEN score_normalized END), 1) as openrepair_score,
            ROUND(AVG(CASE WHEN source='yale'         THEN score_normalized END), 1) as yale_score,
            ROUND(AVG(CASE WHEN source='ifixit'       THEN score_normalized END), 1) as ifixit_score,
            ROUND(AVG(CASE WHEN source='bifl'         THEN score_normalized END), 1) as bifl_score,
            GROUP_CONCAT(DISTINCT canonical_category) as categories
        FROM competitor_scores
        {where}
        GROUP BY brand
        HAVING brand IS NOT NULL AND brand != '' AND LENGTH(brand) >= 2
               AND total_records >= 2
        ORDER BY {sort_sql}
        LIMIT 300
    """, params).fetchall()
    conn.close()
    return [
        dict(r) for r in rows
        if r["brand"].lower() not in _NON_BRAND_WORDS
    ]


def query_brand_categories():
    """Return distinct canonical categories that appear in competitor_scores."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT canonical_category FROM competitor_scores "
        "WHERE canonical_category IS NOT NULL AND canonical_category != '' "
        "ORDER BY canonical_category"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence access log

    def send_json(self, data, status=200, max_age=0):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        # CORS — required when the static frontend is hosted on Cloudflare Pages
        self.send_header("Access-Control-Allow-Origin", "*")
        if max_age > 0:
            self.send_header("Cache-Control", f"public, max-age={max_age}")
        else:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html, status=200, max_age=120):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Cache-Control", f"public, max-age={max_age}")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path == "/":
            self.send_html(build_html(), max_age=120)

        elif path == "/api/products":
            if params.get("source", [""])[0] == "fr_ir":
                self.send_json(query_fr_gov_products(params), max_age=60)
            else:
                self.send_json(query_products(params), max_age=60)

        elif path == "/api/ir-data":
            # Used by the Cloudflare Pages static build (frontend fetches IR scores async)
            self.send_json(query_ir_data(), max_age=IR_TTL)

        elif path == "/api/repair":
            self.send_json(query_repair_scores(), max_age=3600)

        elif path == "/api/categories":
            try:
                country = params.get("country", [""])[0]
                source  = params.get("source",  [""])[0] or None
                if country == "FR_IR":
                    self.send_json(get_fr_gov_categories(), max_age=300)
                    return
                if country not in ("CZ", "DE", "PL", "SK", "US", "FR"):
                    country = None   # None = all countries
                self.send_json(get_categories_hierarchical(country, source), max_age=60)
            except Exception as e:
                import logging
                logging.error(f"/api/categories failed: {e}")
                self.send_json([])

        elif path == "/api/stats":
            self.send_json(query_stats(), max_age=300)

        elif path == "/api/keywords":
            self.send_json([{"tag": t, "count": c} for t, c in query_keywords()], max_age=600)

        elif path == "/api/cross-market":
            try:
                from scraper.cross_market import find_cross_market_matches
                min_m   = int(params.get("min_markets", ["2"])[0])
                inc_amz = params.get("include_amazon", ["0"])[0] == "1"
                conn    = open_db()
                groups  = find_cross_market_matches(conn, min_markets=min_m,
                                                    include_amazon_us=inc_amz)
                conn.close()
                self.send_json(groups, max_age=600)
            except Exception as e:
                import logging; logging.error(f"/api/cross-market: {e}", exc_info=True)
                self.send_json({"error": str(e)})

        elif path == "/api/snapshot-movers":
            try:
                days   = int(params.get("days",   ["7"])[0])
                limit  = int(params.get("limit",  ["40"])[0])
                metric = params.get("metric", ["recommend"])[0]
                if metric not in ("recommend", "stars", "price"):
                    metric = "recommend"
                self.send_json(query_snapshot_movers(days=days, limit=limit, metric=metric), max_age=300)
            except Exception as e:
                import logging; logging.error(f"/api/snapshot-movers: {e}", exc_info=True)
                self.send_json({"risers": [], "fallers": [], "error": str(e)})

        elif path == "/api/product-history":
            try:
                url = params.get("url", [""])[0]
                if not url:
                    self.send_json({"error": "url param required"}, status=400)
                else:
                    self.send_json(query_product_history(url), max_age=300)
            except Exception as e:
                import logging; logging.error(f"/api/product-history: {e}", exc_info=True)
                self.send_json({"error": str(e)})

        elif path == "/api/scrape-status":
            self.send_json(_query_scrape_status())

        elif path == "/api/health":
            self.send_json(_query_health(), max_age=120)

        elif path == "/api/run-scraper":
            # Trigger today's due scrapers immediately via scheduler --now flag.
            # Runs as a separate subprocess so the server stays responsive.
            subprocess.Popen(
                [sys.executable, _SCHEDULER_PY, "--now"],
                stdout=open(os.path.join(os.path.dirname(__file__), "scraper", "logs", "scheduler.log"), "a"),
                stderr=subprocess.STDOUT,
            )
            # Invalidate caches so the next request reflects fresh data once scrapers finish
            _invalidate_html_cache()
            self.send_json({"status": "started", "message": "Scheduler triggered with --now flag. Check /api/scrape-status for progress."})

        elif path == "/api/stop-scraper":
            # Stop the background scheduler daemon process.
            global _scheduler_proc
            if _scheduler_proc and _scheduler_proc.poll() is None:
                _scheduler_proc.terminate()
                self.send_json({"status": "stopped", "message": "Scheduler process terminated."})
            else:
                self.send_json({"status": "not_running", "message": "Scheduler was not running."})

        elif path == "/api/start-scraper":
            _start_scheduler()
            self.send_json({"status": "started", "message": f"Scheduler restarted as PID {_scheduler_proc.pid}."})

        elif path == "/api/brands":
            q      = params.get("q",        [""])[0].strip()
            cat    = params.get("category", [""])[0].strip()
            sort   = params.get("sort",     ["avg_score"])[0].strip()
            self.send_json(query_brands(q, cat, sort))

        elif path == "/api/brand-categories":
            self.send_json(query_brand_categories())

        elif path == "/api/competitor-scores":
            brand = params.get("brand", [""])[0].strip()
            self.send_json(query_competitor_scores(brand))

        elif path == "/api/competitor-brands":
            self.send_json(query_competitor_brands())

        elif path.startswith("/static/"):
            fname = path[len("/static/"):].split("?")[0]  # strip ?v= cache-buster
            fpath = os.path.join(STATIC, fname)
            if os.path.isfile(fpath):
                mime, _ = mimetypes.guess_type(fpath)
                with open(fpath, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", mime or "application/octet-stream")
                self.send_header("Content-Length", len(body))
                self.send_header("Cache-Control", "public, max-age=86400")  # 1 day
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404); self.end_headers()
        else:
            self.send_response(404); self.end_headers()


if __name__ == "__main__":
    # Create an empty DB with the right schema if it doesn't exist yet.
    # This lets the server start on a fresh Fly.io volume — upload products.db
    # afterwards via: fly sftp shell -a database-of-high-quality-products
    if not os.path.exists(DB_PATH):
        print(f"No database at {DB_PATH} — creating empty DB. Upload products.db to populate.")
        _init = sqlite3.connect(DB_PATH)
        _init.execute("""CREATE TABLE IF NOT EXISTS products (
            id INTEGER, Name TEXT, Category TEXT, MainCategory TEXT,
            ProductURL TEXT, Price_CZK REAL, Price_EUR REAL,
            AvgStarRating REAL, StarRatingsCount INTEGER, ReviewsCount INTEGER,
            RecommendRate_pct REAL, ReturnRate_pct REAL,
            Stars5_Count INTEGER, Stars4_Count INTEGER, Stars3_Count INTEGER,
            Stars2_Count INTEGER, Stars1_Count INTEGER,
            source TEXT, country TEXT, currency TEXT,
            keywords TEXT, details_json TEXT, Description TEXT
        )""")
        _init.commit()
        _init.close()
    # On Render/cloud the PORT env var is set automatically.
    # Locally it defaults to 8080.
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"

    # Ensure all indexes exist (fast no-op if already created)
    # Ensure all indexes exist (fast no-op if already created)
    _conn = open_db()
    ensure_indexes(_conn)
    _conn.close()

    # Start the master scheduler as a background subprocess.
    # It manages all scraper schedules (daily/weekly/monthly) and logs runs
    # to the scraper_runs table.  See scraper/scheduler.py for the full schedule.
    os.makedirs(os.path.join(os.path.dirname(__file__), "scraper", "logs"), exist_ok=True)
    _start_scheduler()

    server = HTTPServer((host, port), Handler)
    print(f"✦ QualityDB running at http://localhost:{port}")
    print(f"  Scheduler running as PID {_scheduler_proc.pid} — daily wake-up at 03:00.")
    print("  Press Ctrl+C to stop.")
    server.serve_forever()
