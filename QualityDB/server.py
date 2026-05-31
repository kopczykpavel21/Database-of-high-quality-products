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

# /api/snapshot-deltas is expensive (~1s query on 100k rows) but changes slowly.
# Cache result for 20 minutes so repeated page loads don't hammer the DB.
_delta_cache: dict = {"data": None, "ts": 0.0}
DELTA_TTL = 1200   # 20 minutes

# /api/snapshot-movers is expensive (~1.5s query on 100k rows) but changes slowly.
# Key: (days, limit, metric) tuple → cached dict.
_movers_cache: dict = {}
_movers_ts:    dict = {}
MOVERS_TTL = 600   # 10 minutes

# /api/cross-market is expensive (~2s query) but changes at most once per day.
# Key: (min_markets, include_amazon_us) tuple → cached list.
_cross_market_cache: dict = {}
_cross_market_ts:    dict = {}
CROSS_MARKET_TTL = 1800   # 30 minutes

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
    """Create missing indexes for common filter/sort queries. Safe to call repeatedly.
    Also adds missing columns (image_url, brand, NormalizedCategory, NormalizedMainGroup)
    so the server can start even if products.db was created with an older schema."""
    # ── Schema migration: add columns that may be missing from old DBs ────────
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info(products)")}
    for col, typedef in [
        ("image_url",           "TEXT"),
        ("brand",               "TEXT"),
        ("NormalizedCategory",  "TEXT"),
        ("NormalizedMainGroup", "TEXT"),
        ("first_seen_at",       "TEXT"),   # Set once on first INSERT; never updated
        ("qt_brand_score",      "REAL"),   # Brand avg score from expert tests (institutkvality)
    ]:
        if col not in existing_cols:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {typedef}")
    conn.commit()

    # Trigger: automatically set first_seen_at on first INSERT (only when NULL).
    # ON CONFLICT DO UPDATE paths don't fire AFTER INSERT, so existing rows are safe.
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS trg_products_first_seen
        AFTER INSERT ON products
        WHEN NEW.first_seen_at IS NULL
        BEGIN
            UPDATE products
               SET first_seen_at = STRFTIME('%Y-%m-%d %H:%M:%S', 'NOW')
             WHERE rowid = NEW.rowid;
        END
    """)
    conn.commit()
    # ── Indexes ───────────────────────────────────────────────────────────────
    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_source          ON products(source);
        CREATE INDEX IF NOT EXISTS idx_main_category   ON products(MainCategory);
        CREATE INDEX IF NOT EXISTS idx_country         ON products(country);
        CREATE INDEX IF NOT EXISTS idx_source_cat      ON products(source, Category);
        CREATE INDEX IF NOT EXISTS idx_rec_rate        ON products(RecommendRate_pct);
        CREATE INDEX IF NOT EXISTS idx_price_czk       ON products(Price_CZK);
        CREATE INDEX IF NOT EXISTS idx_price_eur       ON products(Price_EUR);
        CREATE INDEX IF NOT EXISTS idx_keywords          ON products(keywords);
        CREATE INDEX IF NOT EXISTS idx_norm_category      ON products(NormalizedCategory);
        CREATE INDEX IF NOT EXISTS idx_norm_main_group    ON products(NormalizedMainGroup);
        CREATE INDEX IF NOT EXISTS idx_scraped_at         ON products(scraped_at);
        CREATE INDEX IF NOT EXISTS idx_first_seen_at      ON products(first_seen_at);
        CREATE INDEX IF NOT EXISTS idx_name                ON products(Name);
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

        # Latest scraped_at per source (for "imported" label on bulk-uploaded DBs)
        latest_scraped = {
            row[0]: row[1]
            for row in conn.execute(
                "SELECT source, MAX(scraped_at) FROM products GROUP BY source"
            ).fetchall()
            if row[1]  # skip nulls
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
        # Include both old (no-TLD) and new (.cz/.sk) variants so bulk-imported
        # DBs (which may have either naming convention) are all counted correctly.
        SCRAPER_SOURCE_MAP = {
            "Alza.cz":            ["alza", "alza.cz"],
            "Heureka.cz":         ["heureka", "heureka.cz"],
            "Heureka CZ":         ["heureka", "heureka.cz"],
            "Heureka.sk":         ["heureka_sk", "heureka.sk"],
            "Heureka SK":         ["heureka_sk", "heureka.sk"],
            "Zbozi.cz":           ["zbozi", "zbozi.cz"],
            "Datart.cz":          ["datart", "datart.cz"],
            "Planeo.cz":          ["planeo", "planeo.cz"],
            "CZC.cz":             ["czc", "czc.cz"],
            "Amazon.de":          ["amazon", "amazon_de", "amazon.de"],
            "Amazon.com":         ["amazon_us", "amazon.com"],
            "Otto.de":            ["otto", "otto_de", "otto.de"],
            "Conrad.de":          ["conrad", "conrad.de"],
            "Saturn.de":          ["saturn_de", "saturn.de"],
            "MediaMarkt":         ["mediamarkt"],
            "Testberichte.de":    ["testberichte"],
            "Geizhals.de":        ["geizhals", "geizhals.de"],
            "Geizhals.at":        ["geizhals", "geizhals.at"],
            "Digitec.ch":         ["digitec", "digitec.ch"],
            "Fnac.fr":            ["fnac", "fnac.fr"],
            "Darty.fr":           ["darty", "darty.fr"],
            "Ceneo.pl":           ["ceneo", "ceneo.pl"],
            "Coolblue.nl":        ["coolblue", "coolblue.nl"],
            "Prisjakt.nu":        ["prisjakt", "prisjakt.nu"],
            "PriceRunner.dk":     ["pricerunner", "pricerunner.dk"],
            "PriceRunner.se":     ["pricerunner_se", "pricerunner.se"],
            "Warentest.de":       ["warentest"],
            "Dtest.cz":           ["dtest"],
        }

        # Fallback market label when a source has products but no scraper_runs row yet
        SOURCE_MARKET_MAP = {
            "alza": "CZ", "alza.cz": "CZ",
            "heureka": "CZ", "heureka.cz": "CZ",
            "zbozi": "CZ", "zbozi.cz": "CZ",
            "datart": "CZ", "datart.cz": "CZ",
            "planeo": "CZ", "planeo.cz": "CZ",
            "czc": "CZ", "czc.cz": "CZ",
            "heureka_sk": "SK", "heureka.sk": "SK",
            "amazon": "DE", "amazon_de": "DE", "amazon.de": "DE",
            "otto": "DE", "otto_de": "DE", "otto.de": "DE",
            "conrad": "DE", "conrad.de": "DE",
            "saturn_de": "DE", "saturn.de": "DE",
            "mediamarkt": "DE",
            "testberichte": "DE",
            "geizhals": "AT", "geizhals.de": "AT", "geizhals.at": "AT",
            "digitec": "CH", "digitec.ch": "CH",
            "fnac": "FR", "fnac.fr": "FR",
            "darty": "FR", "darty.fr": "FR",
            "ceneo": "PL", "ceneo.pl": "PL",
            "coolblue": "NL", "coolblue.nl": "NL",
            "prisjakt": "SE", "prisjakt.nu": "SE",
            "pricerunner_se": "SE", "pricerunner.se": "SE",
            "pricerunner": "DK", "pricerunner.dk": "DK",
            "amazon_us": "US", "amazon.com": "US",
        }

        # Review-only sources — kept in the DB but hidden from the Sources panel
        # because they aren't buyable products. Also matches the same exclusion
        # already applied to /api/products at server.py:45.
        HIDDEN_SOURCES = {"dtest", "warentest"}

        sources = []
        seen_scrapers = set()

        for row in run_rows:
            scraper_name, market, last_ok, last_attempt, total_added, total_updated, errors_30d = row
            seen_scrapers.add(scraper_name)

            # Compute product count (sum across all mapped source keys)
            source_keys = SCRAPER_SOURCE_MAP.get(scraper_name, [scraper_name.lower()])
            if source_keys and all(k in HIDDEN_SOURCES for k in source_keys):
                continue
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

        # Add any sources with products but no scraper_runs entry yet.
        # Also fold raw source keys (e.g. "digitec") into their friendly scraper
        # name (e.g. "Digitec.ch") if a SCRAPER_SOURCE_MAP entry covers them
        # but no scraper_runs row exists yet — avoids duplicate "digitec" + "Digitec.ch" rows.
        seen_source_keys = set()
        for run_name in seen_scrapers:
            for k in SCRAPER_SOURCE_MAP.get(run_name, []):
                seen_source_keys.add(k)

        SOURCE_NAME_DISPLAY = {
            keys[0]: name for name, keys in SCRAPER_SOURCE_MAP.items() if keys
        }

        for src_key, cnt in counts.items():
            if src_key in seen_source_keys:
                continue
            if src_key in HIDDEN_SOURCES:
                continue
            display_name = SOURCE_NAME_DISPLAY.get(src_key, src_key)
            # Use MAX(scraped_at) from products as the "last import" timestamp
            last_scraped = latest_scraped.get(src_key)
            days_since   = None
            if last_scraped:
                try:
                    last_dt    = datetime.datetime.fromisoformat(last_scraped.replace("Z", ""))
                    days_since = (now - last_dt).days
                except Exception:
                    pass
            # Sources with products but no scraper_runs = bulk-imported data → blue "imported" dot
            status = "imported" if cnt > 0 else "unknown"
            sources.append({
                "scraper":       display_name,
                "market":        SOURCE_MARKET_MAP.get(src_key, "??"),
                "product_count": cnt,
                "last_ok":       last_scraped,
                "last_attempt":  None,
                "days_since_ok": days_since,
                "status":        status,
                "total_added":   0,
                "total_updated": 0,
                "errors_30d":    0,
            })

        # Sort: stale first, then warn, then ok, then imported (bulk), then unknown
        order = {"stale": 0, "warn": 1, "ok": 2, "imported": 3, "unknown": 4}
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
    """Return list of {main, subs: [{sub, count}]} using NormalizedMainGroup /
    NormalizedCategory when available, falling back to the old mapping logic."""
    conn = open_db()

    cols = {r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()}
    use_normalized = "NormalizedCategory" in cols and "NormalizedMainGroup" in cols

    clauses = ["source NOT IN ('dtest','warentest')"]
    params = []
    if country:
        clauses.append("COALESCE(country,'CZ') = ?")
        params.append(country)
    if source:
        clauses.append("source = ?")
        params.append(source)
    where_clause = "WHERE " + " AND ".join(clauses)

    if use_normalized:
        rows = conn.execute(
            f"SELECT COALESCE(NormalizedMainGroup,'Other') as mg, "
            f"       COALESCE(NormalizedCategory,'Other') as nc, "
            f"       COUNT(*) as cnt "
            f"FROM products {where_clause} "
            f"GROUP BY mg, nc ORDER BY mg, cnt DESC",
            params,
        ).fetchall()
        conn.close()

        from collections import defaultdict
        tree = defaultdict(dict)
        for mg, nc, cnt in rows:
            tree[mg][nc] = tree[mg].get(nc, 0) + cnt

        # Filter out empty subcategories and obvious garbage text
        # (real subcategories with 1+ products are kept — garbage was already cleaned by normalization)
        _GARBAGE_SUBS = frozenset({
            'Čisticí Prostředek', 'Montážní Klíč', 'Hroty',
            'Antivibrační Sloupky Pro Uchycení Ventilátorů',
            'Bezdrátový Systém Nová Generace Bezdrátových Setů Wireless Go Gen 3 Nabízí Dva Vysílače Tx A Jeden Dvojitý Přijímač Rx. Hodí Se Pro Širokou Škálu Použití',
        })
        for mg in list(tree.keys()):
            tree[mg] = {nc: cnt for nc, cnt in tree[mg].items()
                        if cnt > 0 and nc not in _GARBAGE_SUBS}

        # Order main groups sensibly — first 12 appear as quick-filter pills in the UI
        GROUP_ORDER = [
            "Phones & Tablets", "Computers", "Audio", "TV & Video",
            "Wearables", "Gaming", "Home Appliances", "Cameras",
            "Smart Home", "Accessories", "Toys & Games", "Storage",
            "Networking", "Sports & Outdoor", "Garden & Outdoors",
            "Health & Beauty", "Baby & Kids", "Home & Garden", "Other",
        ]
        result = []
        shown = set()
        for mg in GROUP_ORDER:
            if mg in tree and tree[mg]:
                subs = sorted(
                    [{"sub": nc, "count": c} for nc, c in tree[mg].items()],
                    key=lambda x: -x["count"],
                )
                result.append({"main": mg, "subs": subs})
                shown.add(mg)
        # Append any groups not in the ordered list
        for mg in sorted(tree):
            if mg not in shown and tree[mg]:
                subs = sorted(
                    [{"sub": nc, "count": c} for nc, c in tree[mg].items()],
                    key=lambda x: -x["count"],
                )
                result.append({"main": mg, "subs": subs})
        return result

    # ── Legacy fallback (pre-normalization) ──────────────────────────────────
    has_main = "MainCategory" in cols
    if has_main:
        rows = conn.execute(
            f"SELECT COALESCE(MainCategory,'Ostatní') as main, Category, COUNT(*) as cnt "
            f"FROM products {where_clause} "
            f"GROUP BY main, Category ORDER BY main, cnt DESC",
            params,
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT COALESCE(Category,'Ostatní') as main, Category, COUNT(*) as cnt "
            f"FROM products {where_clause} "
            f"GROUP BY Category ORDER BY cnt DESC",
            params,
        ).fetchall()
    conn.close()

    from collections import defaultdict
    super_tree = defaultdict(dict)
    for main, sub, cnt in rows:
        super_label = _SUPER_CAT_MAP.get(main, "📦 Other")
        sub_raw = sub or "Nezařazeno"
        if sub_raw in _SUB_CLEAN_MAP:
            clean_sub = _SUB_CLEAN_MAP[sub_raw]
            if clean_sub is None:
                continue
        else:
            clean_sub = sub_raw
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
            reverse=True,
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
    max_price   = params.get("max_price", [""])[0]
    min_price   = params.get("min_price", [""])[0]
    has_image   = params.get("has_image", [""])[0]  # "1" = only products with photos
    has_history = params.get("has_history", [""])[0]  # "1" = only products with ≥2 snapshots
    price_drop  = params.get("price_drop",  [""])[0]  # "1" = only products with notable price drop
    brand_filter = params.get("brand", [""])[0]  # exact brand match (case-insensitive)

    # Country filter only applied when a specific source is selected.
    # When no source is chosen we show ALL countries so the default view
    # includes CZ + DE + PL products together.
    SOURCE_COUNTRY = {
        "otto": "DE", "otto_de": "DE", "otto.de": "DE",
        "amazon": "DE", "amazon_de": "DE", "amazon.de": "DE",
        "warentest": "DE", "saturn_de": "DE", "saturn.de": "DE",
        "mediamarkt": "DE", "testberichte": "DE", "geizhals": "DE",
        "geizhals.de": "AT", "geizhals.at": "AT",
        "conrad": "DE", "conrad.de": "DE",
        "dtest": "CZ",
        "alza": "CZ", "alza.cz": "CZ",
        "heureka": "CZ", "heureka.cz": "CZ",
        "zbozi": "CZ", "zbozi.cz": "CZ",
        "datart": "CZ", "datart.cz": "CZ",
        "planeo": "CZ", "planeo.cz": "CZ",
        "czc": "CZ", "czc.cz": "CZ",
        "ceneo": "PL", "ceneo.pl": "PL",
        "amazon_us": "US", "amazon.com": "US",
        "heureka_sk": "SK", "heureka.sk": "SK",
        "fnac": "FR", "fnac.fr": "FR",
        "darty": "FR", "darty.fr": "FR",
        "digitec": "CH", "digitec.ch": "CH",
        "coolblue": "NL", "coolblue.nl": "NL",
        "prisjakt": "SE", "prisjakt.nu": "SE",
        "pricerunner_se": "SE", "pricerunner.se": "SE",
        "pricerunner": "DK", "pricerunner.dk": "DK",
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
        # Tokenise the query so "galaxy s25 samsung" matches "Samsung Galaxy S25"
        # regardless of word order.  Each token must appear somewhere in Name or Category.
        # Single-character tokens and pure-whitespace are skipped.
        tokens = [t for t in q.strip().split() if len(t) >= 1]
        if len(tokens) <= 1:
            # Fast path: single token → original LIKE behaviour
            conditions.append("(Name LIKE ? OR NormalizedCategory LIKE ?)")
            plist += [f"%{q}%", f"%{q}%"]
        else:
            # Multi-token: require ALL tokens present anywhere in Name or Category
            # (any word order, each token checked independently)
            token_clauses = []
            for tok in tokens:
                pat = f"%{tok}%"
                token_clauses.append("(Name LIKE ? OR NormalizedCategory LIKE ?)")
                plist += [pat, pat]
            conditions.append("(" + " AND ".join(token_clauses) + ")")
    if main_category:
        # Use NormalizedMainGroup if it exists (preferred), else fall back to old mapping
        conditions.append(
            "(NormalizedMainGroup = ? OR (NormalizedMainGroup IS NULL AND MainCategory IN ("
            + ",".join("?" * len(list(dict.fromkeys(_SUPER_CAT_REVERSE.get(main_category, []) + [main_category]))))
            + ")))"
        )
        raw_mains = list(dict.fromkeys(
            _SUPER_CAT_REVERSE.get(main_category, []) + [main_category]
        ))
        plist.append(main_category)
        plist.extend(raw_mains)
    if category:
        # Use NormalizedCategory if it exists (preferred), else fall back to old mapping
        raw_cats = list(dict.fromkeys(
            _SUB_CLEAN_REVERSE.get(category, []) + [category]
        ))
        conditions.append(
            "(NormalizedCategory = ? OR (NormalizedCategory IS NULL AND Category IN ("
            + ",".join("?" * len(raw_cats))
            + ")))"
        )
        plist.append(category)
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
        # Map UI filter values → actual source values stored in DB.
        # Some sources exist under multiple keys (old name + new .cz/.de name).
        _OTTO_CLAUSE    = "source IN ('otto','otto_de','otto.de')"
        _AMAZON_CLAUSE  = "source IN ('amazon','amazon_de','amazon.de','amazon_us','amazon.com')"
        _HEUREKA_CLAUSE = "source IN ('heureka','heureka.cz')"
        source_map = {
            "amazon":    _AMAZON_CLAUSE,
            "amazon_de": _AMAZON_CLAUSE,
            "otto":      _OTTO_CLAUSE,
            "otto_de":   _OTTO_CLAUSE,
            "heureka":   _HEUREKA_CLAUSE,
            "heureka.cz":_HEUREKA_CLAUSE,
            "alza":      "source IN ('alza','alza.cz')",
            "alza.cz":   "source IN ('alza','alza.cz')",
            "zbozi":     "source IN ('zbozi','zbozi.cz')",
            "zbozi.cz":  "source IN ('zbozi','zbozi.cz')",
            "datart":    "source IN ('datart','datart.cz')",
            "datart.cz": "source IN ('datart','datart.cz')",
            "planeo":    "source IN ('planeo','planeo.cz')",
            "planeo.cz": "source IN ('planeo','planeo.cz')",
            "heureka_sk":"source IN ('heureka_sk','heureka.sk')",
            "heureka.sk":"source IN ('heureka_sk','heureka.sk')",
            "geizhals":  "source IN ('geizhals','geizhals.de','geizhals.at')",
            "prisjakt":  "source IN ('prisjakt','prisjakt.nu')",
        }
        if source in source_map:
            conditions.append(source_map[source])
        else:
            conditions.append("source = ?"); plist.append(source)
    if keyword:
        conditions.append('keywords LIKE ?'); plist.append(f'%"{keyword}"%')
    if brand_filter:
        # Brand match: exact or LIKE-prefix on brand column; exact first-word match on Name
        # for older rows that don't have the brand column populated yet.
        bf_like = brand_filter + "%"
        conditions.append(
            "(UPPER(brand) = UPPER(?) "
            " OR UPPER(brand) LIKE UPPER(?) "
            " OR (brand IS NULL "
            "     AND UPPER(TRIM(SUBSTR(Name, 1, INSTR(Name || ' ', ' ') - 1))) LIKE UPPER(?)))"
        )
        plist += [brand_filter, bf_like, bf_like]
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
    # Currency-normalised price (CZK equivalent) used for both filter and sort.
    # Price_CZK stores the *local* currency for each source, so 49 CHF (digitec)
    # or 74 EUR (heureka.sk) must be multiplied to compare against CZK thresholds.
    _PRICE_CZK_EQUIV = (
        "Price_CZK * CASE currency "
        "WHEN 'EUR' THEN 25.0 WHEN 'CHF' THEN 26.0 "
        "WHEN 'SEK' THEN 2.4  WHEN 'DKK' THEN 3.5  "
        "WHEN 'PLN' THEN 5.8  ELSE 1.0 END"
    )
    if max_price:
        try:
            conditions.append(f"({_PRICE_CZK_EQUIV}) <= ?")
            plist.append(float(max_price))
        except ValueError:
            pass
    if min_price:
        try:
            conditions.append(f"({_PRICE_CZK_EQUIV}) >= ?")
            plist.append(float(min_price))
        except ValueError:
            pass
    if has_image == "1":
        conditions.append(
            "image_url IS NOT NULL AND image_url != '' AND image_url != '__none__'"
        )
    if has_history == "1":
        # Filter for products with ≥2 snapshots. We attach the snapshots DB and use
        # a cross-DB subquery — only done when has_history is active.
        conditions.append(
            "LOWER(RTRIM(ProductURL,'/')) IN "
            "(SELECT LOWER(RTRIM(product_url,'/')) FROM snapshots.product_snapshots "
            " GROUP BY LOWER(RTRIM(product_url,'/')) HAVING COUNT(*)>=2)"
        )
    if price_drop == "1":
        # Filter to products where price dropped meaningfully since first snapshot.
        # v[2] is price_d — already filtered to ≥2% AND ≥€2 in query_snapshot_deltas,
        # so any negative value here represents a real price drop.
        # Use the in-memory delta cache (avoids heavy SQL JOIN on snapshots).
        _dd = query_snapshot_deltas()
        _drop_urls = [
            u.lower().rstrip("/")
            for u, v in _dd.items()
            if v[2] is not None and v[2] < 0
        ]
        if _drop_urls:
            conditions.append(
                "LOWER(RTRIM(ProductURL,'/')) IN (%s)" % ",".join("?" * len(_drop_urls))
            )
            plist.extend(_drop_urls)
        else:
            conditions.append("1=0")  # no price drops in cache

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # For price sorting, use whichever price column is available
    ALLOWED_SORT = {
        "RecommendRate_pct", "AvgStarRating", "ReviewsCount",
        "Price_CZK", "Price_EUR", "ReturnRate_pct", "Name",
        "repairability_score_fr", "durability_score_fr",
        "cat_rank", "scraped_at", "value_score",
    }
    if sort_by not in ALLOWED_SORT:
        sort_by = "RecommendRate_pct"

    # Reuse the same currency-normalised expression defined above in the filter block.
    # (It's re-defined here in case this branch is reached without the filter block.)
    _PRICE_CZK_EQUIV = (
        "Price_CZK * CASE currency "
        "WHEN 'EUR' THEN 25.0 WHEN 'CHF' THEN 26.0 "
        "WHEN 'SEK' THEN 2.4  WHEN 'DKK' THEN 3.5  "
        "WHEN 'PLN' THEN 5.8  ELSE 1.0 END"
    )
    sort_expr = sort_by
    if sort_by == "Price_CZK":
        # Sort by CZK-equivalent so products from all markets are comparable.
        # Null out corrupted prices (< 50 Kč equivalent) so they sort last.
        sort_expr = (
            f"CASE WHEN ({_PRICE_CZK_EQUIV}) >= 50 "
            f"THEN ({_PRICE_CZK_EQUIV}) ELSE NULL END"
        )
    elif sort_by == "Price_EUR":
        sort_expr = (
            "CASE WHEN COALESCE(Price_EUR, Price_CZK * CASE currency "
            "  WHEN 'EUR' THEN 1.0 WHEN 'CHF' THEN 26.0/25.0 "
            "  WHEN 'SEK' THEN 2.4/25.0 WHEN 'DKK' THEN 3.5/25.0 "
            "  WHEN 'PLN' THEN 5.8/25.0 ELSE 1.0/25.0 END) >= 2.0 "
            "THEN COALESCE(Price_EUR, Price_CZK / 25.0) ELSE NULL END"
        )
    elif sort_by == "value_score":
        # Value = recommend rate / price (higher = more quality per unit cost).
        # Normalise price to CZK using the currency column so that products from
        # all markets are comparable (EUR≈25, CHF≈26, SEK≈2.4, DKK≈3.5, PLN≈5.8).
        # Require ≥10 reviews and ≥500 Kč (≈€20) to exclude corrupted/trivial prices.
        sort_expr = (
            "CASE WHEN RecommendRate_pct IS NOT NULL "
            "      AND RecommendRate_pct > 0 "
            "      AND Price_CZK IS NOT NULL AND Price_CZK > 0 "
            "      AND COALESCE(ReviewsCount, 0) >= 10 "
            "      AND (Price_CZK * CASE currency "
            "             WHEN 'EUR' THEN 25.0 WHEN 'CHF' THEN 26.0 "
            "             WHEN 'SEK' THEN 2.4  WHEN 'DKK' THEN 3.5  "
            "             WHEN 'PLN' THEN 5.8  ELSE 1.0 END) >= 500 "
            "THEN RecommendRate_pct * 1000.0 / "
            "     (Price_CZK * CASE currency "
            "        WHEN 'EUR' THEN 25.0 WHEN 'CHF' THEN 26.0 "
            "        WHEN 'SEK' THEN 2.4  WHEN 'DKK' THEN 3.5  "
            "        WHEN 'PLN' THEN 5.8  ELSE 1.0 END) "
            "ELSE NULL END"
        )

    null_last = f"CASE WHEN {sort_expr} IS NULL THEN 1 ELSE 0 END"
    # Secondary tie-breaker for rank sort:
    # 1. Products with images come before those without (better UX on first load)
    # 2. Within the same image tier, prefer more reviews
    if sort_by == "cat_rank":
        tie_breaker = (
            "CASE WHEN image_url IS NOT NULL AND image_url != '' AND image_url != '__none__' "
            "THEN 0 ELSE 1 END ASC, "
            "COALESCE(ReviewsCount, 0) DESC"
        )
    else:
        tie_breaker = "rowid ASC"

    conn = open_db()
    # Attach snapshots DB when the has_history filter is active
    if has_history == "1":
        import os as _os
        _snaps_path = _os.environ.get(
            "SNAPSHOTS_DB_PATH",
            _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "snapshots.db"))
        )
        if _os.path.exists(_snaps_path):
            conn.execute(f"ATTACH DATABASE '{_snaps_path}' AS snapshots")
        else:
            # Snapshots DB not found; make the subquery return nothing
            where = where.replace(
                "LOWER(RTRIM(ProductURL,'/')) IN "
                "(SELECT LOWER(RTRIM(product_url,'/')) FROM snapshots.product_snapshots "
                " GROUP BY LOWER(RTRIM(product_url,'/')) HAVING COUNT(*)>=2)",
                "1=0"
            )
    total = conn.execute(f"SELECT COUNT(*) FROM products {where}", plist).fetchone()[0]
    offset = (page - 1) * PAGE_SIZE
    rows = conn.execute(
        f"""SELECT id, Name, MainCategory, Category,
                   NormalizedCategory,
                   ProductURL, Price_CZK,
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
                   image_url,
                   scraped_at,
                   first_seen_at,
                   qt_brand_score,
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
            ORDER BY {null_last}, {sort_expr} {order_sql}, {tie_breaker}
            LIMIT ? OFFSET ?""",
        plist + [PAGE_SIZE, offset]
    ).fetchall()
    conn.close()

    # Attach has_history as days tracked (integer) using in-memory coverage dict (O(1)).
    # 0 = no history, N = N days tracked since first snapshot.
    products = []
    for r in rows:
        p = dict(r)
        url = (p.get("ProductURL") or "")
        p["has_history"] = snapshot_coverage_days(url)
        products.append(p)

    # Include the total snapshot-tracked product count so the frontend can label
    # the "📈 Price history (N)" toolbar button without a separate API call.
    # query_snapshot_coverage() uses an in-memory 10-min cache so this is O(1).
    with_history_count = len(_snapshot_coverage_cache) if _snapshot_coverage_cache else len(query_snapshot_coverage())

    return {
        "products": products,
        "total": total,
        "page": page,
        "pages": math.ceil(total / PAGE_SIZE),
        "page_size": PAGE_SIZE,
        "with_history": with_history_count,
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
    global _movers_cache, _movers_ts
    cache_key = (days, limit, metric)
    now_m = time.monotonic()
    if cache_key in _movers_cache and (now_m - _movers_ts.get(cache_key, 0)) < MOVERS_TTL:
        return _movers_cache[cache_key]

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
            # For correlated subqueries: s3.col IS NOT NULL (simple column)
            s3_not_null = "s3.avg_star_rating IS NOT NULL"
        elif metric == "price":
            col = "COALESCE(price_czk, price_eur)"   # use whichever currency is available
            # Can't do s3.COALESCE(...) — use explicit column checks instead
            s3_not_null = "(s3.price_czk IS NOT NULL OR s3.price_eur IS NOT NULL)"
        else:
            col = "recommend_pct"
            s3_not_null = "s3.recommend_pct IS NOT NULL"

        # Compare latest snapshot vs the first snapshot.
        # `days` is used as a minimum tracking age: we only include products whose
        # first snapshot is at least `days` days old. This filters out newly-added
        # products and focuses the view on well-tracked items.
        # This fixes the original bug where the query compared a snapshot to itself
        # (same date), always producing delta=0.
        # Note: col_chk is used in correlated subqueries where we cannot prefix
        # compound expressions with a table alias (e.g. s3.COALESCE(...) is invalid).
        rows = sc.execute(f"""
            WITH latest AS (
                -- Most recent snapshot per (product_url, source)
                SELECT product_url, source,
                       {col} AS new_val,
                       snapshot_date AS new_date
                FROM   product_snapshots p
                WHERE  {col} IS NOT NULL
                  AND  snapshot_date = (
                      SELECT MAX(s2.snapshot_date) FROM product_snapshots s2
                      WHERE  s2.product_url = p.product_url
                        AND  s2.source      = p.source
                  )
            ),
            oldest AS (
                -- Earliest snapshot per (product_url, source) that is at least `days` old.
                -- Requiring a minimum age avoids comparing brand-new products to themselves.
                SELECT product_url, source,
                       {col} AS old_val,
                       snapshot_date AS old_date
                FROM   product_snapshots p2
                WHERE  {col} IS NOT NULL
                  AND  snapshot_date <= date('now', '-' || ? || ' days')
                  AND  snapshot_date = (
                      SELECT MIN(s3.snapshot_date) FROM product_snapshots s3
                      WHERE  s3.product_url = p2.product_url
                        AND  s3.source      = p2.source
                        AND  {s3_not_null}
                        AND  s3.snapshot_date <= date('now', '-' || ? || ' days')
                  )
            )
            SELECT l.product_url, l.source,
                   l.new_date, l.new_val,
                   o.old_date, o.old_val,
                   ROUND(l.new_val - o.old_val, 2) AS delta
            FROM   latest l
            JOIN   oldest o ON o.product_url = l.product_url
                           AND o.source       = l.source
                           AND o.old_date     < l.new_date
            WHERE  ABS(l.new_val - o.old_val) >= 0.5
            ORDER  BY delta DESC
        """, (days, days)).fetchall()
        sc.close()

        # Enrich with product names from products.db
        # Note: snapshots use abbreviated sources (e.g. "heureka") while products
        # use full ones (e.g. "heureka.cz"), so match by URL alone.
        pconn = open_db()
        result = []
        for row in rows:
            prod = pconn.execute(
                "SELECT Name, Category, MainCategory, source FROM products WHERE ProductURL=? LIMIT 1",
                (row["product_url"],)
            ).fetchone()
            result.append({
                "url":       row["product_url"],
                "source":    (prod["source"] if prod else row["source"]),
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

        # Sanity-filter: reject implausible changes
        if metric == "price":
            # Reject >85% price swings (int÷100 artifacts from some scrapers)
            result = [
                r for r in result
                if not (r["old_val"] and r["old_val"] > 0
                        and abs(r["delta"] / r["old_val"]) > 0.85)
            ]
        elif metric in ("recommend", "stars"):
            # Reject transitions from/to zero (first-scrape artifact where
            # products with no reviews were stored as 0% / 0 stars, then corrected).
            result = [
                r for r in result
                if r["old_val"] is not None and r["old_val"] > 0
                and r["new_val"] is not None and r["new_val"] > 0
            ]

        risers  = sorted([r for r in result if r["delta"] > 0],  key=lambda x: -x["delta"])[:limit]
        fallers = sorted([r for r in result if r["delta"] < 0],  key=lambda x:  x["delta"])[:limit]
        out = {"risers": risers, "fallers": fallers, "metric": metric, "days": days}
        _movers_cache[cache_key] = out
        _movers_ts[cache_key]    = time.monotonic()
        return out

    except Exception as e:
        import logging
        logging.error(f"query_snapshot_movers failed: {e}", exc_info=True)
        return {"risers": [], "fallers": [], "error": str(e)}


_snapshot_coverage_cache: list | None = None
_snapshot_coverage_set:   set  | None = None
_snapshot_coverage_days:  dict | None = None   # normalised_url → days tracked
_snapshot_coverage_ts: float = 0.0
_SNAPSHOT_COVERAGE_TTL = 600  # 10 minutes

def query_snapshot_coverage() -> list:
    """Return list of product URLs that have >= 2 snapshots.
    Also populates _snapshot_coverage_days with per-URL tracking duration.
    Result is cached in-memory for 10 min to avoid scanning 100k rows.
    """
    import os as _os
    import time as _time
    global _snapshot_coverage_cache, _snapshot_coverage_set, _snapshot_coverage_days, _snapshot_coverage_ts

    now = _time.monotonic()
    if _snapshot_coverage_cache is not None and (now - _snapshot_coverage_ts) < _SNAPSHOT_COVERAGE_TTL:
        return _snapshot_coverage_cache

    _snaps_path = _os.environ.get(
        "SNAPSHOTS_DB_PATH",
        _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "snapshots.db"))
    )
    if not _os.path.exists(_snaps_path):
        return []
    try:
        sc = sqlite3.connect(_snaps_path, timeout=20)
        rows = sc.execute(
            "SELECT LOWER(RTRIM(product_url, '/')) AS url, "
            "CAST(julianday(MAX(snapshot_date)) - julianday(MIN(snapshot_date)) AS INTEGER) AS days "
            "FROM product_snapshots "
            "GROUP BY LOWER(RTRIM(product_url, '/')) "
            "HAVING COUNT(*) >= 2"
        ).fetchall()
        sc.close()
        result = [r[0] for r in rows]
        _snapshot_coverage_cache = result
        _snapshot_coverage_set   = set(result)                     # O(1) lookup
        _snapshot_coverage_days  = {r[0]: int(r[1] or 0) for r in rows}
        _snapshot_coverage_ts    = now
        return result
    except Exception as e:
        import logging
        logging.error(f"query_snapshot_coverage failed: {e}")
        return _snapshot_coverage_cache or []


def snapshot_coverage_days(url: str) -> int:
    """Return days tracked for this URL (0 if < 2 snapshots or unknown)."""
    global _snapshot_coverage_days
    import time as _time
    now = _time.monotonic()
    if _snapshot_coverage_days is None or (now - _snapshot_coverage_ts) >= _SNAPSHOT_COVERAGE_TTL:
        query_snapshot_coverage()
    if _snapshot_coverage_days is None:
        return 0
    return _snapshot_coverage_days.get(url.lower().rstrip("/"), 0)


def snapshot_coverage_has(url: str) -> bool:
    """O(1) check — was this URL (normalised) seen in ≥2 snapshots?"""
    return snapshot_coverage_days(url) > 0


def _extract_image_from_html(html: str) -> str:
    """Extract best product image URL from HTML using multiple strategies."""
    import re as _re
    import json as _json

    # Strategy 1: og:image meta tag
    m = _re.search(
        r'property=["\']og:image["\'][^>]+content=["\']([^"\']{10,})["\']'
        r'|content=["\']([^"\']{10,})["\'][^>]+property=["\']og:image["\']',
        html, _re.I
    )
    if m:
        return (m.group(1) or m.group(2)).strip().replace("&amp;", "&")

    # Strategy 2: JSON-LD Product schema (supports top-level, list, and @graph)
    jld_blocks = _re.findall(
        r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', html, _re.S | _re.I
    )
    def _jld_img(item):
        """Extract image URL from a single JSON-LD item dict."""
        imgs = item.get("image", [])
        if isinstance(imgs, str) and len(imgs) > 10:
            return imgs.strip()
        if isinstance(imgs, dict):
            u = imgs.get("url", "") or imgs.get("contentUrl", "")
            if len(u) > 10: return u.strip()
        if isinstance(imgs, list) and imgs:
            first = imgs[0]
            if isinstance(first, str) and len(first) > 10:
                return first.strip()
            if isinstance(first, dict):
                u = first.get("url", "") or first.get("contentUrl", "")
                if len(u) > 10: return u.strip()
        return ""
    for block in jld_blocks:
        try:
            data = _json.loads(block)
            # Flatten: handle list, @graph wrapper, and plain object
            candidates = data if isinstance(data, list) else [data]
            expanded = []
            for c in candidates:
                if isinstance(c, dict):
                    expanded.append(c)
                    expanded.extend(c.get("@graph", []))
            for item in expanded:
                if not isinstance(item, dict): continue
                t = item.get("@type", "")
                if (t == "Product" or (isinstance(t, list) and "Product" in t)
                        or str(t).lower() == "product"):
                    u = _jld_img(item)
                    if u: return u
        except Exception:
            pass

    # Strategy 3: twitter:image
    m2 = _re.search(
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']{10,})["\']'
        r'|<meta[^>]+content=["\']([^"\']{10,})["\'][^>]+name=["\']twitter:image["\']',
        html, _re.I
    )
    if m2:
        return (m2.group(1) or m2.group(2)).strip().replace("&amp;", "&")

    # Strategy 4: HTML microdata — <meta itemprop="image" content="...">
    m3b = _re.search(
        r'itemprop=["\']image["\'][^>]+content=["\']([^"\']{10,})["\']'
        r'|content=["\']([^"\']{10,})["\'][^>]+itemprop=["\']image["\']',
        html, _re.I
    )
    if m3b:
        return (m3b.group(1) or m3b.group(2)).strip().replace("&amp;", "&")

    # Strategy 6: coolblue.nl — images served from image.coolblue.nl/{size}/products/ID
    # Two URL patterns: "max/NxNauto/products/ID" and "NxN/products/ID"
    m4 = _re.search(
        r'https://image\.coolblue\.nl/(?:max/\d+xauto|\d+x\d+)/products/(\d+)', html
    )
    if m4:
        return f"https://image.coolblue.nl/500x500/products/{m4.group(1)}"

    # Strategy 7: Heureka CDN — images served from cdn.heureka.cz or im9.cz
    m5 = _re.search(r'https://(?:cdn\.heureka\.cz|im9\.cz|img\.cz)/[^\s"\']{10,}\.(?:jpg|jpeg|png|webp)', html, _re.I)
    if m5:
        return m5.group(0).rstrip('?&').split('?')[0]

    return ""


def _populate_alza_images() -> int:
    """Construct alza.cz image URLs from SKU codes — no HTTP requests needed.

    Alza exposes product images at a public CDN:
        https://cdn.alza.cz/ImgW.ashx?f=&cd={SKU}&i=1.jpg
    79% of alza products have a SKU so we can populate their image_url instantly.
    Returns the number of rows updated.
    """
    conn = open_db()
    rows = conn.execute(
        """SELECT rowid, SKU FROM products
           WHERE source = 'alza.cz'
             AND SKU IS NOT NULL AND SKU != ''
             AND (image_url IS NULL OR image_url = '' OR image_url = '__none__')"""
    ).fetchall()

    if not rows:
        conn.close()
        return 0

    # Use fd=f10 for compact thumbnails (~20KB vs ~300KB for the full image)
    updates = [
        (f"https://cdn.alza.cz/ImgW.ashx?fd=f10&cd={sku}&i=1.jpg", rowid)
        for rowid, sku in rows
    ]
    conn.executemany("UPDATE products SET image_url = ? WHERE rowid = ?", updates)
    conn.commit()
    conn.close()
    logging.info(f"[alza-images] populated {len(updates)} alza image URLs from SKU codes")
    return len(updates)


def _populate_prisjakt_images() -> int:
    """Construct prisjakt.nu image URLs from product IDs — no HTTP requests needed.

    Prisjakt CDN pattern: https://pricespy-75b8.kxcdn.com/product/standard/800/{ID}.jpg
    The product ID is the 'p=' query parameter in the product URL.
    Returns the number of rows updated.
    """
    import re as _re
    conn = open_db()
    rows = conn.execute(
        """SELECT rowid, ProductURL FROM products
           WHERE source IN ('prisjakt', 'prisjakt.nu')
             AND ProductURL IS NOT NULL AND ProductURL != ''
             AND (image_url IS NULL OR image_url = '' OR image_url = '__none__')"""
    ).fetchall()

    if not rows:
        conn.close()
        return 0

    updates = []
    for rowid, url in rows:
        m = _re.search(r'[?&]p=(\d+)', url)
        if m:
            pid = m.group(1)
            img = f"https://pricespy-75b8.kxcdn.com/product/standard/800/{pid}.jpg"
            updates.append((img, rowid))

    if updates:
        conn.executemany("UPDATE products SET image_url = ? WHERE rowid = ?", updates)
        conn.commit()
    conn.close()
    logging.info(f"[prisjakt-images] populated {len(updates)} image URLs from product IDs")
    return len(updates)


def _populate_amazon_images() -> int:
    """Construct Amazon product image URLs from ASIN codes — no HTTP requests needed.

    Amazon exposes product images at a public CDN (no hotlink protection):
        https://images-eu.ssl-images-amazon.com/images/P/{ASIN}.01.LZZZZZZZ.jpg
    The ASIN is extracted from the product URL (/dp/{ASIN} or ASIN= query param).
    Returns the number of rows updated.
    """
    import re as _re
    conn = open_db()
    rows = conn.execute(
        """SELECT rowid, ProductURL FROM products
           WHERE source IN ('amazon_de', 'amazon_us', 'amazon', 'amazon.de', 'amazon.com')
             AND ProductURL IS NOT NULL AND ProductURL != ''
             AND (image_url IS NULL OR image_url = '' OR image_url = '__none__')"""
    ).fetchall()

    if not rows:
        conn.close()
        return 0

    updates = []
    for rowid, url in rows:
        m = _re.search(r'/dp/([A-Z0-9]{10})', url)
        if not m:
            m = _re.search(r'ASIN=([A-Z0-9]{10})', url)
        if m:
            asin = m.group(1)
            img = f"https://images-eu.ssl-images-amazon.com/images/P/{asin}.01.LZZZZZZZ.jpg"
            updates.append((img, rowid))

    if updates:
        conn.executemany("UPDATE products SET image_url = ? WHERE rowid = ?", updates)
        conn.commit()
    conn.close()
    logging.info(f"[amazon-images] populated {len(updates)} image URLs from ASINs")
    return len(updates)


def _populate_pricerunner_images() -> int:
    """Construct PriceRunner image URLs from product URLs — no HTTP requests needed.

    PriceRunner (SE and DK) uses a CDN pattern derivable from the URL:
    URL: https://www.pricerunner.se/pl/39-202003/Bildskärmar/ASUS-ROG-Swift-OLED-PG27AQDM-Offer.html
    No clean CDN pattern identified — skip for now.
    """
    return 0


def _post_scrape_normalize() -> int:
    """Run category normalization after a nightly scrape completes.

    Call this from the scheduler after run_due_scrapers() to ensure
    newly scraped products get proper NormalizedCategory values without
    waiting for the next server restart.

    Returns the number of products newly normalized.
    """
    try:
        from scraper.normalize_categories import run_normalization as _norm
        conn = open_db()
        n = _norm(conn)
        conn.close()
        return n or 0
    except Exception as _e:
        import logging as _log
        _log.warning(f"[post-scrape-normalize] failed: {_e}")
        return 0


def _fetch_og_images(batch: int = 300) -> int:
    """Fetch product images for up to *batch* products with no image_url cached yet.

    Uses curl_cffi (Chrome TLS fingerprint) to bypass bot-detection on retailer
    sites. Commits every COMMIT_EVERY rows so progress survives server restarts.
    Marks permanent failures as '__none__' to avoid re-fetching.
    Returns the number of products processed.
    """
    import re as _re
    import time as _time
    import logging as _log

    COMMIT_EVERY = 50   # write progress every N products
    SLEEP = 0.3         # seconds between requests

    # Sources that never yield useful images — skip in SQL to avoid wasting requests
    # alza.cz: SKU products handled by _populate_alza_images(); products WITHOUT SKU
    #          can still be fetched via og:image from their product URL.
    # dtest/warentest: in HIDDEN_SOURCES, never shown publicly — no point fetching images.
    # geizhals: price-comparison aggregator, returns empty (bot-protected), skip.
    # prisjakt/amazon handled by dedicated URL-ID-based populators (no HTTP needed).
    # testberichte: editorial review site with valid og:image — fetch is allowed.
    # geizhals: returns og:image on residential IPs but strips meta tags for datacenter IPs (Fly.io)
    BLOCKED_SOURCES_SQL = ("('dtest','warentest','geizhals',"
                           "'prisjakt','prisjakt.nu','amazon_de','amazon_us','amazon','amazon.de','amazon.com')")

    # Try curl_cffi first (bypasses TLS fingerprinting / bot checks); fall back to urllib
    try:
        from curl_cffi import requests as _cffi
        def _get(url):
            r = _cffi.get(url, impersonate="chrome120", timeout=10,
                          headers={"Accept-Language": "en-US,en;q=0.9",
                                   "Accept": "text/html,application/xhtml+xml,*/*"})
            return r.text if r.status_code == 200 else None
    except ImportError:
        import urllib.request as _urlreq
        UA = ("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
        def _get(url):
            try:
                req = _urlreq.Request(url, headers={"User-Agent": UA})
                with _urlreq.urlopen(req, timeout=10) as resp:
                    return resp.read(40000).decode("utf-8", errors="ignore")
            except Exception:
                return None

    conn = open_db()
    # Prioritise sources most likely to return images:
    #   coolblue, heureka, prisjakt, fnac, datart → tier 1
    #   others → tier 2
    # Primary batch: untried products (image_url IS NULL or '')
    primary_size = int(batch * 0.85)
    retry_size   = batch - primary_size   # ~15% retries from previous __none__ failures
    rows = conn.execute(
        f"""SELECT rowid, ProductURL, source FROM products
           WHERE (image_url IS NULL OR image_url = '')
             AND ProductURL IS NOT NULL AND ProductURL != ''
             AND source NOT IN {BLOCKED_SOURCES_SQL}
           ORDER BY
             CASE source
               WHEN 'coolblue'    THEN 1
               WHEN 'heureka.cz'  THEN 2
               WHEN 'heureka.sk'  THEN 3
               WHEN 'prisjakt'    THEN 4
               WHEN 'fnac'        THEN 5
               WHEN 'datart.cz'   THEN 6
               WHEN 'zbozi.cz'    THEN 7
               WHEN 'digitec'     THEN 8
               WHEN 'ceneo'       THEN 9
               WHEN 'amazon_de'   THEN 10
               ELSE 99
             END,
             rowid
           LIMIT ?""",
        (primary_size,),
    ).fetchall()
    # Retry a sample of __none__ failures (in case temporary blocks lifted).
    # Exclude digitec (permanent 403) and JS-only sites (datart/zbozi/ceneo/otto)
    # from retries — they'll never succeed with our scraper.
    NO_RETRY_SOURCES = ("'digitec','datart.cz','zbozi.cz','ceneo',"
                        "'pricerunner_se','otto_de','otto'")
    if retry_size > 0:
        retry_rows = conn.execute(
            f"""SELECT rowid, ProductURL, source FROM products
               WHERE image_url = '__none__'
                 AND ProductURL IS NOT NULL AND ProductURL != ''
                 AND source NOT IN {BLOCKED_SOURCES_SQL}
                 AND source NOT IN ({NO_RETRY_SOURCES})
               ORDER BY RANDOM()
               LIMIT ?""",
            (retry_size,),
        ).fetchall()
        rows = list(rows) + list(retry_rows)
    conn.close()

    if not rows:
        return 0

    processed = 0
    pending = []

    def _flush(pending_updates):
        if not pending_updates:
            return
        c = open_db()
        c.executemany("UPDATE products SET image_url = ? WHERE rowid = ?", pending_updates)
        c.commit()
        c.close()

    for rowid, url, source in rows:
        img = "__none__"
        try:
            html = _get(url)
            if html:
                found = _extract_image_from_html(html)
                img = found if found else "__none__"
        except Exception:
            pass

        pending.append((img, rowid))
        processed += 1

        # Commit in small batches so progress survives a restart
        if len(pending) >= COMMIT_EVERY:
            _flush(pending)
            good = sum(1 for u, _ in pending if u != "__none__")
            _log.info(f"[og-images] batch commit: {len(pending)} rows, {good} with images")
            pending = []

        _time.sleep(SLEEP)

    _flush(pending)
    if pending:
        good = sum(1 for u, _ in pending if u != "__none__")
        _log.info(f"[og-images] final commit: {len(pending)} rows, {good} with images")

    _log.info(f"[og-images] run complete: {processed} products processed")
    return processed


def query_snapshot_deltas() -> dict:
    """Return per-product rating/price deltas (oldest vs newest snapshot).
    Returns compact dict: { url: [rec_delta, stars_delta, price_delta, days] }
    Only includes URLs with >= 2 snapshots and at least one non-null delta.
    Result is cached for DELTA_TTL seconds to avoid repeated 1s queries.
    """
    global _delta_cache
    now = time.monotonic()
    if _delta_cache["data"] is not None and (now - _delta_cache["ts"]) < DELTA_TTL:
        return _delta_cache["data"]

    import os as _os
    _snaps_path = _os.environ.get(
        "SNAPSHOTS_DB_PATH",
        _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "snapshots.db"))
    )
    if not _os.path.exists(_snaps_path):
        return {}
    try:
        sc = sqlite3.connect(_snaps_path, timeout=20)
        sc.row_factory = sqlite3.Row
        # Ensure composite index exists — dramatically speeds up the first/last date JOINs
        sc.execute("CREATE INDEX IF NOT EXISTS idx_snap_url_date "
                   "ON product_snapshots(product_url, snapshot_date)")
        sc.commit()
        # Get oldest and newest snapshot per URL in a single pass using MIN/MAX subqueries.
        # SQLite 3.9 compatible (no window functions needed).
        rows = sc.execute("""
            SELECT
                d.product_url,
                f.recommend_pct   AS first_rec,
                l.recommend_pct   AS last_rec,
                f.avg_star_rating AS first_stars,
                l.avg_star_rating AS last_stars,
                COALESCE(f.price_czk, f.price_eur) AS first_price,
                COALESCE(l.price_czk, l.price_eur) AS last_price,
                CAST(julianday(d.last_date) - julianday(d.first_date) AS INTEGER) AS days
            FROM (
                SELECT product_url,
                       MIN(snapshot_date) AS first_date,
                       MAX(snapshot_date) AS last_date
                FROM product_snapshots
                GROUP BY product_url
                HAVING COUNT(*) >= 2
            ) d
            JOIN product_snapshots f
              ON f.product_url = d.product_url AND f.snapshot_date = d.first_date
            JOIN product_snapshots l
              ON l.product_url = d.product_url AND l.snapshot_date = d.last_date
        """).fetchall()
        sc.close()

        result = {}
        for r in rows:
            rec_d   = round(r["last_rec"]   - r["first_rec"],   1) if r["last_rec"]   is not None and r["first_rec"]   is not None else None
            star_d  = round(r["last_stars"] - r["first_stars"], 2) if r["last_stars"] is not None and r["first_stars"] is not None else None
            price_d = round(r["last_price"] - r["first_price"], 0) if r["last_price"] is not None and r["first_price"] is not None else None
            days    = r["days"] or 0
            # Only include entries with a meaningful change to keep payload small
            has_rating = (rec_d is not None and abs(rec_d) >= 0.5) or \
                         (star_d is not None and abs(star_d) >= 0.05)
            # Use relative threshold (≥2% change) so EUR products (€200-2000)
            # and CZK products (5000-50000 Kč) are both handled correctly.
            # Sanity check: reject >90% drops/rises — these are data artifacts
            # (e.g., old int÷100 bug in MediaMarkt prices).
            fp_val = r["first_price"]
            lp_val = r["last_price"]
            pct    = abs(price_d / fp_val) if (price_d is not None and fp_val) else None
            has_price  = (price_d is not None and fp_val and pct is not None and
                          abs(price_d) >= 2 and pct >= 0.02 and pct < 0.85)
            if has_rating or has_price:
                # Include first/last prices only if they pass the sanity check
                if has_price:
                    fp = round(r["first_price"], 0) if r["first_price"] is not None else None
                    lp = round(r["last_price"],  0) if r["last_price"]  is not None else None
                else:
                    fp = lp = None   # suppress corrupt price data from the card strip
                entry = [rec_d, star_d, price_d if has_price else None, days, fp, lp]
                result[r["product_url"]] = entry
        _delta_cache["data"] = result
        _delta_cache["ts"]   = time.monotonic()
        return result
    except Exception as e:
        import logging
        logging.error(f"query_snapshot_deltas failed: {e}", exc_info=True)
        return {}


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
        # Normalise for matching: lowercase + strip trailing slash
        _norm = product_url.lower().rstrip("/")
        rows = sc.execute("""
            SELECT snapshot_date, recommend_pct, review_count,
                   avg_star_rating, price_czk, price_eur
            FROM   product_snapshots
            WHERE  LOWER(RTRIM(product_url, '/')) = ?
            ORDER  BY snapshot_date ASC
        """, (_norm,)).fetchall()
        sc.close()
        return [dict(r) for r in rows]
    except Exception as e:
        import logging
        logging.error(f"query_product_history failed: {e}")
        return []


# ── Live scan: single-page scrape on demand ──────────────────────────────────
# Category → Heureka listing URL
_HEUREKA_SCAN_URLS: dict = {
    "Smartphones":        "https://mobilni-telefony.heureka.cz/",
    "Laptops":            "https://notebooky.heureka.cz/",
    "Tablets":            "https://tablety.heureka.cz/",
    "Headphones":         "https://sluchatka.heureka.cz/",
    "Smartwatches":       "https://chytre-hodinky.heureka.cz/",
    "Cameras":            "https://digitalni-fotoaparaty.heureka.cz/",
    "Televisions":        "https://televize.heureka.cz/",
    "Vacuum Cleaners":    "https://vysavace.heureka.cz/",
    "Robot Vacuums":      "https://roboticke-vysavace.heureka.cz/",
    "Coffee Machines":    "https://male-spotrebice.heureka.cz/",
    "Washing Machines":   "https://pracky.heureka.cz/",
    "Dishwashers":        "https://mycky-nadobi.heureka.cz/",
    "Fridges & Freezers": "https://chladnicky.heureka.cz/",
}

_HEUREKA_SK_SCAN_URLS: dict = {
    "Smartphones": "https://mobilne-telefony.heureka.sk/",
    "Laptops":     "https://notebooky.heureka.sk/",
    "Tablets":     "https://tablety.heureka.sk/",
    "Televisions": "https://televizor.heureka.sk/",
    "Headphones":  "https://sluchadla.heureka.sk/",
    "Smartwatches":"https://inteligentne-hodinky.heureka.sk/",
}

_CENEO_SCAN_URLS: dict = {
    "Smartphones":    "https://www.ceneo.pl/Smartfony",
    "Laptops":        "https://www.ceneo.pl/Laptopy",
    "Tablets":        "https://www.ceneo.pl/Tablety",
    "Headphones":     "https://www.ceneo.pl/Sluchawki",
    "Televisions":    "https://www.ceneo.pl/Telewizory",
    "Smartwatches":   "https://www.ceneo.pl/Smartwatche_i_opaski_fitness",
    "Washing Machines": "https://www.ceneo.pl/Pralki",
    "Vacuum Cleaners": "https://www.ceneo.pl/Odkurzacze",
    "Coffee Machines": "https://www.ceneo.pl/Ekspresy_do_kawy",
}

# Zbozi.cz — values are API categoryPath slugs (not full URLs)
_ZBOZI_SCAN_SLUGS: dict = {
    "Headphones":       "elektronika/audio/sluchatka/",
    "Speakers":         "elektronika/audio/reproduktory/",
    "Smartwatches":     "elektronika/chytre-hodinky-a-fitness/chytre-hodinky/",
    "Coffee Machines":  "domaci-spotrebice/kuchyne/kavovary/",
    "Vacuum Cleaners":  "domaci-spotrebice/uklid/vysavace/",
    "Robot Vacuums":    "domaci-spotrebice/uklid/roboticke-vysavace/",
    "Air Purifiers":    "domaci-spotrebice/klimatizace-a-vzduch/cisticky-vzduchu/",
    "Televisions":      "elektronika/televize/",
    "SSD":              "pocitace-a-it/uloziste/ssd-disky/",
    "Keyboards":        "pocitace-a-it/prislusenstvi-k-pc/klavesnice/",
    "Mice":             "pocitace-a-it/prislusenstvi-k-pc/mysi/",
}

# Coolblue.nl — values are listing URL slugs
_COOLBLUE_SCAN_SLUGS: dict = {
    "Smartphones":       "smartphones",
    "Tablets":           "tablets/tablets",
    "Laptops":           "laptops/laptops",
    "Televisions":       "televisies/smart-tv",
    "Headphones":        "hoofdtelefoons/bluetooth-hoofdtelefoons",
    "Soundbars":         "soundbars/soundbars",
    "Bluetooth Speakers":"bluetooth-speakers",
    "Smartwatches":      "smartwatches/smartwatches",
    "Washing Machines":  "wasmachines/wasmachines",
    "Dishwashers":       "vaatwassers/vaatwassers",
    "Refrigerators":     "koelkasten/koelkasten",
    "Air Fryers":        "airfryers/airfryers",
    "Stick Vacuums":     "stofzuigers/steelstofzuigers",
}

# ── Otto.de — category slugs appended to https://www.otto.de/
_OTTO_SCAN_SLUGS: dict = {
    "Smartphones":      "technik/smartphone/",
    "Tablets":          "technik/tablet/",
    "Laptops":          "technik/notebook/",
    "Televisions":      "technik/fernseher/",
    "Headphones":       "technik/kopfhoerer/",
    "Smartwatches":     "technik/smartwatch/",
    "Washing Machines": "haushalt/waschmaschinen/",
    "Dishwashers":      "haushalt/geschirrspueler/",
    "Fridges & Freezers": "haushalt/kuehlschraenke/",
    "Vacuum Cleaners":  "haushalt/staubsauger/",
    "Coffee Machines":  "haushalt/kaffeemaschinen/",
    "Speakers":         "technik/lautsprecher/",
}

# ── MediaMarkt.de — search queries
_MEDIAMARKT_QUERIES: dict = {
    "Smartphones":      "smartphone",
    "Tablets":          "tablet",
    "Laptops":          "laptop notebook",
    "Televisions":      "fernseher",
    "Headphones":       "kopfhörer",
    "Smartwatches":     "smartwatch",
    "Speakers":         "bluetooth lautsprecher",
    "Washing Machines": "waschmaschine",
    "Coffee Machines":  "kaffeevollautomat",
    "Vacuum Cleaners":  "staubsauger",
    "Gaming Consoles":  "spielkonsole",
    "Cameras":          "digitalkamera",
}

# ── Amazon.de — bestseller category nodes
_AMAZON_DE_NODES: dict = {
    # Now used as search keywords (bestsellers page is bot-blocked, search works)
    "Smartphones":      "smartphone",
    "Laptops":          "laptop notebook",
    "Tablets":          "tablet",
    "Televisions":      "fernseher",
    "Headphones":       "kopfhörer",
    "Smart Home":       "smart home",
    "Vacuum Cleaners":  "staubsauger",
    "Kitchen":          "küchengeräte",
    "Large Appliances": "haushaltsgeräte",
    "Cameras":          "kamera",
    "PC Hardware":      "pc hardware",
    "Gaming":           "gaming",
}

# ── Geizhals.at — category codes
_GEIZHALS_CODES: dict = {
    "Smartphones":      "umtsover",
    "Tablets":          "umtstab",
    "Laptops":          "nb",
    "Televisions":      "tvger",
    "Headphones":       "koph",
    "Smartwatches":     "fitness",
    "Speakers":         "multls",
    "Cameras":          "dcam",
    "SSD":              "hd",
    "Keyboards":        "tastatur",
    "Mice":             "maus",
    "Routers":          "wlanrouter",
}

# ── Idealo.de — category URL paths
_IDEALO_PATHS: dict = {
    "Smartphones":      "ProductCategory/3513I16-705.html",
    "Tablets":          "ProductCategory/8095.html",
    "Laptops":          "ProductCategory/703.html",
    "Televisions":      "ProductCategory/691.html",
    "Headphones":       "ProductCategory/14013.html",
    "Smartwatches":     "ProductCategory/14139.html",
    "Speakers":         "ProductCategory/3513I16-836.html",
    "Cameras":          "ProductCategory/684.html",
    "Gaming Consoles":  "ProductCategory/11178.html",
    "SSDs":             "ProductCategory/3513I16-1199.html",
    "Keyboards":        "ProductCategory/3513I16-746.html",
    "Mice":             "ProductCategory/3513I16-745.html",
}

# ── Prisjakt.nu — category slugs
_PRISJAKT_SLUGS: dict = {
    "Smartphones":      "mobiltelefoner",
    "Laptops":          "laptops-barbara-datorer",
    "Tablets":          "surfplattor",
    "Televisions":      "tv",
    "Headphones":       "horlurar",
    "Smartwatches":     "smartwatch",
    "Speakers":         "mobilhogtalare",
    "Washing Machines": "tvattmaskiner",
    "Dishwashers":      "diskmaskiner",
    "Fridges":          "kylskap",
    "Coffee Machines":  "espressomaskiner",
    "Vacuum Cleaners":  "dammsugare",
    "Robot Vacuums":    "robotdammsugare",
}

# ── PriceRunner.dk — category slugs (id/name)
_PRICERUNNER_DK_SLUGS: dict = {
    "Smartphones":      "1/Mobiltelefoner",
    "Laptops":          "27/Baerbar",
    "Tablets":          "224/Tablets",
    "Televisions":      "2/TV",
    "Headphones":       "94/Hoeretelefoner",
    "Smartwatches":     "1438/Wearables",
    "Speakers":         "267/Bluetooth-hojttalere",
    "Washing Machines": "14/Vaskemaskiner",
    "Dishwashers":      "13/Opvaskemaskiner",
    "Fridges":          "18/Koeleskabe",
    "Coffee Machines":  "82/Kaffemaskiner",
    "Vacuum Cleaners":  "19/Stoevsugere",
    "Robot Vacuums":    "1613/Robotstoevsugere",
    "SSD":              "36/SSD",
}

# ── PriceRunner.se — same IDs, Swedish site
_PRICERUNNER_SE_SLUGS: dict = _PRICERUNNER_DK_SLUGS.copy()

# ── Fnac.fr — full listing page URLs
_FNAC_URLS: dict = {
    "Smartphones":    "https://www.fnac.com/Tous-les-telephones-portables-et-smartphones/Tous-les-telephones/nsh130385/w-1",
    "Tablets":        "https://www.fnac.com/Toutes-les-tablettes/Toutes-les-tablettes/nsh227099/w-1",
    "Laptops":        "https://www.fnac.com/Tous-les-ordinateurs-portables/Ordinateurs-portables/nsh154425/w-1",
    "Televisions":    "https://www.fnac.com/Tous-les-televiseurs/nsh130314/w-1",
    "Headphones":     "https://www.fnac.com/Casques-et-ecouteurs/shi227166/w-1",
    "Smartwatches":   "https://www.fnac.com/Montres-connectees/shi314861/w-1",
    "Cameras":        "https://www.fnac.com/Tous-les-appareils-photo/nsh130354/w-1",
    "Vacuum Cleaners":"https://www.fnac.com/Aspirateurs/shi314755/w-1",
    "Coffee Machines":"https://www.fnac.com/Machines-a-expresso/shi314742/w-1",
}

# ── Darty.fr — full listing page URLs
_DARTY_URLS: dict = {
    "Smartphones":    "https://www.darty.com/nav/achat/telephonie/mobile_smartphone/",
    "Tablets":        "https://www.darty.com/nav/achat/telephonie/tablette/",
    "Laptops":        "https://www.darty.com/nav/achat/informatique/ordinateur_portable/",
    "Televisions":    "https://www.darty.com/nav/achat/image_son/television/",
    "Headphones":     "https://www.darty.com/nav/achat/son/casque_et_ecouteurs/",
    "Washing Machines":"https://www.darty.com/nav/achat/gros_electromenager/lave_linge/",
    "Dishwashers":    "https://www.darty.com/nav/achat/gros_electromenager/lave_vaisselle/",
    "Fridges":        "https://www.darty.com/nav/achat/gros_electromenager/refrigerateur/",
    "Coffee Machines":"https://www.darty.com/nav/achat/petit_electromenager/cafetiere_a_expresso/",
    "Vacuum Cleaners":"https://www.darty.com/nav/achat/petit_electromenager/aspirateur/",
}

# ── CZC.cz — category slugs
_CZC_SLUGS: dict = {
    "Smartphones":    "mobilni-telefony",
    "Tablets":        "tablety",
    "Laptops":        "notebooky",
    "Headphones":     "sluchatka",
    "Smartwatches":   "chytre-hodinky",
    "Televisions":    "televizory",
    "Cameras":        "fotoaparaty",
    "SSD":            "ssd",
    "Keyboards":      "klavesnice",
    "Mice":           "mysi",
    "Speakers":       "reproduktory",
}

# ── Source → ISO country code ─────────────────────────────────────────────────
_SOURCE_COUNTRY: dict = {
    "heureka":        "CZ",
    "zbozi":          "CZ",
    "czc":            "CZ",
    "heureka_sk":     "SK",
    "ceneo":          "PL",
    "coolblue":       "NL",
    "otto":           "DE",
    "mediamarkt":     "DE",
    "amazon_de":      "DE",
    "geizhals":       "AT",
    "idealo":         "DE",
    "prisjakt":       "SE",
    "pricerunner":    "DK",
    "pricerunner_se": "SE",
    "fnac":           "FR",
    "darty":          "FR",
}

_SCAN_SOURCES: dict = {
    "heureka":       {"urls": _HEUREKA_SCAN_URLS,      "module": "heureka"},
    "heureka_sk":    {"urls": _HEUREKA_SK_SCAN_URLS,   "module": "heureka_sk"},
    "ceneo":         {"urls": _CENEO_SCAN_URLS,         "module": "ceneo"},
    "zbozi":         {"urls": _ZBOZI_SCAN_SLUGS,        "module": "zbozi"},
    "coolblue":      {"urls": _COOLBLUE_SCAN_SLUGS,     "module": "coolblue"},
    "otto":          {"urls": _OTTO_SCAN_SLUGS,         "module": "otto"},
    "mediamarkt":    {"urls": _MEDIAMARKT_QUERIES,      "module": "mediamarkt"},
    "amazon_de":     {"urls": _AMAZON_DE_NODES,         "module": "amazon_de"},
    "geizhals":      {"urls": _GEIZHALS_CODES,          "module": "geizhals"},
    "idealo":        {"urls": _IDEALO_PATHS,            "module": "idealo"},
    "prisjakt":      {"urls": _PRISJAKT_SLUGS,          "module": "prisjakt"},
    "pricerunner":   {"urls": _PRICERUNNER_DK_SLUGS,    "module": "pricerunner"},
    "pricerunner_se":{"urls": _PRICERUNNER_SE_SLUGS,    "module": "pricerunner_se"},
    "fnac":          {"urls": _FNAC_URLS,               "module": "fnac"},
    "darty":         {"urls": _DARTY_URLS,              "module": "darty"},
    "czc":           {"urls": _CZC_SLUGS,               "module": "czc"},
}


def _pw_get_html(url, warm_url=None, locale="de-DE",
                 wait_selector=None, extra_sleep=2.0):
    """
    Fetch *url* with a headless Chromium browser (Playwright).
    Used as fallback for sites that block curl_cffi (Cloudflare, DataDome, etc.).
    Returns the fully-rendered HTML string, or "" on error.
    """
    try:
        from playwright.sync_api import sync_playwright
        import time as _t
        with sync_playwright() as _pw:
            browser = _pw.chromium.launch(headless=True)
            ctx = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                locale=locale,
                viewport={"width": 1280, "height": 800},
            )
            page = ctx.new_page()
            if warm_url:
                page.goto(warm_url, wait_until="domcontentloaded", timeout=20_000)
                _t.sleep(1.5)
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            if wait_selector:
                try:
                    page.wait_for_selector(wait_selector, timeout=8_000)
                except Exception:
                    pass
            _t.sleep(extra_sleep)
            html = page.content()
            browser.close()
            return html
    except Exception as _e:
        import logging
        logging.warning(f"_pw_get_html error ({url}): {_e}")
        return ""


def _live_scan(source: str, category: str, limit: int = 30) -> dict:
    """
    Run a single-page scrape of the given source/category and compare results
    against what is already in products.db.  Returns a JSON-serialisable dict.
    """
    import time as _t
    t0 = _t.time()

    cfg = _SCAN_SOURCES.get(source)
    if not cfg:
        return {"error": f"Source '{source}' not available for live scan.", "products": []}

    try:
        from curl_cffi import requests as cffi_requests
        from bs4 import BeautifulSoup
    except ImportError:
        return {"error": "curl_cffi / beautifulsoup4 not installed on server.", "products": []}

    slug = cfg["urls"].get(category) or next(iter(cfg["urls"].values()))

    try:
        if cfg["module"] in ("heureka", "heureka_sk"):
            from scraper.heureka_scraper import scrape_page, warm_up_session
            session = cffi_requests.Session(impersonate="chrome120")
            warm_up_session(session)
            raw = scrape_page(slug, session)

        elif cfg["module"] == "ceneo":
            # Ceneo requires a real browser (F-detection JS challenge)
            from scraper.ceneo_scraper import scrape_listing_page as ceneo_scrape_page
            from playwright.sync_api import sync_playwright
            import time as _time
            with sync_playwright() as _pw:
                _browser = _pw.chromium.launch(headless=True)
                _ctx = _browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    locale="pl-PL", viewport={"width": 1280, "height": 800},
                )
                _page = _ctx.new_page()
                _page.goto("https://www.ceneo.pl/", wait_until="load", timeout=30000)
                _time.sleep(2)
                items = ceneo_scrape_page(slug, _page)
                _browser.close()
            raw = [{
                "Name":              p.get("name", ""),
                "ProductURL":        p.get("product_url", ""),
                "RecommendRate_pct": p.get("score_pct"),
                "ReviewsCount":      p.get("review_count") or 0,
                "Price_CZK":         p.get("price_pln"),   # PLN (shown as-is)
            } for p in items if p.get("name")]

        elif cfg["module"] == "zbozi":
            from scraper.zbozi_scraper import fetch_page, warm_up_session as zbozi_warm_up
            session = cffi_requests.Session(impersonate="chrome120")
            zbozi_warm_up(session)
            data  = fetch_page(slug, 0, session)
            items = data.get("products", [])
            raw   = []
            for item in items:
                name    = item.get("displayName", "").strip()
                purl    = item.get("url", "")
                rating  = item.get("rating")
                reviews = item.get("experienceCount", 0)
                price_h = item.get("minPrice")
                if not name or not purl.startswith("https://www.zbozi.cz/"):
                    continue
                raw.append({
                    "Name":              name,
                    "ProductURL":        purl,
                    "RecommendRate_pct": float(rating) if rating is not None else None,
                    "ReviewsCount":      reviews or 0,
                    "Price_CZK":         price_h / 100.0 if price_h else None,
                })

        elif cfg["module"] == "coolblue":
            from scraper.coolblue_scraper import listing_url as cb_listing_url, parse_listing_jsonld, fetch_text
            session = cffi_requests.Session(impersonate="chrome120")
            _, text = fetch_text(cb_listing_url(slug, 1), session)
            stubs = parse_listing_jsonld(text)
            raw = [{
                "Name":              s["Name"],
                "ProductURL":        s["ProductURL"],
                "RecommendRate_pct": None,
                "ReviewsCount":      0,
                "Price_CZK":         None,
            } for s in stubs]

        elif cfg["module"] == "otto":
            from scraper.otto_scraper_v2 import fetch_category, make_session as otto_session
            session = otto_session()
            products, _ = fetch_category(session, slug, page=1)
            raw = [{
                "Name":              p.get("Name", ""),
                "ProductURL":        p.get("ProductURL", ""),
                "RecommendRate_pct": round((p["AvgStarRating"] / 5.0) * 100, 1)
                                     if p.get("AvgStarRating") else None,
                "ReviewsCount":      p.get("ReviewsCount") or 0,
                "Price_CZK":         p.get("Price_EUR"),   # stored as price field for display
            } for p in products if p.get("Name")]

        elif cfg["module"] == "mediamarkt":
            from scraper.mediamarkt_scraper import fetch_query, make_session as mm_session
            session = mm_session()
            products = fetch_query(session, slug)
            raw = [{
                "Name":              p.get("Name", ""),
                "ProductURL":        p.get("ProductURL", ""),
                "RecommendRate_pct": round((p["AvgStarRating"] / 5.0) * 100, 1)
                                     if p.get("AvgStarRating") else None,
                "ReviewsCount":      p.get("ReviewsCount") or 0,
                "Price_CZK":         p.get("Price_EUR"),
            } for p in products if p.get("Name")]

        elif cfg["module"] == "amazon_de":
            # Amazon DE bestsellers page is bot-blocked; search page works
            from scraper.amazon_de_scraper import (parse_eur, parse_rating,
                                                    parse_int as parse_reviews)
            from bs4 import BeautifulSoup
            from urllib.parse import quote_plus
            import re as _re
            search_url = f"https://www.amazon.de/s?k={quote_plus(slug)}&sort=review-rank"
            _html = _pw_get_html(search_url, warm_url="https://www.amazon.de/",
                                 locale="de-DE",
                                 wait_selector="[data-asin][data-component-type='s-search-result']",
                                 extra_sleep=3.0)
            _soup = BeautifulSoup(_html, "html.parser")
            _items = _soup.select("[data-asin][data-component-type='s-search-result']")
            products = []
            for item in _items:
                asin = item.get("data-asin") or ""
                if not asin or len(asin) != 10:
                    continue
                name_el = item.select_one("h2 span, .a-text-normal span")
                name = name_el.get_text(" ", strip=True) if name_el else ""
                if not name:
                    continue
                price_el = item.select_one(".a-price .a-offscreen, .a-price-whole")
                rating_el = item.select_one(".a-icon-star-small .a-icon-alt, .a-icon-star .a-icon-alt")
                reviews_el = item.select_one("[aria-label$='ratings'], .a-size-base.s-underline-text")
                products.append({
                    "Name": name,
                    "ProductURL": f"https://www.amazon.de/dp/{asin}",
                    "Price_EUR": parse_eur(price_el.get_text(strip=True) if price_el else None),
                    "AvgStarRating": parse_rating(rating_el.get_text(strip=True) if rating_el else None),
                    "ReviewsCount": parse_reviews(reviews_el.get_text(strip=True) if reviews_el else None),
                })
            raw = [{
                "Name":              p.get("Name", ""),
                "ProductURL":        p.get("ProductURL", ""),
                "RecommendRate_pct": round((p["AvgStarRating"] / 5.0) * 100, 1)
                                     if p.get("AvgStarRating") else None,
                "ReviewsCount":      p.get("ReviewsCount") or 0,
                "Price_CZK":         p.get("Price_EUR"),
            } for p in products if p.get("Name")]

        elif cfg["module"] == "geizhals":
            # Try curl_cffi first; fall back to Playwright if 0 results (Cloudflare Turnstile)
            from scraper.geizhals_scraper import (scrape_page as gz_scrape,
                                                   make_session as gz_session,
                                                   parse_product_card, BASE_URL as GZ_BASE)
            from bs4 import BeautifulSoup
            session = gz_session()
            products, _ = gz_scrape(session, slug, 1)
            if not products:
                gz_url = f"{GZ_BASE}/?cat={slug}&sort=r&pg=1"
                _html = _pw_get_html(gz_url, warm_url=GZ_BASE + "/",
                                     locale="de-DE",
                                     wait_selector="li.productlist__item",
                                     extra_sleep=3.0)
                _soup = BeautifulSoup(_html, "html.parser")
                _cards = (
                    _soup.select("li.productlist__item") or
                    _soup.select("article.productlist__item") or
                    _soup.select("div.productlist__item")
                )
                products = [p for p in (parse_product_card(c) for c in _cards) if p]
            raw = [{
                "Name":              p.get("Name", ""),
                "ProductURL":        p.get("ProductURL", ""),
                "RecommendRate_pct": round((p["AvgStarRating"] / 5.0) * 100, 1)
                                     if p.get("AvgStarRating") else None,
                "ReviewsCount":      p.get("ReviewsCount") or 0,
                "Price_CZK":         p.get("Price_EUR"),
            } for p in products if p.get("Name")]

        elif cfg["module"] == "idealo":
            # Try curl_cffi first; fall back to Playwright if 0 results (custom challenge)
            from scraper.idealo_scraper import (scrape_category_page as idealo_scrape,
                                                 make_session as idealo_session,
                                                 parse_product_card as idealo_parse_card,
                                                 BASE_URL as IDEALO_BASE)
            from bs4 import BeautifulSoup
            session = idealo_session()
            products, _ = idealo_scrape(session, slug, 1)
            if not products:
                idealo_url = f"{IDEALO_BASE}/preisvergleich/{slug}?sortby=rating"
                _html = _pw_get_html(idealo_url, warm_url=IDEALO_BASE + "/",
                                     locale="de-DE",
                                     wait_selector="div.sr-resultItem,[class*='productCard']",
                                     extra_sleep=3.0)
                _soup = BeautifulSoup(_html, "html.parser")
                _cards = (
                    _soup.select("div.sr-resultItem") or
                    _soup.select("article[class*='productCard']") or
                    _soup.select("[data-testid*='product-card']")
                )
                products = [p for p in (idealo_parse_card(c) for c in _cards) if p]
            raw = [{
                "Name":              p.get("Name", ""),
                "ProductURL":        p.get("ProductURL", ""),
                "RecommendRate_pct": round((p["AvgStarRating"] / 5.0) * 100, 1)
                                     if p.get("AvgStarRating") else None,
                "ReviewsCount":      p.get("ReviewsCount") or 0,
                "Price_CZK":         p.get("Price_EUR"),
            } for p in products if p.get("Name")]

        elif cfg["module"] == "prisjakt":
            from scraper.prisjakt_scraper import fetch_page as pj_fetch
            session = cffi_requests.Session(impersonate="chrome120")
            raw = pj_fetch(slug, 1, session)

        elif cfg["module"] in ("pricerunner", "pricerunner_se"):
            mod = "pricerunner_scraper" if cfg["module"] == "pricerunner" else "pricerunner_se_scraper"
            import importlib
            pr_mod = importlib.import_module(f"scraper.{mod}")
            session = cffi_requests.Session(impersonate="chrome120")
            raw = pr_mod.fetch_page(slug, 1, session)

        elif cfg["module"] == "fnac":
            # Fnac blocks curl_cffi — go straight to Playwright (skip slow warm-up session)
            from scraper.fnac_scraper import scrape_listing_page
            from bs4 import BeautifulSoup
            _html = _pw_get_html(slug, warm_url="https://www.fnac.com/",
                                 locale="fr-FR",
                                 wait_selector=".Article-item",
                                 extra_sleep=2.5)
            soup = BeautifulSoup(_html, "html.parser") if _html else None
            raw  = scrape_listing_page(soup, {"url": slug, "name": category,
                                              "has_repairability": False,
                                              "has_durability": False}) if soup else []
            raw = [{
                "Name":              p.get("Name", ""),
                "ProductURL":        p.get("ProductURL", ""),
                "RecommendRate_pct": round((p["AvgStarRating"] / 5.0) * 100, 1)
                                     if p.get("AvgStarRating") else None,
                "ReviewsCount":      p.get("ReviewsCount") or 0,
                "Price_CZK":         p.get("Price_EUR"),
            } for p in raw if p.get("Name")]

        elif cfg["module"] == "darty":
            # Darty blocks curl_cffi — go straight to Playwright
            from scraper.darty_scraper import scrape_listing
            from bs4 import BeautifulSoup
            _html = _pw_get_html(slug, warm_url="https://www.darty.com/",
                                 locale="fr-FR",
                                 wait_selector=".product-grid .product-item, .product-tile",
                                 extra_sleep=2.5)
            soup = BeautifulSoup(_html, "html.parser") if _html else None
            raw  = scrape_listing(soup, {"url": slug, "name": category,
                                         "has_repairability": False,
                                         "has_durability": False}) if soup else []
            raw = [{
                "Name":              p.get("Name", ""),
                "ProductURL":        p.get("ProductURL", ""),
                "RecommendRate_pct": round((p["AvgStarRating"] / 5.0) * 100, 1)
                                     if p.get("AvgStarRating") else None,
                "ReviewsCount":      p.get("ReviewsCount") or 0,
                "Price_CZK":         p.get("Price_EUR"),
            } for p in raw if p.get("Name")]

        elif cfg["module"] == "czc":
            # CZC requires Playwright (DataDome CAPTCHA blocks curl_cffi)
            from scraper.czc_scraper import fetch_page_html as czc_fetch_html, scrape_page as czc_scrape
            from playwright.sync_api import sync_playwright
            import time as _time
            url = f"https://www.czc.cz/{slug}/produkty"
            with sync_playwright() as _pw:
                _browser = _pw.chromium.launch(headless=True)
                _ctx = _browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    locale="cs-CZ", viewport={"width": 1280, "height": 800},
                )
                _page = _ctx.new_page()
                _page.goto("https://www.czc.cz/", wait_until="domcontentloaded", timeout=20000)
                _time.sleep(1.5)
                html = czc_fetch_html(_page, url)
                _browser.close()
            raw = czc_scrape(html) if html else []

        else:
            raw = []

    except Exception as exc:
        import logging
        logging.error(f"_live_scan scrape failed: {exc}", exc_info=True)
        return {"error": str(exc), "products": []}

    raw = raw[:limit]

    conn = open_db()
    enriched = []
    for p in raw:
        purl  = (p.get("ProductURL") or "").strip()
        price = p.get("Price_CZK") or p.get("Price_EUR")   # normalise price field
        existing = None
        if purl:
            row = conn.execute(
                "SELECT RecommendRate_pct, ReviewsCount, AvgStarRating, Price_CZK "
                "FROM products WHERE ProductURL = ?", (purl,)
            ).fetchone()
            if row:
                existing = {
                    "rec_pct":  row[0],
                    "reviews":  row[1],
                    "stars":    row[2],
                    "price":    row[3],
                }

        delta = None
        if existing:
            rec_d = None
            if p.get("RecommendRate_pct") is not None and existing["rec_pct"] is not None:
                rec_d = round(p["RecommendRate_pct"] - existing["rec_pct"], 1)
            price_d = None
            if price is not None and existing["price"] is not None:
                price_d = int(price - existing["price"])
            if rec_d is not None or price_d is not None:
                delta = {"rec_d": rec_d, "price_d": price_d}

        enriched.append({
            "Name":              p.get("Name", ""),
            "ProductURL":        purl,
            "RecommendRate_pct": p.get("RecommendRate_pct"),
            "ReviewsCount":      p.get("ReviewsCount"),
            "Price_CZK":         price,
            "source":            source,
            "country":           _SOURCE_COUNTRY.get(source, "??"),
            "existing":          existing,
            "delta":             delta,
            "is_new":            existing is None,
        })
    conn.close()

    changed = sum(1 for p in enriched if p["delta"] and (
        (p["delta"].get("rec_d") or 0) != 0 or (p["delta"].get("price_d") or 0) != 0
    ))
    return {
        "products":     enriched,
        "source":       source,
        "category":     category,
        "scan_url":     slug,
        "total":        len(enriched),
        "new_count":    sum(1 for p in enriched if p["is_new"]),
        "changed_count": changed,
        "duration_ms":  int((_t.time() - t0) * 1000),
    }


def query_also_at(name: str, source: str, limit: int = 6) -> list:
    """Find other sources selling the same product using model-token matching.

    Extracts alphanumeric model tokens (e.g. 'WH-1000XM5', 'QE65QN85D') from the
    product name, then searches for other products containing the same primary token.
    Falls back to a broad substring match when no model tokens are found.
    """
    if not name or len(name.strip()) < 3:
        return []
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(__file__))
        from scraper.cross_market import extract_model_tokens
        tokens = extract_model_tokens(name)
    except Exception:
        tokens = []

    conn = open_db()
    try:
        if tokens:
            primary = tokens[0]   # longest / most specific token
            rows = conn.execute(
                """SELECT source, Name, Price_CZK, Price_EUR, AvgStarRating,
                          ReviewsCount, RecommendRate_pct, ProductURL, image_url, currency
                   FROM products
                   WHERE Name LIKE ? COLLATE NOCASE
                     AND source != ?
                     AND source NOT IN ('dtest','warentest')
                   ORDER BY COALESCE(RecommendRate_pct, 0) DESC,
                            COALESCE(AvgStarRating, 0) DESC
                   LIMIT ?""",
                (f"%{primary}%", source, limit)
            ).fetchall()
        else:
            # No model tokens — fall back to first word(s) match
            words = name.strip().split()[:3]
            pattern = "%" + " ".join(words[:2]) + "%"
            rows = conn.execute(
                """SELECT source, Name, Price_CZK, Price_EUR, AvgStarRating,
                          ReviewsCount, RecommendRate_pct, ProductURL, image_url, currency
                   FROM products
                   WHERE Name LIKE ? COLLATE NOCASE
                     AND source != ?
                     AND source NOT IN ('dtest','warentest')
                   ORDER BY COALESCE(RecommendRate_pct, 0) DESC,
                            COALESCE(AvgStarRating, 0) DESC
                   LIMIT ?""",
                (pattern, source, limit)
            ).fetchall()
    finally:
        conn.close()

    return [dict(r) for r in rows]


def query_search_suggest(q: str, limit: int = 7) -> list:
    """Return top product suggestions for the given query string.
    Returns dicts with name, image_url, NormalizedCategory ordered by review count.
    Prefers product rows that already have a product image."""
    if not q or len(q) < 2:
        return []
    conn = open_db()
    tokens = [t for t in q.strip().split() if len(t) >= 1]
    if len(tokens) <= 1:
        where_clause = "Name LIKE ? COLLATE NOCASE"
        where_params = [f"%{q}%"]
    else:
        # Multi-token: AND-match all tokens so "galaxy s25 samsung" → "Samsung Galaxy S25"
        parts = " AND ".join("Name LIKE ? COLLATE NOCASE" for _ in tokens)
        where_clause = f"({parts})"
        where_params = [f"%{t}%" for t in tokens]
    # First pass: find top names by combined review count; prefer products with images
    name_rows = conn.execute(
        f"""SELECT Name,
                  SUM(COALESCE(ReviewsCount, 0)) as total_reviews,
                  MAX(CASE WHEN image_url IS NOT NULL AND image_url != '' AND image_url != '__none__'
                           THEN image_url END) as img,
                  COALESCE(NormalizedCategory, Category) as cat
           FROM products
           WHERE {where_clause}
             AND source NOT IN ('dtest','warentest')
           GROUP BY Name
           ORDER BY
             (MAX(CASE WHEN image_url IS NOT NULL AND image_url != '' AND image_url != '__none__'
                       THEN 1 END)) DESC NULLS LAST,
             total_reviews DESC, COUNT(*) DESC
           LIMIT ?""",
        where_params + [limit * 2]   # fetch extra to allow de-duplication
    ).fetchall()
    conn.close()

    # De-duplicate by case-insensitive name; keep the one with the most reviews (already sorted)
    seen = set()
    results = []
    for r in name_rows:
        key = r[0].lower().strip()
        if key in seen:
            continue
        seen.add(key)
        results.append({"name": r[0], "img": r[2], "cat": r[3]})
        if len(results) >= limit:
            break
    return results


_stats_cache: dict | None = None
_stats_ts: float = 0.0
_STATS_TTL = 300  # 5 minutes

def query_stats():
    global _stats_cache, _stats_ts
    import time as _time
    now = _time.monotonic()
    if _stats_cache is not None and (now - _stats_ts) < _STATS_TTL:
        return _stats_cache

    conn = open_db()
    r = conn.execute("""
        SELECT COUNT(*) as total,
               ROUND(AVG(AvgStarRating),2) as avg_stars,
               ROUND(AVG(ReturnRate_pct),2) as avg_return,
               ROUND(AVG(RecommendRate_pct),2) as avg_recommend,
               COUNT(DISTINCT Category) as categories,
               COUNT(DISTINCT source) as sources_count,
               SUM(CASE WHEN image_url IS NOT NULL AND image_url != ''
                             AND image_url != '__none__' THEN 1 ELSE 0 END) as with_images,
               SUM(CASE WHEN source='alza' THEN 1 ELSE 0 END) as from_alza,
               SUM(CASE WHEN source!='alza' THEN 1 ELSE 0 END) as from_scraper
        FROM products
    """).fetchone()
    conn.close()
    result = dict(r)
    # Add with_history count from snapshot coverage dict (has_history is computed, not stored)
    try:
        cov = query_snapshot_coverage()   # returns list of tracked URLs; also warms the dict
        result["with_history"] = len(cov) if cov else 0
    except Exception:
        result["with_history"] = 0
    _stats_cache = result
    _stats_ts = now
    return result


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence access log

    def send_json(self, data, status=200, max_age=0):
        import gzip as _gzip
        body = json.dumps(data, default=str).encode()
        accept_enc = self.headers.get("Accept-Encoding", "")
        if "gzip" in accept_enc and len(body) > 1024:
            body = _gzip.compress(body, compresslevel=6)
            use_gzip = True
        else:
            use_gzip = False
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        if use_gzip:
            self.send_header("Content-Encoding", "gzip")
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

        elif path == "/api/search-suggest":
            q = params.get("q", [""])[0].strip()
            suggestions = query_search_suggest(q)
            self.send_json({"suggestions": suggestions}, max_age=60)

        elif path == "/api/keywords":
            self.send_json([{"tag": t, "count": c} for t, c in query_keywords()], max_age=600)

        elif path == "/api/cross-market":
            try:
                from scraper.cross_market import find_cross_market_matches
                min_m   = int(params.get("min_markets", ["2"])[0])
                inc_amz = params.get("include_amazon", ["0"])[0] == "1"
                cache_key = (min_m, inc_amz)
                _now = time.monotonic()
                if (cache_key in _cross_market_cache
                        and (_now - _cross_market_ts.get(cache_key, 0)) < CROSS_MARKET_TTL):
                    groups = _cross_market_cache[cache_key]
                else:
                    conn    = open_db()
                    groups  = find_cross_market_matches(conn, min_markets=min_m,
                                                        include_amazon_us=inc_amz)
                    conn.close()
                    _cross_market_cache[cache_key] = groups
                    _cross_market_ts[cache_key]    = _now
                self.send_json(groups, max_age=600)
            except Exception as e:
                import logging; logging.error(f"/api/cross-market: {e}", exc_info=True)
                self.send_json({"error": str(e)})

        elif path == "/api/brands":
            try:
                min_products = int(params.get("min_products", ["3"])[0])
                conn = open_db()
                # Use the pre-extracted brand column where available; fall back to
                # first word of Name for products scraped before the brand column existed.
                rows = conn.execute("""
                    SELECT
                        COALESCE(NULLIF(brand,''), TRIM(SUBSTR(Name, 1, INSTR(Name || ' ', ' ') - 1))) as brand,
                        COUNT(*) as products,
                        ROUND(AVG(CASE WHEN AvgStarRating > 0 THEN AvgStarRating END), 2) as avg_stars,
                        ROUND(AVG(CASE WHEN RecommendRate_pct > 0 THEN RecommendRate_pct END), 1) as avg_recommend,
                        SUM(COALESCE(ReviewsCount, 0)) as total_reviews,
                        COUNT(DISTINCT source) as sources
                    FROM products
                    WHERE Name IS NOT NULL AND Name != ''
                      AND source NOT IN ('dtest','warentest')
                    GROUP BY UPPER(COALESCE(NULLIF(brand,''), TRIM(SUBSTR(Name, 1, INSTR(Name || ' ', ' ') - 1))))
                    HAVING products >= ?
                    ORDER BY avg_stars DESC, products DESC
                """, (min_products,)).fetchall()
                conn.close()
                # Filter out Czech common words and single-character fragments
                # that scrapers sometimes extract as brand names
                _NON_BRAND = {
                    "prací", "mobilní", "bílé", "bílá", "černé", "černá", "velké", "malé",
                    "nové", "nový", "domácí", "elektrické", "digitální", "chytré", "chytrá",
                    "herní", "výkonné", "přenosné", "bezdrátové",
                    # Czech category words from datart.cz that prefix product names
                    "televize", "sluchátka", "myčka", "dotykový", "dotyková",
                    "mikrovlnná", "pračka", "robotický", "sušička", "podlahový",
                    "automatický", "klávesnice", "tyčový", "chladnička", "vestavná",
                    "chytrý", "bezdrátová", "bezdrátový", "přenosný", "vysavač",
                    "sporák", "trouba", "lednice", "varná", "rychlovarná",
                    "slim", "ultra", "mini", "nano", "pro", "max", "plus", "air", "go",
                    "lite", "neo", "one", "fit", "new", "top", "best", "cool", "smart",
                    "eco", "turbo", "flex", "jet", "flow", "wave", "edge", "core",
                    "be", "by", "do", "na", "ze", "se", "to", "je", "od", "ve",
                    "laptop", "notebook", "phone", "tablet", "watch", "camera", "speaker",
                    "headphone", "keyboard", "mouse", "monitor", "router", "printer",
                    "set", "kit", "pack", "plus", "series", "model", "type",
                    # French category words (fnac.fr product names start with category)
                    "smartphone", "pc", "enceinte", "casque", "tablette", "imprimante",
                    "aspirateur", "ordinateur", "télévision", "television", "réfrigérateur",
                    "lave-linge", "lave-vaisselle", "climatiseur", "cafetière", "micro-ondes",
                    "sèche-linge", "montre", "bracelet", "écouteurs", "haut-parleur",
                    "appareil", "caméra", "robot", "nettoyeur", "fer",
                    # French descriptive words used as "brand" in product names
                    "téléphone", "lunettes", "série",
                    # Polish category words (ceneo.pl product names often start with category)
                    "ekspres",    # Polish: coffee machine / espresso machine
                    "lodówka",    # Polish: fridge
                    "pralka",     # Polish: washing machine
                    "zmywarka",   # Polish: dishwasher
                    "telewizor",  # Polish: TV
                    "słuchawki",  # Polish: headphones
                    "głośnik",    # Polish: speaker
                    "mikrofon",   # Polish: microphone
                    "polecany",   # Polish: "recommended"
                    # Apple product names that lack "Apple" prefix in product name
                    "iphone", "ipad", "imac", "macbook", "airpods",
                    # Generic product category words that sometimes appear as brand (EN)
                    "ssd", "hdd", "nas", "ups", "gpu", "cpu", "ram",
                    "gaming", "portable", "wireless", "bluetooth",
                    # Color words that sometimes appear as brand when used as product line name
                    "black", "white", "silver", "gold", "red", "blue", "green",
                    "gray", "grey", "pink", "purple", "yellow", "orange",
                    # Czech descriptive/category words appearing as fake brands
                    "dálkový",    # Czech: "remote/distant"
                    "koaxiální",  # Czech: "coaxial"
                    "redukce",    # Czech: "adapter/reducer"
                    "sáčky",      # Czech: "bags"
                    "americká",   # Czech: "American" (e.g. "Americká lednice" = side-by-side fridge)
                    "aku",        # Czech: "battery/acccu"
                    # German descriptive words appearing as fake brands
                    "spannbettlaken",    # German: "fitted sheet"
                    "verdunkelungsrollo", # German: "blackout roller blind"
                    "digitalkamera",     # German: "digital camera"
                    # Generic English words that appear as false brands
                    "kitchen", "home", "all", "it", "little", "clean",
                    # Game title first words (alza.cz sells games; first word is not a brand)
                    "god", "dead", "death", "mad", "ghost", "cyberpunk", "spyro",
                    "horizon", "farming", "snowrunner", "crysis", "mafia", "elden",
                    "gran", "wrc", "ufc", "f1", "need", "teenage",
                    "assassin's",  # from "Assassin's Creed"
                    "diablo",      # Diablo game series
                    "marvels",     # Marvel's Avengers / Spider-Man games
                    "zaklínač",    # Czech: "The Witcher"
                    "split",       # "Split Fiction" game
                    "tax",         # German tax software "tax 2026" — not a brand
                    # Articles / prepositions that appear as first word of a real brand name
                    "de",          # French/Dutch particle ("De Gusto", "de Buyer" → "De" alone is not a brand)
                    "the",         # English article ("The Witcher", "The Last of Us" games)
                    "van", "von",  # Dutch/German particles
                    # Czech category word
                    "myš",         # Czech: "mouse" (computer peripheral)
                    # Game title words missed earlier
                    "far",         # "Far Cry"
                    # Generic connector / tech standards
                    "tv", "usb", "iec",
                    # Manufacturer placeholder
                    "oem",         # "Original Equipment Manufacturer" — not a real brand
                }
                brands = [
                    dict(brand=r[0], products=r[1], avg_stars=r[2],
                         avg_recommend=r[3], total_reviews=r[4], sources=r[5])
                    for r in rows
                    # Skip if the brand is in the non-brand word list
                    if r[0] and len(r[0]) >= 2 and r[0].lower() not in _NON_BRAND
                    # Skip brands that start with a digit (e.g. "10-pack,Datacom")
                    and not r[0][0].isdigit()
                    # Skip brands containing a comma (pack descriptors like "10-pack,Datacom")
                    and "," not in r[0]
                    # Skip connector-standard "brands" like "F/IEC", "RCA/XLR"
                    and "/" not in r[0]
                ]
                self.send_json({"brands": brands}, max_age=3600)
            except Exception as e:
                import logging; logging.error(f"/api/brands: {e}", exc_info=True)
                self.send_json({"brands": [], "error": str(e)})

        elif path == "/api/snapshot-coverage":
            try:
                self.send_json({"urls": query_snapshot_coverage()}, max_age=600)
            except Exception as e:
                import logging; logging.error(f"/api/snapshot-coverage: {e}", exc_info=True)
                self.send_json({"urls": [], "error": str(e)})

        elif path == "/api/snapshot-deltas":
            try:
                self.send_json(query_snapshot_deltas(), max_age=600)
            except Exception as e:
                import logging; logging.error(f"/api/snapshot-deltas: {e}", exc_info=True)
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

        elif path == "/api/also-at":
            try:
                pname  = params.get("name",   [""])[0].strip()
                psrc   = params.get("source", [""])[0].strip()
                self.send_json(query_also_at(pname, psrc), max_age=300)
            except Exception as e:
                import logging; logging.error(f"/api/also-at: {e}", exc_info=True)
                self.send_json([])

        elif path == "/api/product":
            # Fetch a single product by rowid — used for deep links (?p=ID)
            try:
                pid = params.get("id", [""])[0].strip()
                if not pid or not pid.isdigit():
                    self.send_json({"error": "id required"}, status=400)
                else:
                    conn = open_db()
                    row = conn.execute(
                        """SELECT id, Name, MainCategory, Category, NormalizedCategory,
                                  ProductURL, Price_CZK, Price_EUR, country, currency,
                                  AvgStarRating, StarRatingsCount, ReviewsCount,
                                  RecommendRate_pct, ReturnRate_pct,
                                  Stars5_Count, Stars4_Count, Stars3_Count,
                                  Stars2_Count, Stars1_Count, source,
                                  COALESCE(cat_rank,0) as source_rank,
                                  COALESCE(cat_total,0) as source_total,
                                  keywords, brand, image_url, scraped_at,
                                  Description, details_json
                           FROM products WHERE id = ? LIMIT 1""",
                        (int(pid),)
                    ).fetchone()
                    conn.close()
                    if row:
                        self.send_json({"product": dict(row)}, max_age=300)
                    else:
                        self.send_json({"error": "not found"}, status=404)
            except Exception as e:
                import logging; logging.error(f"/api/product: {e}", exc_info=True)
                self.send_json({"error": str(e)}, status=500)

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

        elif path == "/api/admin/rerank":
            # Recompute cat_rank/cat_total for all sources except dtest/warentest.
            # Runs in a background thread so the response returns immediately.
            import threading as _threading
            def _do_rerank():
                import logging as _log
                _log.info("rerank: starting …")
                try:
                    _db = open_db()
                    _db.execute("PRAGMA synchronous=OFF")
                    _db.execute("PRAGMA cache_size=-64000")
                    _db.execute("UPDATE products SET cat_rank=NULL, cat_total=NULL")
                    rows = _db.execute("""
                        SELECT rowid, NormalizedCategory, RecommendRate_pct, AvgStarRating, ReviewsCount
                        FROM products
                        WHERE source NOT IN ('dtest','warentest')
                          AND NormalizedCategory IS NOT NULL AND NormalizedCategory!=''
                          AND (RecommendRate_pct IS NOT NULL OR AvgStarRating IS NOT NULL)
                    """).fetchall()
                    _log.info(f"rerank: {len(rows)} eligible products")
                    def _wilson(p, n, z=1.96):
                        """Wilson score lower bound (95% CI) — penalises low review counts."""
                        if not n or n <= 0:
                            return p * 0.5  # no reviews: halve the raw score
                        return (p + z*z/(2*n) - z*((p*(1-p) + z*z/(4*n))/n)**0.5) / (1 + z*z/n)
                    from collections import defaultdict as _dd
                    cat_items = _dd(list)
                    for (rrid, cat, rec, stars, n_rev) in rows:
                        n = int(n_rev) if n_rev else 0
                        if rec is not None:
                            score = _wilson(float(rec) / 100.0, n) * 100.0
                        else:
                            score = _wilson(float(stars) / 5.0, n) * 100.0
                        cat_items[cat].append((score, rrid))
                    updates = []
                    for cat, items in cat_items.items():
                        items.sort(key=lambda x: x[0], reverse=True)
                        n = len(items)
                        for rank_idx, (_, rrid) in enumerate(items, 1):
                            updates.append((rank_idx, n, rrid))
                    for i in range(0, len(updates), 5000):
                        _db.executemany("UPDATE products SET cat_rank=?,cat_total=? WHERE rowid=?",
                                        updates[i:i+5000])
                        _log.info(f"rerank: wrote {min(i+5000,len(updates))}/{len(updates)}")
                    _db.execute("DROP INDEX IF EXISTS idx_norm_cat_rank")
                    _db.execute("CREATE INDEX idx_norm_cat_rank ON products(NormalizedCategory,cat_rank)")
                    _db.commit()
                    _db.close()
                    _log.info(f"rerank: done — {len(updates)} products ranked")
                except Exception as _e:
                    _log.error(f"rerank failed: {_e}", exc_info=True)
            _threading.Thread(target=_do_rerank, daemon=True).start()
            self.send_json({"status": "started",
                            "message": "Rerank running in background. Check server logs for progress."})

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

        elif path == "/api/scan-sources":
            # Returns the sources and categories available for live scanning
            data = {
                src: list(cfg["urls"].keys())
                for src, cfg in _SCAN_SOURCES.items()
            }
            self.send_json(data, max_age=3600)

        elif path == "/api/live-scan":
            try:
                source   = params.get("source",   ["heureka"])[0]
                category = params.get("category", ["Smartphones"])[0]
                limit    = min(int(params.get("limit", ["30"])[0]), 50)
                self.send_json(_live_scan(source, category, limit))
            except Exception as e:
                import logging; logging.error(f"/api/live-scan: {e}", exc_info=True)
                self.send_json({"error": str(e), "products": []})

        elif path == "/api/me":
            try:
                from scraper.auth import get_user_by_token
                token = (self.headers.get("Authorization", "") or "").removeprefix("Bearer ").strip()
                user = get_user_by_token(token)
                if user:
                    self.send_json({"ok": True, "user": user})
                else:
                    self.send_json({"ok": False, "error": "Invalid or expired token."}, status=401)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, status=500)

        elif path == "/api/contrib-stats":
            try:
                from scraper.auth import get_contrib_stats
                self.send_json(get_contrib_stats(), max_age=120)
            except Exception as e:
                self.send_json({"error": str(e)}, status=500)

        elif path == "/api/my-sources":
            try:
                from scraper.auth import get_user_by_token, COUNTRY_SOURCES
                token = (self.headers.get("Authorization", "") or "").removeprefix("Bearer ").strip()
                user = get_user_by_token(token)
                if not user:
                    self.send_json({"ok": False, "error": "Unauthorised."}, status=401)
                    return
                country = user.get("country", "CZ")
                self.send_json({"ok": True, "country": country,
                                "sources": COUNTRY_SOURCES.get(country, [])})
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, status=500)

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

    def do_OPTIONS(self):
        """Handle CORS preflight requests."""
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path

        # Read JSON body
        try:
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length)) if length > 0 else {}
        except Exception:
            self.send_json({"ok": False, "error": "Invalid JSON body."}, status=400)
            return

        if path == "/api/register":
            try:
                from scraper.auth import register_user
                result = register_user(
                    email    = body.get("email", ""),
                    password = body.get("password", ""),
                    country  = body.get("country", "CZ"),
                    q1       = body.get("q1", ""),
                    q2       = body.get("q2", ""),
                    q3       = body.get("q3", ""),
                )
                self.send_json(result, status=200 if result["ok"] else 400)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, status=500)

        elif path == "/api/login":
            try:
                from scraper.auth import login_user
                result = login_user(
                    email    = body.get("email", ""),
                    password = body.get("password", ""),
                )
                self.send_json(result, status=200 if result["ok"] else 401)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, status=500)

        elif path == "/api/request-code":
            try:
                from scraper.auth import request_verification_code
                result = request_verification_code(body.get("email", ""))
                self.send_json(result, status=200 if result["ok"] else 400)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, status=500)

        elif path == "/api/verify-code":
            try:
                from scraper.auth import verify_code_and_login
                result = verify_code_and_login(
                    email = body.get("email", ""),
                    code  = body.get("code", ""),
                )
                self.send_json(result, status=200 if result["ok"] else 400)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, status=500)

        elif path == "/api/complete-profile":
            try:
                from scraper.auth import complete_profile
                result = complete_profile(
                    setup_token = body.get("setup_token", ""),
                    country     = body.get("country", ""),
                    q1          = body.get("q1", ""),
                    q2          = body.get("q2", ""),
                    q3          = body.get("q3", ""),
                )
                self.send_json(result, status=200 if result["ok"] else 400)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, status=500)

        elif path == "/api/google-auth":
            try:
                from scraper.auth import google_auth
                result = google_auth(
                    email     = body.get("email", ""),
                    google_id = body.get("google_id", ""),
                    name      = body.get("name", ""),
                )
                self.send_json(result, status=200 if result["ok"] else 400)
            except Exception as e:
                self.send_json({"ok": False, "error": str(e)}, status=500)

        elif path == "/api/contribute":
            try:
                from scraper.auth import get_user_by_token, record_contribution
                token = (self.headers.get("Authorization", "") or "").removeprefix("Bearer ").strip()
                user = get_user_by_token(token)
                if not user:
                    self.send_json({"ok": False, "error": "Unauthorised."}, status=401)
                    return
                source   = body.get("source", "")
                products = body.get("products", [])
                if not isinstance(products, list):
                    self.send_json({"ok": False, "error": "products must be a list."}, status=400)
                    return
                result = record_contribution(user["id"], source, products)
                self.send_json({"ok": True, **result})
            except Exception as e:
                import logging; logging.error(f"/api/contribute: {e}", exc_info=True)
                self.send_json({"ok": False, "error": str(e)}, status=500)

        elif path == "/api/ingest":
            # ── External scraper ingest ───────────────────────────────────────
            # Accepts the same JSON format as ikor-auth.fly.dev/api/ingest so
            # the data-pipeline scrapers (datart, planeo, heureka) can POST here.
            #
            # Request body:
            #   { "source": "planeo.cz", "category": "smartphone",
            #     "products": [ {"Name":…, "ProductURL":…, "Price_CZK":…,
            #                    "RecommendRate_pct":…, "ReviewsCount":…,
            #                    "image_url":…} ],
            #     "url": "https://…" }
            # Auth header:  X-Scraper-Key: <SCRAPER_KEY env var>
            try:
                expected_key = os.environ.get("SCRAPER_KEY", "")
                if expected_key:
                    given_key = self.headers.get("X-Scraper-Key", "")
                    if given_key != expected_key:
                        self.send_json({"ok": False, "error": "Unauthorised."}, status=401)
                        return

                source   = body.get("source", "")
                category = body.get("category", "")
                products = body.get("products", [])
                if not isinstance(products, list) or not source:
                    self.send_json({"ok": False, "error": "source and products required."}, status=400)
                    return

                # Derive country from source domain (planeo.cz→CZ, heureka.cz→CZ, etc.)
                _DOMAIN_COUNTRY = {
                    "planeo.cz": "CZ", "datart.cz": "CZ", "heureka.cz": "CZ",
                    "alza.cz": "CZ", "zbozi.cz": "CZ", "mall.cz": "CZ",
                    "heureka.sk": "SK", "alza.sk": "SK",
                    "coolblue.nl": "NL", "coolblue.be": "BE",
                }
                country = _DOMAIN_COUNTRY.get(source, "CZ")

                conn  = open_db()
                added = 0
                try:
                    from scraper.snapshots import ensure_snapshot_table, record_snapshot
                    ensure_snapshot_table(conn)
                except Exception:
                    pass

                for p in products:
                    name = (p.get("Name") or "").strip()
                    url  = (p.get("ProductURL") or "").strip()
                    if not name or not url:
                        continue
                    try:
                        conn.execute(
                            """INSERT OR IGNORE INTO products
                               (Name, Category, ProductURL, Price_CZK,
                                RecommendRate_pct, ReviewsCount,
                                image_url, source, country, currency, scraped_at)
                               VALUES (?,?,?,?,?,?,?,?,?,?,datetime('now'))""",
                            (
                                name, category, url,
                                p.get("Price_CZK"),
                                p.get("RecommendRate_pct"),
                                p.get("ReviewsCount") or 0,
                                p.get("image_url") or p.get("ImageURL"),
                                source, country, "CZK",
                            )
                        )
                        if conn.execute("SELECT changes()").fetchone()[0]:
                            added += 1
                        try:
                            record_snapshot(conn, url, source, p, country=country)
                        except Exception:
                            pass
                    except Exception:
                        pass

                conn.commit()
                conn.close()
                _invalidate_html_cache()
                import logging; logging.info(f"/api/ingest: {source}/{category} — {added} new, {len(products)-added} dupes")
                self.send_json({"ok": True, "queued": added, "total": len(products)})
            except Exception as e:
                import logging; logging.error(f"/api/ingest: {e}", exc_info=True)
                self.send_json({"ok": False, "error": str(e)}, status=500)

        else:
            self.send_json({"error": "Not found."}, status=404)


if __name__ == "__main__":
    # ── Pre-start: recompute ranks if stale (runs BEFORE HTTP server binds) ───
    # This guarantees exclusive DB access with no reader contention.
    # With synchronous=OFF the 30k-row recompute takes ~20-30 s — within
    # Fly.io's default 5-minute machine-start timeout.
    if os.path.exists(DB_PATH):
        try:
            _pre = __import__("sqlite3").connect(DB_PATH, timeout=60)
            _pre.execute("PRAGMA synchronous=OFF")
            _pre.execute("PRAGMA cache_size=-64000")
            _pre.execute("PRAGMA temp_store=MEMORY")
            _ranked_pre = _pre.execute(
                "SELECT COUNT(*) FROM products WHERE cat_rank IS NOT NULL"
            ).fetchone()[0]
            if _ranked_pre < 20000:
                import time as _time
                _t0 = _time.monotonic()
                print(f"[pre-start] {_ranked_pre} products ranked — recomputing with Wilson score …", flush=True)
                # Wilson score lower bound (95% CI) — penalises low review counts so that
                # e.g. 1500 reviews @ 95% ranks above 10 reviews @ 100%.
                # products.id is NOT a primary key (37k rows have id=NULL); use rowid.
                def _wilson_pre(p, n, z=1.96):
                    if not n or n <= 0: return p * 0.5
                    return (p + z*z/(2*n) - z*((p*(1-p) + z*z/(4*n))/n)**0.5) / (1 + z*z/n)
                _rows = _pre.execute("""
                    SELECT rowid, NormalizedCategory, RecommendRate_pct, AvgStarRating, ReviewsCount
                    FROM products
                    WHERE source NOT IN ('dtest','warentest')
                      AND NormalizedCategory IS NOT NULL AND NormalizedCategory != ''
                      AND (RecommendRate_pct IS NOT NULL OR AvgStarRating IS NOT NULL)
                """).fetchall()
                from collections import defaultdict as _pre_dd
                _cat_items = _pre_dd(list)
                for (_rid, _cat, _rec, _stars, _n_rev) in _rows:
                    _n = int(_n_rev) if _n_rev else 0
                    if _rec is not None:
                        _score = _wilson_pre(float(_rec) / 100.0, _n) * 100.0
                    else:
                        _score = _wilson_pre(float(_stars) / 5.0, _n) * 100.0
                    _cat_items[_cat].append((_score, _rid))
                _updates = []
                for _cat, _items in _cat_items.items():
                    _items.sort(key=lambda x: x[0], reverse=True)
                    _n_cat = len(_items)
                    for _rank_idx, (_, _rid) in enumerate(_items, 1):
                        _updates.append((_rank_idx, _n_cat, _rid))
                print(f"[pre-start]   {len(_updates):,} products scored — writing to DB …", flush=True)
                _pre.isolation_level = None  # manual transaction control
                _pre.execute("BEGIN EXCLUSIVE")
                _pre.execute("UPDATE products SET cat_rank=NULL, cat_total=NULL WHERE cat_rank IS NOT NULL OR cat_total IS NOT NULL")
                for _i in range(0, len(_updates), 5000):
                    _pre.executemany(
                        "UPDATE products SET cat_rank=?, cat_total=? WHERE rowid=?",
                        _updates[_i:_i+5000]
                    )
                _pre.execute("DROP INDEX IF EXISTS idx_norm_cat_rank")
                _pre.execute("CREATE INDEX idx_norm_cat_rank ON products(NormalizedCategory, cat_rank)")
                _pre.execute("COMMIT")
                _ranked_post = _pre.execute(
                    "SELECT COUNT(*) FROM products WHERE cat_rank IS NOT NULL"
                ).fetchone()[0]
                _elapsed = _time.monotonic() - _t0
                print(f"[pre-start] Ranks done — {_ranked_post:,} products ranked in {_elapsed:.1f}s", flush=True)
            else:
                print(f"[pre-start] Ranks OK ({_ranked_pre:,})", flush=True)
            _pre.close()
        except Exception as _pre_e:
            print(f"[pre-start] Rank recompute skipped: {_pre_e}", flush=True)

    # ── One-time price corrections (idempotent guards prevent re-running) ──────
    if os.path.exists(DB_PATH):
        try:
            _pc = __import__("sqlite3").connect(DB_PATH, timeout=30)
            # Otto scraper had a bug: parse_eur("689.00") stripped the period
            # → stored 68900.0 instead of 689.0.  Fix: divide by 100 when any
            # Otto price exceeds a plausible maximum (€50 000 for a luxury item).
            _otto_max = _pc.execute(
                "SELECT MAX(Price_EUR) FROM products "
                "WHERE source IN ('otto','otto_de') AND Price_EUR IS NOT NULL"
            ).fetchone()[0]
            if _otto_max and _otto_max > 50000:
                _pc.execute(
                    "UPDATE products SET Price_EUR = ROUND(Price_EUR / 100.0, 2) "
                    "WHERE source IN ('otto','otto_de') AND Price_EUR IS NOT NULL"
                )
                _pc.commit()
                print(f"[pre-start] Corrected Otto prices (max was {_otto_max:.0f} → now ÷100)", flush=True)
            # MediaMarkt scraper had a bug: parse_eur(599) divided int by 100
            # → stored 5.99 instead of 599.  Fix: multiply by 100 when max is
            # suspiciously small (< €100 for a category that includes TVs etc.)
            # Guard: prices < €50 are int-div-100 artifacts (real products start at ~€99)
            _mm_bad = _pc.execute(
                "SELECT MAX(Price_EUR) FROM products "
                "WHERE source IN ('mediamarkt','mediamarkt_de') AND Price_EUR IS NOT NULL "
                "AND Price_EUR < 50"
            ).fetchone()[0]
            if _mm_bad:
                _pc.execute(
                    "UPDATE products SET Price_EUR = ROUND(Price_EUR * 100.0, 2) "
                    "WHERE source IN ('mediamarkt','mediamarkt_de') AND Price_EUR IS NOT NULL"
                )
                _pc.commit()
                print(f"[pre-start] Corrected MediaMarkt prices (max bad was {_mm_bad:.2f} → now ×100)", flush=True)
            _pc.close()
        except Exception as _pc_e:
            print(f"[pre-start] Price correction skipped: {_pc_e}", flush=True)

    # ── Bind HTTP port so Fly.io health checks pass ────────────────────────────
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"   # listen on all interfaces (required for cloud hosting)

    server = HTTPServer((host, port), Handler)
    print(f"✦ QualityDB bound on port {port} — initialising in background…", flush=True)

    def _background_init():
        """Run all startup tasks that must not block the HTTP server from starting."""
        # 1. Schema migration + indexes (handles old DBs missing columns)
        # Note: we do NOT auto-create an empty stub here. If no DB exists yet
        # (fresh volume), requests fail gracefully until products.db is uploaded
        # via: fly sftp shell -a database-of-high-quality-products
        if not os.path.exists(DB_PATH):
            print(f"[init] No DB at {DB_PATH} — waiting for upload.", flush=True)
            return  # nothing more to do; server still accepts HTTP requests

        # 2. Schema migration + indexes (handles old DBs missing columns)
        try:
            _conn = open_db()
            ensure_indexes(_conn)
            _conn.close()
            print("[init] Indexes OK", flush=True)
        except Exception as _e:
            print(f"[init] ensure_indexes failed: {_e}", flush=True)

        # 3. Normalize categories for any new products
        try:
            from scraper.normalize_categories import run_normalization as _norm
            _norm_conn = open_db()
            _n = _norm(_norm_conn)
            _norm_conn.close()
            if _n:
                print(f"[init] Normalized {_n} new products", flush=True)
        except Exception as _e:
            print(f"[init] Category normalization skipped: {_e}", flush=True)

        # 3a. Re-normalize any products whose NormalizedCategory still contains
        #     non-ASCII chars (untranslated Czech/Polish/German names from before
        #     new EXACT entries were added). Runs quickly via inline SQL.
        try:
            from scraper.normalize_categories import run_normalization as _renorm
            _rc = open_db()
            # Clear NormalizedCategory for products still using known foreign-language names
            _rc.execute("""
                UPDATE products SET NormalizedCategory = NULL, NormalizedMainGroup = NULL
                WHERE NormalizedCategory IS NOT NULL
                  AND NormalizedCategory != ''
                  AND NormalizedCategory IN (
                    'Głośniki przenośne','Głośniki','Malé spotřebiče','Malé domácí spotřebiče',
                    'Domácí spotřebiče','Mixéry a roboty','Varné konvice','Toustovače',
                    'Fény a stylingové přístroje','Žehličky','Ventilátory','Závodní příslušenství',
                    'Kaffeevollautomaten','Hudební nástroje','Hudební příslušenství',
                    'Zvuk a hudba','Rádia a Hi-Fi','Zvukové karty','Sítě a konektivita',
                    'Anténní příslušenství','Ostatní příslušenství','Brýle na počítač',
                    'Ostatní spotřebiče','Dětské autosedačky','Opalovací krémy',
                    'Dětské kočárky','Holicí strojky','Dentální hygiena',
                    'Baterie a nabíječky','Zdravotnické pomůcky','Běhání a atletika',
                    'Grilování','Příslušenství'
                  )
            """)
            _rc.commit()
            _cleared_nc = _rc.execute("SELECT changes()").fetchone()[0]
            if _cleared_nc:
                print(f"[init] Cleared {_cleared_nc} stale foreign NormalizedCategory values for re-norm", flush=True)
                _renorm(_rc)
            # Fix ceneo camera products misclassified as TVs (Category "Aparaty fotograficzne"
            # is in MainCategory "TV i foto" which triggers the "tv" keyword rule)
            try:
                _rc.execute("""
                    UPDATE products SET NormalizedCategory = NULL, NormalizedMainGroup = NULL
                    WHERE source = 'ceneo'
                      AND Category IN (
                        'Aparaty fotograficzne', 'Cyfrowe aparaty fotograficzne',
                        'Lustrzanki i hybrydowe'
                      )
                      AND NormalizedCategory = 'TVs'
                """)
                _rc.commit()
                _cam_fixed = _rc.execute("SELECT changes()").fetchone()[0]
                if _cam_fixed:
                    _renorm(_rc)
                    print(f"[init] Fixed {_cam_fixed} ceneo cameras misclassified as TVs", flush=True)
            except Exception as _ce:
                print(f"[init] Ceneo camera fix skipped: {_ce}", flush=True)

            # Fix Grafikkarten (German GPU name from saturn_de) → Graphics Cards
            _rc.execute("""
                UPDATE products SET NormalizedCategory = NULL, NormalizedMainGroup = NULL
                WHERE NormalizedCategory = 'Grafikkarten'
            """)
            _rc.commit()
            _gk_cleared = _rc.execute("SELECT changes()").fetchone()[0]
            if _gk_cleared:
                print(f"[init] Cleared {_gk_cleared} Grafikkarten rows for re-norm", flush=True)
                _renorm(_rc)

            # Fix Gaming-Headsets hyphen → Gaming Headsets (no hyphen)
            _rc.execute(
                "UPDATE products SET NormalizedCategory = 'Gaming Headsets' "
                "WHERE NormalizedCategory = 'Gaming-Headsets'"
            )
            _rc.commit()

            # Clear all non-standard NormalizedCategory values in the 'Other' main group
            # (raw Czech/German product attribute strings that should map to real categories)
            _VALID_OTHER_CATS = frozenset({
                'Software', 'Sports & Outdoor', 'Other', 'Cables & Hubs', 'Office Supplies',
            })
            _rc.execute(
                "UPDATE products SET NormalizedCategory = NULL, NormalizedMainGroup = NULL "
                "WHERE NormalizedMainGroup = 'Other' "
                "  AND NormalizedCategory IS NOT NULL AND NormalizedCategory != '' "
                "  AND NormalizedCategory NOT IN ("
                + ",".join(f"'{v}'" for v in _VALID_OTHER_CATS) + ")"
            )
            _rc.commit()
            _other_cleared = _rc.execute("SELECT changes()").fetchone()[0]
            if _other_cleared:
                print(f"[init] Cleared {_other_cleared} Other-group raw categories for re-norm", flush=True)
                _renorm(_rc)

            # Fix Czech NormalizedMainGroup values that weren't corrected by normalization
            _MAIN_GROUP_FIX = {
                'Ostatní': 'Other', 'Herní technika': 'Gaming',
                'PC komponenty': 'Computers', 'Průmyslové zboží': 'Other',
                'Velké domácí spotřebiče': 'Home Appliances',
                'Malé domácí spotřebiče': 'Home Appliances',
                'Domácí spotřebiče': 'Home Appliances',
                'Zvuk a hudba': 'Audio', 'Sítě a konektivita': 'Networking',
                # Czech group names that fall through as NormalizedMainGroup
                'Bytové vybavení': 'Home & Garden',
                'Cestování': 'Accessories',
                'Dětské zboží': 'Baby & Kids',
                'Foto a video': 'Cameras',
                'Sport': 'Sports & Outdoor',
                'Sport a kola': 'Sports & Outdoor',
                'Zahrada a dílna': 'Garden & Outdoors',
                'Zdraví a hygiena': 'Health & Beauty',
            }
            _mg_fixed = 0
            for _old, _new in _MAIN_GROUP_FIX.items():
                _rc.execute(
                    "UPDATE products SET NormalizedMainGroup = ? WHERE NormalizedMainGroup = ?",
                    (_new, _old)
                )
                _mg_fixed += _rc.execute("SELECT changes()").fetchone()[0]
            if _mg_fixed:
                _rc.commit()
                print(f"[init] Fixed {_mg_fixed} Czech NormalizedMainGroup values", flush=True)

            # Fix NormalizedMainGroup consistency for known categories
            _rc.execute("""
                UPDATE products SET NormalizedMainGroup = 'Sports & Outdoor'
                WHERE NormalizedCategory IN (
                    'Sports & Outdoor', 'Running & Athletics', 'Cycling',
                    'E-Bikes', 'E-Scooters', 'Outdoor & Hiking', 'Water Sports',
                    'Fitness Equipment', 'Treadmills'
                ) AND (NormalizedMainGroup = 'Other' OR NormalizedMainGroup = 'Sport'
                        OR NormalizedMainGroup = 'Sport a kola')
            """)
            _rc.execute("""
                UPDATE products SET
                    NormalizedCategory = 'Cables & Accessories',
                    NormalizedMainGroup = 'Accessories'
                WHERE NormalizedCategory = 'Cables & Hubs'
            """)
            _rc.commit()

            # Refine broad 'Smart Home' category into subcategories by product name
            # (these sources use 'Smart Home' as a catch-all Category)
            _smart_home_rules = [
                ("Smart Lighting", (
                    "%philips hue%", "%smart bulb%", "%smart light%", "%led strip%",
                    "%smart lamp%", "%tradfri%", "%osram smart%", "%tint smarte led%",
                    "%smarte led%", "%zigbee lampe%", "%smart+ led%",
                )),
                ("Smart Thermostats", (
                    "%thermostat%", "%heizkörperthermostat%", "%wandthermostat%",
                    "%raumthermostat%", "%tado%", "%heatmiser%",
                )),
                ("Smart Cameras", (
                    "%überwachungskamera%", "%security camera%", "%smart camera%",
                    "%ring stick up%", "%ring außenkamera%", "%videotürklingel%",
                    "%door view cam%", "%indoor kamera%", "%wlan kamera%",
                    "%hundekamera%",
                    "%ezviz%",              # EZVIZ = Hikvision consumer security camera brand
                )),
                ("Smart Speakers", (
                    "%echo dot%", "%echo show%", "% echo (%", "%alexa%",
                    "%google home%", "%google nest%", "%homepod%",
                    "%sonos play%", "%sonos beam%", "%musiccast%",
                )),
                ("Smart Plugs", (
                    "%smart plug%", "%steckdose%", "%wifi steckdose%",
                    "%wlan steckdose%", "%wifi switch%", "%smart socket%",
                    "%schalt-mess-aktor%", "%schaltsteckdose%",
                )),
            ]
            _sh_updated = 0
            for _sh_cat, _patterns in _smart_home_rules:
                for _pat in _patterns:
                    _rc.execute(
                        f"UPDATE products SET NormalizedCategory = ?, NormalizedMainGroup = 'Smart Home' "
                        f"WHERE NormalizedCategory = 'Smart Home' "
                        f"  AND LOWER(Name) LIKE ?",
                        (_sh_cat, _pat)
                    )
                    _sh_updated += _rc.execute("SELECT changes()").fetchone()[0]
            if _sh_updated:
                _rc.commit()
                print(f"[init] Refined {_sh_updated} Smart Home products into subcategories", flush=True)

            # Fix: cameras that were incorrectly classified as Smart Speakers
            # (happened because %alexa% matched before %überwachungskamera% could run)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Smart Cameras', NormalizedMainGroup = 'Smart Home'
                WHERE NormalizedCategory = 'Smart Speakers'
                  AND (LOWER(Name) LIKE '%überwachungskamera%'
                    OR LOWER(Name) LIKE '%wlan kamera%'
                    OR LOWER(Name) LIKE '%ip kamera%'
                    OR LOWER(Name) LIKE '%ip camera%'
                    OR LOWER(Name) LIKE '%security cam%'
                    OR LOWER(Name) LIKE '%kamera außen%'
                    OR LOWER(Name) LIKE '%außenkamera%'
                    OR LOWER(Name) LIKE '%indoor kamera%'
                    OR LOWER(Name) LIKE '%outdoor kamera%')
            """)
            _rc.commit()
            _cam_fix2 = _rc.execute("SELECT changes()").fetchone()[0]
            if _cam_fix2:
                print(f"[init] Moved {_cam_fix2} cameras from Smart Speakers → Smart Cameras", flush=True)

            # Refine "Other Appliances" into specific subcategories by product name
            # These Czech products have Category "Ostatní spotřebiče" (catch-all)
            _other_app_rules = [
                ("Hair Dryers", (
                    "%hair dry%", "%haartrockner%", "% bhd%", "%rowenta cv%",
                    "%rowenta cf%", "%rowenta hp%", "% cv58%", "% cv57%",
                    "% cv56%", "% cv55%", "%studio dry%", "%studio silence%",
                    "% phd%", "%pro300%", "%pro500%",
                    "%laifen%",           # Laifen hair dryer brand
                    "%siguro hr%",        # Siguro HR = hair styler/dryer
                    "%vysoušeč vlasů%",   # Czech: hair dryer (vysousec vlasu)
                    "%concept vv%",       # Concept VV = vlasový vysoušeč (hair dryer)
                    "%rowenta x%",        # Rowenta X Karl Lagerfeld etc. = hair tools
                    "%rowenta%express%",  # Rowenta Express Style (CV model, brand+series+model)
                    "%babyliss%",         # BaByliss — hair dryers, straighteners, curlers
                    "%remington%",        # Remington — hair styling tools
                    "%valera%",           # Valera — Swiss hair dryer brand
                    "%dyson supersonic%", # Dyson Supersonic = hair dryer (not vacuum)
                    "%philips%bhc%",      # Philips BHC = hair care (BHC010 EssentialCare etc.)
                    "%salente%",          # Salente — Czech hair styling brand
                    "%braun hd%",         # Braun HD = hair dryer series (HD435, HD785 SensoDryer)
                    "%bellissima%",       # Bellissima = Italian hair styling brand
                    "%clatronic ht%",     # Clatronic HT = travel hair dryer
                    "%clatronic htd%",    # Clatronic HTD = hair dryer
                    "%proficare ht%",     # ProfiCare HT = hair dryer
                    "%shark%flexstyle%",  # Shark FlexStyle = 5-in-1 hair styling tool
                )),
                ("Kettles", (
                    "% twk%", "% ek-%", "%siguro ek%", "%tefal ko%",
                    "%rýchlovarná kanvica%", "%rychlovarná konvice%",
                    "%wasserkocher%", "%bouilloire%",
                    "%tefal ki5%", "%tefal ki4%", "%tefal ki3%",
                    "%sencor swk%",       # Sencor SWK = kettle series
                    "concept rk%",        # Concept RK = rychlovarná konvice
                    "%concept rk0%", "%concept rk1%", "%concept rk2%",
                    "%concept rk3%", "%concept rk4%", "%concept rk5%",
                    "%concept lk%",       # Concept LK = electric kettle (LK series)
                    "%russell hobbs%",    # Russell Hobbs makes many kettles
                    "%ecg rk%",           # ECG RK = electric kettles
                    "%severin wk%",       # Severin WK = water kettles
                    "%smeg%1,7l%",        # SMEG 50's Retro Style 1.7l = kettle
                    "%kitchen aid 5kek%", # KitchenAid 5KEK = electric kettle
                    "%varná konvice%",    # Czech: electric kettle (broader than rychlovarná)
                    "%braun purshine%",   # Braun PurShine WK = kettle
                    "%braun%wk1%",        # Braun WK1xxx = kettle series
                    "%aeno ek%",          # AENO EK series = electric kettles
                    "%tefal ki%",         # Tefal KI = glass kettle series (KI605, KI883 etc.)
                    "%sage ske%",         # Sage SKE = Smart Kettle series
                    "%sage bke%",         # Sage BKE = brushed kettle series
                    "%kitchenaid%kek%",   # KitchenAid 5KEK = electric kettle (no-space brand)
                    "%tesla kt%",         # Tesla KT = electric kettle
                    "%noaton k%",         # Noaton K1W/K2W = electric kettle brand
                    "%wmf%lono%",         # WMF LONO = electric kettle
                    "%zwilling%",         # Zwilling ENFINIGY = premium kitchen tech (mostly kettles)
                    "%keramická konvice%", # Czech: ceramic kettle
                    "%orava%konvice%",    # Orava ceramic kettle
                    "%eta crystal%",      # ETA Crystal = electric kettle series
                    "%tefal%ki8%",        # Tefal KI8 = Majestuo glass kettle
                    "%home hm-ek%",       # HOME HM-EK = electric kettle
                )),
                ("Coffee Machines", (
                    "%ecam%", "% ea9%", "% ea87%", "% ea81%",
                    "%kaffeevollautomat%", "%espresso%", "%barista%",
                    "%de'longhi%", "%delonghi%",
                    "%jura e%", "%jura s%", "%jura j%", "%jura d%",  # JURA coffee machines
                    "%jura z%",           # JURA Z-series = premium coffee machines
                    "%bosch tas%",        # Bosch Tassimo
                    "%severin ka%",       # Severin KA = coffee machine
                    "%ecg kp%",           # ECG KP = capsule coffee machine
                    "%ecg kg%",           # ECG KG = coffee grinder/machine
                    "%siemens eq%", "%siemens tp%",   # Siemens EQ/TP = coffee machines
                    "%siemens tf%",       # Siemens TF = EQ300/500/700 bean-to-cup
                    "%philips ep%", "%philips hd8%",  # Philips EP/HD8 = espresso machines
                    "%philips%ep22%", "%philips%ep29%",  # Philips EP2200/EP2900 (series in name)
                    "%philips saeco%",    # Philips Saeco = espresso brand
                    "%philips hd74%",     # Philips HD74xx = drip coffee machines
                    "%philips series 2200%", "%philips series 3300%",  # Philips coffee series
                    "%lattego%",          # Philips LatteGo = automatic coffee machine
                    "%philips series 5%", "%philips series 4%",  # Philips Series 4/5k = coffee machines
                    "%sage ses%",         # Sage SES = espresso machine (Barista Express etc.)
                    "%sage sme%",         # Sage SME = espresso machines
                    "%sage stm%",         # Sage STM = steamer/milk frother
                    "%tefal cm%",         # Tefal CM = coffee machines (Sense, Smart & Light)
                    "%tchibo%",           # Tchibo = coffee machines brand
                    "%moccamaster%",      # Moccamaster KBG = premium drip coffee machine
                    "%espro%press%",      # ESPRO Press = French press/travel coffee press
                    "%krups ea%",         # Krups EA = bean-to-cup espresso machines
                    "%krups xp%",         # Krups XP = pump espresso machines
                    "%miele cm%",         # Miele CM = built-in/standalone coffee machines
                    "%ecg forza%",        # ECG Forza = pour-over coffee machine
                    "%tesla coffeemaster%", # Tesla CoffeeMaster = espresso machine
                    "%bosch tka%",        # Bosch TKA = drip coffee machine (MyMoments etc.)
                    "%smeg%1,4l%",        # SMEG 50's Retro Style 1.4l = 10-cup drip coffee machine
                    "%sencor scc%",       # Sencor SCC = capsule coffee machine (Nespresso-compatible)
                    "%sencor sce%",       # Sencor SCE = espresso machine
                    "%nedis kacm%",       # Nedis KACM = coffee machine series
                    "%eta%barello%",      # ETA Barello = espresso/coffee machine series
                )),
                ("Vacuum Cleaners", (
                    "% fc9%", "% fc8%", "% fc6%", "%powerpro%",
                    "%speedpro%", "%powermax%",
                    "%concept vp%",       # Concept VP = vacuum cleaner series
                    "%sencor svc%",       # Sencor SVC = vacuum cleaner
                    "%amica vc%", "%amica vt%", "%amica vd%",  # AMICA vacuum series
                    "%hyundai vc%",       # Hyundai VC = vacuum cleaner
                    "%ecg vv%",           # ECG VV = vysavač válcový (cylinder vac)
                    "%electrolux euoc%",  # Electrolux UltraOne = vacuum
                    "%electrolux eq%",    # Electrolux EQ = vacuum cleaner
                    "%kärcher wd%", "%karcher wd%",  # Kärcher WD = wet & dry vacuum
                    "%eta stormy%",       # ETA Stormy = vacuum cleaner series
                    # Rowenta cordless vacuums (RH = all Rowenta vacuum models)
                    "%rowenta rh%",
                    # Bosch cordless/handheld vacuums (BCS/BBS/BBHF/BHN/BSS = Unlimited/Readyy/Move)
                    "%bosch bcs%", "%bosch bbs%", "%bosch bbh%",
                    "%bosch bhn%", "%bosch bss%",
                    # ETA Czech cordless vacuums (Fenix, Fenité, Moneto, Crystela, Avanto etc.)
                    "%eta fenix%", "%eta fenit%", "%eta moneto%",
                    "%eta%avanto%", "%eta%crystela%", "%eta emma%",
                    "%eta holiday%", "%eta%adela%",
                    # Electrolux cordless vacuums (Pure, UltraOne, Explore, Ease, Ergorapido)
                    "%electrolux pure d%", "%electrolux ultraone%",
                    "%electrolux explore%", "%electrolux ease c%",
                    "%electrolux ergorapido%", "%electrolux%eerc%",
                    # Dyson cordless vacuums (V-series: V7/V8/V10/V11/V12/V15)
                    "%dyson v7%", "%dyson v8%", "%dyson v10%",
                    "%dyson v11%", "%dyson v12%", "%dyson v15%",
                    # Philips PowerPro/SpeedPro robots with XC prefix
                    "%philips%xc8%", "%philips%xc7%",
                    # Hoover H-Free cordless (HF prefix = all Hoover H-Free models)
                    "%hoover hf%",
                    # Bosch professional wet/dry vacuums (GAS series) + UniversalVac
                    "%bosch gas%", "%bosch universalvac%",
                    # Bissell CrossWave = wet/dry cordless floor cleaner (vacuum category)
                    "%bissell%",
                    # Deerma = Chinese cordless vacuum brand
                    "%deerma%",
                    # Miele CX1 Blizzard = cylinder vacuum
                    "%miele blizzard%",
                    # ETA cordless/robotic vacuums (Rover = robot, Verto = cordless)
                    "%eta rover%", "%eta verto%",
                    # Electrolux Pure/PC series vacuums (PC91 etc. missed by earlier patterns)
                    "%electrolux%pc9%", "%electrolux%pc8%",
                    "%eta aquasim%",      # ETA Aquasim = wet-dry vacuum / steam cleaner
                    # Lauben = Czech brand, mostly cordless vacuum cleaners
                    "%lauben%",
                    # Kärcher AD = dry vacuum cleaner
                    "%kärcher ad%", "%karcher ad%",
                    # Orava VK = Czech brand vacuum cleaner (VK = vysavač kabinový)
                    "%orava vk%",
                    # Hyundai VK = vacuum cleaner
                    "%hyundai vk%",
                    # Electrolux Ease/Explore/Perform = cordless vacuum model ranges
                    "%electrolux%eb6%",   # Electrolux EB6 = Ease C6 cordless vacuum
                    "%electrolux%el6%",   # Electrolux EL6 = Explore 6 cordless vacuum
                    "%electrolux%es3%",   # Electrolux ES3 = Explore 3/Ease C3 vacuum
                    # Orava VY = Czech brand cordless vacuum (VY = vysavač)
                    "%orava vy%",
                    # Hoover HOCT = steam-and-vacuum cleaner
                    "%hoover hoct%",
                )),
                ("Robot Vacuums", (
                    "%roborock%", "%roomba%", "%irobot%",
                    "%dreame%", "%ecovacs%", "%deebot%",
                    "%evolveo%robo%",     # EVOLVEO RoboTrex = robot vacuum
                    "%evolveo%trex%",
                    "%concept lr%",       # Concept LR = robot vacuum (LR = laserový robotický)
                )),
                ("Washing Machines", (
                    "%ecobubble%", "%addwash%", "%quickdrive%",
                    "%gorenje wpn%", "%gorenje wp%", # Gorenje WPNEI series = washing machines
                    "%whirlpool ffb%", "%whirlpool wbo%",
                    "candy cs4 %", "candy cso%", "candy csow%",
                    "%dualcare%",          # AEG/Electrolux washer-dryer combo
                    "% lwr%",              # AEG LWR prefix = washer-dryer
                    "%hisense wf%",        # Hisense WF = washing machine
                    "%hisense wfx%",       # Hisense WFX = washing machine
                    "%hisense wd%",        # Hisense WD = washer-dryer
                    "%candy cf%",          # Candy CF = washing machine
                    "%candy cbd%",         # Candy CBD = washer-dryer
                    "%candy co4%",         # Candy CO4 = washing machine (RapidÓ)
                    "%candy cow%",         # Candy COW = washer-dryer combo
                    "%candy ro %",         # Candy RO = front-load washing machine
                    "%candy rp %",         # Candy RP = RapidPro washing machine
                    "%candy rpw%",         # Candy RPW = washer with dryer
                    "%candy cbw%",         # Candy CBW = compact/built-in washer
                    "%lg fs%",             # LG FS = front-load washer
                    "%lg fa%",             # LG FA = front-load washer
                    "%lg fblr%",           # LG FBLR = front-load washer
                    "%lg fpsr%",           # LG FPSR = front-load washer
                    "%lg washtower%",      # LG WashTower = combined washer+dryer unit
                    "gorenje w1ng%",       # Gorenje W1NGxx = washing machine
                    "gorenje w1d%",        # Gorenje W1Dxx = washing machine
                    "%gorenje wnh%",       # Gorenje WNHEI SteamTech = washing machine
                    "%gorenje wdsi%",      # Gorenje WDSI = washer-dryer combo
                    "%gorenje w3d%",       # Gorenje W3D = washing machine
                    "%siemens wm%",        # Siemens WM = washer
                    "%bosch wav%", "%bosch wgg%", "%bosch wge%",  # Bosch WAV/WGG = washer
                    "%bosch wan%",         # Bosch WAN = washing machine
                    "%bosch wgh%",         # Bosch WGH = washing machine (Serie 6/8)
                    "%bosch wuu%",         # Bosch WUU = washing machine
                    "%samsung ww%",        # Samsung WW = front-load washer
                    "%samsung wd%",        # Samsung WD = washer-dryer combo
                    "%haier hw%",          # Haier HW = washing machine
                    "%haier tha%",         # Haier THASN = T Series washing machine
                    "%haier%hke%",         # Haier HKE = I-Master Series washer (series name between brand and model)
                    "%haier hcw%",         # Haier HCW = washing machine
                    "%hoover hwp%", "%hoover hw4%", "%hoover hw3%",  # Hoover HW = washer
                    "%hoover h7w%",        # Hoover H7W = H-WASH 700 Slim
                    "%hoover ow5%",        # Hoover OW50 = washing machine
                    "%whirlpool tdlr%",    # Whirlpool TDLR = top-load washer
                    "%whirlpool bi wmwg%", # Whirlpool WMWG = built-in washer
                    "%amica gwas%",        # Amica GWAS = washing machine
                    "%concept la%",        # Concept LA = washing machine (laundry)
                    "%candy ci%",          # Candy CI = washing machine
                    "%candy co %",         # Candy CO = washing machine (RapidÓ series)
                    "%samsung%bespoke%ww%", # Samsung Bespoke WW = washing machine
                    "%lg f2d%",            # LG F2D = washer-dryer combo
                    "%lg fcr%",            # LG FCR = washing machine
                    "%lg flr%",            # LG FLR = washing machine
                    "%hoover h3d%",        # Hoover H3D = washer-dryer
                    "%hoover h5w%",        # Hoover H5W = washing machine
                    "%gorenje gi%",        # Gorenje GI = built-in washing machine
                    "%aeg lfr%",           # AEG LFR = washing machine
                    "%aeg%prosense%",      # AEG ProSense = washing machine (6000 series)
                    "%aeg%absolutecare%",  # AEG AbsoluteCare = washing machine (9000 series)
                    "%aeg%multiswitch%",   # AEG MultiSwitch RMB = washer-dryer combo
                    "%aeg%prosteam%",      # AEG ProSteam = washing machine (4k series)
                    "%aeg%komix%",         # AEG (Ö)KOMix = washer tech; avoids SQLite LOWER non-ASCII bug
                    "%aeg%powercare%",     # AEG PowerCare = washing machine (8k series)
                    "%electrolux eem%",    # Electrolux EEM = washing machine
                    "%sensicare%",         # Electrolux SensiCare = washing machine
                    "%steamcare%",         # Electrolux SteamCare = washing machine
                    "%ultracare%",         # Electrolux UltraCare = washing machine
                    "%perfectcare%",       # Electrolux PerfectCare = washing machine
                    "%siguro wd%",         # Siguro WD = Wash & Dry washer-dryer
                    "%mora cmdn%",         # MORA CMDN = narrow/slim washing machine
                    "%beko wue%",          # Beko WUE = washing machine
                    "%beko b3wf%",         # Beko B3WFU = washing machine
                    "%beko btl%",          # Beko BTL = front-load washing machine
                    # Bosch washer-dryer combos (WNA/WNC/WNG = combined Series 4/6/8)
                    "%bosch wna%", "%bosch wnc%", "%bosch wng%",
                    "%bosch ctl%",         # Bosch CTL = washer-dryer combo unit
                    # Gorenje wider washer patterns
                    "%gorenje w2n%",       # Gorenje W2N = washing machine
                    # CANDY patterns without space between prefix and number
                    "%candy ro1%", "%candy ro14%", # Candy RO1xxx = front-loader
                    "%candy row%",         # Candy ROW = washer-dryer combo
                    "%candy rp4%",         # Candy RP4 = RapidPro washer
                    "%candy he%",          # Candy HE = heat-pump Ultra Hygiene washer
                    "%candy cs %",         # Candy CS = washing machine
                    # Whirlpool FFL/FFF = front-load with Freshcare
                    "%whirlpool ffl%", "%whirlpool fff%",
                    # Hoover THO = H-WASH 500 series washing machine
                    "%hoover tho%",
                    # Toshiba TW = washing machine
                    "%toshiba tw%",
                    # Electrolux numeric-series washers
                    "%electrolux%lxb%",    # Electrolux LXB = washing machine (e.g. "ELECTROLUX 600 LXB2AE82S")
                    "%electrolux%lrt%",    # Electrolux LRT = washing machine (e.g. "ELECTROLUX 700 MultiFlow LRT7ME39X")
                    "%electrolux%ew2%",    # Electrolux EW2 = washing machine (500 TimeCare etc.)
                    "%whirlpool%wdwg%",    # Whirlpool WDWG = washer-dryer combo
                )),
                ("Dishwashers", (
                    "%bosch sms%", "%bosch smv%",      # Bosch SMS/SMV dishwashers
                    "%bosch spv%", "%bosch sps%",      # Bosch SPV/SPS slimline
                    "%bosch smi%",                     # Bosch SMI = semi-integrated dishwasher
                    "%bosch smh%",                     # Bosch SMH = fully integrated dishwasher
                    "%beko bdfn%", "%beko bdin%",      # Beko BDFN/BDIN = dishwasher
                    "%beko bm3%",                      # Beko BM3 = dishwasher
                    "%beko bey%",                      # Beko BEY = dishwasher
                    "%beko din%",                      # Beko DIN = built-in dishwasher
                    "%beko dis%",                      # Beko DIS = dishwasher series
                    "%gorenje gv%",                    # Gorenje GV = dishwasher
                    "%gorenje gs5%", "%gorenje gs6%",  # Gorenje GS50/GS60 = dishwasher
                    "%siguro dw%",                     # Siguro DW = dishwasher
                    "%amica dfv%",                     # Amica DFV = dishwasher
                    "%candy cdp%",                     # Candy CDP = dishwasher
                    "%candy cdih%",                    # Candy CDIH = compact integrated dishwasher
                    "%satelliteclean%",                # Electrolux SatelliteClean = dishwasher
                    "%maxiflex%",                      # Electrolux MaxiFlex = dishwasher
                    "%glasscare%",                     # Electrolux GlassCare = dishwasher
                    "%quickselect%",                   # Electrolux QuickSelect = dishwasher
                    "%electrolux esf%",                # Electrolux ESF = freestanding dishwasher
                    "%electrolux%500 clean%",          # Electrolux 500 Clean series = dishwasher
                    "%aeg master%",                    # AEG Mastery = dishwasher series
                    "%aeg%sprayzone%",                 # AEG SprayZone = dishwasher (8000 series)
                    "aeg 600 %", "aeg 700 %",          # AEG 600/700 = dishwashers (space avoids AEG 6000 washers)
                    "aeg 800 %", "aeg 900 %",          # AEG 800/900 = dishwashers
                    "electrolux 900 %",                # Electrolux 900 SENSE = dishwasher
                    "%siemens sr%",                    # Siemens SR = dishwasher
                    "%siemens sn%",                    # Siemens SN = dishwasher (iQ500 etc.)
                    "%whirlpool wsfo%",                # Whirlpool WSFO = semi-integrated DW
                    "%whirlpool wfo%",                 # Whirlpool WFO = freestanding DW
                    "%whirlpool wbc%",                 # Whirlpool WBC = compact dishwasher
                    "%whirlpool wio%",                 # Whirlpool WIO = fully integrated DW
                    "%supremeclean%",                  # Whirlpool SupremeClean = dishwasher brand
                    "%supreme clean%",                 # Whirlpool Supreme Clean (with space) = dishwasher
                    "%maxispace%",                     # Whirlpool MaxiSpace = dishwasher brand
                    "%whirlpool wi %",                 # Whirlpool WI = semi-integrated dishwasher
                    "%lg db%",                         # LG DB = dishwasher (DB365/DB965/DB976)
                    "%hoover hdp%",                    # Hoover HDP = dishwasher
                    "%bosch smu%",                     # Bosch SMU = semi-integrated dishwasher
                    "%bosch spi%",                     # Bosch SPI = slimline partially integrated
                    # Whirlpool W2x/WH7 series = dishwashers (W2F HD, W2I HD, WH7IA etc.)
                    "%whirlpool w2f%", "%whirlpool w2i%",
                    "%whirlpool wh7%",                 # Whirlpool WH7 = DW with half-door
                    "%whirlpool wsbo%",                # Whirlpool WSBO = dishwasher
                    "%whirlpool wsfc%",                # Whirlpool WSFC = dishwasher
                    "%whirlpool wsic%",                # Whirlpool WSIC = integrated DW
                    "%whirlpool wfc%",                 # Whirlpool WFC = dishwasher (wider range)
                    "%whirlpool wip%",                 # Whirlpool WIP = integrated DW
                    "%mora cb%",                       # MORA CB = myčka built-in dishwasher
                )),
                ("Refrigerators", (
                    "%lg gbp%", "%lg gbb%", "%lg gbv%",  # LG GB* = fridge
                    "%lg gml%",                          # LG GML = InstaView French door fridge
                    "%gorenje nrr%", "%gorenje nrs%",    # Gorenje NRR/NRS = fridge
                    "%gorenje nrk%",                     # Gorenje NRK = fridge-freezer
                    "%gorenje n61%", "%gorenje n62%",    # Gorenje N61x/N62x = freezer/fridge
                    "%gorenje r41%", "%gorenje r42%",    # Gorenje R41x/R42x = fridge
                    "%gorenje k17%",                     # Gorenje K17 = wine cooler
                    "%liebherr ikgn%", "%liebherr ik%",  # Liebherr IK = fridge
                    "%liebherr kpe%", "%liebherr kb%",   # Liebherr KP/KB = fridge
                    "%liebherr kgn%",                    # Liebherr KGN = fridge-freezer
                    "%liebherr ctp%",                    # Liebherr CTP = table-top fridge/freezer
                    "%liebherr tk%",                     # Liebherr TK = table-top cooler
                    "%liebherr rd%",                     # Liebherr Rd = fridge
                    "%hisense rb%", "%hisense rs%",      # Hisense RB/RS = fridge
                    "%hisense rm%",                      # Hisense RM = fridge-freezer
                    "%hisense rl%",                      # Hisense RL = larder fridge
                    "%siemens ki%",                      # Siemens KI = built-in fridge
                    "%siemens kb%",                      # Siemens KB = built-in fridge (iQ500)
                    "%haier h3r%",                       # Haier H3R = fridge
                    "%haier hcr%",                       # Haier HCR = fridge-freezer
                    "%haier hfr%",                       # Haier HFR = French door fridge
                    "%beko bcha%", "%beko b5r%",         # Beko fridge models
                    "%beko bcna%",                       # Beko BCNA = built-in fridge-freezer
                    "%beko bcsa%",                       # Beko BCSA = built-in fridge-freezer
                    "%beko rcsa%",                       # Beko RCSA = fridge-freezer
                    "%beko rssa%",                       # Beko RSSA = fridge-freezer
                    "%gorenje r6%", "%gorenje nrc%",     # Gorenje fridge models
                    "%siguro bf%", "%siguro md%",        # Siguro fridge models
                    "%siguro tt%",                       # Siguro TT-E = Chill & Freeze fridge
                    "%siguro bi%",                       # Siguro BI = built-in fridge
                    "%samsung rb%",                      # Samsung RB = fridge-freezer (incl. Bespoke)
                    "%samsung%bespoke r%",               # Samsung Bespoke RB/RS fridge
                    "%samsung rq%",                      # Samsung RQ = side-by-side fridge
                    "%samsung rs%",                      # Samsung RS = side-by-side fridge
                    "%samsung rf%",                      # Samsung RF = French door fridge
                    "%samsung brb%",                     # Samsung BRB = built-in fridge
                    "%bosch kgn%", "%bosch kge%",        # Bosch KGN/KGE = fridge-freezer
                    "%bosch kin%",                       # Bosch KIN = built-in fridge
                    "%twintech%",                        # Electrolux TwinTech = fridge-freezer
                    "%beko bn%",                         # Beko BN = fridge-freezer
                    "%hisense rq%",                      # Hisense RQ = side-by-side fridge
                    "%hisense rf%",                      # Hisense RF = French door fridge
                    "%whirlpool whc%",                   # Whirlpool WHC = wine cooler
                    "%gorenje onr%",                     # Gorenje ONRK = no-frost fridge
                    "%snaige%",                          # SNAIGE = Lithuanian fridge brand
                    "%haier hsr%",                       # Haier HSR = side-by-side fridge
                    "%lg gsl%",                          # LG GSL = French door fridge
                    "%hoover hhcr%",                     # Hoover HHCR = fridge-freezer
                    # Bosch fridge model codes not covered by KGN/KGE
                    "%bosch kbn%",                       # Bosch KBN = built-in fridge
                    "%bosch kgv%",                       # Bosch KGV = fridge-freezer
                    "%bosch kil%",                       # Bosch KIL = built-in larder fridge
                    "%bosch kis%",                       # Bosch KIS = built-in fridge-freezer
                    "%bosch ksv%",                       # Bosch KSV = upright fridge
                    "%amica fk%",                        # Amica FK = fridge (Kühlschrank)
                    "%amica kgcr%",                      # Amica KGCR = fridge-freezer
                    "%aeg rtb%",                         # AEG RTB = table-top fridge
                    # Gorenje additional fridge/freezer codes
                    "%gorenje nrm%",                     # Gorenje NRM = no-frost fridge
                    "%gorenje rb%",                      # Gorenje RB = fridge-freezer
                    "%gorenje rbi%",                     # Gorenje RBIU = built-in fridge
                    "%gorenje ri%",                      # Gorenje RI = built-in fridge
                    "%gorenje rk4%",                     # Gorenje RK4 = fridge
                    "%gorenje rki%",                     # Gorenje RKI = built-in fridge
                    "%gorenje rp%",                      # Gorenje RP = fridge
                    "%gorenje k15%",                     # Gorenje K15 = wine cooler
                    # TCL fridge models (FF, RC, RF, RP series)
                    "%tcl ff%", "%tcl rc4%", "%tcl rc5%",
                    "%tcl rf%", "%tcl rp%",
                    # Whirlpool American/side-by-side/built-in fridges
                    "%whirlpool sw8%",                   # Whirlpool SW8 = side-by-side fridge
                    "%whirlpool w5 %",                   # Whirlpool W5 = large fridge-freezer
                    "%whirlpool w9%",                    # Whirlpool W9 = XXL fridge-freezer
                    "%whirlpool wrb%", "%whirlpool wrs%", # Whirlpool WRB/WRS = built-in fridges
                    "%whirlpool natis%",                 # Whirlpool Natis = built-in fridge brand
                    "%amica vj%",                        # Amica VJ = chladnička (fridge)
                )),
                ("Dryers", (
                    "%airdry%",        # Electrolux AirDry = heat-pump dryer (EEA series)
                    "%candy ro4%",     # Candy RO4 = condenser dryer
                    "%candy tca%",     # Candy TCA = heat-pump dryer
                    "%aeg sensidry%",  # AEG SensiDry = heat-pump dryer brand
                    "%beko dtc%",      # Beko DTC = tumble dryer (condenser)
                    "%beko htv%",      # Beko HTV = heat-pump tumble dryer
                    "%beko ts%",       # Beko TS/TSE = heat-pump dryer series
                    "%hoover hd %",    # Hoover HD = dryer (H-Dry series)
                    "%hoover hd4%", "%hoover hd5%",  # Hoover HD4/HD5 = heat-pump dryers
                    "%electrolux%dynamicair%",  # Electrolux DynamicAir = dryer series
                )),
                ("Ovens & Stoves", (
                    "%mora im%",            # MORA IM = induction stove/oven
                    "%mora sm%",            # MORA SM = sporák multifunkční (multifunction oven)
                    "%mora vm%",            # MORA VM = vestavná multifunkční trouba (built-in oven)
                    "%candy ccg%",          # Candy CCG = gas cooker (plynový sporák)
                    "%candy cch%",          # Candy CCH = ceramic hob
                    "%candy cc%",           # Candy CC = cooker
                )),
                ("Microwaves", (
                    "%amica mv%", "%amica mw%",   # AMICA MV/MW = microwave
                    "%amica mi%",                 # AMICA MI = microwave (alternate model code)
                    "%amica vm%",                 # AMICA VM = vestavná mikrovlnná (built-in microwave)
                )),
                ("Fans", (
                    "rohnson r-7%",         # Rohnson R-7xxx = electric fans
                )),
                ("Kitchen Appliances", (
                    "%philips hd9%",        # Philips HD9 = air fryer
                    "%philips%hd93%",       # Philips HD93xx = air fryer (Series 3000 HD9318/HD9395 etc.)
                    "%philips airfryer%",   # Philips Airfryer
                    "%buydeem%",            # Buydeem = steam ovens/kitchen appliances
                    "%ariete%",             # Ariete = Italian small kitchen appliances
                    "%guzzanti%",           # Guzzanti = Italian small kitchen appliances
                    "%electrolux create%",  # Electrolux Create = kitchen appliance line
                    "%eta storio%",         # ETA Storio = food processor/stand mixer
                    "%eta 1604%",           # ETA 1604 = multifunctional kitchen appliance
                    "%eta%duna%",           # ETA Duna = kitchen food processor series
                    "%eta%dita%",           # ETA Dita = kitchen food processor series
                    "%eta%rosalia%",        # ETA Rosalia = stand mixer series
                    "%eta%ela mini%",       # ETA Ela Mini = compact kitchen appliance
                )),
                ("Blenders & Mixers", (
                    "%food processor%", "%stand mixer%", "%küchenmaschine%",
                    "%thermomix%",
                    "%catler bm%",          # Catler BM = blender/mixer series
                    "%kitchenaid artisan%", # KitchenAid Artisan = iconic stand mixer
                )),
                ("Irons", (
                    "%steam iron%", "%dampfbügel%", "%dampfstation%",
                    "%ironing%",
                    "%rohnson r-1239%",     # Rohnson R-1239 Flexi Force = garment steamer
                )),
                ("Air Purifiers", (
                    "%air purif%", "%luftreiniger%", "%hepa filter%",
                    "% ap%hepa%",
                    "rohnson r-98%",        # Rohnson R-98xxx = air purifiers (Hot & Cold)
                    "rohnson r-99%",        # Rohnson R-99xx = air purifiers
                    "%rowenta hy%",         # Rowenta HY = humidifier (closest category)
                )),
            ]
            _oa_updated = 0
            for _oa_cat, _patterns in _other_app_rules:
                for _pat in _patterns:
                    _rc.execute(
                        "UPDATE products SET NormalizedCategory = ?, NormalizedMainGroup = 'Home Appliances' "
                        "WHERE NormalizedCategory = 'Other Appliances' "
                        "  AND LOWER(Name) LIKE ?",
                        (_oa_cat, _pat)
                    )
                    _oa_updated += _rc.execute("SELECT changes()").fetchone()[0]
            if _oa_updated:
                _rc.commit()
                print(f"[init] Refined {_oa_updated} Other Appliances into specific subcategories", flush=True)

            # Fix Czech "Trouby" NormalizedCategory (= ovens/stoves) → Ovens & Stoves
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Ovens & Stoves', NormalizedMainGroup = 'Home Appliances'
                WHERE NormalizedCategory = 'Trouby'
            """)
            _rc.commit()
            _trouby = _rc.execute("SELECT changes()").fetchone()[0]
            if _trouby:
                print(f"[init] Fixed {_trouby} Trouby products → Ovens & Stoves", flush=True)

            # Rescue smartphones/computers misclassified as Other Appliances
            # (alza.cz sometimes puts them in 'Ostatní spotřebiče')
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Smartphones', NormalizedMainGroup = 'Phones & Tablets'
                WHERE NormalizedCategory = 'Other Appliances'
                  AND (LOWER(Name) LIKE '%nothing phone%'
                    OR LOWER(Name) LIKE '%iphone%'
                    OR LOWER(Name) LIKE '%samsung galaxy s%'
                    OR LOWER(Name) LIKE '%xiaomi %phone%'
                    OR LOWER(Name) LIKE '%oneplus %phone%'
                    OR LOWER(Name) LIKE '%honor magic%'
                    OR LOWER(Name) LIKE '%motorola edge%'
                    OR LOWER(Name) LIKE '%motorola moto%'
                    OR LOWER(Name) LIKE '%poco c%'
                    OR LOWER(Name) LIKE '%poco x%'
                    OR LOWER(Name) LIKE '%poco f%'
                    OR LOWER(Name) LIKE '%poco m%'
                    OR LOWER(Name) LIKE '%blackview bv%'
                    OR LOWER(Name) LIKE '%blackview bl%'
                    OR LOWER(Name) LIKE '%ulefone armor%'
                    OR LOWER(Name) LIKE '%cmf phone%'
                    OR LOWER(Name) LIKE '%doogee s%'
                    OR LOWER(Name) LIKE '%oukitel wp%'
                    OR LOWER(Name) LIKE '%asus rog phone%'
                    OR LOWER(Name) LIKE '%honor x7%'
                    OR LOWER(Name) LIKE '%honor x8%'
                    OR LOWER(Name) LIKE '%honor x9%'
                    OR LOWER(Name) LIKE '%huawei pura%'
                    OR LOWER(Name) LIKE '%huawei nova%'
                    OR LOWER(Name) LIKE '%infinix%'
                    OR LOWER(Name) LIKE '%mobiola mb%'
                    OR LOWER(Name) LIKE '%motorola razr%'
                    OR LOWER(Name) LIKE '%motorola thinkphone%'
                    OR LOWER(Name) LIKE '%vivo x1%'
                    OR LOWER(Name) LIKE '%vivo x2%'
                    OR LOWER(Name) LIKE '%vivo v5%'
                    OR LOWER(Name) LIKE '%zte blade%')
            """)
            _rc.commit()
            _phone_fix = _rc.execute("SELECT changes()").fetchone()[0]
            if _phone_fix:
                print(f"[init] Rescued {_phone_fix} phones from Other Appliances → Smartphones", flush=True)

            # Rescue mini PCs / all-in-ones misclassified as Other Appliances
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Desktop PCs', NormalizedMainGroup = 'Computers'
                WHERE NormalizedCategory = 'Other Appliances'
                  AND (LOWER(Name) LIKE '%asus nuc%'
                    OR LOWER(Name) LIKE '%intel nuc%'
                    OR LOWER(Name) LIKE '%mac mini%'
                    OR LOWER(Name) LIKE '%hp 24%'
                    OR LOWER(Name) LIKE '%hp all-in-one%'
                    OR LOWER(Name) LIKE '%all in one pc%'
                    OR LOWER(Name) LIKE '%acemagic%'
                    OR LOWER(Name) LIKE '%alzapc%'
                    OR LOWER(Name) LIKE '%acer aspire tc%'
                    OR LOWER(Name) LIKE '%umax%'
                    OR LOWER(Name) LIKE '%dell optiplex%'
                    OR LOWER(Name) LIKE '%hal3000%'
                    OR LOWER(Name) LIKE '%hp 27-c%'
                    OR LOWER(Name) LIKE '%hp proone%'
                    OR LOWER(Name) LIKE '%victus by hp%'
                    OR LOWER(Name) LIKE '%lenovo legion go%'
                    OR LOWER(Name) LIKE '%mac studio%')
            """)
            _rc.commit()
            _pc_fix = _rc.execute("SELECT changes()").fetchone()[0]
            if _pc_fix:
                print(f"[init] Rescued {_pc_fix} mini PCs from Other Appliances → Computers", flush=True)

            # Rescue earphones/headphones misclassified as Other Appliances
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Earphones', NormalizedMainGroup = 'Audio'
                WHERE NormalizedCategory = 'Other Appliances'
                  AND (LOWER(Name) LIKE '%niceboy ion%'
                    OR LOWER(Name) LIKE '%airsonic%'
                    OR LOWER(Name) LIKE '%true wireless%'
                    OR LOWER(Name) LIKE '%tws earb%')
            """)
            _rc.commit()
            _ear_fix = _rc.execute("SELECT changes()").fetchone()[0]
            if _ear_fix:
                print(f"[init] Rescued {_ear_fix} earphones from Other Appliances → Audio", flush=True)

            # Classify streaming media players to TV & Video
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Streaming Devices', NormalizedMainGroup = 'TV & Video'
                WHERE NormalizedMainGroup IN ('Audio', 'TV & Video', 'Accessories', 'Other')
                  AND (LOWER(Name) LIKE '%fire tv stick%'
                    OR LOWER(Name) LIKE '%amazon fire tv%'
                    OR LOWER(Name) LIKE '%apple tv 4k%'
                    OR LOWER(Name) LIKE '%chromecast%'
                    OR LOWER(Name) LIKE '%nvidia shield%'
                    OR (LOWER(Name) LIKE '%apple tv%' AND LOWER(Name) NOT LIKE '%apple tv app%'))
            """)
            _rc.commit()
            _stream_fixed = _rc.execute("SELECT changes()").fetchone()[0]
            if _stream_fixed:
                print(f"[init] Classified {_stream_fixed} streaming devices → TV & Video", flush=True)

            # Refine Toys & Games into LEGO subcategory by product name
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'LEGO', NormalizedMainGroup = 'Toys & Games'
                WHERE NormalizedCategory = 'Toys & Games'
                  AND (LOWER(Name) LIKE '%lego%' OR LOWER(Name) LIKE '%lego®%')
            """)
            _rc.commit()
            _lego_updated = _rc.execute("SELECT changes()").fetchone()[0]
            if _lego_updated:
                print(f"[init] Moved {_lego_updated} LEGO products to LEGO subcategory", flush=True)

            # Move Garmin smartwatches from Sports & Outdoor to Wearables (misclassified)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Smartwatches', NormalizedMainGroup = 'Wearables'
                WHERE NormalizedCategory = 'Sports & Outdoor'
                  AND (LOWER(Name) LIKE '%garmin forerunner%'
                    OR LOWER(Name) LIKE '%garmin fenix%'
                    OR LOWER(Name) LIKE '%garmin vivoactive%'
                    OR LOWER(Name) LIKE '%garmin venu%'
                    OR LOWER(Name) LIKE '%garmin instinct%')
            """)
            _rc.commit()
            _garmin_updated = _rc.execute("SELECT changes()").fetchone()[0]
            if _garmin_updated:
                print(f"[init] Moved {_garmin_updated} Garmin smartwatches to Wearables", flush=True)

            # Split Networking into Routers, Switches, Range Extenders
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Switches', NormalizedMainGroup = 'Networking'
                WHERE Category = 'Síťové přepínače'
            """)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Range Extenders', NormalizedMainGroup = 'Networking'
                WHERE NormalizedCategory = 'Networking' AND Category = 'Extendery'
            """)
            _rc.commit()

            # Move E-Bikes out of catch-all Sports & Outdoor
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'E-Bikes', NormalizedMainGroup = 'Sports & Outdoor'
                WHERE NormalizedCategory = 'Sports & Outdoor'
                  AND (LOWER(Name) LIKE '%e-bike%' OR LOWER(Name) LIKE '% ebike %'
                    OR LOWER(Name) LIKE '%engwe%' OR LOWER(Name) LIKE '%electric bike%'
                    OR LOWER(Name) LIKE '%elektrický bicykel%')
            """)
            _rc.commit()

            # Fix products stuck in garbage/untranslated Czech categories
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Microphones', NormalizedMainGroup = 'Audio'
                WHERE LOWER(Name) LIKE '%rode wireless go%'
            """)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'PC Cooling', NormalizedMainGroup = 'Computers'
                WHERE LOWER(Name) LIKE '%case fan mount%' OR LOWER(Name) LIKE '%fan mount%' AND LOWER(Name) LIKE '%akasa%'
            """)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Laptop Accessories', NormalizedMainGroup = 'Accessories'
                WHERE LOWER(Name) LIKE '%pen tip kit%' OR LOWER(Name) LIKE '%surface pen tip%'
            """)
            # Fix Small Appliances (Klarstein products)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Air Purifiers', NormalizedMainGroup = 'Home Appliances'
                WHERE NormalizedCategory = 'Small Appliances'
                  AND (LOWER(Name) LIKE '%air%' OR LOWER(Name) LIKE '%luftreiniger%'
                    OR LOWER(Name) LIKE '%auraair%' OR LOWER(Name) LIKE '%luft%')
            """)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Refrigerators', NormalizedMainGroup = 'Home Appliances'
                WHERE NormalizedCategory = 'Small Appliances'
                  AND (LOWER(Name) LIKE '%wine%' OR LOWER(Name) LIKE '%fridge%'
                    OR LOWER(Name) LIKE '%mini kühlschrank%' OR LOWER(Name) LIKE '%elegance%')
            """)
            # Fix obvious misclassifications in tiny groups
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Speakers', NormalizedMainGroup = 'Audio'
                WHERE NormalizedMainGroup = 'Garden & Outdoors' AND LOWER(Name) LIKE '%adam hall%'
            """)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Musical Instruments', NormalizedMainGroup = 'Audio'
                WHERE NormalizedMainGroup = 'Health & Beauty'
                  AND (LOWER(Name) LIKE '%dunlop%' AND (LOWER(Name) LIKE '%string%' OR LOWER(Name) LIKE '%guitar%'
                    OR LOWER(Name) LIKE '% formula %' OR LOWER(Name) LIKE '%6502%' OR LOWER(Name) LIKE '%6582%'))
            """)
            _rc.execute("""
                UPDATE products SET NormalizedCategory = 'Gaming Accessories', NormalizedMainGroup = 'Gaming'
                WHERE NormalizedMainGroup = 'Home & Garden'
                  AND LOWER(Name) LIKE '%playstation%'
            """)
            _rc.commit()

            _rc.close()
        except Exception as _e:
            print(f"[init] Re-normalization of foreign cats skipped: {_e}", flush=True)

        # 3b. Ranks are handled at pre-start (before HTTP server binds) to avoid
        #     DB contention. Nothing to do here.

        # 4. Auth / contribution tables
        try:
            from scraper.auth import ensure_tables as _ensure_auth_tables
            _ensure_auth_tables()
            print("[init] Auth tables OK", flush=True)
        except Exception as _e:
            print(f"[init] Auth tables skipped: {_e}", flush=True)

        # 5. Master scheduler subprocess
        try:
            os.makedirs(
                os.path.join(os.path.dirname(__file__), "scraper", "logs"),
                exist_ok=True,
            )
            _start_scheduler()
            _pid = _scheduler_proc.pid if _scheduler_proc else "N/A"
            print(f"[init] Scheduler PID {_pid} — daily wake-up at 03:00", flush=True)
        except Exception as _e:
            print(f"[init] Scheduler not started: {_e}", flush=True)

        # 6. One-time alza.cz snapshot seed — creates today + seed snapshot so
        #    alza products immediately show the 📈 badge (seed_days=7 inserts a
        #    synthetic row dated 7 days ago; INSERT OR IGNORE is safe on reruns).
        def _seed_alza_snapshots():
            try:
                from scraper.alza_snapshot import run_alza_snapshot
                n = run_alza_snapshot(seed_days=7)
                print(f"[init] alza snapshot seed: {n} rows", flush=True)
            except Exception as _se:
                print(f"[init] alza snapshot seed skipped: {_se}", flush=True)

        threading.Thread(target=_seed_alza_snapshots, daemon=True, name="alza-snap-seed").start()

        # 7a. Populate alza images instantly from SKU codes (no HTTP requests)
        def _run_alza_images():
            try:
                n = _populate_alza_images()
                print(f"[init] alza images from SKU: {n} rows", flush=True)
            except Exception as _e:
                print(f"[init] alza image populate failed: {_e}", flush=True)

        threading.Thread(target=_run_alza_images, daemon=True, name="alza-img-sku").start()

        # 7a2. Populate prisjakt images from product ID in URL (no HTTP requests)
        def _run_prisjakt_images():
            try:
                n = _populate_prisjakt_images()
                print(f"[init] prisjakt images from URL IDs: {n} rows", flush=True)
            except Exception as _e:
                print(f"[init] prisjakt image populate failed: {_e}", flush=True)

        threading.Thread(target=_run_prisjakt_images, daemon=True, name="prisjakt-img").start()

        # 7a3. Populate Amazon images from ASIN codes in URLs (no HTTP requests)
        def _run_amazon_images():
            try:
                n = _populate_amazon_images()
                print(f"[init] amazon images from ASINs: {n} rows", flush=True)
            except Exception as _e:
                print(f"[init] amazon image populate failed: {_e}", flush=True)

        threading.Thread(target=_run_amazon_images, daemon=True, name="amazon-img").start()

        # 7b. Background og:image fetcher — fills image_url for heureka, prisjakt, etc.
        # Batch=5000 covers all remaining untried products (~8k total untried) in 2 deploys.
        try:
            threading.Thread(
                target=_fetch_og_images,
                kwargs={"batch": 5000},
                daemon=True,
                name="og-image-fetcher",
            ).start()
            print("[init] og:image fetcher started (batch=5000)", flush=True)
        except Exception as _e:
            print(f"[init] og:image fetcher failed to start: {_e}", flush=True)

        # 8. Pre-warm snapshot coverage cache so first API request is fast
        try:
            n = len(query_snapshot_coverage())
            print(f"[init] Snapshot coverage cache warmed: {n} URLs", flush=True)
        except Exception as _e:
            print(f"[init] Coverage cache warm failed: {_e}", flush=True)

        # 8a-stats. Pre-warm stats cache (depends on coverage, so call after 8)
        try:
            s = query_stats()
            print(f"[init] Stats cache warmed: {s.get('total',0)} products, "
                  f"{s.get('with_history',0)} with history, "
                  f"{s.get('with_images',0)} with images", flush=True)
        except Exception as _e:
            print(f"[init] Stats cache warm failed: {_e}", flush=True)

        # 8b. Pre-warm snapshot delta cache (also builds composite index if missing)
        try:
            n = len(query_snapshot_deltas())
            print(f"[init] Snapshot delta cache warmed: {n} URLs with changes", flush=True)
        except Exception as _e:
            print(f"[init] Delta cache warm failed: {_e}", flush=True)

        # 8c. Pre-warm cross-market cache (default key: min_markets=2, include_amazon=False)
        # This avoids the ~2s cold-start delay when a user first clicks the 🌍 view.
        try:
            from scraper.cross_market import find_cross_market_matches as _cm
            _cm_conn = open_db()
            _cm_groups = _cm(_cm_conn, min_markets=2, include_amazon_us=False)
            _cm_conn.close()
            _cross_market_cache[(2, False)] = _cm_groups
            _cross_market_ts[(2, False)]    = time.monotonic()
            print(f"[init] Cross-market cache warmed: {len(_cm_groups)} product groups", flush=True)
        except Exception as _e:
            print(f"[init] Cross-market cache warm failed: {_e}", flush=True)

        # 8d. Pre-warm movers cache (expensive ~1.5s query, changes rarely)
        try:
            r = query_snapshot_movers(days=7, limit=40, metric="recommend")
            print(f"[init] Movers cache warmed: {len(r.get('risers',[]))+len(r.get('fallers',[]))} products", flush=True)
        except Exception as _e:
            print(f"[init] Movers cache warm failed: {_e}", flush=True)

        print("✦ QualityDB init complete.", flush=True)

    threading.Thread(target=_background_init, daemon=True).start()

    print(f"  Press Ctrl+C to stop.", flush=True)
    server.serve_forever()
