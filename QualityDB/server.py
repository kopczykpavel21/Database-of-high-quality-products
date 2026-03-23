"""
QualityDB – Standalone HTTP server (no external dependencies).
Run: python3 server.py
Then open: http://localhost:8080

Automatic scraping:
    On startup, a background thread waits 60 s then runs both scrapers once.
    After that it runs them again every 24 hours.
    Visit /api/scrape-status to see last run info.
    Visit /api/run-scraper  to trigger a manual run immediately.
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import sqlite3, json, os, math, urllib.parse, mimetypes
import threading, time, datetime

# Support DB on a mounted volume (e.g. Fly.io) via env var, fallback to local
DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "products.db"))
STATIC  = os.path.join(os.path.dirname(__file__), "static")
TMPL    = os.path.join(os.path.dirname(__file__), "templates", "index.html")
PAGE_SIZE = 24

def open_db():
    """Open DB with WAL mode + 30 s timeout so server and scrapers can coexist."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
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

# ── Background scraper state ──────────────────────────────────────────────────
_scraper_status = {
    "running":    False,
    "last_run":   None,   # ISO timestamp string
    "last_added": 0,
    "last_error": None,
    "enabled":    True,   # set False via /api/stop-scraper to pause auto-runs
}
_scraper_lock = threading.Lock()


def _run_scrapers():
    """Run Amazon + Heureka + Zbozi scrapers and update _scraper_status."""
    with _scraper_lock:
        if _scraper_status["running"]:
            return   # already running, skip
        _scraper_status["running"] = True
        _scraper_status["last_error"] = None

    total_added = 0
    try:
        import sys as _sys
        _sys.path.insert(0, os.path.dirname(__file__))

        # Amazon first — most review-rich source
        try:
            from scraper.amazon_scraper import run_scraper as run_amazon
            result = run_amazon()
            total_added += result.get("total_added", 0)
        except Exception as e:
            print(f"[scraper] Amazon error: {e}")

        try:
            from scraper.heureka_scraper import run_scraper as run_heureka
            result = run_heureka()
            total_added += result.get("total_added", 0)
        except Exception as e:
            print(f"[scraper] Heureka error: {e}")

        try:
            from scraper.zbozi_scraper import run_scraper as run_zbozi
            result = run_zbozi()
            total_added += result.get("total_added", 0)
        except Exception as e:
            print(f"[scraper] Zbozi error: {e}")

        try:
            from scraper.heureka_sk_scraper import run_scraper as run_heureka_sk
            result = run_heureka_sk()
            total_added += result.get("total_added", 0)
        except Exception as e:
            print(f"[scraper] Heureka.sk error: {e}")

        try:
            from scraper.czc_scraper import run_scraper as run_czc
            result = run_czc()
            total_added += result.get("total_added", 0)
        except Exception as e:
            print(f"[scraper] CZC error: {e}")

        try:
            from scraper.conrad_scraper import run_scraper as run_conrad
            result = run_conrad()
            total_added += result.get("total_added", 0)
        except Exception as e:
            print(f"[scraper] Conrad error: {e}")

    except Exception as e:
        with _scraper_lock:
            _scraper_status["last_error"] = str(e)
    finally:
        with _scraper_lock:
            _scraper_status["running"]    = False
            _scraper_status["last_run"]   = datetime.datetime.utcnow().isoformat() + "Z"
            _scraper_status["last_added"] = total_added
        print(f"[scraper] Run complete — {total_added} new products added.")


def _scraper_loop():
    """
    Background thread: wait 5 min after startup, then repeat every 24 h.
    Respects _scraper_status["enabled"] — if False, skips the run silently.
    """
    print("[scraper] Background scheduler started — first run in 5 min.")
    time.sleep(5 * 60)
    while True:
        with _scraper_lock:
            enabled = _scraper_status["enabled"]
        if enabled:
            print("[scraper] Starting scheduled scrape run…")
            _run_scrapers()
            print("[scraper] Next run in 24 hours.")
        else:
            print("[scraper] Skipping scheduled run (scraper is paused).")
        time.sleep(24 * 60 * 60)


def get_categories():
    conn = open_db()
    rows = conn.execute(
        "SELECT Category, COUNT(*) as cnt FROM products "
        "GROUP BY Category ORDER BY cnt DESC"
    ).fetchall()
    conn.close()
    return rows


def get_categories_hierarchical(country=None):
    """Return list of {main, subs: [{sub, count}]} sorted by main name.
    Pass country=None to get categories across all markets."""
    from collections import OrderedDict
    conn = open_db()

    # Check whether MainCategory column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()]
    has_main = "MainCategory" in cols

    country_clause = "WHERE COALESCE(country,'CZ') = ?" if country else ""
    params = (country,) if country else ()

    if has_main:
        rows = conn.execute(
            f"SELECT COALESCE(MainCategory,'Ostatní') as main, Category, COUNT(*) as cnt "
            f"FROM products {country_clause} "
            f"GROUP BY main, Category ORDER BY main, cnt DESC",
            params
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT COALESCE(Category,'Ostatní') as main, Category, COUNT(*) as cnt "
            f"FROM products {country_clause} "
            f"GROUP BY Category ORDER BY cnt DESC",
            params
        ).fetchall()

    conn.close()
    tree = OrderedDict()
    for main, sub, cnt in rows:
        tree.setdefault(main, []).append({"sub": sub or "Nezařazeno", "count": cnt})
    ordered = sorted(tree.keys(), key=lambda x: ("zzz" if x == "Ostatní" else x))
    return [{"main": m, "subs": tree[m]} for m in ordered]


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
    with open(TMPL, encoding="utf-8") as f:
        return f.read()


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
        "czc": "CZ",
        "ceneo": "PL",
        "amazon_us": "US",
        "heureka_sk": "SK",
    }
    country = SOURCE_COUNTRY.get(source, None) if source else None

    allowed_sort = {"ReturnRate_pct","AvgStarRating","ReviewsCount",
                    "RecommendRate_pct","Price_CZK","Price_EUR","Name"}
    if sort_by not in allowed_sort:
        sort_by = "RecommendRate_pct"
    order_sql = "ASC" if order == "asc" else "DESC"

    conditions, plist = [], []
    if country:
        conditions.append("COALESCE(country,'CZ') = ?"); plist.append(country)
    if q:
        conditions.append("(Name LIKE ? OR Category LIKE ? OR Description LIKE ?)")
        plist += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if main_category:
        conditions.append("MainCategory = ?"); plist.append(main_category)
    if category:
        conditions.append("Category = ?"); plist.append(category)
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
        conditions.append("""(
            (AvgStarRating IS NOT NULL AND AvgStarRating < 3.5 AND ReviewsCount >= 100)
            OR
            (RecommendRate_pct IS NOT NULL AND RecommendRate_pct < 65 AND ReviewsCount >= 50)
        )""")

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    # For price sorting, use whichever price column is available
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
        f"""WITH ranked AS (
              SELECT *,
                RANK() OVER (
                  PARTITION BY Category
                  ORDER BY (
                    CASE
                      WHEN RecommendRate_pct IS NOT NULL
                      THEN COALESCE(RecommendRate_pct, 0) * COALESCE(ReviewsCount, 0)
                           / (COALESCE(ReviewsCount, 0) + 50.0)
                      ELSE (COALESCE(AvgStarRating, 0) / 5.0) * 100.0
                           * COALESCE(ReviewsCount, 0)
                           / (COALESCE(ReviewsCount, 0) + 200.0)
                    END
                  ) DESC
                ) AS source_rank,
                COUNT(*) OVER (PARTITION BY Category) AS source_total
              FROM products
            )
            SELECT id, Name, MainCategory, Category, ProductURL, Price_CZK,
                   COALESCE(Price_EUR, NULL) as Price_EUR,
                   COALESCE(country, 'CZ') as country,
                   currency,
                   AvgStarRating, StarRatingsCount, ReviewsCount,
                   RecommendRate_pct, ReturnRate_pct,
                   Stars5_Count, Stars4_Count, Stars3_Count,
                   Stars2_Count, Stars1_Count, source,
                   source_rank, source_total, keywords,
                   COALESCE(details_json, NULL) as details_json
            FROM ranked {where}
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
        "page_size": PAGE_SIZE
    }


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


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # silence access log

    def send_json(self, data, status=200):
        body = json.dumps(data, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, html, status=200):
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path
        params = urllib.parse.parse_qs(parsed.query)

        if path == "/":
            self.send_html(build_html())

        elif path == "/api/products":
            self.send_json(query_products(params))

        elif path == "/api/categories":
            try:
                country = params.get("country", [""])[0]
                if country not in ("CZ", "DE", "PL", "SK", "US"):
                    country = None   # None = all countries
                self.send_json(get_categories_hierarchical(country))
            except Exception as e:
                import logging
                logging.error(f"/api/categories failed: {e}")
                self.send_json([])

        elif path == "/api/stats":
            self.send_json(query_stats())

        elif path == "/api/keywords":
            self.send_json([{"tag": t, "count": c} for t, c in query_keywords()])

        elif path == "/api/scrape-status":
            with _scraper_lock:
                self.send_json(dict(_scraper_status))

        elif path == "/api/run-scraper":
            with _scraper_lock:
                already = _scraper_status["running"]
            if already:
                self.send_json({"status": "already_running"})
            else:
                t = threading.Thread(target=_run_scrapers, daemon=True)
                t.start()
                self.send_json({"status": "started"})

        elif path == "/api/stop-scraper":
            with _scraper_lock:
                _scraper_status["enabled"] = False
            self.send_json({"status": "paused", "message": "Scraper paused — current run will finish if in progress, next scheduled run will be skipped."})

        elif path == "/api/start-scraper":
            with _scraper_lock:
                _scraper_status["enabled"] = True
            self.send_json({"status": "resumed", "message": "Scraper resumed — will run on next schedule or use /api/run-scraper to trigger now."})

        elif path.startswith("/static/"):
            fname = path[len("/static/"):]
            fpath = os.path.join(STATIC, fname)
            if os.path.isfile(fpath):
                mime, _ = mimetypes.guess_type(fpath)
                with open(fpath, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", mime or "application/octet-stream")
                self.send_header("Content-Length", len(body))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404); self.end_headers()
        else:
            self.send_response(404); self.end_headers()


if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        print("Database not found. Run load_data.py first.")
        exit(1)
    # On Render/cloud the PORT env var is set automatically.
    # Locally it defaults to 8080.
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"   # listen on all interfaces (required for cloud hosting)

    # Ensure all indexes exist (fast no-op if already created)
    _conn = open_db()
    ensure_indexes(_conn)
    _conn.close()

    # Start background scraper scheduler (runs every 24 h, first run after 60 s)
    bg = threading.Thread(target=_scraper_loop, daemon=True)
    bg.start()

    server = HTTPServer((host, port), Handler)
    print(f"✦ QualityDB running at http://localhost:{port}")
    print("  Background scraper scheduled — first run in 60 s.")
    print("  Press Ctrl+C to stop.")
    server.serve_forever()
