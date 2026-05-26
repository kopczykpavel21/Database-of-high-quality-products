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

_default_db = os.path.join(os.path.dirname(__file__), "products.db")
DB_PATH = os.environ.get("DB_PATH", _default_db)
STATIC  = os.path.join(os.path.dirname(__file__), "static")
TMPL    = os.path.join(os.path.dirname(__file__), "templates", "index.html")
PAGE_SIZE = 24

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
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT Category, COUNT(*) as cnt FROM products "
        "GROUP BY Category ORDER BY cnt DESC"
    ).fetchall()
    conn.close()
    return rows


def get_categories_hierarchical():
    """Return list of {main, subs: [{sub, count}]} sorted by main name.
    Falls back to a flat Category-based list if MainCategory column is missing."""
    from collections import OrderedDict
    conn = sqlite3.connect(DB_PATH)

    # Check whether MainCategory column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()]
    has_main = "MainCategory" in cols

    if has_main:
        rows = conn.execute(
            "SELECT COALESCE(MainCategory,'Ostatní') as main, Category, COUNT(*) as cnt "
            "FROM products GROUP BY main, Category ORDER BY main, cnt DESC"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT COALESCE(Category,'Ostatní') as main, Category, COUNT(*) as cnt "
            "FROM products GROUP BY Category ORDER BY cnt DESC"
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
    conn = sqlite3.connect(DB_PATH)
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
    sort_by     = params.get("sort", ["ReturnRate_pct"])[0]
    order       = params.get("order", ["asc"])[0]
    page        = int(params.get("page", ["1"])[0])
    source      = params.get("source", [""])[0]
    keyword     = params.get("keyword", [""])[0]

    allowed_sort = {"ReturnRate_pct","AvgStarRating","ReviewsCount",
                    "RecommendRate_pct","Price_CZK","Name"}
    if sort_by not in allowed_sort:
        sort_by = "ReturnRate_pct"
    order_sql = "ASC" if order == "asc" else "DESC"

    conditions, plist = [], []
    if q:
        conditions.append("(Name LIKE ? OR Category LIKE ?)")
        plist += [f"%{q}%", f"%{q}%"]
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
        conditions.append("source = ?"); plist.append(source)
    if keyword:
        conditions.append('keywords LIKE ?'); plist.append(f'"%{keyword}%"')

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    null_last = f"CASE WHEN {sort_by} IS NULL THEN 1 ELSE 0 END"

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    total = conn.execute(f"SELECT COUNT(*) FROM products {where}", plist).fetchone()[0]
    offset = (page - 1) * PAGE_SIZE
    rows = conn.execute(
        f"""WITH ranked AS (
              SELECT *,
                RANK() OVER (
                  PARTITION BY source, Category
                  ORDER BY (
                    COALESCE(RecommendRate_pct, 0) * COALESCE(ReviewsCount, 0)
                    / (COALESCE(ReviewsCount, 0) + 50.0)
                  ) DESC
                ) AS source_rank,
                COUNT(*) OVER (PARTITION BY source, Category) AS source_total
              FROM products
            )
            SELECT id, Name, MainCategory, Category, ProductURL, Price_CZK,
                   AvgStarRating, StarRatingsCount, ReviewsCount,
                   RecommendRate_pct, ReturnRate_pct,
                   Stars5_Count, Stars4_Count, Stars3_Count,
                   Stars2_Count, Stars1_Count, source,
                   source_rank, source_total, keywords
            FROM ranked {where}
            ORDER BY {null_last}, {sort_by} {order_sql}
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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
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
                self.send_json(get_categories_hierarchical())
            except Exception as e:
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
            self.send_json({"status": "paused", "message": "Scraper paused."})

        elif path == "/api/start-scraper":
            with _scraper_lock:
                _scraper_status["enabled"] = True
            self.send_json({"status": "resumed", "message": "Scraper resumed."})

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
    port = int(os.environ.get("PORT", 8080))
    host = "0.0.0.0"

    bg = threading.Thread(target=_scraper_loop, daemon=True)
    bg.start()

    server = HTTPServer((host, port), Handler)
    print(f"❆ QualityDB running at http://localhost:{port}")
    print("  Background scraper scheduled — first run in 5 min.")
    print("  Press Ctrl+C to stop.")
    server.serve_forever()
