# QualityDB — Architecture Review & Cost Reduction Plan

## What the App Is Actually Doing

QualityDB is a **read-heavy public data index** — closer to a Wikipedia of product quality scores than a SaaS app. The data changes only when scrapers run (daily/weekly). Users only read and filter.

### Current stack
- **Server:** Python stdlib `http.server` — single-threaded, no external dependencies
- **Database:** SQLite, 1.2 GB on disk, ~1.44 million product rows
- **Frontend:** Vanilla JS (43 KB `app.js`) + CSS (33 KB), no framework
- **Scrapers:** ~25 scrapers launched as a background subprocess (`scheduler.py`) on the same machine as the server
- **Deployment target:** Fly.io (inferred from code comments referencing `fly sftp shell`, `JOURNAL_MODE=delete`, `Fly.io persistent volume`)

### What happens on every request

**`GET /` (homepage):**
1. Opens SQLite connection
2. Runs `SELECT` on `products` for repairability scores (~450 rows)
3. Runs `SELECT` on `fr_repairability_index` (2,195 rows)
4. Serializes both as inline JSON blobs injected into `<script>` tags in the HTML
5. Returns the full HTML every time — no caching

**`GET /api/products` (product list):**
1. Opens new SQLite connection
2. Runs `COUNT(*)` query
3. Runs paginated `SELECT` with `json_patch()` and `COALESCE` on 24 rows
4. Closes connection
5. No cache headers returned

**`GET /api/categories`, `/api/stats`, `/api/keywords`:** same pattern — fresh DB hit every time, no caching.

---

## Main Cost Problems

### 1. `product_snapshots` table — half the database is probably useless
The table has **1,444,133 rows** (nearly identical to `products`), stores price/rating history per scrape date, but `snapshot_date` appears to have no populated values in the current DB. This table alone is eating roughly **600 MB** of the 1.2 GB database for no active benefit in the UI.

### 2. `build_html()` rebuilds the page on every request
Every single homepage hit opens the DB, fetches ~2,200 rows, serializes them to JSON, and stitches them into HTML. There is zero memoization. For 1,000 daily users that's 1,000 redundant DB reads of the same data that doesn't change between scraper runs.

### 3. No HTTP caching headers anywhere
Not one `Cache-Control`, `ETag`, or `Last-Modified` header is sent for any endpoint — not even for the static JS/CSS files. Every browser re-downloads everything from scratch on every visit.

### 4. Scrapers co-located with the web server
The scheduler and all scrapers run on the same Fly.io machine as the HTTP server. During scrape runs (which can take hours), the server competes for CPU and I/O with the scrapers. SQLite with DELETE journal mode also means only one writer at a time — the scraper blocks reads momentarily.

### 5. `build_html()` injects live-DB data that is effectively static between runs
`__IR_SCORES` and `__FR_GOV` change only when `indicereparabilite_scraper.py` runs (likely monthly). Injecting them fresh on every request is pure waste.

### 6. The Python stdlib HTTP server is single-threaded
`http.server.HTTPServer` processes one request at a time. Under any real traffic load it will queue requests. This is not a cost problem but a reliability problem that could force a larger (more expensive) VM.

---

## Cheapest Target Architecture

The product is a **static-data browser with lightweight filtering.** The cheapest correct architecture treats it that way.

```
[Scrapers run offline]  →  [SQLite DB on Fly.io volume]
                               ↓
                    [Python API on Fly.io free tier]
                               ↓
              [Cloudflare Pages — static frontend]
                  (free, global CDN, zero ops)
```

No message queue. No separate cache service. No managed DB. No containers beyond Fly.io's existing setup.

---

## Recommended Stack (Cheapest Viable)

| Layer | Service | Cost |
|---|---|---|
| Frontend hosting | **Cloudflare Pages** (or GitHub Pages) | **$0** |
| API server | **Fly.io free tier** — 1× shared-cpu-1x, 256 MB RAM | **$0** |
| Database | **SQLite on Fly.io 3 GB volume** (free tier) | **$0** |
| Static assets CDN | Built into Cloudflare Pages | **$0** |
| Scraper execution | **Local cron** or **GitHub Actions** (free 2,000 min/month) | **$0** |
| Optional search | **Fuse.js** client-side (already JS-only) | **$0** |
| **Total** | | **$0–$5/month** |

---

## Rough Monthly Cost Table

| Traffic | Current estimate | After optimizations | Notes |
|---|---|---|---|
| Dev / personal (< 1K users/month) | $5–15 | **$0** | Fly.io free tier covers it |
| 100K users/month | $15–30 | **$0–5** | CDN absorbs static; API handles filtered queries only |
| 1M+ users/month | $50–100 | **$5–20** | Fly.io paid tier + Cloudflare Pro optional |

At 100K users/month with caching, the API probably handles fewer than 5,000 actual DB queries (the rest hit CDN or browser cache). SQLite handles 5K queries/day comfortably on a shared-cpu-1x machine.

---

## Migration Steps (Smallest Rewrite First)

### Step 1 — Drop `product_snapshots` (saves ~600 MB, no UI changes needed)
```sql
DROP TABLE product_snapshots;
VACUUM;
```
This halves the DB size immediately. Only do this if the snapshot history is not displayed anywhere in the UI (it currently isn't — the frontend never queries it directly).

### Step 2 — Cache `build_html()` in memory (5 lines of code, immediate win)
Add a module-level cache:
```python
_html_cache = None
_html_cache_ts = 0
HTML_TTL = 300  # seconds

def build_html():
    global _html_cache, _html_cache_ts
    if _html_cache and (time.time() - _html_cache_ts) < HTML_TTL:
        return _html_cache
    # ... existing build logic ...
    _html_cache = html
    _html_cache_ts = time.time()
    return html
```
Invalidate after each scraper run completes. This eliminates 99% of DB reads from the homepage.

### Step 3 — Add Cache-Control headers
For `/static/` files: `Cache-Control: public, max-age=86400`
For `/api/categories` and `/api/stats`: `Cache-Control: public, max-age=300`
For `/api/products`: `Cache-Control: public, max-age=60`
For `/`: `Cache-Control: public, max-age=120`

This alone will drop bandwidth and server CPU dramatically once a CDN or even browser caching kicks in.

### Step 4 — Move scrapers off the production machine
Run scrapers on your local machine (or a cheap GitHub Actions schedule) and upload the resulting `products.db` to Fly.io via `fly sftp`. The production server then only serves reads — no background processes, no resource contention.

### Step 5 — Separate frontend to Cloudflare Pages (optional but free)
Move `templates/index.html`, `static/app.js`, and `static/style.css` to a Cloudflare Pages repo. Point API calls to your Fly.io URL. Cloudflare edges handle all static delivery globally for free, and the server only handles API requests.

### Step 6 — If you outgrow Fly.io free tier: consider Turso
[Turso](https://turso.tech) is managed SQLite at the edge (libSQL). Free tier: 9 GB storage, 1 billion row reads/month. Migration is a `libsql://` URL change and swapping `sqlite3` for `libsql`. The app's query patterns map 1:1.

---

## What to Cut or Simplify

**Cut immediately:**
- `product_snapshots` table — 1.44M rows, 600 MB, no active UI feature
- The EN/CZ language toggle buttons — already removed, had no JS handler
- `debug_*.py` scraper files in production — dead code

**Simplify:**
- `build_html()` should precompute `__IR_SCORES` and `__FR_GOV` once at startup (they change monthly at most) rather than on every request
- `query_keywords()` iterates every row with keywords and parses JSON in Python — replace with a precomputed `keyword_counts` table updated after each scraper run
- The `/api/repair` endpoint fetches repairability scores separately from `/api/products` — these are already merged in the `details_json` column; the separate endpoint may be redundant

**Do not cut:**
- The Python stdlib server — it's correct for this workload and has zero dependencies
- SQLite — it's the right tool for a 1.4M-row read-heavy dataset with rare writes
- Vanilla JS frontend — no build step, no framework bloat, works fine

---

## Comparison: Keep Fly.io vs. Move to Static + Cheap DB

| Approach | Pros | Cons | Monthly cost |
|---|---|---|---|
| **Keep Fly.io as-is** | Zero migration | Scrapers compete with server; no caching | $5–30 |
| **Fly.io + cache fixes** (recommended) | Minimal work, free tier covers it | Still single-threaded server | **$0–5** |
| **Static frontend + Fly.io API** | CDN for frontend, clean separation | Small migration effort | **$0–5** |
| **Static JSON export** (fully static) | $0 forever, zero infra | Search becomes client-side only; 1.4M products = too much JSON to export naively | $0 but impractical at full scale |
| **Turso + Cloudflare Workers** | Edge DB + edge compute, scales to millions | Rewrite DB driver + HTTP handler | $0–10 |

**Recommended path:** Apply Steps 1–4 above. This keeps the existing codebase almost unchanged, costs $0/month on Fly.io free tier, and handles 100K users/month comfortably with the caching fixes in place.
