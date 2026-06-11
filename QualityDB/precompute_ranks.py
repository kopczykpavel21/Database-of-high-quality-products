"""
Precomputes category ranks and totals for every product, stores them as columns.
Also adds missing indexes. Run once after any major DB update:

    python3 precompute_ranks.py

Takes ~1-2 minutes on 1.4M rows. Safe to re-run.
"""
import sqlite3
import time
from collections import defaultdict

DB = "products.db"
conn = sqlite3.connect(DB)
conn.execute(f"PRAGMA journal_mode={os.environ.get('JOURNAL_MODE', 'wal').upper()}")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA cache_size=-128000")  # 128MB cache

# ── Step 1: Add missing indexes ──────────────────────────────────────────────
print("Step 1/4: Adding missing indexes...")
for name, sql in [
    ("idx_category",     "CREATE INDEX IF NOT EXISTS idx_category     ON products(Category)"),
    ("idx_stars",        "CREATE INDEX IF NOT EXISTS idx_stars        ON products(AvgStarRating)"),
    ("idx_reviews",      "CREATE INDEX IF NOT EXISTS idx_reviews      ON products(ReviewsCount)"),
    ("idx_source_stars", "CREATE INDEX IF NOT EXISTS idx_source_stars ON products(source, AvgStarRating)"),
]:
    t = time.time()
    conn.execute(sql)
    conn.commit()
    print(f"  {name} ({time.time()-t:.1f}s)")

# ── Step 2: Add columns if missing ───────────────────────────────────────────
print("\nStep 2/4: Adding cat_rank / cat_total columns...")
cols = [r[1] for r in conn.execute("PRAGMA table_info(products)").fetchall()]
if "cat_rank" not in cols:
    conn.execute("ALTER TABLE products ADD COLUMN cat_rank INTEGER")
    print("  Added cat_rank")
if "cat_total" not in cols:
    conn.execute("ALTER TABLE products ADD COLUMN cat_total INTEGER")
    print("  Added cat_total")
conn.commit()

# ── Step 3: Load rowid + score fields, compute ranks in Python ───────────────
print("\nStep 3/4: Loading rows and computing ranks...")
t = time.time()

def quality_score(source, stars, rec_pct, reviews):
    reviews = reviews or 0
    if source in ('warentest', 'dtest') and stars:
        return (stars / 5.0) * 100.0
    if rec_pct is not None:
        return rec_pct * reviews / (reviews + 50.0)
    return (stars or 0) / 5.0 * 100.0 * reviews / (reviews + 200.0)

rows = conn.execute("""
    SELECT rowid, Category, source, AvgStarRating, RecommendRate_pct, ReviewsCount
    FROM products
""").fetchall()
print(f"  Loaded {len(rows):,} rows in {time.time()-t:.1f}s")

# Group by category, compute scores
t = time.time()
by_cat = defaultdict(list)
for rowid, cat, source, stars, rec, reviews in rows:
    score = quality_score(source, stars, rec, reviews)
    by_cat[cat].append((score, reviews or 0, stars or 0, rowid))

# Sort each category by score desc, then tie-break by reviews desc, stars desc, rowid asc
# This ensures every product gets a unique rank — no more thousands tied at the same position
updates = []  # (cat_rank, cat_total, rowid)
for cat, items in by_cat.items():
    items.sort(key=lambda x: (-x[0], -x[1], -x[2], x[3]))
    total = len(items)
    for rank, (score, reviews, stars, rowid) in enumerate(items, 1):
        updates.append((rank, total, rowid))

print(f"  Ranks computed in {time.time()-t:.1f}s ({len(updates):,} rows)")

# ── Step 4: Write ranks back in batches ──────────────────────────────────────
print("\nStep 4/4: Writing ranks to DB...")
t = time.time()
BATCH = 50_000
for i in range(0, len(updates), BATCH):
    conn.executemany(
        "UPDATE products SET cat_rank=?, cat_total=? WHERE rowid=?",
        updates[i:i+BATCH]
    )
    conn.commit()
    print(f"  {min(i+BATCH, len(updates)):,} / {len(updates):,}")

conn.execute("CREATE INDEX IF NOT EXISTS idx_cat_rank ON products(Category, cat_rank)")
conn.commit()
print(f"  Finished in {time.time()-t:.1f}s")

print("\nAll done! The app will now use precomputed ranks.")
conn.close()
