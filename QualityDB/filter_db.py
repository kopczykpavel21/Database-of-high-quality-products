import sqlite3

src = sqlite3.connect("products.db")
dst = sqlite3.connect("products_filtered.db")
dst.execute("PRAGMA synchronous=NORMAL")

cols = [c[1] for c in src.execute("PRAGMA table_info(products)").fetchall()]
col_str = ", ".join(cols)
ph = ", ".join(["?"] * len(cols))

schema = src.execute(
    "SELECT sql FROM sqlite_master WHERE type='table' AND name='products'"
).fetchone()[0]
dst.execute(schema)

batch, copied, errors = [], 0, 0
for lo in range(0, 2_200_000, 50_000):
    try:
        rows = src.execute(f"""
            SELECT {col_str} FROM products
            WHERE rowid BETWEEN ? AND ?
            AND (
                source != 'amazon_us'
                OR ReviewsCount >= 100
                OR (AvgStarRating >= 4.1 AND ReviewsCount >= 30)
            )
        """, (lo, lo + 49_999)).fetchall()
        batch.extend(rows)
        copied += len(rows)
    except:
        errors += 1
    if len(batch) >= 200_000:
        dst.executemany(f"INSERT INTO products ({col_str}) VALUES ({ph})", batch)
        dst.commit()
        batch = []
        print(f"  {copied:,} rows copied…")

if batch:
    dst.executemany(f"INSERT INTO products ({col_str}) VALUES ({ph})", batch)
    dst.commit()

print(f"Done: {copied:,} rows copied, {errors} error-batches skipped.")
src.close()
dst.close()



