"""
Import ALL Alza products from the two Excel files into products.db.
Filter: ReviewsCount >= 10 only (no return rate filter — full distribution for research).
Uses upsert so re-running updates prices/ratings on existing rows.
Run from the QualityDB folder: python3 import_new_alza.py
"""
import sqlite3, os, re
import pandas as pd

DB_PATH  = os.path.join(os.path.dirname(__file__), "products.db")
UPLOADS  = os.path.join(os.path.dirname(__file__), "..", "mnt", "uploads")

FILE1 = os.path.join(UPLOADS,
    "Alza.cz-Downloaded from sorted as a most sold produts on 1.3.2025- 10 categories -16500 Products.xlsx")
FILE2 = os.path.join(UPLOADS,
    "Alza data -19.9.2025-8 category - 8144 products.xlsx")

# ── helpers ──────────────────────────────────────────────────────────────────

def clean_pct(val):
    """'6,06 %' → 6.06,  '100\\xa0' → 100.0,  NaN → None"""
    if pd.isna(val): return None
    s = str(val).replace('\xa0','').replace('%','').replace(',','.').strip()
    try: return float(s)
    except: return None

def clean_float(val):
    if pd.isna(val): return None
    s = str(val).replace('\xa0','').replace(',','.').strip()
    try: return float(s)
    except: return None

def clean_count(val):
    if pd.isna(val): return None
    s = re.sub(r'[^\d]', '', str(val))
    return int(s) if s else None

def infer_category(url: str) -> str:
    """Best-effort category from Alza URL slug."""
    if not url or not isinstance(url, str): return "Home Appliances"
    url = url.lower()
    mapping = [
        ("lednic",       "Refrigerators"),
        ("mrazak",       "Freezers"),
        ("pracka",       "Washing Machines"),
        ("susicka",      "Tumble Dryers"),
        ("mycka",        "Dishwashers"),
        ("vysavac",      "Vacuum Cleaners"),
        ("roboticky",    "Robot Vacuums"),
        ("tyc",          "Stick Vacuums"),
        ("kavov",        "Coffee Machines"),
        ("airfryer",     "Air Fryers"),
        ("mikrovln",     "Microwaves"),
        ("fritez",       "Deep Fryers"),
        ("truba",        "Ovens"),
        ("varnou",       "Cooktops"),
        ("klimatiz",     "Air Conditioners"),
        ("odvlhcovac",   "Dehumidifiers"),
        ("cistick",      "Air Purifiers"),
        ("ventilator",   "Fans"),
    ]
    for slug, cat in mapping:
        if slug in url:
            return cat
    return "Home Appliances"

# ── load existing DB data for deduplication ──────────────────────────────────

def load_existing(conn):
    names = {r[0] for r in conn.execute("SELECT lower(Name) FROM products")}
    urls  = {r[0] for r in conn.execute(
        "SELECT lower(ProductURL) FROM products WHERE ProductURL IS NOT NULL AND ProductURL != ''"
    )}
    return names, urls

# ── File 1 reader ─────────────────────────────────────────────────────────────

def read_file1(path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df = df.rename(columns={
        'Název (jméno) Lednice':                             'Name',
        'Title_URL':                                          'ProductURL',
        'Počet hvězdiček':                                    'AvgStarRating',
        'Počet hodnocení (kolik lidí dalo hvězdičky)':        'StarRatingsCount',
        'Počet uživatelských Recenzí':                        'ReviewsCount',
        'Reklamovanost (%)':                                  'ReturnRate_pct',
        '  zakazníků, kteří doporučují produkt':              'RecommendRate_pct',
        'Cena':                                               'Price_CZK',
    })
    df['ReturnRate_pct']   = df['ReturnRate_pct'].apply(clean_pct)
    df['RecommendRate_pct']= df['RecommendRate_pct'].apply(clean_pct)
    df['ReviewsCount']     = pd.to_numeric(df['ReviewsCount'], errors='coerce')
    df['AvgStarRating']    = pd.to_numeric(df['AvgStarRating'], errors='coerce')
    df['Price_CZK']        = df['Price_CZK'].apply(clean_float)
    df['Category']         = 'Refrigerators'
    df['source']           = 'alza'
    return df

# ── File 2 reader ─────────────────────────────────────────────────────────────

def read_file2(path) -> pd.DataFrame:
    df = pd.read_excel(path)
    df = df.rename(columns={
        'Name':                   'Name',
        'Name_URL':               'ProductURL',
        'starratingblock_value':  'AvgStarRating',
        'starratingblock_count':  'StarRatingsCount',
        'UzivatelskychRecenzi':   'ReviewsCount',
        'Reklamovanost':          'ReturnRate_pct',
        'YakaynikuDoporucuje':    'RecommendRate_pct',
        'Price3':                 'Price_CZK',
    })
    df['ReturnRate_pct']   = df['ReturnRate_pct'].apply(clean_pct)
    df['RecommendRate_pct']= df['RecommendRate_pct'].apply(clean_pct)
    df['ReviewsCount']     = pd.to_numeric(df['ReviewsCount'], errors='coerce')
    df['AvgStarRating']    = df['AvgStarRating'].apply(clean_float)
    df['StarRatingsCount'] = df['StarRatingsCount'].apply(clean_count)
    df['Price_CZK']        = df['Price_CZK'].apply(clean_float)
    df['Category']         = df['ProductURL'].apply(infer_category)
    df['source']           = 'alza'
    # Deduplicate by URL (keep first, i.e. canonical version over bazar)
    df = df.drop_duplicates(subset='ProductURL', keep='first')
    df = df.drop_duplicates(subset='Name', keep='first')
    return df

# ── upsert all ───────────────────────────────────────────────────────────────

MIN_REVIEWS = 10   # reliability floor — consistent with all other scrapers

def upsert_all(df: pd.DataFrame, label: str, conn: sqlite3.Connection):
    """Insert new rows, update price/rating/return-rate on existing ones (by URL)."""
    cur = conn.cursor()

    qualified = df[df['ReviewsCount'] >= MIN_REVIEWS].copy()
    print(f"\n{label}")
    print(f"  Total rows:       {len(df)}")
    print(f"  With ≥{MIN_REVIEWS} reviews:  {len(qualified)}")

    inserted = updated = skipped = 0
    for _, row in qualified.iterrows():
        name = str(row.get('Name', '') or '').strip()
        url  = str(row.get('ProductURL', '') or '').strip()
        if not name:
            continue

        existing = cur.execute(
            "SELECT rowid FROM products WHERE ProductURL = ? LIMIT 1", (url,)
        ).fetchone() if url else None

        vals = (
            str(row.get('Category', 'Home Appliances')),
            row.get('Price_CZK') if pd.notna(row.get('Price_CZK')) else None,
            row.get('AvgStarRating') if pd.notna(row.get('AvgStarRating')) else None,
            clean_count(row.get('StarRatingsCount')),
            int(row['ReviewsCount']) if pd.notna(row.get('ReviewsCount')) else None,
            row.get('RecommendRate_pct') if pd.notna(row.get('RecommendRate_pct')) else None,
            row.get('ReturnRate_pct') if pd.notna(row.get('ReturnRate_pct')) else None,
        )

        if existing:
            cur.execute(
                """UPDATE products SET
                   Category=?, Price_CZK=?, AvgStarRating=?,
                   StarRatingsCount=?, ReviewsCount=?,
                   RecommendRate_pct=?, ReturnRate_pct=?
                   WHERE ProductURL=?""",
                (*vals, url)
            )
            updated += 1
        else:
            try:
                cur.execute(
                    """INSERT INTO products
                       (Name, Category, ProductURL, Price_CZK, AvgStarRating,
                        StarRatingsCount, ReviewsCount, RecommendRate_pct,
                        ReturnRate_pct, source, country)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (name, vals[0], url or None, *vals[1:], 'alza', 'CZ')
                )
                inserted += 1
            except Exception:
                skipped += 1

    conn.commit()
    print(f"  Inserted:         {inserted}")
    print(f"  Updated:          {updated}")
    print(f"  Skipped (error):  {skipped}")
    return inserted, updated


# ── main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 55)
    print("QualityDB — Alza batch import")
    print("=" * 55)

    if not os.path.exists(DB_PATH):
        print("ERROR: products.db not found. Run load_data.py first.")
        exit(1)

    before_total = sqlite3.connect(DB_PATH).execute("SELECT COUNT(*) FROM products").fetchone()[0]

    conn = sqlite3.connect(DB_PATH)

    df1 = read_file1(FILE1)
    ins1, upd1 = upsert_all(df1, "File 1 — Fridges & Large Appliances", conn)

    df2 = read_file2(FILE2)
    ins2, upd2 = upsert_all(df2, "File 2 — Home Appliances (8 categories)", conn)

    after_total = conn.execute("SELECT COUNT(*) FROM products WHERE source='alza'").fetchone()[0]
    conn.close()

    print()
    print("=" * 55)
    print(f"Done! Inserted {ins1+ins2}, updated {upd1+upd2} Alza products.")
    print(f"Total Alza products in DB: {after_total:,}")
    print("=" * 55)
