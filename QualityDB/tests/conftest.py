import os
import sqlite3
import sys

import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import server  # noqa: E402


# Columns present on the products table as it exists today (see
# `sqlite3 products.db ".schema products"`), i.e. *before* ensure_indexes()
# has ever run on it.
LEGACY_COLUMNS = [
    ("id", "INTEGER"),
    ("Name", "TEXT"),
    ("Category", "TEXT"),
    ("ProductURL", "TEXT"),
    ("Price_CZK", "REAL"),
    ("AvgStarRating", "REAL"),
    ("StarRatingsCount", "REAL"),
    ("ReviewsCount", "REAL"),
    ("RecommendRate_pct", "REAL"),
    ("ReturnRate_pct", "REAL"),
    ("Stars5_Count", "REAL"),
    ("Stars4_Count", "REAL"),
    ("Stars3_Count", "REAL"),
    ("Stars2_Count", "REAL"),
    ("Stars1_Count", "REAL"),
    ("Description", "TEXT"),
    ("SKU", "TEXT"),
    ("source", "TEXT DEFAULT 'alza'"),
    ("keywords", "TEXT"),
    ("MainCategory", "TEXT"),
]

# Columns that query_products()/query_stats() read but that ensure_indexes()
# never adds itself (it only ALTERs in image_url/brand/NormalizedCategory/
# NormalizedMainGroup/first_seen_at/qt_brand_score). On the live DB these
# exist thanks to older one-off migration scripts; the "fully migrated"
# fixture below adds them directly so query-layer tests have a realistic
# schema to run against.
EXTRA_COLUMNS = [
    ("Price_EUR", "REAL"),
    ("country", "TEXT"),
    ("currency", "TEXT"),
    ("cat_rank", "INTEGER"),
    ("cat_total", "INTEGER"),
    ("test_date", "TEXT"),
    ("repairability_score_fr", "REAL"),
    ("repairability_score_date", "TEXT"),
    ("repairability_sub_scores_json", "TEXT"),
    ("durability_score_fr", "REAL"),
    ("durability_sub_scores_json", "TEXT"),
    ("warranty_years", "REAL"),
    ("energy_class", "TEXT"),
    ("details_json", "TEXT"),
    ("scraped_at", "TEXT"),
]


def make_legacy_db(path):
    conn = sqlite3.connect(path)
    cols_sql = ",\n".join(f'"{c}" {t}' for c, t in LEGACY_COLUMNS)
    conn.execute(f"CREATE TABLE products (\n{cols_sql}\n)")
    conn.execute("CREATE INDEX ix_products_id ON products(id)")
    conn.execute("CREATE INDEX idx_category ON products(Category)")
    conn.execute("CREATE INDEX idx_return_rate ON products(ReturnRate_pct)")
    conn.execute("CREATE INDEX idx_stars ON products(AvgStarRating)")
    conn.execute("CREATE INDEX idx_reviews ON products(ReviewsCount)")
    conn.commit()
    return conn


@pytest.fixture
def reset_caches(monkeypatch):
    """Module-level result caches that would otherwise leak between tests."""
    monkeypatch.setattr(server, "_stats_cache", None)
    monkeypatch.setattr(server, "_stats_ts", 0.0)
    monkeypatch.setattr(server, "_snapshot_coverage_cache", None)
    monkeypatch.setattr(server, "_snapshot_coverage_set", None)
    monkeypatch.setattr(server, "_snapshot_coverage_days", None)
    monkeypatch.setattr(server, "_snapshot_coverage_ts", 0.0)


@pytest.fixture
def legacy_db(tmp_path, monkeypatch, reset_caches):
    """Empty DB using the schema products.db has *today*, pre-migration."""
    db_path = tmp_path / "products.db"
    conn = make_legacy_db(db_path)
    conn.close()
    monkeypatch.setattr(server, "DB_PATH", str(db_path))
    monkeypatch.setenv("SNAPSHOTS_DB_PATH", str(tmp_path / "snapshots.db"))
    return db_path


# A small representative catalogue covering each image-population source,
# a sibling-image pair (Sony/Headphones), a low-rated product, and a
# hidden-source (dtest) row.
SAMPLE_PRODUCTS = [
    dict(
        Name="Apple iPhone 15 128GB", Category="Mobilní telefony", MainCategory="Elektronika",
        ProductURL="https://www.alza.cz/apple-iphone-15-d12345678.htm",
        Price_CZK=24999, AvgStarRating=4.8, StarRatingsCount=500, ReviewsCount=500,
        RecommendRate_pct=95.0, ReturnRate_pct=2.1, source="alza.cz", SKU="APP0123",
        keywords='["phone","apple"]',
        brand="Apple", NormalizedCategory="Smartphones", NormalizedMainGroup="Phones & Tablets",
        image_url=None,
    ),
    dict(
        Name="Dell XPS 13", Category="Notebooky", MainCategory="Elektronika",
        ProductURL="https://www.alza.cz/dell-xps-13-d99999999.htm",
        Price_CZK=29999, AvgStarRating=4.2, StarRatingsCount=80, ReviewsCount=80,
        RecommendRate_pct=88.0, ReturnRate_pct=4.0, source="alza.cz", SKU=None,
        brand="Dell", NormalizedCategory="Laptops", NormalizedMainGroup="Computers",
        image_url="https://cdn.alza.cz/ImgW.ashx?fd=f10&cd=DEL999&i=1.jpg",
    ),
    dict(
        Name="Sony WH-1000XM5", Category="Headphones", MainCategory="Elektronika",
        ProductURL="https://www.amazon.de/dp/B09XS7JWHW/ref=sr_1_1",
        Price_CZK=8999, AvgStarRating=4.7, StarRatingsCount=1200, ReviewsCount=1200,
        RecommendRate_pct=92.0, ReturnRate_pct=3.0, source="amazon.de",
        brand="Sony", NormalizedCategory="Headphones", NormalizedMainGroup="Audio",
        image_url=None,
    ),
    dict(
        Name="Sony WH-1000XM4 (refurb)", Category="Headphones", MainCategory="Elektronika",
        ProductURL="https://www.heureka.cz/sony-wh-1000xm4/",
        Price_CZK=6999, AvgStarRating=4.5, StarRatingsCount=300, ReviewsCount=300,
        RecommendRate_pct=89.0, ReturnRate_pct=3.5, source="heureka.cz",
        brand="Sony", NormalizedCategory="Headphones", NormalizedMainGroup="Audio",
        image_url="https://cdn.heureka.cz/sony-wh1000xm4.jpg",
    ),
    dict(
        Name="Samsung Galaxy Buds", Category="Headphones", MainCategory="Elektronika",
        ProductURL="https://www.coolblue.nl/product/912345/samsung-galaxy-buds.html",
        Price_CZK=2499, AvgStarRating=4.0, StarRatingsCount=50, ReviewsCount=50,
        RecommendRate_pct=80.0, ReturnRate_pct=5.0, source="coolblue",
        brand="Samsung", NormalizedCategory="Headphones", NormalizedMainGroup="Audio",
        image_url=None,
    ),
    dict(
        Name="LG OLED TV C3", Category="Televize", MainCategory="Elektronika",
        ProductURL="https://www.prisjakt.nu/produkt.php?p=4567890",
        Price_CZK=24999, AvgStarRating=4.6, StarRatingsCount=200, ReviewsCount=200,
        RecommendRate_pct=91.0, ReturnRate_pct=2.5, source="prisjakt",
        brand="LG", NormalizedCategory="TVs", NormalizedMainGroup="TV & Video",
        image_url=None,
    ),
    dict(
        Name="Cheap Tablet X", Category="Tablety", MainCategory="Elektronika",
        ProductURL="https://www.alza.cz/cheap-tablet-x-d11111111.htm",
        Price_CZK=1999, AvgStarRating=2.8, StarRatingsCount=150, ReviewsCount=150,
        RecommendRate_pct=40.0, ReturnRate_pct=18.0, source="alza.cz", SKU="CHP001",
        brand="NoName", NormalizedCategory="Tablets", NormalizedMainGroup="Phones & Tablets",
        image_url=None,
    ),
    dict(
        Name="D-Test Washing Machine", Category="Pračky", MainCategory="Domácnost",
        ProductURL="https://www.dtest.cz/washing-machine",
        Price_CZK=0, AvgStarRating=None, StarRatingsCount=0, ReviewsCount=0,
        RecommendRate_pct=None, ReturnRate_pct=None, source="dtest",
        brand=None, NormalizedCategory="Washing Machines", NormalizedMainGroup="Home Appliances",
        image_url=None,
    ),
]


def _insert_products(conn, rows):
    for row in rows:
        cols = list(row.keys())
        placeholders = ",".join("?" for _ in cols)
        conn.execute(
            f"INSERT INTO products ({','.join(cols)}) VALUES ({placeholders})",
            [row[c] for c in cols],
        )
    conn.commit()


@pytest.fixture
def db(tmp_path, monkeypatch, reset_caches):
    """Fully migrated DB (legacy schema + EXTRA_COLUMNS + ensure_indexes())
    seeded with SAMPLE_PRODUCTS, with server.DB_PATH pointed at it."""
    db_path = tmp_path / "products.db"
    conn = make_legacy_db(db_path)
    for col, typedef in EXTRA_COLUMNS:
        conn.execute(f"ALTER TABLE products ADD COLUMN {col} {typedef}")
    conn.commit()
    server.ensure_indexes(conn)
    conn.commit()
    _insert_products(conn, SAMPLE_PRODUCTS)
    conn.close()

    monkeypatch.setattr(server, "DB_PATH", str(db_path))
    monkeypatch.setenv("SNAPSHOTS_DB_PATH", str(tmp_path / "snapshots.db"))
    return db_path
