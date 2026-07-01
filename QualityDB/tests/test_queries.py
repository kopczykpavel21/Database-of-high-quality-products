import sqlite3

import server


# ── query_stats ────────────────────────────────────────────────────────────

def test_query_stats_basic_counts(db):
    stats = server.query_stats()
    assert stats["total"] == 8
    assert stats["categories"] == 6          # distinct Category values
    assert stats["with_images"] == 2         # Dell XPS + Sony WH-1000XM4 already have image_url
    assert stats["avg_stars"] == 4.23        # AVG over the 7 rows with a star rating
    assert stats["with_history"] == 0        # no snapshots.db present


def test_query_stats_is_cached(db):
    first = server.query_stats()

    conn = sqlite3.connect(db)
    conn.execute("UPDATE products SET AvgStarRating = 1.0")
    conn.commit()
    conn.close()

    assert server.query_stats() == first  # served from the in-memory cache


# ── get_categories_hierarchical ──────────────────────────────────────────────

def test_get_categories_hierarchical_excludes_hidden_sources(db):
    cats = server.get_categories_hierarchical()
    mains = [c["main"] for c in cats]
    assert "Home Appliances" not in mains  # only present via the dtest row


def test_get_categories_hierarchical_orders_by_group_order(db):
    cats = server.get_categories_hierarchical()
    mains = [c["main"] for c in cats]
    assert mains.index("Phones & Tablets") < mains.index("Computers")
    assert mains.index("Computers") < mains.index("Audio")
    assert mains.index("Audio") < mains.index("TV & Video")


def test_get_categories_hierarchical_counts_subcategories(db):
    cats = server.get_categories_hierarchical()

    audio = next(c for c in cats if c["main"] == "Audio")
    assert audio["subs"] == [{"sub": "Headphones", "count": 3}]

    phones = next(c for c in cats if c["main"] == "Phones & Tablets")
    subs_by_name = {s["sub"]: s["count"] for s in phones["subs"]}
    assert subs_by_name == {"Smartphones": 1, "Tablets": 1}


# ── query_products ────────────────────────────────────────────────────────

def test_query_products_basic_pagination(db):
    result = server.query_products({})
    assert result["page"] == 1
    assert result["page_size"] == 24
    assert result["total"] == 7  # 8 rows minus the hidden dtest row
    assert len(result["products"]) == 7
    assert result["pages"] == 1


def test_query_products_category_filter(db):
    result = server.query_products({"category": ["Headphones"]})
    assert result["total"] == 3
    names = {p["Name"] for p in result["products"]}
    assert names == {"Sony WH-1000XM5", "Sony WH-1000XM4 (refurb)", "Samsung Galaxy Buds"}


def test_query_products_source_filter_matches_alza_variants(db):
    result = server.query_products({"source": ["alza"]})
    assert result["total"] == 3  # all three source='alza.cz' rows


def test_query_products_min_stars_filter(db):
    result = server.query_products({"min_stars": ["4.5"]})
    assert result["total"] == 4
    assert all(p["AvgStarRating"] >= 4.5 for p in result["products"])


def test_query_products_hidden_source_excluded_even_when_matching_query(db):
    result = server.query_products({"q": ["Washing"]})
    assert result["total"] == 0


def test_query_products_invalid_sort_falls_back_to_default(db):
    # ALLOWED_SORT whitelist must reject arbitrary strings rather than
    # interpolating them into the ORDER BY clause.
    result = server.query_products({"sort": ["Name; DROP TABLE products"]})
    assert result["total"] == 7


def test_query_products_has_history_defaults_to_zero(db):
    result = server.query_products({})
    assert result["with_history"] == 0
    assert all(p["has_history"] == 0 for p in result["products"])
