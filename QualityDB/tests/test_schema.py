import sqlite3

import server


def _columns(conn):
    return {r[1] for r in conn.execute("PRAGMA table_info(products)")}


def _indexes(conn):
    return {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='products'"
        )
    }


def test_ensure_indexes_adds_missing_columns(legacy_db):
    conn = sqlite3.connect(legacy_db)
    new_cols = {"image_url", "brand", "NormalizedCategory", "NormalizedMainGroup",
                "first_seen_at", "qt_brand_score"}
    assert new_cols.isdisjoint(_columns(conn))

    server.ensure_indexes(conn)

    assert new_cols.issubset(_columns(conn))
    conn.close()


def test_ensure_indexes_creates_first_seen_trigger(legacy_db):
    conn = sqlite3.connect(legacy_db)
    server.ensure_indexes(conn)
    triggers = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'")}
    assert "trg_products_first_seen" in triggers
    conn.close()


def test_first_seen_at_set_on_insert_only_when_null(legacy_db):
    conn = sqlite3.connect(legacy_db)
    server.ensure_indexes(conn)

    conn.execute("INSERT INTO products (Name, source) VALUES ('Auto-stamped', 'alza.cz')")
    conn.execute(
        "INSERT INTO products (Name, source, first_seen_at) VALUES (?, 'alza.cz', ?)",
        ("Pre-stamped", "2020-01-01 00:00:00"),
    )
    conn.commit()

    auto = conn.execute("SELECT first_seen_at FROM products WHERE Name='Auto-stamped'").fetchone()[0]
    pre = conn.execute("SELECT first_seen_at FROM products WHERE Name='Pre-stamped'").fetchone()[0]
    assert auto is not None
    assert pre == "2020-01-01 00:00:00"
    conn.close()


def test_ensure_indexes_idempotent(legacy_db):
    conn = sqlite3.connect(legacy_db)
    server.ensure_indexes(conn)
    server.ensure_indexes(conn)  # must not raise on a second call
    conn.close()


def test_ensure_indexes_creates_all_documented_indexes(legacy_db):
    """Regression test: the executescript references country/Price_EUR/scraped_at
    columns in CREATE INDEX statements. If the column-migration loop above ever
    stops adding those three, executescript() fails on the idx_country statement
    with 'OperationalError: no such column: country' and silently drops the
    other 10 indexes (idx_price_czk, idx_keywords, idx_norm_category,
    idx_norm_main_group, idx_scraped_at, idx_first_seen_at, idx_name, ...).
    """
    conn = sqlite3.connect(legacy_db)
    server.ensure_indexes(conn)

    expected = {
        "idx_source", "idx_main_category", "idx_country", "idx_source_cat",
        "idx_rec_rate", "idx_price_czk", "idx_price_eur", "idx_keywords",
        "idx_norm_category", "idx_norm_main_group", "idx_scraped_at",
        "idx_first_seen_at", "idx_name",
    }
    missing = expected - _indexes(conn)
    assert not missing, f"ensure_indexes did not create: {sorted(missing)}"
    conn.close()
