"""
Competitor scraper — shared SQLite helpers.
"""

from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from scraper_competitors_config import CHECKPOINT_DIR, DB_PATH


# ── Table initialisation ──────────────────────────────────────────────────────

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS competitor_scores (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source              TEXT NOT NULL,
    source_url          TEXT,
    scraped_at          TEXT DEFAULT (datetime('now')),
    product_name        TEXT NOT NULL,
    brand               TEXT,
    model               TEXT,
    product_category    TEXT,
    canonical_category  TEXT,
    raw_score           REAL,
    raw_score_min       REAL,
    raw_score_max       REAL,
    raw_score_label     TEXT,
    score_normalized    REAL,
    sub_scores_json     TEXT,
    meta_json           TEXT,
    source_product_id   TEXT,
    UNIQUE(source, source_product_id)
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_cs_source   ON competitor_scores(source);
CREATE INDEX IF NOT EXISTS idx_cs_brand    ON competitor_scores(brand);
CREATE INDEX IF NOT EXISTS idx_cs_category ON competitor_scores(canonical_category);
"""


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_table() -> None:
    """Create competitor_scores table and indexes if they don't exist."""
    with get_conn() as conn:
        conn.executescript(CREATE_TABLE_SQL + CREATE_INDEX_SQL)
    print(f"[DB] competitor_scores table ready ({DB_PATH})")


def upsert_record(record: dict[str, Any]) -> None:
    """
    Insert or update a competitor score record.
    Serialises dict fields to JSON automatically.
    """
    # Serialise any dict/list fields
    for field in ("sub_scores_json", "meta_json"):
        val = record.get(field)
        if isinstance(val, (dict, list)):
            record[field] = json.dumps(val, ensure_ascii=False)

    sql = """
    INSERT INTO competitor_scores
        (source, source_url, product_name, brand, model,
         product_category, canonical_category,
         raw_score, raw_score_min, raw_score_max, raw_score_label,
         score_normalized, sub_scores_json, meta_json, source_product_id)
    VALUES
        (:source, :source_url, :product_name, :brand, :model,
         :product_category, :canonical_category,
         :raw_score, :raw_score_min, :raw_score_max, :raw_score_label,
         :score_normalized, :sub_scores_json, :meta_json, :source_product_id)
    ON CONFLICT(source, source_product_id) DO UPDATE SET
        source_url         = excluded.source_url,
        scraped_at         = datetime('now'),
        product_name       = excluded.product_name,
        brand              = excluded.brand,
        model              = excluded.model,
        product_category   = excluded.product_category,
        canonical_category = excluded.canonical_category,
        raw_score          = excluded.raw_score,
        raw_score_min      = excluded.raw_score_min,
        raw_score_max      = excluded.raw_score_max,
        raw_score_label    = excluded.raw_score_label,
        score_normalized   = excluded.score_normalized,
        sub_scores_json    = excluded.sub_scores_json,
        meta_json          = excluded.meta_json
    """

    # Fill optional keys with None
    defaults = dict(
        source=None, source_url=None, product_name=None, brand=None, model=None,
        product_category=None, canonical_category=None,
        raw_score=None, raw_score_min=None, raw_score_max=None, raw_score_label=None,
        score_normalized=None, sub_scores_json=None, meta_json=None, source_product_id=None,
    )
    defaults.update(record)

    with get_conn() as conn:
        conn.execute(sql, defaults)


def count_records(source: str | None = None) -> int:
    with get_conn() as conn:
        if source:
            row = conn.execute(
                "SELECT COUNT(*) FROM competitor_scores WHERE source = ?", (source,)
            ).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) FROM competitor_scores").fetchone()
    return row[0]


# ── Checkpoint helpers ────────────────────────────────────────────────────────

def _ckpt_path(name: str) -> str:
    os.makedirs(CHECKPOINT_DIR, exist_ok=True)
    return os.path.join(CHECKPOINT_DIR, f"competitor_{name}.json")


def get_checkpoint(name: str) -> dict[str, Any]:
    path = _ckpt_path(name)
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_checkpoint(name: str, data: dict[str, Any]) -> None:
    with open(_ckpt_path(name), "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
