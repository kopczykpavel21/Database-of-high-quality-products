#!/usr/bin/env python3
"""
extract_fr_spare_parts.py — the spare-parts panel hidden in the FR index CSVs
═════════════════════════════════════════════════════════════════════════════

`indicedurabilite_scraper.py` stores ~26 of the source CSV's 234 columns. Most
of the remainder is a spare-parts declaration matrix:

    4 supply channels (repairer / producer / distributor / consumer)
  × 15 parts (washing machines) or 12 (televisions)
  × {part name, delivery delay, availability duration}

That is ~116k model×part×channel observations with a standardised part
taxonomy, legally mandated and published per model — the panel for the
spare-parts price/availability study.

Why the existing importer drops it
──────────────────────────────────
Every value is BANDED, not continuous: delays are `<3` / `<5` / `<10` / `>11`,
availability is `>15` / `15` / `13` / `<11` (washers) or `>11` / `9` / `7` /
`<7` (televisions), and either can be `Non disponible`. The importer's
`_safe_float()` silently nulls all of them, so a naive read sees an empty
matrix rather than an error.

How this stores them
────────────────────
Never coerced to a false point estimate. Each observation keeps:
  * the verbatim band (`*_band`)
  * interval bounds `*_lo` / `*_hi`, NULL where the interval is open
  * `*_censoring` ∈ exact | below | above | unavailable
  * `*_ordinal`, the band's rank within its own category's vocabulary — the
    analytically correct variable, since these are ordered categories

Ordinal ranks are derived from the parsed bounds at run time, NOT hardcoded, so
the categories the AGEC law adds later rank themselves. The vocabularies really
are category-specific (washers are regulated on 15/13/11 years, televisions on
11/9/7), so ranks are computed per (category, field) and are NOT comparable
across categories.

Usage:
    python3 extract_fr_spare_parts.py --db ~/QualityData/qualitydb/products.db
    python3 extract_fr_spare_parts.py --db ... --dry-run
"""
from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import sqlite3
import sys
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone

log = logging.getLogger("fr_spare_parts")

RESOURCE_URL_TPL = "https://www.data.gouv.fr/api/1/datasets/r/{rid}"

# Same resources indicedurabilite_scraper.py pulls; slot counts differ per
# category because the two arrêtés list different mandated parts.
DATASETS = {
    "Lave-linge":  {"resource": "53b4d56e-5e23-4e52-9527-9921ef1934d0", "slots": 15},
    "Téléviseur":  {"resource": "3df7e799-cba8-4f74-b33c-6d9db9b6361e", "slots": 12},
}

CHANNELS = ("reparateur", "producteur", "distributeur", "consommateur")
CHANNEL_EN = {
    "reparateur":   "repairer",
    "producteur":   "producer",
    "distributeur": "distributor",
    "consommateur": "consumer",
}

UNAVAILABLE = {"non disponible", "non disponibles", "indisponible"}
NOT_DISASSEMBLABLE = {"non démontable", "non demontable"}

DDL = """
CREATE TABLE IF NOT EXISTS fr_spare_parts (
    fr_id              INTEGER,
    id_unique          TEXT    NOT NULL,
    main_category      TEXT,
    categorie_produit  TEXT,
    market_placer      TEXT,
    nom_modele         TEXT,
    channel            TEXT    NOT NULL,   -- repairer|producer|distributor|consumer
    slot               INTEGER NOT NULL,
    part_name          TEXT,
    delay_band         TEXT,
    delay_days_lo      REAL,
    delay_days_hi      REAL,
    delay_censoring    TEXT,
    delay_ordinal      INTEGER,
    years_band         TEXT,
    years_lo           REAL,
    years_hi           REAL,
    years_censoring    TEXT,
    years_ordinal      INTEGER,
    extracted_at       TEXT NOT NULL,
    PRIMARY KEY (id_unique, channel, slot)
);
CREATE INDEX IF NOT EXISTS ix_frsp_fr_id   ON fr_spare_parts(fr_id);
CREATE INDEX IF NOT EXISTS ix_frsp_part    ON fr_spare_parts(part_name);
CREATE INDEX IF NOT EXISTS ix_frsp_chan    ON fr_spare_parts(main_category, channel);

CREATE TABLE IF NOT EXISTS fr_disassembly_steps (
    fr_id              INTEGER,
    id_unique          TEXT    NOT NULL,
    main_category      TEXT,
    categorie_produit  TEXT,
    market_placer      TEXT,
    nom_modele         TEXT,
    part_list          INTEGER NOT NULL,   -- 2 = priority parts list
    slot               INTEGER NOT NULL,
    part_name          TEXT,
    steps_band         TEXT,
    steps_lo           REAL,
    steps_hi           REAL,
    steps_censoring    TEXT,
    steps_ordinal      INTEGER,
    not_disassemblable INTEGER NOT NULL DEFAULT 0,
    extracted_at       TEXT NOT NULL,
    PRIMARY KEY (id_unique, part_list, slot)
);
CREATE INDEX IF NOT EXISTS ix_frds_fr_id ON fr_disassembly_steps(fr_id);
CREATE INDEX IF NOT EXISTS ix_frds_part  ON fr_disassembly_steps(part_name);
"""


def parse_band(v: str | None) -> tuple[str | None, float | None, float | None, str | None]:
    """'<3' -> (band, lo, hi, censoring). Returns (None,…) for blanks.

    `<N`  the declared value is under a threshold  -> [0, N],  censoring 'below'
    `>N`  the declared value exceeds a threshold   -> [N, ∞),  censoring 'above'
    `N`   an exact declared figure                 -> [N, N],  censoring 'exact'
    Textual unavailability keeps the verbatim band with no bounds.
    """
    if v is None:
        return None, None, None, None
    s = v.strip()
    if not s:
        return None, None, None, None
    low = s.lower()
    if low in UNAVAILABLE:
        return s, None, None, "unavailable"
    if low in NOT_DISASSEMBLABLE:
        return s, None, None, "not_disassemblable"
    t = s.replace(",", ".").replace(" ", "")
    try:
        if t.startswith("<"):
            return s, 0.0, float(t[1:]), "below"
        if t.startswith(">"):
            return s, float(t[1:]), None, "above"
        if t.startswith("≤"):
            return s, 0.0, float(t[1:]), "below"
        if t.startswith("≥"):
            return s, float(t[1:]), None, "above"
        n = float(t)
        return s, n, n, "exact"
    except ValueError:
        log.debug("unparseable band: %r", s)
        return s, None, None, "unparsed"


def sort_key(lo, hi, cens):
    """Representative magnitude, so bands rank in true numeric order."""
    if cens == "below":
        return (hi if hi is not None else 0.0) - 1e-6
    if cens == "above":
        return (lo if lo is not None else 0.0) + 1e-6
    if cens == "exact":
        return lo
    return None  # unavailable / unparsed: unranked


def fetch_csv(rid: str) -> list[dict]:
    url = RESOURCE_URL_TPL.format(rid=rid)
    log.info("downloading %s", url)
    with urllib.request.urlopen(url, timeout=180) as r:
        raw = r.read()
    text = raw.decode("utf-8-sig", errors="replace")
    return list(csv.DictReader(io.StringIO(text), delimiter=","))


def extract(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    id_map = {u: (i, mc) for u, i, mc in conn.execute(
        "SELECT id_unique, id, main_category FROM fr_durability_index WHERE id_unique IS NOT NULL")}

    parts_rows: list[list] = []
    steps_rows: list[list] = []
    stats: dict = {
        "models": 0, "part_obs": 0, "step_obs": 0, "unlinked": 0,
        "delay_bands": defaultdict(int), "years_bands": defaultdict(int),
        "steps_bands": defaultdict(int), "channels": defaultdict(int),
    }
    # (category, field) -> {band: (lo, hi, cens)} for ordinal ranking
    vocab: dict[tuple, dict] = defaultdict(dict)

    for cat_fr, meta in DATASETS.items():
        rows = fetch_csv(meta["resource"])
        slots = meta["slots"]
        log.info("%s: %d models, %d slots x %d channels", cat_fr, len(rows), slots, len(CHANNELS))

        for r in rows:
            uniq = (r.get("id_unique") or "").strip()
            if not uniq:
                continue
            fr_id, main_cat = id_map.get(uniq, (None, None))
            if fr_id is None:
                stats["unlinked"] += 1
            stats["models"] += 1
            placer = (r.get("nom_metteur_sur_le_marche") or "").strip()
            model = (r.get("nom_modele") or "").strip()
            cp = (r.get("categorie_produit") or cat_fr).strip()

            for ch in CHANNELS:
                for i in range(1, slots + 1):
                    pname = (r.get(f"nom_piece_detachee_{i}_{ch}") or "").strip()
                    dband, dlo, dhi, dcen = parse_band(r.get(f"delai_jours_piece_detachee_{i}_{ch}"))
                    yband, ylo, yhi, ycen = parse_band(
                        r.get(f"nb_annees_disponibilite_piece_detachee_{i}_{ch}"))
                    if not pname and dband is None and yband is None:
                        continue
                    if dband:
                        stats["delay_bands"][dband] += 1
                        vocab[(cat_fr, "delay")][dband] = (dlo, dhi, dcen)
                    if yband:
                        stats["years_bands"][yband] += 1
                        vocab[(cat_fr, "years")][yband] = (ylo, yhi, ycen)
                    stats["channels"][CHANNEL_EN[ch]] += 1
                    stats["part_obs"] += 1
                    parts_rows.append([fr_id, uniq, main_cat, cp, placer, model,
                                       CHANNEL_EN[ch], i, pname or None,
                                       dband, dlo, dhi, dcen, None,
                                       yband, ylo, yhi, ycen, None, now, cat_fr])

            for lst in (1, 2):
                for i in range(1, 6):
                    pname = (r.get(f"nom_piece_{i}_liste_{lst}") or "").strip()
                    sband, slo, shi, scen = parse_band(r.get(f"etape_demontage_piece_{i}_liste_{lst}"))
                    if not pname and sband is None:
                        continue
                    if sband:
                        stats["steps_bands"][sband] += 1
                        vocab[(cat_fr, "steps")][sband] = (slo, shi, scen)
                    stats["step_obs"] += 1
                    steps_rows.append([fr_id, uniq, main_cat, cp, placer, model,
                                       lst, i, pname or None,
                                       sband, slo, shi, scen, None,
                                       1 if scen == "not_disassemblable" else 0, now, cat_fr])

    # ── rank the bands within each (category, field) vocabulary ──────────────
    ranks: dict[tuple, dict] = {}
    for k, bands in vocab.items():
        ordered = sorted(
            ((b, sort_key(*v)) for b, v in bands.items() if sort_key(*v) is not None),
            key=lambda x: x[1])
        ranks[k] = {b: n for n, (b, _) in enumerate(ordered, start=1)}
        log.info("ordinal scale %-24s %s", f"{k[0]}/{k[1]}",
                 " < ".join(f"{b}({n})" for b, n in ranks[k].items()))

    for row in parts_rows:
        cat_fr = row[-1]
        row[13] = ranks.get((cat_fr, "delay"), {}).get(row[9])
        row[18] = ranks.get((cat_fr, "years"), {}).get(row[14])
    for row in steps_rows:
        cat_fr = row[-1]
        row[13] = ranks.get((cat_fr, "steps"), {}).get(row[9])

    if dry_run:
        return stats

    conn.executescript(DDL)
    conn.executemany(
        """INSERT INTO fr_spare_parts
             (fr_id,id_unique,main_category,categorie_produit,market_placer,nom_modele,
              channel,slot,part_name,delay_band,delay_days_lo,delay_days_hi,delay_censoring,
              delay_ordinal,years_band,years_lo,years_hi,years_censoring,years_ordinal,
              extracted_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(id_unique,channel,slot) DO UPDATE SET
             fr_id=excluded.fr_id, part_name=excluded.part_name,
             delay_band=excluded.delay_band, delay_days_lo=excluded.delay_days_lo,
             delay_days_hi=excluded.delay_days_hi, delay_censoring=excluded.delay_censoring,
             delay_ordinal=excluded.delay_ordinal, years_band=excluded.years_band,
             years_lo=excluded.years_lo, years_hi=excluded.years_hi,
             years_censoring=excluded.years_censoring, years_ordinal=excluded.years_ordinal,
             extracted_at=excluded.extracted_at""",
        [r[:-1] for r in parts_rows])
    conn.executemany(
        """INSERT INTO fr_disassembly_steps
             (fr_id,id_unique,main_category,categorie_produit,market_placer,nom_modele,
              part_list,slot,part_name,steps_band,steps_lo,steps_hi,steps_censoring,
              steps_ordinal,not_disassemblable,extracted_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(id_unique,part_list,slot) DO UPDATE SET
             fr_id=excluded.fr_id, part_name=excluded.part_name,
             steps_band=excluded.steps_band, steps_lo=excluded.steps_lo,
             steps_hi=excluded.steps_hi, steps_censoring=excluded.steps_censoring,
             steps_ordinal=excluded.steps_ordinal,
             not_disassemblable=excluded.not_disassemblable,
             extracted_at=excluded.extracted_at""",
        [r[:-1] for r in steps_rows])
    conn.commit()
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--db", default=os.environ.get("DB_PATH"),
                    help="path to products.db (REQUIRED — config.DB_PATH is a stale copy)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(levelname)s %(message)s")
    if not args.db:
        log.error("--db is required (or set DB_PATH)")
        return 2
    db = os.path.expanduser(args.db)
    if not os.path.exists(db):
        log.error("no such database: %s", db)
        return 2

    conn = sqlite3.connect(db)
    try:
        s = extract(conn, dry_run=args.dry_run)
    finally:
        conn.close()

    log.info("models read            : %d", s["models"])
    log.info("part observations      : %d%s", s["part_obs"],
             "  (dry run, nothing written)" if args.dry_run else "")
    log.info("disassembly observations: %d", s["step_obs"])
    if s["unlinked"]:
        log.warning("models with no fr_durability_index row: %d", s["unlinked"])
    log.info("by channel: %s", dict(s["channels"]))
    log.info("delay bands: %s", dict(s["delay_bands"]))
    log.info("years bands: %s", dict(s["years_bands"]))
    log.info("steps bands: %s", dict(s["steps_bands"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
