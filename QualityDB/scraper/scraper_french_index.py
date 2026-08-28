"""
French Repairability Index scraper.

Data source: data.gouv.fr — open CC BY licence
Each manufacturer publishes their own dataset.
We discover all ~100+ datasets via the API, then download each CSV.

Column mapping (semicolon-delimited CSV):
  nom_modele              → product_name
  categorie_produit       → product_category
  nom_metteur_sur_le_marche → brand
  note_ir                 → raw_score (0–20 scale! then normalized to 0–100)
  note_c1..note_c5        → sub-scores (C1=documentation, C2=disassembly,
                            C3=spare parts, C4=price, C5=category-specific)

NOTE: The raw score is on a 0–20 scale (sum of 5 criteria, each 0–4 max),
not 0–10 as originally assumed. Normalize: score_normalized = note_ir * 5
"""

from __future__ import annotations

import csv
import hashlib
import io
import time

import requests

from scraper_competitors_config import REQUEST_TIMEOUT, USER_AGENT, canonical_category
from scraper_competitors_db import count_records, get_checkpoint, init_table, save_checkpoint, upsert_record

SOURCE = "french_index"
API_BASE = "https://www.data.gouv.fr/api/1"
DATASET_PAGE = "https://www.data.gouv.fr/fr/datasets/?q=indice+de+reparabilite"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    s.headers["Accept"] = "application/json"
    return s


def _make_product_id(product_id: str, manufacturer_id: str) -> str:
    """Use the dataset's own unique ID field."""
    key = f"{product_id}|{manufacturer_id}"
    return hashlib.md5(key.encode()).hexdigest()[:20]


def _parse_score(val: str) -> float | None:
    """Parse '8.3' or '8,3' or '' into float."""
    if not val or str(val).strip() in ("", "-", "N/A", "NA", "None"):
        return None
    try:
        return float(str(val).strip().replace(",", "."))
    except ValueError:
        return None


def _discover_datasets(session: requests.Session) -> list[dict]:
    """
    Query data.gouv.fr API to discover all French Index datasets.
    Returns list of {id, title, csv_url} dicts.
    """
    datasets = []
    page = 1
    page_size = 100

    print("  Discovering datasets via data.gouv.fr API...")
    while True:
        params = {
            "q": "indice de reparabilite",
            "page_size": page_size,
            "page": page,
        }
        try:
            resp = session.get(f"{API_BASE}/datasets/", params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [!] API error on page {page}: {e}")
            break

        items = data.get("data", [])
        if not items:
            break

        for dataset in items:
            title = dataset.get("title", "")
            # Find CSV resource
            for resource in dataset.get("resources", []):
                fmt = resource.get("format", "").lower()
                url = resource.get("url", "")
                if fmt == "csv" or url.endswith(".csv"):
                    datasets.append({
                        "id": dataset["id"],
                        "title": title,
                        "csv_url": url,
                        "organization": dataset.get("organization", {}).get("name", "") if dataset.get("organization") else "",
                    })
                    break  # one CSV per dataset

        print(f"    Page {page}: found {len(items)} datasets ({len(datasets)} with CSV so far)")

        total = data.get("total", 0)
        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.3)

    print(f"  Total datasets with CSV: {len(datasets)}")
    return datasets


def _process_csv(content: str, dataset_info: dict) -> int:
    """Parse CSV content and upsert records. Returns count of rows processed."""
    # Detect BOM
    if content.startswith("﻿"):
        content = content[1:]

    # Semicolon-delimited
    reader = csv.DictReader(io.StringIO(content), delimiter=";")

    count = 0
    for row in reader:
        # Normalise column names
        row_norm = {k.strip().lower(): v for k, v in row.items() if k}

        # Product name
        product_name = row_norm.get("nom_modele", "").strip()
        if not product_name:
            continue

        # Brand — from the dataset directly
        brand = row_norm.get("nom_metteur_sur_le_marche", "").strip()
        if not brand:
            brand = dataset_info.get("organization", "").strip()
        if not brand:
            brand = dataset_info.get("title", "").replace("Indice de réparabilité", "").strip(" -()").strip()

        # Category
        category_raw = row_norm.get("categorie_produit", "").strip()

        # Main score — note_ir is on 0–20 scale
        raw_score = _parse_score(row_norm.get("note_ir", ""))
        if raw_score is None:
            continue

        # Normalize: 0–20 → 0–100
        score_normalized = round(raw_score * 5, 1)

        # Sub-scores (each criterion)
        sub_scores = {}
        criterion_names = {
            "note_c1": "documentation",
            "note_c2": "disassembly",
            "note_c3": "spare_parts_availability",
            "note_c4": "spare_parts_price",
            "note_c5": "category_specific",
        }
        for col, key in criterion_names.items():
            val = _parse_score(row_norm.get(col, ""))
            if val is not None:
                sub_scores[key] = val

        # Unique product ID (use the dataset's own ID field if available)
        dataset_own_id = row_norm.get("id_unique", "").strip()
        if not dataset_own_id:
            dataset_own_id = row_norm.get("id_modele", "").strip()
        source_id = _make_product_id(dataset_own_id or product_name, dataset_info["id"])

        # Extra metadata
        meta = {
            "dataset_id": dataset_info["id"],
            "dataset_title": dataset_info.get("title", ""),
        }
        for col in ("date_calcul", "id_modele", "referentiel_id_modele"):
            v = row_norm.get(col, "").strip()
            if v:
                meta[col] = v

        upsert_record({
            "source":             SOURCE,
            "source_url":         dataset_info["csv_url"],
            "product_name":       product_name,
            "brand":              brand.title() if brand else None,
            "model":              product_name,
            "product_category":   category_raw,
            "canonical_category": canonical_category(category_raw.lower()),
            "raw_score":          raw_score,
            "raw_score_min":      0.0,
            "raw_score_max":      20.0,
            "raw_score_label":    f"{raw_score}/20",
            "score_normalized":   score_normalized,
            "sub_scores_json":    sub_scores if sub_scores else None,
            "meta_json":          meta,
            "source_product_id":  source_id,
        })
        count += 1

    return count


def scrape() -> int:
    """Download all French Index CSV files, parse and store. Returns total rows inserted."""
    init_table()
    session = _session()
    checkpoint = get_checkpoint(SOURCE)
    total = 0

    # Step 1: Discover all datasets (cache the list)
    if checkpoint.get("datasets_discovered"):
        datasets = checkpoint["datasets_discovered"]
        print(f"\n[french_index] Using {len(datasets)} cached dataset URLs")
    else:
        print(f"\n[french_index] Discovering datasets...")
        datasets = _discover_datasets(session)
        checkpoint["datasets_discovered"] = datasets
        save_checkpoint(SOURCE, checkpoint)

    print(f"[french_index] Processing {len(datasets)} manufacturer CSVs")

    # Step 2: Download and process each CSV
    for i, dataset in enumerate(datasets):
        ds_id = dataset["id"]

        if checkpoint.get(ds_id) == "done":
            continue

        title = dataset.get("title", "")[:50]
        print(f"  [{i+1}/{len(datasets)}] {title}...")

        try:
            resp = session.get(dataset["csv_url"], timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
        except Exception as e:
            print(f"    [!] Download failed: {e}")
            checkpoint[ds_id] = "failed"
            save_checkpoint(SOURCE, checkpoint)
            time.sleep(0.5)
            continue

        # Detect encoding
        content = resp.content.decode(resp.encoding or "utf-8-sig", errors="replace")
        n = _process_csv(content, dataset)
        total += n
        print(f"    {n} rows")

        checkpoint[ds_id] = "done"
        if (i + 1) % 10 == 0:
            save_checkpoint(SOURCE, checkpoint)
        time.sleep(0.3)

    save_checkpoint(SOURCE, checkpoint)
    print(f"\n[french_index] Done — {total} rows total. DB now has {count_records(SOURCE)} {SOURCE} records.")
    return total


if __name__ == "__main__":
    scrape()
