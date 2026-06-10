# Data Cleaning Outline — Legacy QualityDB → TestDB

Goal: transform the existing `products.db` (SQLite) into the new Postgres schema for the washing-machine pilot. Everything is scoped to washing machines first; scripts generalise later.

## 0. Scope filters per agency

| Agency | Source filter | Category filter |
|---|---|---|
| Warentest | `products.source = 'warentest'` | `Category = 'Washing Machines'` OR URL contains `Waschmaschin` |
| Dtest | `products.source = 'dtest'` | `details_json.subgroup` ∈ {`Pračky od 2025`, `Pračky 2023–2024`, `Pračky 2018–2022`, `Pračky 2014-2017`, `Pračky 2011-2013`, `Pračky 2010`, `Pračky 2009`, ...} (manual list — curate once) |
| Darty | `darty_products` | All 20 rows (`category = 'lave-linge hublot'`) |
| French durability | `french_durability_scores` | `category LIKE 'Washing Machines%'` |

Output: a staging CSV per agency so the Postgres load is deterministic and re-runnable.

## 1. Warentest cleaning

**Problems found:**
- `brand`, `model_number`, `ean`, `release_year` all NULL
- `Name` field often contains only a model number (e.g. `L6FBG51470`)
- Sub-ratings duplicated in `warentest_sub_ratings` table AND `details_json.sub_ratings`
- Test date stored in a weird URL suffix `!2023-3` (year-cohort)

**Steps:**
1. Parse brand from model prefix using a lookup table (e.g. `L6...` → AEG, `WAE...` → Bosch, `WW...` → Samsung, etc.). Maintain a small `warentest_model_prefix_to_brand.yaml`.
2. For products where prefix lookup fails, fall back to Google/parent page scrape or flag for manual tagging.
3. Use `warentest_sub_ratings` as source of truth for sub-ratings; verify against `details_json.sub_ratings`; log discrepancies.
4. Extract test year from the `!YYYY-N` URL suffix when `test_date` is NULL.
5. Map the Warentest 1.0–5.5 grade → `score_normalized` via:
   `score_normalized = (5.5 - grade) / 4.5 * 100`

## 2. Dtest cleaning

**Problems found:**
- `brand` column polluted with article metadata (`"Chanteclair Podrobný článek: ..."`)
- No `test_date` — lives inside `details_json.pub_date` (often NULL)
- Subgroup tells us true product type — must be used for filtering

**Steps:**
1. Parse `details_json` for every row:
   - `subgroup` → maps via `agency_categories.raw_subgroup`
   - `overall_score` (0–100, higher=better) → direct `score_normalized`
   - `overall_grade` → `score_raw_label`
   - `pub_date` → `test_batch.published_date`
   - `scores` dict → `sub_ratings` rows (one per key)
2. Clean `brand` field: strip everything after the model name. Heuristic: take text up to first occurrence of `"Podrobný článek"` or `":"`.
3. For rows with empty/bad brand, re-parse from `Name` field (usually `BrandName ModelCode`).
4. Extract `test_date` from the subgroup string when possible:
   - `"Pračky od 2025"` → 2025-01
   - `"Pračky 2023–2024"` → 2023-06 (midpoint)
   - `"Pračky 2018–2022"` → 2020-06
5. Dtest scores are already 0–100 higher-is-better — no transformation needed.

## 3. Darty cleaning

Largely clean already. Steps:
1. Map `sc_performance`, `sc_repairability`, `sc_reliability`, `sc_durability_use`, `sc_support` → canonical criteria via `agency_criteria`.
2. `durability_score` is the overall 0–100 score.
3. Parse `pdf_url` for test methodology reference.

## 4. French durability table

The `french_durability_scores` table has the structured data already. Each row has `repairability_*` sub-scores (documentation, spare parts, price, software, ease) and `durability_*` sub-scores (reliability, spare parts avail, years, software updates, repairability). Map these directly to `sub_ratings`.

## 5. Brand normalization (all agencies)

Build a single `brands` + `brand_aliases` table. Steps:
1. Collect all distinct `brand_raw` values across `agency_products`.
2. Run a fuzzy-grouping pass (RapidFuzz token_sort_ratio > 90) to cluster aliases.
3. Manual review queue for clusters with <100% confidence (will be small; maybe 50–100 brands for washing machines).
4. Populate `brand_aliases` with every seen variant.

## 6. Entity resolution across agencies

Order of attack:
1. **EAN exact match** — highest priority, zero manual review.
2. **Brand + normalized model number** (strip spaces/dashes, lowercase). High confidence.
3. **Brand + fuzzy model name** (RapidFuzz ≥ 85) → medium confidence, flag for review.
4. **Unmatched** — stay as agency_products with `variant_id = NULL`. Still valuable for per-agency analysis.

All matches land in `product_matches` with method + confidence + reviewer audit trail.

## 7. Price normalization

1. Populate `fx_rates` from ECB reference rates API (daily, back to 2000).
2. Populate `cpi_index` from Eurostat HICP (monthly, by country).
3. Compute `price_eur_at_test` at scrape time.
4. Compute `price_eur_real_2024` using CPI of product's country at `test_date`.

## 8. Sub-criteria mapping (washing machines)

Canonical criteria for washing machines (8 fits the data):

| slug | EN label | Maps from |
|---|---|---|
| wash_performance | Wash performance | WT `wash`, Dtest `praní`, Which `cleaning` |
| rinse_performance | Rinse performance | Dtest `máchání`, Which `rinse` |
| spin_efficiency | Spin efficiency | Dtest `odstřeďování`, WT (partial) |
| energy_efficiency | Energy use | Dtest `spotřeba elektrické energie`, WT `environmental` (partial) |
| water_efficiency | Water consumption | Dtest `spotřeba vody` |
| noise_level | Noise | Dtest `hlučnost`, WT (if present) |
| ease_of_use | Handling / usability | WT `handling`, Dtest `obsluha` |
| durability | Endurance / long-term | WT `endurance`, french_durability `durability_reliability` |
| cycle_time | Cycle duration | Dtest `délka pracího cyklu` |

Each mapping stored in `agency_criteria`. Manual curation — half a day of work.

## 9. Deliverables

- `/scripts/01_extract_warentest.py`
- `/scripts/02_extract_dtest.py`
- `/scripts/03_extract_darty.py`
- `/scripts/04_extract_french_durability.py`
- `/scripts/05_brand_normalization.py`
- `/scripts/06_entity_resolution.py`
- `/scripts/07_load_to_postgres.py` (uses COPY for speed)
- `/data/staging/*.csv` (intermediate output, .gitignored)
- `/data/lookups/warentest_model_prefix_to_brand.yaml` (manual curation)
- `/data/lookups/brand_aliases.yaml` (manual curation)
- `/data/lookups/agency_criteria_washing.yaml` (manual curation)
