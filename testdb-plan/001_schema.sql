-- =====================================================================
-- TestDB — Multi-Agency Product Testing Database
-- Initial schema (Alembic revision 001)
-- Target: PostgreSQL 15+
-- =====================================================================

-- ---------- ENUMS ----------
CREATE TYPE score_direction AS ENUM ('higher_is_better', 'lower_is_better');
CREATE TYPE recommendation_normalized AS ENUM ('recommended', 'neutral', 'avoid');
CREATE TYPE match_method AS ENUM ('ean_exact', 'gtin_exact', 'fuzzy_auto', 'manual', 'unmatched');

-- ---------- REFERENCE DATA ----------

CREATE TABLE agencies (
    id                   SERIAL PRIMARY KEY,
    slug                 TEXT UNIQUE NOT NULL,      -- 'warentest', 'dtest', 'which', 'ufc', '60m', 'darty'
    name                 TEXT NOT NULL,
    country_code         CHAR(2) NOT NULL,          -- ISO 3166-1
    base_url             TEXT,
    score_min            NUMERIC NOT NULL,
    score_max            NUMERIC NOT NULL,
    direction            score_direction NOT NULL,
    reliability_weight   NUMERIC NOT NULL DEFAULT 1.0, -- tweak for composite brand scoring
    notes                TEXT
);

-- Canonical category taxonomy (agency-agnostic).
-- Hierarchical: 'large_home_appliances' -> 'washing_machines' -> 'washer_dryers'.
CREATE TABLE categories (
    id          SERIAL PRIMARY KEY,
    slug        TEXT UNIQUE NOT NULL,           -- 'washing_machines'
    name_en     TEXT NOT NULL,
    parent_id   INT REFERENCES categories(id),
    icon        TEXT
);
CREATE INDEX ix_categories_parent ON categories(parent_id);

-- ---------- LAYER 1: Raw / provenance ----------

-- Every HTTP fetch is logged here. Raw HTML is stored on disk/object storage;
-- content_hash lets us detect silent revisions.
CREATE TABLE scrapes (
    id              BIGSERIAL PRIMARY KEY,
    agency_id       INT NOT NULL REFERENCES agencies(id),
    url             TEXT NOT NULL,
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    http_status     INT,
    content_hash    CHAR(64) NOT NULL,  -- SHA-256 of response body
    raw_artifact    TEXT,               -- filesystem or s3 path to gzipped HTML/JSON
    parser_version  TEXT NOT NULL,      -- e.g. 'warentest_v1.3'
    notes           TEXT
);
CREATE INDEX ix_scrapes_agency_url ON scrapes(agency_id, url);
CREATE INDEX ix_scrapes_hash ON scrapes(content_hash);

-- Maps an agency's raw category strings (possibly hierarchical) to a canonical category.
-- For Dtest: raw_category='Pračky a péče o prádlo', raw_subgroup='Pračky od 2025' -> washing_machines
CREATE TABLE agency_categories (
    id             SERIAL PRIMARY KEY,
    agency_id      INT NOT NULL REFERENCES agencies(id),
    raw_category   TEXT NOT NULL,
    raw_subgroup   TEXT,                    -- Dtest-style finer level; NULL if agency doesn't expose
    category_id    INT NOT NULL REFERENCES categories(id),
    UNIQUE(agency_id, raw_category, raw_subgroup)
);

-- Canonical sub-criteria per category.
CREATE TABLE criteria (
    id          SERIAL PRIMARY KEY,
    category_id INT NOT NULL REFERENCES categories(id),
    slug        TEXT NOT NULL,              -- 'wash_performance', 'spin_efficiency', 'energy_use', 'noise'
    name_en     TEXT NOT NULL,
    description TEXT,
    UNIQUE(category_id, slug)
);

-- Agency-specific sub-criterion labels -> canonical criterion.
CREATE TABLE agency_criteria (
    id              SERIAL PRIMARY KEY,
    agency_id       INT NOT NULL REFERENCES agencies(id),
    category_id     INT NOT NULL REFERENCES categories(id),
    raw_label       TEXT NOT NULL,          -- 'praní' (Dtest), 'Waschen' (Warentest), 'Cleaning' (Which)
    criterion_id   INT NOT NULL REFERENCES criteria(id),
    default_weight_pct NUMERIC,             -- if agency publishes weights
    UNIQUE(agency_id, category_id, raw_label)
);

-- ---------- BRANDS ----------

CREATE TABLE brands (
    id               SERIAL PRIMARY KEY,
    slug             TEXT UNIQUE NOT NULL,
    name             TEXT NOT NULL,
    country_origin   CHAR(2),
    parent_brand_id  INT REFERENCES brands(id),     -- e.g. Lenovo -> Motorola
    logo_url         TEXT,
    notes            TEXT
);

CREATE TABLE brand_aliases (
    id       SERIAL PRIMARY KEY,
    brand_id INT NOT NULL REFERENCES brands(id) ON DELETE CASCADE,
    alias    TEXT NOT NULL,                         -- 'SENNHEISER', 'Sennheiser GmbH & Co. KG'
    UNIQUE(alias)                                   -- each alias maps to exactly one brand
);

-- ---------- PRODUCTS (canonical) ----------

CREATE TABLE products (
    id            BIGSERIAL PRIMARY KEY,
    brand_id      INT NOT NULL REFERENCES brands(id),
    category_id   INT NOT NULL REFERENCES categories(id),
    model_family  TEXT NOT NULL,                    -- 'Galaxy S24 Ultra', 'EcoVacs Deebot X2'
    notes         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(brand_id, category_id, model_family)
);

-- Physical variants (256GB vs 512GB, "hublot" vs "top", colour-specific SKUs).
-- If a product has no meaningful variants, create a single variant with variant_label='default'.
CREATE TABLE product_variants (
    id             BIGSERIAL PRIMARY KEY,
    product_id     BIGINT NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    variant_label  TEXT NOT NULL,                   -- '256GB', 'hublot', 'default'
    model_number   TEXT,                            -- manufacturer SKU, e.g. 'SM-S928B'
    ean            TEXT,                            -- GTIN/EAN — the golden matching key
    release_year   SMALLINT,
    release_date   DATE,
    discontinued   BOOLEAN DEFAULT FALSE,
    image_url      TEXT,
    UNIQUE(product_id, variant_label)
);
CREATE INDEX ix_variants_ean ON product_variants(ean) WHERE ean IS NOT NULL;
CREATE INDEX ix_variants_model_number ON product_variants(model_number) WHERE model_number IS NOT NULL;

-- ---------- LAYER 1 continued: agency-side product records ----------

-- One row per product per agency. agency_products can be UNMATCHED (variant_id NULL)
-- until entity resolution runs.
CREATE TABLE agency_products (
    id               BIGSERIAL PRIMARY KEY,
    agency_id        INT NOT NULL REFERENCES agencies(id),
    variant_id       BIGINT REFERENCES product_variants(id),
    -- Raw fields exactly as scraped
    name_raw         TEXT NOT NULL,
    brand_raw        TEXT,
    model_raw        TEXT,
    ean_raw          TEXT,
    category_raw     TEXT,                          -- matches agency_categories.raw_category
    subgroup_raw     TEXT,                          -- matches agency_categories.raw_subgroup
    url              TEXT NOT NULL,
    image_url        TEXT,
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Entity resolution metadata
    match_confidence NUMERIC,                       -- 0.0-1.0, NULL until matched
    match_method     match_method NOT NULL DEFAULT 'unmatched',
    match_verified   BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE(agency_id, url)
);
CREATE INDEX ix_ap_variant ON agency_products(variant_id);
CREATE INDEX ix_ap_unmatched ON agency_products(agency_id) WHERE variant_id IS NULL;

-- ---------- TEST BATCHES + RESULTS ----------

CREATE TABLE test_batches (
    id                   BIGSERIAL PRIMARY KEY,
    agency_id            INT NOT NULL REFERENCES agencies(id),
    category_id          INT NOT NULL REFERENCES categories(id),
    title                TEXT,
    test_date            DATE,              -- when testing was conducted
    published_date       DATE,
    issue_ref            TEXT,              -- 'Heft 06/2024'
    url                  TEXT,
    methodology_version  TEXT,              -- you assign: 'WT-washing-2023', 'Dtest-washing-2024'
    methodology_notes    TEXT,
    superseded_by_id     BIGINT REFERENCES test_batches(id)
);
CREATE INDEX ix_batches_agency_cat_date ON test_batches(agency_id, category_id, test_date DESC);

CREATE TABLE test_results (
    id                           BIGSERIAL PRIMARY KEY,
    agency_product_id            BIGINT NOT NULL REFERENCES agency_products(id) ON DELETE CASCADE,
    batch_id                     BIGINT REFERENCES test_batches(id),
    scrape_id                    BIGINT NOT NULL REFERENCES scrapes(id),
    -- Raw, exactly as the agency published
    score_raw                    NUMERIC,
    score_raw_label              TEXT,            -- 'gut', '72%', 'uspokojivě'
    rank_in_batch                INT,
    total_in_batch               INT,
    -- Normalized 0-100, higher=better. Computed by the pipeline.
    score_normalized             NUMERIC,
    -- Cohort context
    batch_mean_normalized        NUMERIC,
    batch_stddev_normalized      NUMERIC,
    -- Context
    price_at_test                NUMERIC,
    currency                     CHAR(3),
    price_eur_at_test            NUMERIC,         -- FX-adjusted at test_date
    price_eur_real_2024          NUMERIC,         -- CPI-adjusted
    verdict                      TEXT,
    -- Recommendation
    recommendation_raw           TEXT,            -- 'Best Buy', 'Mangelhaft', 'Doporučeno'
    recommendation_norm          recommendation_normalized,
    -- Audit
    created_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(agency_product_id, batch_id)           -- one result per product per batch
);
CREATE INDEX ix_results_batch ON test_results(batch_id);

-- ---------- SUB-RATINGS ----------

CREATE TABLE sub_ratings (
    id                   BIGSERIAL PRIMARY KEY,
    test_result_id       BIGINT NOT NULL REFERENCES test_results(id) ON DELETE CASCADE,
    criterion_id         INT REFERENCES criteria(id),  -- NULL if not yet mapped
    raw_label            TEXT NOT NULL,                -- 'praní', 'Waschen'
    score_raw            NUMERIC,
    score_raw_label      TEXT,
    score_normalized     NUMERIC,                      -- 0-100
    weight_pct           NUMERIC,
    UNIQUE(test_result_id, raw_label)
);
CREATE INDEX ix_subratings_criterion ON sub_ratings(criterion_id);

-- ---------- PRICE NORMALIZATION AUX ----------

CREATE TABLE fx_rates (
    rate_date      DATE NOT NULL,
    currency_from  CHAR(3) NOT NULL,
    currency_to    CHAR(3) NOT NULL,
    rate           NUMERIC NOT NULL,
    source         TEXT,
    PRIMARY KEY (rate_date, currency_from, currency_to)
);

CREATE TABLE cpi_index (
    year           SMALLINT NOT NULL,
    month          SMALLINT NOT NULL,
    country_code   CHAR(2) NOT NULL,
    index_value    NUMERIC NOT NULL,
    base_year      SMALLINT NOT NULL,
    PRIMARY KEY (year, month, country_code)
);

-- ---------- LAYER 3: COMPUTED ----------

-- Brand scores, recomputed nightly. category_id NULL = overall; agency_id NULL = composite.
CREATE TABLE brand_scores (
    id                   BIGSERIAL PRIMARY KEY,
    brand_id             INT NOT NULL REFERENCES brands(id),
    category_id          INT REFERENCES categories(id),
    agency_id            INT REFERENCES agencies(id),
    score                NUMERIC NOT NULL,           -- 0-100 Bayesian-shrunk, time-weighted
    score_raw_avg        NUMERIC,                    -- simple mean for transparency
    score_stddev         NUMERIC,
    ci95_low             NUMERIC,
    ci95_high            NUMERIC,
    sample_size          INT NOT NULL,
    effective_sample_size NUMERIC,                   -- after time-decay + agency-weight
    computed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    methodology_version  TEXT NOT NULL,              -- 'brand_score_v1'
    UNIQUE(brand_id, category_id, agency_id, methodology_version)
);
CREATE INDEX ix_brand_scores_lookup ON brand_scores(category_id, agency_id, score DESC);

-- Entity-matching audit trail (optional but useful for reproducibility).
CREATE TABLE product_matches (
    id                   BIGSERIAL PRIMARY KEY,
    variant_id           BIGINT NOT NULL REFERENCES product_variants(id),
    agency_product_id    BIGINT NOT NULL REFERENCES agency_products(id),
    confidence           NUMERIC NOT NULL,
    method               match_method NOT NULL,
    matched_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    matched_by           TEXT,                       -- 'auto' or reviewer name
    notes                TEXT,
    UNIQUE(variant_id, agency_product_id)
);

-- ---------- USEFUL VIEWS (materialized for Layer 3 analytics) ----------

-- Flat cross-agency result view: one row per (product_variant, agency, batch).
CREATE MATERIALIZED VIEW mv_cross_agency_results AS
SELECT
    pv.id                    AS variant_id,
    pv.product_id,
    b.slug                   AS brand_slug,
    c.slug                   AS category_slug,
    a.slug                   AS agency_slug,
    tr.id                    AS test_result_id,
    tb.test_date,
    tr.score_raw,
    tr.score_normalized,
    tr.recommendation_norm,
    tr.price_eur_at_test,
    tr.price_eur_real_2024
FROM test_results tr
JOIN agency_products ap   ON ap.id = tr.agency_product_id
JOIN agencies a           ON a.id = ap.agency_id
LEFT JOIN test_batches tb ON tb.id = tr.batch_id
JOIN product_variants pv  ON pv.id = ap.variant_id
JOIN products p           ON p.id = pv.product_id
JOIN brands b             ON b.id = p.brand_id
JOIN categories c         ON c.id = p.category_id;

CREATE INDEX ix_mv_cross_agency_brand ON mv_cross_agency_results(brand_slug, category_slug);
CREATE INDEX ix_mv_cross_agency_variant ON mv_cross_agency_results(variant_id);
