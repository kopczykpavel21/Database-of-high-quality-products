# QualityDB — German Market Integration Guide

## What's in this folder

| File | Purpose |
|------|---------|
| `migrate_add_german_support.py` | One-time DB migration — adds `country`, `currency`, `Price_EUR` columns |
| `scraper/amazon_de_scraper.py` | Amazon.de bestseller lists (curl_cffi / Chrome impersonation) |
| `scraper/otto_scraper.py` | Otto.de JSON API + JSON-LD fallback |
| `scraper/mediamarkt_scraper.py` | MediaMarkt.de JSON API + __NEXT_DATA__ + JSON-LD fallback |
| `scraper/saturn_scraper.py` | Saturn.de (reuses MediaMarkt parsers, different base URL) |
| `scraper/config_de_additions.py` | New constants — paste into or import from `config.py` |
| `scraper/scheduler_de_additions.py` | Drop-in additions for `scheduler.py`; can also run standalone |

---

## Step-by-step integration

### 1. Copy files into your project

```
cp migrate_add_german_support.py   QualityDB/
cp scraper/amazon_de_scraper.py    QualityDB/scraper/
cp scraper/otto_scraper.py         QualityDB/scraper/
cp scraper/mediamarkt_scraper.py   QualityDB/scraper/
cp scraper/saturn_scraper.py       QualityDB/scraper/
cp scraper/config_de_additions.py  QualityDB/scraper/
cp scraper/scheduler_de_additions.py QualityDB/scraper/
```

### 2. Run the DB migration (once)

```bash
cd QualityDB
python3 migrate_add_german_support.py
# Expected output:
# Products before migration : 12,600
# Added column 'country'  (TEXT, default 'CZ')
# Added column 'currency' (TEXT, default 'CZK')
# Added column 'Price_EUR' (REAL, default NULL)
# Back-filled 12,600 existing rows → country='CZ', currency='CZK'
```

The script is **idempotent** — safe to run again if columns already exist.

### 3. Patch config.py

Open `QualityDB/scraper/config.py` and append the block from
`config_de_additions.py` (the large comment at the top shows exactly what to paste), or add this single line at the bottom:

```python
from config_de_additions import *
```

### 4. Patch scheduler.py

Open `QualityDB/scraper/scheduler.py` and follow the instructions in
`scheduler_de_additions.py` (3 import lines + a small `if ENABLE_*:` block per scraper). Full snippet is in the docstring of that file.

Alternatively, test the German scrapers standalone first:

```bash
cd QualityDB/scraper
python3 scheduler_de_additions.py            # uses products.db in parent dir
python3 scheduler_de_additions.py ../products.db   # explicit path
```

### 5. Update server.py for EUR prices

The `/api/products` endpoint currently reads `Price_CZK`.  For German products you'll want to expose `Price_EUR` too.  Add this to your SELECT:

```python
# In the products query, add the two new columns:
"Price_EUR, country, currency"
```

And in the product card rendering in `static/app.js`, display the correct price:

```js
const price = product.country === 'DE'
  ? (product.Price_EUR != null ? `€${product.Price_EUR.toFixed(2)}` : '—')
  : (product.Price_CZK != null ? `${product.Price_CZK} Kč` : '—');
```

### 6. Add a country filter to the UI (optional)

In `templates/index.html`, add a country toggle alongside the existing filters:

```html
<select id="country-filter">
  <option value="">All countries</option>
  <option value="CZ">🇨🇿 Czech</option>
  <option value="DE">🇩🇪 German</option>
</select>
```

Then in `static/app.js`, read the value and pass `&country=DE` (or `CZ`) to `/api/products`.  In `server.py`, add:

```python
country = params.get("country", [""])[0]
if country:
    conditions.append("country = ?")
    values.append(country)
```

---

## Notes on bot detection

| Source | Technique | Notes |
|--------|-----------|-------|
| Amazon.de | curl_cffi Chrome 131 | Bestseller pages are public but heavily fingerprinted. If you hit CAPTCHA, increase `DELAY_OK` to 5 s or use rotating proxies. |
| Otto.de | curl_cffi Chrome 131 | Moderate detection. Homepage warm-up + session cookies usually sufficient. |
| MediaMarkt.de | curl_cffi Chrome 131 | Next.js SSR; `?format=json` works for most categories. Falls back to `__NEXT_DATA__` or JSON-LD. |
| Saturn.de | curl_cffi Chrome 131 | Same platform as MediaMarkt. Category slugs may occasionally differ. |

All scrapers use **polite delays** (2–3 s between requests) and **exponential back-off** on 429/503 responses — the same pattern as the existing `zbozi_scraper`.

---

## Fields available per source

| Field | Amazon.de | Otto.de | MediaMarkt | Saturn |
|-------|:---------:|:-------:|:----------:|:------:|
| Name | ✓ | ✓ | ✓ | ✓ |
| Price_EUR | ✓ | ✓ | ✓ | ✓ |
| AvgStarRating | ✓ | ✓ | ✓ | ✓ |
| ReviewsCount | ✓ | ✓ | ✓ | ✓ |
| SKU / ASIN | ✓ | ✓ | ✓ | ✓ |
| RecommendRate_pct | — | — | — | — |
| ReturnRate_pct | — | — | — | — |
| Description | — | — | — | — |

`RecommendRate_pct` and `ReturnRate_pct` are stored as NULL for DE products — the same as is the case for some CZ sources. The quality filter in the UI already handles NULL gracefully (`min_recommend` only filters rows where the field is non-null).
