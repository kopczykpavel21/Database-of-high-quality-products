#!/usr/bin/env python3
"""
dtest_scraper.py — dtest.cz (Czech consumer testing magazine) scraper
──────────────────────────────────────────────────────────────────────
3-phase scraper:

  Phase 1 — Category discovery
    Fetch each /testy/cNNN-slug category page → find sub-group links
    (/testy-vyrobku-NNNN/slug).

  Phase 2 — Product listing
    Paginate through /testy-vyrobku-NNNN/slug?pg=N listing pages →
    extract product name, price, pub date, and detail URL
    (/test/product-slug/NNNN).

  Phase 3 — Product detail (the rich data)
    Fetch /test/product-slug/NNNN → extract:
      • overall score (0–100 %)
      • per-category scores (displej 80%, výkon 92%, baterie 59%…)
      • per-param grades within each category (dt/dd pairs)
      • full technical specs (brand, dimensions, weight, processor,
        connectivity, battery capacity, camera specs…)

  ✅ No paywall on product detail pages — all data is publicly
     accessible without a subscription.
     (The listing-page scores DO need a subscription, but we bypass
     that by going straight to the detail page.)

Usage:
  python3 scraper/dtest_scraper.py              # full run (all 3 phases)
  python3 scraper/dtest_scraper.py --debug      # dump category page sub-groups
  python3 scraper/dtest_scraper.py --test-url https://www.dtest.cz/test/google-pixel-10-pro/136765
  python3 scraper/dtest_scraper.py --catalog-only
  python3 scraper/dtest_scraper.py --no-details   # skip Phase 3 (fast, less data)
  python3 scraper/dtest_scraper.py --limit 100    # max 100 products

Cookies:
  Place cookies_dtest.json next to products.db (QualityDB folder).
  Format: {"PHPSESSID": "...", "nette-browser": "..."}
  (Not strictly needed for detail pages, but helps avoid bot blocks.)
"""

import os, sys, re, json, time, sqlite3, logging, argparse
from urllib.parse import urljoin

try:
    from curl_cffi import requests as cffi_requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing dependencies. Run:\n  pip install curl_cffi beautifulsoup4")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from scraper.config import DB_PATH
except ImportError:
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "products.db")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_URL = "https://www.dtest.cz"
DELAY    = 2.0   # seconds between requests — be polite

# ── Grade text → numeric score (0–100) ────────────────────────────────────────
# Used for sub-params that only show grade text (no %)
GRADE_SCORES = {
    "výborně":        95,
    "velmi dobře":    80,
    "dobře":          65,
    "uspokojivě":     45,
    "nedostatečně":   20,
    "nevyhovuje":     10,
    "ano":            100,
    "ne":             0,
}

# ── Category pages to scrape ──────────────────────────────────────────────────
CATALOG_CATEGORIES = [
    # Electronics
    ("/testy/c3-mobilni-telefony",                "Chytré telefony",          "Telefony a tablety"),
    ("/testy/c345-tablety-a-ctecky",              "Tablety a čtečky",         "Telefony a tablety"),
    ("/testy/c577-notebooky-monitory-a-software", "Notebooky",                "Počítače a notebooky"),
    ("/testy/c835-pevne-disky-usb-a-pametove-karty","Úložiště a USB",         "Počítače a notebooky"),
    ("/testy/c38-televizory",                     "Televizory",               "Televize a video"),
    ("/testy/c17-fotoaparaty",                    "Fotoaparáty",              "Foto a video"),
    ("/testy/c41-videokamery",                    "Videokamery",              "Foto a video"),
    ("/testy/c571-zvuk",                          "Zvuk a sluchátka",         "Zvuk"),
    ("/testy/c799-drony",                         "Drony",                    "Foto a video"),
    ("/testy/c81-baterie-a-nabijecky",            "Baterie a nabíječky",      "Příslušenství"),
    ("/testy/c1123-prislusenstvi",                "Příslušenství",            "Příslušenství"),
    # Large household appliances
    ("/testy/c122-chladnicky-a-mraznicky",        "Chladničky a mrazničky",   "Velké domácí spotřebiče"),
    ("/testy/c551-pracky-a-pece-o-pradlo",        "Pračky a péče o prádlo",   "Velké domácí spotřebiče"),
    ("/testy/c556-mycky-a-myti-nadobi",           "Myčky nádobí",             "Velké domácí spotřebiče"),
    ("/testy/c552-vareni-a-peceni",               "Vaření a pečení",          "Velké domácí spotřebiče"),
    ("/testy/c881-energie-vytapeni-a-klimatizace","Vytápění a klimatizace",   "Velké domácí spotřebiče"),
    # Small household appliances
    ("/testy/c557-kuchynske-spotrebice",          "Kuchyňské spotřebiče",     "Malé domácí spotřebiče"),
    ("/testy/c553-uklid",                         "Úklid (vysavače)",         "Malé domácí spotřebiče"),
    # Health & Personal care
    ("/testy/c579-dentalni-hygiena",              "Dentální hygiena",         "Zdraví a hygiena"),
    ("/testy/c582-holici-strojky-a-holeni",       "Holicí strojky",           "Zdraví a hygiena"),
    ("/testy/c578-zdravotnicke-pomucky",          "Zdravotnické pomůcky",     "Zdraví a hygiena"),
    ("/testy/c583-opalovani-a-opalovaci-kremy",   "Opalovací krémy",          "Zdraví a hygiena"),
    # Baby & Kids
    ("/testy/c108-detske-kocarky",                "Dětské kočárky",           "Dětské zboží"),
    ("/testy/c343-cestovani-s-detmi",             "Dětské autosedačky",       "Dětské zboží"),
    # Garden & Tools
    ("/testy/c115-sekacky-a-pece-o-travnik",      "Sekačky a péče o trávník", "Zahrada"),
    ("/testy/c84-elektricke-naradi-a-dilna",      "Elektrické nářadí",        "Zahrada a dílna"),
    ("/testy/c975-grilovani",                     "Grilování",                "Zahrada"),
    # Sport & Outdoor
    ("/testy/c573-cyklistika",                    "Cyklistika",               "Sport"),
    ("/testy/c572-behani-a-atletika",             "Běhání a atletika",        "Sport"),
    ("/testy/c576-outdoor-a-turistika",           "Outdoor a turistika",      "Sport"),
    ("/testy/c845-zavazadla-kufry-a-batohy",      "Zavazadla a kufry",        "Cestování"),
]


# ── HTTP ──────────────────────────────────────────────────────────────────────

def fetch(url, cookies):
    """Fetch with Chrome impersonation. Returns HTML string or None."""
    try:
        r = cffi_requests.get(
            url,
            impersonate="chrome131",
            cookies=cookies,
            headers={
                "Accept":          "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "cs-CZ,cs;q=0.9,en;q=0.7",
                "Referer":         BASE_URL + "/",
            },
            timeout=30,
        )
        if r.status_code == 200:
            return r.text
        log.warning(f"HTTP {r.status_code}  {url}")
        return None
    except Exception as e:
        log.error(f"Request error: {e}  {url}")
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_cookies():
    qdb  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(qdb, "cookies_dtest.json")
    if not os.path.exists(path):
        log.warning(f"Cookie file not found: {path} — continuing without cookies")
        return {}
    with open(path) as f:
        c = json.load(f)
    log.info(f"Loaded {len(c)} cookies from {path}")
    return c


def parse_price_czk(text):
    if not text:
        return None
    m = re.search(r'([\d\s\u00a0]+)\s*Kč', text)
    if m:
        s = re.sub(r'[\s\u00a0]', '', m.group(1))
        try:
            return float(s)
        except ValueError:
            pass
    return None


def extract_pct(text):
    """Extract integer percentage from text like 'velmi dobře (80 %)'. Returns int or None."""
    if not text:
        return None
    m = re.search(r'\((\d{1,3})\s*%\)', text)
    if m:
        v = int(m.group(1))
        return v if 0 <= v <= 100 else None
    return None


def grade_to_score(grade_text):
    """Convert Czech grade text to 0–100 integer score. Returns None if unrecognised."""
    if not grade_text:
        return None
    clean = grade_text.strip().lower()
    # Direct lookup
    if clean in GRADE_SCORES:
        return GRADE_SCORES[clean]
    # Partial match (e.g. "velmi dobře" may appear with trailing notes)
    for key, val in GRADE_SCORES.items():
        if clean.startswith(key):
            return val
    return None


def score_to_stars(score):
    if score is None:
        return None
    return round(max(1.0, min(5.0, score / 20.0)), 2)


def score_to_recommend(score):
    if score is None:
        return None
    return max(5, min(98, score))


# ── Phase 1: Discover sub-groups ──────────────────────────────────────────────

def discover_subgroups(cat_path, category, main_cat, cookies):
    """
    Fetch /testy/cNNN-slug page, return list of
    (subgroup_path, subgroup_name, category, main_cat).
    """
    html = fetch(BASE_URL + cat_path, cookies)
    if not html:
        return []

    soup   = BeautifulSoup(html, "html.parser")
    result = []
    seen   = set()

    for a in soup.find_all("a", href=True):
        href = a["href"]
        m    = re.match(r'^(/testy-vyrobku-\d+/[^?#\s]+)', href)
        if not m:
            continue
        path = m.group(1)
        if path in seen:
            continue
        seen.add(path)
        name = re.sub(r'\s*\(\d+\)\s*$', '', a.get_text(strip=True)).strip()
        if not name:
            name = path.split("/")[-1].replace("-", " ").title()
        result.append((path, name, category, main_cat))

    log.info(f"  [{category}]: {len(result)} sub-groups")
    return result


# ── Phase 2: Scrape listing pages → collect product stubs ─────────────────────

def scrape_listing(sg_path, sg_name, category, main_cat, cookies):
    """
    Paginate through /testy-vyrobku-NNNN/slug listing.
    Returns list of product stubs: {name, price_czk, pub_date, detail_url,
    listing_url, category, main_category, subgroup}.
    """
    stubs = []
    seen  = set()
    page  = 1

    while True:
        url  = BASE_URL + sg_path + (f"?pg={page}" if page > 1 else "")
        html = fetch(url, cookies)
        if not html:
            break

        soup      = BeautifulSoup(html, "html.parser")
        new_count = _parse_listing_page(soup, url, sg_name, category, main_cat, stubs, seen)

        log.info(f"    Listing [{sg_name}] page {page}: {new_count} new products")

        if new_count == 0:
            break

        # Check if we have all products
        total_m = re.search(r'Zobrazeno\s+\d+[-–]\d+\s+z\s+(\d+)', soup.get_text())
        if total_m and len(stubs) >= int(total_m.group(1)):
            break

        # Next page?
        if not (soup.find("a", string=re.compile(r'Další|»|›', re.I)) or
                soup.find("link", rel="next")):
            break

        page += 1
        time.sleep(DELAY)

    return stubs


def _parse_listing_page(soup, page_url, sg_name, category, main_cat, stubs, seen):
    cards     = soup.find_all("li", class_="product-item")
    new_count = 0

    for card in cards:
        # ── Name ─────────────────────────────────────────────────────────────
        name_el = card.find("h2") or card.find("h3") or \
                  card.find(class_=re.compile(r'\bname\b|\btitle\b', re.I))
        if not name_el:
            for a in card.find_all("a", href=True):
                t = a.get_text(strip=True)
                if len(t) > 5 and not t.startswith(("Více", "Detail", "Přidat")):
                    name_el = a
                    break
        if not name_el:
            continue

        name = re.sub(r'\s+', ' ', name_el.get_text()).strip()
        if not name or len(name) < 3 or name in seen:
            continue
        seen.add(name)

        # ── Detail URL (/test/slug/NNNN) ─────────────────────────────────────
        detail_url = None
        for a in card.find_all("a", href=True):
            href = a["href"]
            if re.match(r'^/test/[^?#]+/\d+', href):
                detail_url = BASE_URL + href
                break
            # Also look for full URL
            if re.match(r'https?://www\.dtest\.cz/test/', href):
                detail_url = href
                break
        if not detail_url:
            # Fallback: product detail link text
            for a in card.find_all("a", href=True):
                txt  = a.get_text(strip=True)
                href = a["href"]
                if txt in ("Detail produktu",) and href and href != "#":
                    detail_url = href if href.startswith("http") else BASE_URL + href
                    break

        # ── Price ─────────────────────────────────────────────────────────────
        price_czk = None
        price_el  = card.find(class_=re.compile(r'\bprice\b|\bcena\b', re.I))
        if price_el:
            price_czk = parse_price_czk(price_el.get_text())
        if price_czk is None:
            price_czk = parse_price_czk(card.get_text())

        # ── Publication date ──────────────────────────────────────────────────
        pub_date = None
        pub_str  = card.find(string=re.compile(r'Publikováno', re.I))
        if pub_str:
            parent_text = (pub_str.parent.get_text(" ", strip=True)
                           if pub_str.parent else "")
            m = re.search(r'Publikováno[:\s]*([\w\s]+\d{4})', parent_text, re.I)
            if m:
                pub_date = m.group(1).strip()

        stubs.append({
            "name":          name,
            "price_czk":     price_czk,
            "pub_date":      pub_date,
            "detail_url":    detail_url,
            "listing_url":   page_url,
            "category":      category,
            "main_category": main_cat,
            "subgroup":      sg_name,
        })
        new_count += 1

    return new_count


# ── Phase 3: Parse product detail page ────────────────────────────────────────

def parse_detail_page(html, detail_url):
    """
    Parse a /test/slug/NNNN page.
    Returns a dict with:
      overall_score, overall_grade, brand, pub_date, price_czk,
      scores    → {category_name: {score, grade, params: {param: grade}}}
      specs     → {spec_section: {spec_key: spec_value}}
    """
    if not html:
        return {}

    soup   = BeautifulSoup(html, "html.parser")
    result = {}

    # ── Overall score ─────────────────────────────────────────────────────────
    h2 = soup.find("h2", class_="group-title")
    if h2:
        text = h2.get_text(strip=True)
        result["overall_score"] = extract_pct(text)
        # Extract just the grade word (e.g. "velmi dobře") from "Celkové hodnocení: velmi dobře (80 %)"
        grade_text = re.sub(r'\s*\(\d+\s*%\)', '', text)
        m = re.search(r':\s*(.+)$', grade_text)
        result["overall_grade"] = m.group(1).strip() if m else grade_text.strip()

    # ── Product info block (brand, date, price) ────────────────────────────────
    info = soup.find(class_="product__info")
    if info:
        info_text = info.get_text(" ", strip=True)
        m = re.search(r'Značka[:\s]+([^\n]+?)(?:\s+Publikováno|\s+Cena|$)', info_text)
        if m:
            result["brand"] = m.group(1).strip()
        m = re.search(r'Publikováno na webu[:\s]+([\d/]+)', info_text)
        if m:
            result["pub_date"] = m.group(1).strip()
        price = parse_price_czk(info_text)
        if price:
            result["price_czk"] = price

    # ── Category scores + sub-params ─────────────────────────────────────────
    # Each .tested-group has:
    #   .group-head  → <strong class="name">category</strong>
    #                  <span class="val">grade (XX %)</span>
    #   .group-content → <dl><dt>param</dt><dd>grade</dd>…</dl>
    scores = {}
    for group in soup.find_all("div", class_="tested-group"):
        head = group.find(class_="group-head")
        if not head:
            continue

        cat_name_el = head.find("strong", class_="name")
        if not cat_name_el:
            continue
        cat_name = cat_name_el.get_text(strip=True)

        # Skip "cena" — that's just the price
        if cat_name == "cena":
            continue

        # Skip specs group (handled separately)
        if "technické údaje" in cat_name:
            continue

        val_el    = head.find(class_="val")
        val_text  = val_el.get_text(strip=True) if val_el else ""
        cat_score = extract_pct(val_text)
        cat_grade = re.sub(r'\s*\(\d+\s*%\)', '', val_text).strip() if val_text else ""

        # Sub-params from dl dt/dd pairs
        params = {}
        content = group.find(class_="group-content")
        if content:
            dl = content.find("dl")
            if dl:
                dts = dl.find_all("dt")
                dds = dl.find_all("dd")
                for dt, dd in zip(dts, dds):
                    param_name  = dt.get_text(strip=True)
                    param_grade = dd.get_text(strip=True)
                    if param_name:
                        params[param_name] = param_grade

        scores[cat_name] = {
            "score":  cat_score,
            "grade":  cat_grade,
            "params": params,
        }

    if scores:
        result["scores"] = scores

    # ── Technical specs ───────────────────────────────────────────────────────
    specs = {}
    tech_group = next(
        (g for g in soup.find_all("div", class_="tested-group")
         if "technické údaje" in g.get_text()[:50]),
        None
    )
    if tech_group:
        content = tech_group.find(class_="group-content")
        if content:
            # Sections are in <div> blocks, each containing a label + dl or dt/dd
            for section_div in content.find_all("div", recursive=False):
                section_text = section_div.get_text(" ", strip=True)
                # First line is the section name (e.g. "rozměry a hmotnost")
                lines = [l.strip() for l in section_text.split("\n") if l.strip()]
                if not lines:
                    continue
                section_name = lines[0]
                section_specs = {}

                # Try dl dt/dd pairs
                dl = section_div.find("dl")
                if dl:
                    dts = dl.find_all("dt")
                    dds = dl.find_all("dd")
                    for dt, dd in zip(dts, dds):
                        key = dt.get_text(strip=True)
                        val = dd.get_text(strip=True)
                        if key:
                            section_specs[key] = val
                else:
                    # Fallback: parse remaining text as key-value pairs
                    # "rozměry 153 x 72 x 8,6 mm hmotnost 208 g"
                    rest = " ".join(lines[1:])
                    # Pattern: word/phrase followed by a value until next phrase
                    # (Best effort parsing)
                    parts = re.split(r'\s{2,}', rest)
                    for part in parts:
                        kv = part.strip()
                        if kv:
                            section_specs[kv] = ""

                if section_specs:
                    specs[section_name] = section_specs

    if specs:
        result["specs"] = specs

    return result


# ── DB ────────────────────────────────────────────────────────────────────────

def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            Name              TEXT,
            Category          TEXT,
            ProductURL        TEXT UNIQUE,
            Price_CZK         REAL,
            AvgStarRating     REAL,
            StarRatingsCount  INTEGER,
            ReviewsCount      INTEGER,
            RecommendRate_pct REAL,
            ReturnRate_pct    REAL,
            Stars5_Count      INTEGER,
            Stars4_Count      INTEGER,
            Stars3_Count      INTEGER,
            Stars2_Count      INTEGER,
            Stars1_Count      INTEGER,
            Description       TEXT,
            SKU               TEXT,
            source            TEXT,
            keywords          TEXT,
            MainCategory      TEXT,
            country           TEXT,
            currency          TEXT,
            Price_EUR         REAL,
            details_json      TEXT
        )
    """)
    conn.commit()


def upsert(conn, p):
    """Insert or update a product record. Returns 'ins' or 'upd'."""
    cur = conn.cursor()
    cur.execute("SELECT rowid FROM products WHERE ProductURL = ?", (p["url"],))
    existing = cur.fetchone()

    if existing:
        cur.execute("""
            UPDATE products SET
                Name              = ?,
                Category          = ?,
                Price_CZK         = COALESCE(?, Price_CZK),
                AvgStarRating     = COALESCE(?, AvgStarRating),
                RecommendRate_pct = COALESCE(?, RecommendRate_pct),
                MainCategory      = ?,
                source            = 'dtest',
                country           = 'CZ',
                currency          = 'CZK',
                details_json      = ?
            WHERE ProductURL = ?
        """, (
            p["name"], p["category"],
            p["price_czk"], p["stars"], p["recommend"],
            p["main_category"], p["details_json"],
            p["url"],
        ))
    else:
        cur.execute("""
            INSERT INTO products
              (Name, Category, ProductURL, Price_CZK, AvgStarRating,
               RecommendRate_pct, source, MainCategory, country, currency, details_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            p["name"], p["category"], p["url"],
            p["price_czk"], p["stars"], p["recommend"],
            "dtest", p["main_category"], "CZ", "CZK",
            p["details_json"],
        ))

    conn.commit()
    return "upd" if existing else "ins"


def build_product(stub, detail):
    """Merge a listing stub with detail page data into a DB-ready product dict."""
    # Price: prefer detail page (fresher), fallback to listing
    price_czk  = detail.get("price_czk") or stub.get("price_czk")
    pub_date   = detail.get("pub_date") or stub.get("pub_date")
    brand      = detail.get("brand", "")
    overall    = detail.get("overall_score")
    stars      = score_to_stars(overall)
    recommend  = score_to_recommend(overall)

    detail_payload = {
        "overall_score":  overall,
        "overall_grade":  detail.get("overall_grade"),
        "brand":          brand,
        "pub_date":       pub_date,
        "subgroup":       stub.get("subgroup"),
        "listing_url":    stub.get("listing_url"),
        "scores":         detail.get("scores", {}),
        "specs":          detail.get("specs", {}),
    }

    url = stub.get("detail_url") or stub.get("listing_url", "")

    return {
        "name":          stub["name"],
        "category":      stub["category"],
        "main_category": stub["main_category"],
        "price_czk":     price_czk,
        "stars":         stars,
        "recommend":     recommend,
        "url":           url,
        "details_json":  json.dumps(detail_payload, ensure_ascii=False),
        "source":        "dtest",
        "country":       "CZ",
        "currency":      "CZK",
    }


# ── Debug helpers ─────────────────────────────────────────────────────────────

def debug_category(cat_path, cookies):
    html = fetch(BASE_URL + cat_path, cookies)
    if not html:
        log.error(f"Failed to fetch {BASE_URL + cat_path}")
        return
    soup = BeautifulSoup(html, "html.parser")
    print(f"\nCategory: {BASE_URL + cat_path}")
    print(f"Title: {soup.title.get_text(strip=True) if soup.title else '?'}")
    seen = set()
    print("\n--- Sub-groups ---")
    for a in soup.find_all("a", href=True):
        m = re.match(r'^(/testy-vyrobku-\d+/[^?#\s]+)', a["href"])
        if m and m.group(1) not in seen:
            seen.add(m.group(1))
            name = re.sub(r'\s*\(\d+\)\s*$', '', a.get_text(strip=True)).strip()
            print(f"  {name:50s}  {BASE_URL + m.group(1)}")
    print(f"\nTotal: {len(seen)} sub-groups")


def debug_detail(url, cookies):
    print(f"\n=== Detail page: {url} ===")
    html   = fetch(url, cookies)
    detail = parse_detail_page(html, url)
    print(f"Overall score: {detail.get('overall_score')}%  ({detail.get('overall_grade')})")
    print(f"Brand:         {detail.get('brand')}")
    print(f"Price:         {detail.get('price_czk')} Kč")
    print(f"Published:     {detail.get('pub_date')}")
    print(f"\n--- Category scores ---")
    for cat, data in detail.get("scores", {}).items():
        print(f"  {cat:30s}  {data['score']}%  ({data['grade']})")
        for param, grade in list(data.get("params", {}).items())[:5]:
            print(f"    • {param:35s}  {grade}")
    print(f"\n--- Spec sections ---")
    for section, kv in detail.get("specs", {}).items():
        print(f"  [{section}]")
        for k, v in list(kv.items())[:5]:
            print(f"    {k}: {v}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="dtest.cz scraper — all 3 phases")
    ap.add_argument("--debug",        action="store_true",
                    help="Dump first category page sub-groups and exit")
    ap.add_argument("--test-url",     metavar="URL",
                    help="Parse a single product detail URL and print all data")
    ap.add_argument("--catalog-only", action="store_true",
                    help="Run Phase 1+2 only — list discovered products, don't save to DB")
    ap.add_argument("--no-details",   action="store_true",
                    help="Skip Phase 3 (no detail page fetching — faster but no scores)")
    ap.add_argument("--limit",        type=int, default=0,
                    help="Max products to insert (0 = unlimited)")
    ap.add_argument("--skip-existing",action="store_true", default=True,
                    help="Skip products already in DB (default: True)")
    args = ap.parse_args()

    cookies = load_cookies()

    # ── Single detail URL debug ────────────────────────────────────────────────
    if args.test_url:
        debug_detail(args.test_url, cookies)
        return

    # ── Category page debug ───────────────────────────────────────────────────
    if args.debug:
        debug_category(CATALOG_CATEGORIES[0][0], cookies)
        return

    # ── Full run ──────────────────────────────────────────────────────────────
    log.info("=== dtest.cz scraper starting ===")
    log.info(f"DB: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    init_db(conn)

    # Load already-scraped URLs to skip
    existing_urls = set()
    if args.skip_existing:
        cur = conn.cursor()
        cur.execute("SELECT ProductURL FROM products WHERE source='dtest'")
        existing_urls = {row[0] for row in cur.fetchall()}
        log.info(f"  {len(existing_urls)} existing dtest products in DB — will skip")

    # ── Phase 1: Discover sub-groups ──────────────────────────────────────────
    log.info("Phase 1: discovering sub-group listing pages…")
    all_subgroups = []
    seen_sg       = set()

    for cat_path, category, main_cat in CATALOG_CATEGORIES:
        log.info(f"  Category: {category}")
        for sg in discover_subgroups(cat_path, category, main_cat, cookies):
            if sg[0] not in seen_sg:
                seen_sg.add(sg[0])
                all_subgroups.append(sg)
        time.sleep(DELAY)

    log.info(f"Phase 1 complete: {len(all_subgroups)} unique sub-groups")

    # ── Phase 2: Collect product stubs from listing pages ─────────────────────
    log.info("Phase 2: collecting product stubs from listing pages…")
    all_stubs = []
    seen_names = set()

    for i, (sg_path, sg_name, category, main_cat) in enumerate(all_subgroups):
        log.info(f"  [{i+1}/{len(all_subgroups)}] {category} › {sg_name}")
        stubs = scrape_listing(sg_path, sg_name, category, main_cat, cookies)
        for stub in stubs:
            if stub["name"] not in seen_names:
                seen_names.add(stub["name"])
                all_stubs.append(stub)
        time.sleep(DELAY)

    log.info(f"Phase 2 complete: {len(all_stubs)} unique products found")

    if args.catalog_only:
        for stub in all_stubs:
            print(f"{stub['main_category']:30s}  {stub['category']:30s}  "
                  f"{stub['name'][:55]:55s}  {stub.get('detail_url','?')}")
        conn.close()
        return

    # ── Phase 3: Fetch detail pages and save to DB ────────────────────────────
    if args.no_details:
        log.info("Phase 3: SKIPPED (--no-details). Saving stubs without scores…")
        total_ins = total_upd = 0
        for stub in all_stubs:
            if args.limit and (total_ins + total_upd) >= args.limit:
                break
            url    = stub.get("detail_url") or stub.get("listing_url", "")
            detail = {}
            prod   = build_product(stub, detail)
            op     = upsert(conn, prod)
            if op == "ins":
                total_ins += 1
            else:
                total_upd += 1
        log.info(f"Done (no details). Inserted: {total_ins}  Updated: {total_upd}")
        conn.close()
        return

    log.info("Phase 3: fetching product detail pages…")
    total_ins = total_upd = total_skip = 0

    for i, stub in enumerate(all_stubs):
        if args.limit and (total_ins + total_upd) >= args.limit:
            log.info(f"Reached --limit {args.limit}, stopping")
            break

        detail_url = stub.get("detail_url")

        # Skip already scraped
        if detail_url and detail_url in existing_urls:
            log.debug(f"  SKIP (exists): {stub['name']}")
            total_skip += 1
            continue

        log.info(f"  [{i+1}/{len(all_stubs)}] {stub['category']} › {stub['name'][:55]}")

        if detail_url:
            html   = fetch(detail_url, cookies)
            detail = parse_detail_page(html, detail_url)
            time.sleep(DELAY)
        else:
            log.warning(f"    No detail URL found — using listing data only")
            detail = {}

        prod = build_product(stub, detail)
        op   = upsert(conn, prod)
        if op == "ins":
            total_ins += 1
        else:
            total_upd += 1

        score = detail.get("overall_score")
        cats  = len(detail.get("scores", {}))
        log.info(f"    → score={score}%  categories={cats}  "
                 f"ins={total_ins} upd={total_upd}")

    log.info(
        f"=== Done. Inserted: {total_ins}  Updated: {total_upd}  "
        f"Skipped (existing): {total_skip} ==="
    )
    conn.close()


if __name__ == "__main__":
    main()
