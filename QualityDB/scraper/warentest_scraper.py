#!/usr/bin/env python3
"""
warentest_scraper.py — Stiftung Warentest (test.de) scraper
─────────────────────────────────────────────────────────────
Scrapes product test results using premium account cookies.

Usage:
  python3 scraper/warentest_scraper.py              # full run (hardcoded + discovered URLs)
  python3 scraper/warentest_scraper.py --discover   # check RSS for new tests, then scrape all
  python3 scraper/warentest_scraper.py --discover-sitemap  # crawl sitemaps for ALL historical tests
  python3 scraper/warentest_scraper.py --discover-search   # search test.de by topic for older tests
  python3 scraper/warentest_scraper.py --discover-produkte # crawl products sitemap for individual product pages
  python3 scraper/warentest_scraper.py --discover-all      # run all discovery methods (RSS + sitemap + produkte)
  python3 scraper/warentest_scraper.py --discover-sitemap --discover-only  # discover only, no scrape
  python3 scraper/warentest_scraper.py --details    # also scrape sub-ratings from detail pages
  python3 scraper/warentest_scraper.py --debug       # show HTML structure & exit
  python3 scraper/warentest_scraper.py --debug-full   # dump raw div content

Discovery:
  --discover         RSS feeds (recent tests only, run monthly)
  --discover-sitemap Crawls test.de XML sitemaps to find ALL historical test listing URLs.
                     Fetches sitemap index → each sub-sitemap → filters for -N-0/ pattern.
                     Run once to bootstrap, then monthly with --discover for new ones.
  --discover-search  Uses test.de's search API with ~40 category keywords to surface older
                     tests that may not appear in sitemaps (e.g. archived/renamed categories).
  --discover-all     Runs RSS + sitemap + search in one pass (recommended for first-time setup).

  Discovered URLs are persisted to warentest_discovered.json in the QualityDB root.

Cookies:
  Place cookies_warentest.json in the QualityDB folder (next to products.db).
  Format: {"cookie_name": "value", ...}
"""

import os, sys, re, json, time, sqlite3, logging, random

try:
    from curl_cffi import requests as cffi_requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Missing: pip install curl_cffi beautifulsoup4")
    sys.exit(1)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    from scraper.config import DB_PATH
except ImportError:
    DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "products.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-7s  %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger(__name__)

# ── Test URLs ────────────────────────────────────────────────────────────────
TEST_URLS = [
    # ── Electronics ──────────────────────────────────────────────────────────
    ("https://www.test.de/Smartphones-im-Test-4222793-0/",                                           "Smartphones",              "Telefony a tablety"),
    ("https://www.test.de/Tablets-im-Test-4627215-0/",                                               "Tablets",                  "Telefony a tablety"),
    ("https://www.test.de/Tablets-mit-Tastatur-Notebooks-Ultrabooks-Convertibles-im-Test-4734961-0/","Laptops & Notebooks",      "Počítače a notebooky"),
    ("https://www.test.de/Fernseher-im-Test-1629201-0/",                                             "Televisions",              "Televize a video"),
    ("https://www.test.de/Bluetooth-Kopfhoerer-Test-4378783-0/",                                     "Headphones",               "Zvuk a hudba"),
    ("https://www.test.de/Test-Soundbars-und-Soundplates-4931024-0/",                                "Soundbars",                "Zvuk a hudba"),
    ("https://www.test.de/Smartwatch-und-Fitnesstracker-im-Test-5254021-0/",                         "Smartwatches",             "Chytré zařízení"),
    ("https://www.test.de/tv-box-streaming-stick-vergleich-5114866-0/",                              "Streaming Devices",        "Televize a video"),
    ("https://www.test.de/Drucker-im-Test-4339831-0/",                                               "Printers",                 "Počítače a notebooky"),
    ("https://www.test.de/Monitore-im-Test-4840919-0/",                                              "Monitors",                 "Počítače a notebooky"),
    ("https://www.test.de/DSL-WLan-Repeater-Router-im-Test-4733659-0/",                              "Router & Repeater",        "Počítače a notebooky"),
    ("https://www.test.de/Mesh-WLan-Systeme-Test-5369960-0/",                                        "Mesh-WLAN",                "Počítače a notebooky"),
    ("https://www.test.de/Digitalkameras-im-Test-1538975-0/",                                        "Cameras",                  "Foto a video"),
    ("https://www.test.de/Dashcams-im-Test-Viele-schwaecheln-bei-Dunkelheit-6263647-0/",             "Dashcams",                 "Auto a moto"),

    # ── Large household appliances ────────────────────────────────────────────
    ("https://www.test.de/Waschmaschinen-im-Test-4296800-0/",                                        "Washing Machines",         "Velké domácí spotřebiče"),
    ("https://www.test.de/Waeschetrockner-im-Test-4735809-0/",                                       "Tumble Dryers",            "Velké domácí spotřebiče"),
    ("https://www.test.de/Geschirrspueler-im-Test-4685888-0/",                                       "Dishwashers",              "Velké domácí spotřebiče"),
    ("https://www.test.de/Kuehlschraenke-im-Test-4735177-0/",                                        "Refrigerators",            "Velké domácí spotřebiče"),
    ("https://www.test.de/Gefriergeraete-im-Test-4981546-0/",                                        "Freezers",                 "Velké domácí spotřebiče"),
    ("https://www.test.de/Backoefen-im-Test-4434994-0/",                                             "Ovens",                    "Velké domácí spotřebiče"),
    ("https://www.test.de/Klimageraete-im-Test-4722766-0/",                                          "Air Conditioners",         "Velké domácí spotřebiče"),
    ("https://www.test.de/Mikrowellen-im-Test-5048533-0/",                                           "Microwaves",               "Velké domácí spotřebiče"),

    # ── Small household appliances ────────────────────────────────────────────
    ("https://www.test.de/Staubsauger-im-Test-1838262-0/",                                           "Vacuum Cleaners",          "Vysavače a úklid"),
    ("https://www.test.de/Saugroboter-im-Test-4806685-0/",                                           "Saugroboter",              "Vysavače a úklid"),
    ("https://www.test.de/Dampfreiniger-im-Test-Kaercher-dampft-am-besten-1523412-0/",               "Dampfreiniger",            "Vysavače a úklid"),
    ("https://www.test.de/Kaffeevollautomaten-im-Test-4635644-0/",                                   "Coffee Machines",          "Malé domácí spotřebiče"),
    ("https://www.test.de/Heissluftfritteusen-im-Test-5115675-0/",                                   "Air Fryers",               "Malé domácí spotřebiče"),
    ("https://www.test.de/Mixer-Standmixer-im-Test-5073614-0/",                                      "Blenders",                 "Malé domácí spotřebiče"),
    ("https://www.test.de/Luftreiniger-im-Test-5579439-0/",                                          "Air Purifiers",            "Malé domácí spotřebiče"),
    ("https://www.test.de/Buegeleisen-im-Test-5098871-0/",                                           "Clothes Irons",            "Malé domácí spotřebiče"),

    # ── Garden, Tools & Mobility ──────────────────────────────────────────────
    ("https://www.test.de/Maehroboter-im-Test-4698387-0/",                                           "Robot Lawn Mowers",        "Zahrada a dílna"),
    ("https://www.test.de/akkuschrauber-bohrschrauber-test-4817111-0/",                              "Akkuschrauber",            "Zahrada a dílna"),
    ("https://www.test.de/E-Bike-Test-4733454-0/",                                                   "E-Bikes",                  "Sport a kola"),

    # ── Health & Personal care ────────────────────────────────────────────────
    ("https://www.test.de/elektrische-Zahnbuersten-im-Test-4621863-0/",                              "Electric Toothbrushes",    "Zdraví a hygiena"),
    ("https://www.test.de/Blutdruckmessgeraete-im-Test-5007166-0/",                                  "Blood Pressure Monitors",  "Zdraví a hygiena"),
    ("https://www.test.de/Trockenrasur-Nassrasur-Elektrorasierer-Test-4633728-0/",                   "Electric Shavers",         "Zdraví a hygiena"),
    ("https://www.test.de/Haartrockner-im-Test-4796779-0/",                                          "Hair Dryers",              "Zdraví a hygiena"),
    ("https://www.test.de/Epilierer-im-Test-1680642-0/",                                             "Epilators",                "Zdraví a hygiena"),
    ("https://www.test.de/Test-Sonnencreme-und-Sonnenspray-fuer-Erwachsene-4868984-0/",              "Sonnencreme",              "Zdraví a hygiena"),

    # ── Baby & Family ─────────────────────────────────────────────────────────
    ("https://www.test.de/Autokindersitze-im-Test-1806826-0/",                                       "Kindersitze",              "Dětské zboží"),
    ("https://www.test.de/Kinderwagen-im-Test-4805700-0/",                                           "Kinderwagen",              "Dětské zboží"),
    ("https://www.test.de/Pre-Nahrung-im-Test-5032864-0/",                                           "Babynahrung (Pre)",        "Dětské zboží"),
    ("https://www.test.de/Fahrradhelme-fuer-Kinder-im-Test-5020018-0/",                              "Fahrradhelme Kinder",      "Dětské zboží"),

    # ── Sport, Travel & Safety ────────────────────────────────────────────────
    ("https://www.test.de/Test-Fahrradhelme-fuer-Erwachsene-4884138-0/",                             "Fahrradhelme Erwachsene",  "Sport a kola"),
    ("https://www.test.de/Koffer-im-Test-4379681-0/",                                                "Koffer & Reisetaschen",    "Cestování"),

    # ── Furniture & Sleep ─────────────────────────────────────────────────────
    ("https://www.test.de/Matratzen-im-Test-1830877-0/",                                             "Matratzen",                "Bytové vybavení"),
]

DELAY_MIN = 4.0   # minimum seconds between requests
DELAY_MAX = 8.0   # maximum seconds between requests (random range avoids pattern detection)

def _sleep():
    """Random delay between requests to appear human."""
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

# ── Progress checkpoint ───────────────────────────────────────────────────────
_QDB_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PROGRESS_PATH = os.path.join(_QDB_ROOT, "warentest_progress.json")


def load_progress() -> set:
    """Load set of URLs already completed in a previous interrupted run."""
    if not os.path.exists(_PROGRESS_PATH):
        return set()
    try:
        with open(_PROGRESS_PATH, encoding="utf-8") as f:
            return set(json.load(f).get("done", []))
    except Exception:
        return set()


def save_progress(done: set) -> None:
    """Persist completed URLs so an interrupted run can resume."""
    try:
        with open(_PROGRESS_PATH, "w", encoding="utf-8") as f:
            json.dump({"done": list(done), "saved_at": time.strftime("%Y-%m-%d %H:%M:%S")}, f)
    except Exception as e:
        log.warning(f"Could not save progress: {e}")


def clear_progress() -> None:
    """Delete checkpoint after a successful full run."""
    try:
        if os.path.exists(_PROGRESS_PATH):
            os.remove(_PROGRESS_PATH)
    except Exception:
        pass


# ── Discovered-URL persistence ────────────────────────────────────────────────
_DISCOVERED_PATH = os.path.join(_QDB_ROOT, "warentest_discovered.json")

def load_discovered_urls() -> dict:
    """
    Load previously discovered test URLs (outside hardcoded TEST_URLS).
    Returns dict: {url: {"category": str, "main_category": str, "discovered_at": str}}
    """
    if not os.path.exists(_DISCOVERED_PATH):
        return {}
    try:
        with open(_DISCOVERED_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"Could not load discovered URLs: {e}")
        return {}


def save_discovered_urls(discovered: dict) -> None:
    """Persist discovered URLs to disk."""
    try:
        with open(_DISCOVERED_PATH, "w", encoding="utf-8") as f:
            json.dump(discovered, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"Could not save discovered URLs: {e}")


def _known_urls() -> set:
    """Return set of all currently known test base URLs."""
    return {url for url, _, _ in TEST_URLS}


# ── RSS-based discovery ────────────────────────────────────────────────────────

# RSS feeds published by test.de — each feed lists the latest test articles
# for a topic area. We parse item links to find new -0/ test URLs.
_RSS_FEEDS = [
    "https://www.test.de/rss/",           # site-wide feed (all new tests)
    "https://www.test.de/rss/?format=rss&rubrik=Unterhaltungselektronik",
    "https://www.test.de/rss/?format=rss&rubrik=Haushaltsgeraete",
    "https://www.test.de/rss/?format=rss&rubrik=Computer",
    "https://www.test.de/rss/?format=rss&rubrik=Gesundheit",
    "https://www.test.de/rss/?format=rss&rubrik=Haus+Garten",
]

# Pattern that identifies a test listing URL (ends with -<digits>-0/)
_TEST_LISTING_RE = re.compile(r'https://www\.test\.de/[^"<>\s]+-\d+-0/?$')
# Exclude the -tabelle- and -detail- pages
_EXCLUDE_RE = re.compile(r'(?:-tabelle|-detail|-vergleich-\d+-\d+)/')
# Require the slug to contain a product-test keyword (filters out articles/guides/news)
# Matches: "im-Test", "im-Vergleich", "in-Test", "Schnelltest", or slug ending "-Test-<ID>-0"
_PRODUCT_TEST_RE = re.compile(r'(?i)-im-Test-|-im-Vergleich-|-in-Test-|-Schnelltest-|-Test-\d')

# Pattern for individual product pages (sitemap/produkte):
# URLs like https://www.test.de/Brand-Model-Name-LISTID-PRODUCTID/
# (two numeric IDs, second is NOT 0)
_PRODUCT_PAGE_RE = re.compile(r'https://www\.test\.de/[^"<>\s]+-\d+-(?!0/)(\d+)/?$')


def discover_via_rss(cookies: dict) -> list[tuple[str, str, str]]:
    """
    Fetch test.de RSS feeds and extract new test listing URLs not yet in TEST_URLS.

    Returns list of (url, category_guess, main_category_guess) tuples.
    """
    import xml.etree.ElementTree as ET

    known = _known_urls()
    new_entries: dict[str, tuple[str, str]] = {}

    for feed_url in _RSS_FEEDS:
        try:
            log.info(f"  RSS: {feed_url}")
            r = cffi_requests.get(feed_url, impersonate="chrome131", cookies=cookies,
                                   headers={"Accept-Language": "de-DE,de;q=0.9"}, timeout=15)
            if r.status_code != 200:
                log.warning(f"  RSS {feed_url} → HTTP {r.status_code}")
                continue

            def _process_rss_link(link, title):
                link = link.strip()
                title = title.strip()
                if not link.endswith('/'):
                    link += '/'
                if _TEST_LISTING_RE.match(link) and not _EXCLUDE_RE.search(link) and link not in known:
                    cat_guess = re.sub(r'\s+(im|in|Vergleich|Test|test)\b.*', '', title, flags=re.I).strip()
                    main_guess = _guess_main_category(cat_guess)
                    new_entries[link] = (cat_guess, main_guess)

            # Try XML parse first
            parsed_ok = False
            try:
                root = ET.fromstring(r.text)
                ns = {"atom": "http://www.w3.org/2005/Atom"}
                # RSS 2.0
                for item in root.findall(".//item"):
                    link_el = item.find("link")
                    title_el = item.find("title")
                    link = (link_el.text or "") if link_el is not None else ""
                    title = (title_el.text or "") if title_el is not None else ""
                    _process_rss_link(link, title)
                # Atom
                for entry in root.findall("atom:entry", ns):
                    link_el = entry.find('atom:link[@rel="alternate"]', ns) or entry.find("atom:link", ns)
                    title_el = entry.find("atom:title", ns)
                    link = link_el.get("href", "") if link_el is not None else ""
                    title = (title_el.text or "") if title_el is not None else ""
                    _process_rss_link(link, title)
                parsed_ok = True
            except ET.ParseError:
                pass  # fall through to HTML parse

            # HTML fallback — test.de may serve HTML for certain cookie states
            if not parsed_ok:
                soup = BeautifulSoup(r.text, "html.parser")
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if not href.startswith('http'):
                        href = "https://www.test.de" + href
                    title = a.get_text(strip=True)
                    _process_rss_link(href, title)

            time.sleep(random.uniform(1, 2))
        except Exception as e:
            log.warning(f"  RSS error for {feed_url}: {e}")

    result = [(url, cat, main) for url, (cat, main) in new_entries.items()]
    log.info(f"RSS discovery: {len(result)} new test URLs found")
    return result


# ── Sitemap-based discovery ────────────────────────────────────────────────────
#
# test.de publishes a sitemap index at /sitemap.xml which links to dozens of
# sub-sitemaps (one per content section). Each sub-sitemap <loc> entry that
# matches the test-listing pattern (-<digits>-0/) is a historical test we can
# scrape. This covers tests going back many years that never appear in RSS.

_SITEMAP_INDEX = "https://www.test.de/sitemap.xml"


def discover_via_sitemap(cookies: dict) -> list[tuple[str, str, str]]:
    """
    Crawl test.de sitemaps and return all historical test listing URLs not yet known.

    Strategy:
      1. Fetch sitemap index → collect all sub-sitemap URLs
      2. For each sub-sitemap fetch its <loc> entries
      3. Keep only URLs matching _TEST_LISTING_RE and not _EXCLUDE_RE
      4. Return as (url, category_guess, main_category_guess)

    This can yield hundreds of historical test categories. Be patient — it
    fetches one sub-sitemap per second and there may be 20-50 of them.
    """
    import xml.etree.ElementTree as ET

    known = _known_urls()
    discovered = load_discovered_urls()
    already_known = known | set(discovered.keys())

    new_entries: dict[str, tuple[str, str]] = {}

    def _get_xml(url):
        try:
            r = cffi_requests.get(url, impersonate="chrome131", cookies=cookies,
                                  headers={"Accept-Language": "de-DE,de;q=0.9"}, timeout=20)
            if r.status_code != 200:
                log.warning(f"  Sitemap HTTP {r.status_code}: {url}")
                return None
            return r.text
        except Exception as e:
            log.warning(f"  Sitemap fetch error {url}: {e}")
            return None

    def _extract_locs(xml_text):
        """Return all <loc> text values from any sitemap XML."""
        locs = []
        try:
            root = ET.fromstring(xml_text)
            # Strip namespace for easier parsing
            for el in root.iter():
                tag = el.tag.split('}')[-1] if '}' in el.tag else el.tag
                if tag == 'loc' and el.text:
                    locs.append(el.text.strip())
        except ET.ParseError as e:
            log.warning(f"  XML parse error: {e}")
        return locs

    # Step 1: fetch index
    log.info(f"  Sitemap index: {_SITEMAP_INDEX}")
    index_xml = _get_xml(_SITEMAP_INDEX)
    if not index_xml:
        log.warning("  Could not fetch sitemap index — aborting sitemap discovery")
        return []

    sub_sitemaps = _extract_locs(index_xml)
    # Filter: only sub-sitemaps that look like they contain test/product pages
    # (skip image, video, news sitemaps)
    sub_sitemaps = [u for u in sub_sitemaps if 'sitemap' in u.lower()
                    and not any(x in u.lower() for x in ['image', 'video', 'news'])]
    # Prioritise the 'archiv' sitemap first — it's the richest source of historical tests
    sub_sitemaps.sort(key=lambda u: (0 if 'archiv' in u.lower() else 1))
    log.info(f"  Found {len(sub_sitemaps)} sub-sitemaps to crawl")

    # Step 2: crawl each sub-sitemap
    for i, sm_url in enumerate(sub_sitemaps, 1):
        log.info(f"  Sub-sitemap {i}/{len(sub_sitemaps)}: {sm_url}")
        sm_xml = _get_xml(sm_url)
        if not sm_xml:
            continue

        locs = _extract_locs(sm_xml)
        found_here = 0
        for loc in locs:
            # Normalise: ensure trailing slash
            if not loc.endswith('/'):
                loc = loc + '/'
            if (_TEST_LISTING_RE.match(loc) and not _EXCLUDE_RE.search(loc)
                    and _PRODUCT_TEST_RE.search(loc)):
                if loc not in already_known and loc not in new_entries:
                    # Derive a category guess from the URL slug
                    slug = loc.rstrip('/').rsplit('/', 1)[-1]
                    # Remove trailing -<digits>-0
                    slug_clean = re.sub(r'-\d+-0$', '', slug)
                    # Strip -im-Test / -im-Vergleich / -Schnelltest suffix from the display name
                    slug_clean = re.sub(r'(?i)-im-(?:Test|Vergleich)$|-in-Test$|-Schnelltest.*$', '', slug_clean)
                    # Also strip trailing -Test if it ends the slug
                    slug_clean = re.sub(r'(?i)-Test$', '', slug_clean)
                    # Convert hyphens to spaces
                    cat_guess = slug_clean.replace('-', ' ').strip()
                    cat_guess = cat_guess[:1].upper() + cat_guess[1:] if cat_guess else slug
                    main_guess = _guess_main_category(cat_guess)
                    new_entries[loc] = (cat_guess, main_guess)
                    found_here += 1

        if found_here:
            log.info(f"    → {found_here} new test URLs")
        time.sleep(random.uniform(3.0, 6.0))  # slow crawl to avoid triggering IP blocks

    result = [(url, cat, main) for url, (cat, main) in new_entries.items()]
    log.info(f"Sitemap discovery: {len(result)} new test URLs found")
    return result


# ── Search-based discovery ─────────────────────────────────────────────────────
#
# test.de has a JSON search API that returns article links. We query it with
# category keywords to surface older tests that may not be in sitemaps (e.g.
# tests published under different slugs, or ones in categories not covered by
# the sitemap partitioning).

_SEARCH_API = "https://www.test.de/api/suche/"

# (search term, candidate main_category) — broad enough to catch archived tests
_SEARCH_TERMS = [
    ("Smartphone Test",          "Telefony a tablety"),
    ("Tablet Test",              "Telefony a tablety"),
    ("Notebook Laptop Test",     "Počítače a notebooky"),
    ("Fernseher Test",           "Televize a video"),
    ("Kopfhörer Test",           "Zvuk a hudba"),
    ("Lautsprecher Test",        "Zvuk a hudba"),
    ("Soundbar Test",            "Zvuk a hudba"),
    ("Smartwatch Test",          "Chytré zařízení"),
    ("Kamera Test",              "Foto a video"),
    ("Drucker Test",             "Počítače a notebooky"),
    ("Monitor Test",             "Počítače a notebooky"),
    ("Router WLAN Test",         "Počítače a notebooky"),
    ("Waschmaschine Test",       "Velké domácí spotřebiče"),
    ("Geschirrspüler Test",      "Velké domácí spotřebiče"),
    ("Kühlschrank Test",         "Velké domácí spotřebiče"),
    ("Backofen Test",            "Velké domácí spotřebiče"),
    ("Wäschetrockner Test",      "Velké domácí spotřebiče"),
    ("Gefriergerät Test",        "Velké domácí spotřebiče"),
    ("Klimagerät Test",          "Velké domácí spotřebiče"),
    ("Mikrowelle Test",          "Velké domácí spotřebiče"),
    ("Staubsauger Test",         "Vysavače a úklid"),
    ("Saugroboter Test",         "Vysavače a úklid"),
    ("Dampfreiniger Test",       "Vysavače a úklid"),
    ("Kaffeevollautomat Test",   "Malé domácí spotřebiče"),
    ("Heißluftfritteuse Test",   "Malé domácí spotřebiče"),
    ("Standmixer Test",          "Malé domácí spotřebiče"),
    ("Luftreiniger Test",        "Malé domácí spotřebiče"),
    ("Bügeleisen Test",          "Malé domácí spotřebiče"),
    ("Mähroboter Test",          "Zahrada a dílna"),
    ("Akkuschrauber Test",       "Zahrada a dílna"),
    ("E-Bike Test",              "Sport a kola"),
    ("Fahrradhelm Test",         "Sport a kola"),
    ("Elektrozahnbürste Test",   "Zdraví a hygiena"),
    ("Blutdruckmessgerät Test",  "Zdraví a hygiena"),
    ("Elektrorasierer Test",     "Zdraví a hygiena"),
    ("Haartrockner Test",        "Zdraví a hygiena"),
    ("Sonnencreme Test",         "Zdraví a hygiena"),
    ("Kindersitz Test",          "Dětské zboží"),
    ("Kinderwagen Test",         "Dětské zboží"),
    ("Matratze Test",            "Bytové vybavení"),
    ("Dashcam Test",             "Auto a moto"),
    ("Koffer Test",              "Cestování"),
]


def discover_via_search(cookies: dict) -> list[tuple[str, str, str]]:
    """
    Search-based discovery is disabled — the /api/suche/ endpoint does not exist
    on test.de and every request times out. The sitemap archiv covers all historical
    tests adequately. This function is kept as a stub so --discover-search / --discover-all
    flags don't crash.
    """
    log.info("Search discovery: skipped (endpoint unavailable — sitemap covers historical tests)")
    return []


# ── Products sitemap discovery ─────────────────────────────────────────────────
#
# test.de has a separate sitemap/produkte that lists individual product test pages.
# These are NOT category comparison tables but individual product test articles,
# with URLs like https://www.test.de/Brand-Model-Name-LISTID-PRODUCTID/
# Each has its own rating, test date, and sub-ratings.
# This is the richest source of historical data — going back years.

_PRODUKTE_SITEMAP = "https://www.test.de/sitemap/produkte"

# Financial/non-product keywords to skip even in produkte sitemap
_PRODUKTE_SKIP_RE = re.compile(
    r'(?i)versicherung|girokonto|tagesgeld|festgeld|fonds|etf|kredit|'
    r'aktien|depot|broker|bauspar|rente|steuer|bank|kreditkarte|'
    r'ernaehrungs|lebensmittel|nahrung|getränk|bier|wein|wasser|lachs|'
    r'joghurt|eis-|vitamin|praeparat|medikament|arznei|impf|corona|covid|'
    r'hundesnack|hundefutter|katzenfutter|tierfutter|'
    r'apps-fuer-kinder|spiele-app|migrae|schmerz-app|backup-software'
)


def discover_via_produkte(cookies: dict) -> list[tuple[str, str, str]]:
    """
    Crawl test.de's products sitemap to find individual product test article pages.
    These are separate from category comparison tables and contain historical product
    ratings going back many years.

    Returns list of (url, category_guess, main_category_guess).
    These URLs are treated as single-product listing pages by the scraper.
    """
    import xml.etree.ElementTree as ET

    known = _known_urls()
    discovered = load_discovered_urls()
    already_known = known | set(discovered.keys())

    new_entries: dict[str, tuple[str, str]] = {}

    def _get_xml(url):
        try:
            r = cffi_requests.get(url, impersonate="chrome131", cookies=cookies,
                                  headers={"Accept-Language": "de-DE,de;q=0.9"}, timeout=20)
            if r.status_code != 200:
                log.warning(f"  Sitemap HTTP {r.status_code}: {url}")
                return None
            return r.text
        except Exception as e:
            log.warning(f"  Sitemap fetch error {url}: {e}")
            return None

    def _extract_locs(xml_text):
        locs = []
        try:
            root = ET.fromstring(xml_text)
            for el in root.iter():
                tag = el.tag.split('}')[-1] if '}' in el.tag else el.tag
                if tag == 'loc' and el.text:
                    locs.append(el.text.strip())
        except ET.ParseError:
            # May be paginated sitemaps or HTML — try BeautifulSoup
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(xml_text, "lxml")
            locs = [t.get_text(strip=True) for t in soup.find_all('loc')]
        return locs

    log.info(f"  Products sitemap: {_PRODUKTE_SITEMAP}")
    xml_text = _get_xml(_PRODUKTE_SITEMAP)
    if not xml_text:
        log.warning("  Could not fetch products sitemap")
        return []

    # The products sitemap may be a sitemap index (linking to sub-sitemaps)
    # or a direct sitemap. Check both.
    locs = _extract_locs(xml_text)

    sub_sitemaps = [l for l in locs if 'sitemap' in l.lower() and l.endswith('.xml')]
    product_urls = [l for l in locs if not l.endswith('.xml')]

    if sub_sitemaps:
        log.info(f"  Products sitemap is an index with {len(sub_sitemaps)} sub-sitemaps")
        all_product_urls = list(product_urls)
        for i, sm_url in enumerate(sub_sitemaps, 1):
            log.info(f"  Products sub-sitemap {i}/{len(sub_sitemaps)}: {sm_url}")
            sm_xml = _get_xml(sm_url)
            if sm_xml:
                all_product_urls.extend(_extract_locs(sm_xml))
            time.sleep(random.uniform(2.0, 4.0))
        product_urls = all_product_urls

    log.info(f"  Total product URLs found: {len(product_urls)}")

    # Filter: keep only top-level product test pages (two numeric IDs, second != 0)
    # Pattern: https://www.test.de/[slug]-LISTID-PRODUCTID/  (no sub-paths)
    # Exclude: URLs with /detail/ sub-paths, financial/food junk, and listing pages (-0/)
    _prod_url_re = re.compile(r'https://www\.test\.de/[^/"<>\s]+-\d+-([1-9]\d*)/?$')

    for loc in product_urls:
        if not loc.endswith('/'):
            loc = loc + '/'
        # Skip sub-paths like /detail/, /tabelle/ etc.
        if '/detail/' in loc or '/tabelle/' in loc:
            continue
        if not _prod_url_re.match(loc):
            continue
        if _PRODUKTE_SKIP_RE.search(loc):
            continue
        if loc in already_known or loc in new_entries:
            continue

        # Derive category from slug (strip product ID and list ID)
        slug = loc.rstrip('/').rsplit('/', 1)[-1]
        # Remove trailing -LISTID-PRODUCTID
        slug_clean = re.sub(r'-\d+-\d+$', '', slug)
        # Strip common suffixes
        slug_clean = re.sub(r'(?i)-im-(?:Test|Vergleich)$|-in-Test$|-Schnelltest.*$|-Test$', '', slug_clean)
        # Convert hyphens to spaces, capitalise
        cat_guess = slug_clean.replace('-', ' ').strip()
        cat_guess = cat_guess[:1].upper() + cat_guess[1:] if cat_guess else slug
        main_guess = _guess_main_category(cat_guess)
        new_entries[loc] = (cat_guess, main_guess)

    result = [(url, cat, main) for url, (cat, main) in new_entries.items()]
    log.info(f"Products sitemap discovery: {len(result)} new individual product URLs found")
    return result


def _guess_main_category(cat_str: str) -> str:
    """
    Heuristically map a German category name to a Czech main_category.
    Falls back to "Ostatní" if no match.
    """
    cat_lower = cat_str.lower()
    MAP = [
        (["smartphone", "handy", "telefon"],             "Telefony a tablety"),
        (["tablet"],                                      "Telefony a tablety"),
        (["notebook", "laptop", "monitor", "drucker",
          "computer", "router", "wlan"],                 "Počítače a notebooky"),
        (["fernseher", "tv ", "streaming", "soundbar"],  "Televize a video"),
        (["kopfhörer", "kopfhoerer", "lautsprecher"],    "Zvuk a hudba"),
        (["smartwatch", "fitness"],                       "Chytré zařízení"),
        (["kamera", "kamera", "digitalkamera"],          "Foto a video"),
        (["waschmaschine", "wäschetrockner",
          "geschirrspüler", "kühlschrank",
          "gefriergerät", "backofen", "klimagerät"],     "Velké domácí spotřebiče"),
        (["staubsauger", "saugroboter", "dampfreiniger"],"Vysavače a úklid"),
        (["kaffeemaschine", "kaffeevollautomat",
          "heißluftfritteuse", "mixer", "luftreiniger",
          "bügeleisen", "mikrowelle"],                   "Malé domácí spotřebiče"),
        (["e-bike", "fahrrad", "fahrradhelm"],           "Sport a kola"),
        (["mähroboter", "akkuschrauber"],                "Zahrada a dílna"),
        (["zahnbürste", "zahnbuerste", "rasierer",
          "blutdruck", "haartrockner", "epilierer",
          "sonnencreme"],                                 "Zdraví a hygiena"),
        (["kindersitz", "kinderwagen",
          "babynahrung"],                                 "Dětské zboží"),
        (["koffer", "reisetasche"],                      "Cestování"),
        (["matratze"],                                    "Bytové vybavení"),
        (["dashcam", "auto"],                            "Auto a moto"),
    ]
    for keywords, main_cat in MAP:
        if any(kw in cat_lower for kw in keywords):
            return main_cat
    return "Ostatní"


# ── Cookie loader ────────────────────────────────────────────────────────────

def load_cookies():
    """Load cookies from cookies_warentest.json in the QualityDB folder."""
    qdb = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # QualityDB/
    path = os.path.join(qdb, "cookies_warentest.json")
    if not os.path.exists(path):
        log.warning(f"Cookie file not found: {path}")
        return {}
    try:
        with open(path) as f:
            cookies = json.load(f)
        log.info(f"Loaded {len(cookies)} cookies from {path}")
        return cookies
    except Exception as e:
        log.error(f"Failed to load cookies: {e}")
        return {}


# ── Auth wall detection ──────────────────────────────────────────────────────

# Phrases that appear in test.de login/paywall pages (both German and English)
_AUTH_WALL_PHRASES = [
    "Bitte melden Sie sich an",
    "Jetzt einloggen",
    "Passwort vergessen",
    "Registrieren Sie sich",
    "login-form",
    "Zum Login",
    "Sie sind nicht eingeloggt",
    "Testen Sie jetzt",
    "Abo abschließen",
    "Jetzt Mitglied werden",
]

def _is_auth_wall(html: str) -> bool:
    """Return True if the page is a login/paywall page, not real content."""
    if not html or len(html) < 500:
        return True  # suspiciously short — likely a redirect stub
    for phrase in _AUTH_WALL_PHRASES:
        if phrase in html:
            return True
    return False


def preflight_check(cookies: dict) -> bool:
    """
    Verify that cookies grant access to premium content before starting a full run.
    Fetches a known-premium listing page and checks:
      - We get a real HTML response (not a timeout)
      - We're not hitting a login/paywall wall
      - The page contains at least one product detail link

    Returns True if everything looks good, False if we should abort.
    """
    test_url = "https://www.test.de/Smartphones-im-Test-4222793-0/"
    log.info(f"Pre-flight check: {test_url}")
    try:
        r = cffi_requests.get(test_url, impersonate="chrome131", cookies=cookies,
                              headers={"Accept-Language": "de-DE,de;q=0.9"}, timeout=15)
        if r.status_code != 200:
            log.error(f"Pre-flight: HTTP {r.status_code} — aborting")
            return False
        if _is_auth_wall(r.text):
            log.error("Pre-flight: got login/paywall page — cookies are expired or invalid")
            log.error("  → Export fresh cookies from a logged-in test.de browser session")
            return False
        # Check for at least one detail link
        if '-detail/' not in r.text:
            log.error("Pre-flight: no product detail links found — page structure may have changed")
            return False
        log.info("Pre-flight: OK — cookies valid, content accessible")
        return True
    except Exception as e:
        log.error(f"Pre-flight: connection failed — {e}")
        log.error("  → test.de may be unreachable or you may be IP-blocked. Wait 30-60 min.")
        return False


# ── HTTP fetch ───────────────────────────────────────────────────────────────

# Circuit breaker: if this many consecutive requests all time out (TCP-level),
# assume we're IP-blocked and pause for BLOCK_PAUSE seconds before resuming.
_CONSECUTIVE_TIMEOUTS = 0
_CIRCUIT_BREAK_THRESHOLD = 4   # 4 timeouts in a row → pause
_BLOCK_PAUSE = 600             # wait 10 minutes then try again


def fetch(url, cookies):
    """
    Fetch page with curl_cffi Chrome 131 impersonation.

    Retries once after a short back-off on timeout.  If consecutive timeouts
    exceed _CIRCUIT_BREAK_THRESHOLD the circuit breaker triggers a long pause
    (likely an IP block) before resuming.
    """
    global _CONSECUTIVE_TIMEOUTS

    for attempt in range(2):  # try twice before giving up
        try:
            r = cffi_requests.get(url, impersonate="chrome131", cookies=cookies,
                                  headers={"Accept-Language": "de-DE,de;q=0.9"},
                                  timeout=20)
            _CONSECUTIVE_TIMEOUTS = 0  # success — reset counter
            if r.status_code == 429:
                log.warning(f"HTTP 429 rate-limited — sleeping 90s")
                time.sleep(90)
                continue
            if r.status_code in (401, 403):
                log.warning(f"HTTP {r.status_code} — cookies may have expired: {url}")
                return None
            if r.status_code != 200:
                log.warning(f"HTTP {r.status_code} for {url}")
                return None
            # Detect auth-wall: page is returned as 200 but is actually a login redirect
            if _is_auth_wall(r.text):
                log.warning(f"Auth wall detected (cookies expired?) — {url}")
                return None
            return r.text

        except Exception as e:
            err = str(e)
            is_timeout = "timed out" in err.lower() or "time out" in err.lower() or "(28)" in err
            if is_timeout:
                _CONSECUTIVE_TIMEOUTS += 1
                if attempt == 0:
                    backoff = 15
                    log.warning(f"Timeout ({_CONSECUTIVE_TIMEOUTS} consecutive) — retry in {backoff}s")
                    time.sleep(backoff)
                    continue
                else:
                    log.error(f"Timeout on retry: {url}")
                    # Check circuit breaker
                    if _CONSECUTIVE_TIMEOUTS >= _CIRCUIT_BREAK_THRESHOLD:
                        log.warning(
                            f"⚠️  Circuit breaker: {_CONSECUTIVE_TIMEOUTS} consecutive timeouts — "
                            f"likely IP-blocked. Pausing {_BLOCK_PAUSE // 60} min before resuming..."
                        )
                        time.sleep(_BLOCK_PAUSE)
                        _CONSECUTIVE_TIMEOUTS = 0
            else:
                log.error(f"Request error: {e}")
            return None

    return None


# ── Grade conversion ─────────────────────────────────────────────────────────

def grade_to_stars(grade):
    """Warentest grade 1.0-5.0 (1=best) → 5-star scale (5=best)."""
    try:
        g = float(grade)
        return round(max(1.0, min(5.0, 6.0 - g)), 1)
    except (ValueError, TypeError):
        return None

def grade_to_recommend(grade):
    """Warentest grade → recommend percentage."""
    try:
        g = float(grade)
        if g <= 1.5: return 98
        if g <= 2.5: return 90
        if g <= 3.5: return 75
        if g <= 4.5: return 50
        return 25
    except (ValueError, TypeError):
        return None

def parse_grade(text):
    """Extract a Warentest grade from text like 'GUT (1,7)' or '2,3' or 'befriedigend (2,8)'."""
    if not text:
        return None
    # Look for number in parentheses first: "(1,7)"
    m = re.search(r'\((\d[.,]\d)\)', text)
    if m:
        try:
            return float(m.group(1).replace(',', '.'))
        except ValueError:
            pass
    # Look for standalone grade: "1,7" or "2.3"
    m = re.search(r'\b(\d[.,]\d)\b', text)
    if m:
        try:
            v = float(m.group(1).replace(',', '.'))
            if 0.5 <= v <= 5.9:
                return v
        except ValueError:
            pass
    return None

def parse_price_eur(text):
    """Extract EUR price from text like '349 €' or '1.299,00 €'."""
    if not text:
        return None
    m = re.search(r'([\d.]+,?\d*)\s*€', text)
    if not m:
        m = re.search(r'€\s*([\d.]+,?\d*)', text)
    if not m:
        return None
    s = m.group(1).replace('.', '').replace(',', '.')
    try:
        return float(s)
    except ValueError:
        return None


# ── HTML parsers ─────────────────────────────────────────────────────────────

# Grade pattern: "gut (1,7)" or "befriedigend (2,8)" etc.
GRADE_RE = re.compile(
    r'(sehr\s*gut|gut|befriedigend|ausreichend|mangelhaft)\s*\((\d[.,]\d)\)',
    re.IGNORECASE
)


def parse_detail_links(soup):
    """
    Primary strategy: find ALL product detail links on the page.
    Warentest product links contain '-detail/' in the URL.
    Returns list of (name, url) tuples.
    """
    results = []
    seen_urls = set()

    for link in soup.find_all('a', href=True):
        href = link['href']
        if '-detail/' not in href:
            continue

        name = link.get_text(strip=True)
        if not name or len(name) < 3:
            continue

        url = href if href.startswith('http') else "https://www.test.de" + href

        if url not in seen_urls:
            seen_urls.add(url)
            results.append((name, url))

    return results


def parse_comparison_teaser(soup):
    """
    Parse the product-comparison-teaser div.
    Products are detail links, grades follow in sequence as "gut (1,7)" etc.
    """
    products = []

    teaser = soup.find('div', class_='product-comparison-teaser')
    if not teaser:
        # Try broader search
        teaser = soup.find('div', class_=lambda c: c and 'comparison' in c.lower())
    if not teaser:
        return products

    # Get product detail links from the teaser
    detail_links = []
    for link in teaser.find_all('a', href=True):
        if '-detail/' in link['href']:
            name = link.get_text(strip=True)
            href = link['href']
            url = href if href.startswith('http') else "https://www.test.de" + href
            if name and len(name) > 3:
                detail_links.append((name, url))

    # Deduplicate while preserving order
    seen = set()
    unique_links = []
    for name, url in detail_links:
        if url not in seen:
            seen.add(url)
            unique_links.append((name, url))

    # Get ALL grade matches from the teaser text (in order)
    teaser_text = teaser.get_text(" ", strip=True)
    grade_matches = GRADE_RE.findall(teaser_text)

    # The first N grades correspond to the "Qualitätsurteil" row
    # Match them 1-to-1 with the product links
    for i, (name, url) in enumerate(unique_links):
        grade = None
        if i < len(grade_matches):
            try:
                grade = float(grade_matches[i][1].replace(',', '.'))
            except ValueError:
                pass

        products.append({
            "name": name,
            "grade": grade,
            "price": None,
            "url": url,
        })

    return products


def parse_product_cards(soup):
    """
    Parse individual product cards that may appear below the comparison table.
    These are often in a list/grid showing all tested products.
    """
    products = []

    # Look for product listing items (cards, tiles, list items)
    for el in soup.find_all(['div', 'li', 'a'], class_=lambda c: c and any(
        k in c.lower() for k in ['product-tile', 'product-card', 'product-list-item',
                                   'product-overview', 'test-result']
    ) if c else False):
        try:
            # Find product link
            link = el if el.name == 'a' else el.find('a', href=True)
            if not link or '-detail/' not in link.get('href', ''):
                continue

            name = link.get_text(strip=True)
            href = link['href']
            url = href if href.startswith('http') else "https://www.test.de" + href

            if not name or len(name) < 3:
                continue

            # Find grade near this element
            text = el.get_text(" ", strip=True)
            grade_match = GRADE_RE.search(text)
            grade = None
            if grade_match:
                try:
                    grade = float(grade_match.group(2).replace(',', '.'))
                except ValueError:
                    pass

            # Find price
            price = parse_price_eur(text)

            products.append({
                "name": name,
                "grade": grade,
                "price": price,
                "url": url,
            })
        except Exception:
            continue

    return products


def tabelle_url(listing_url):
    """Convert a listing URL (...-0/) to the full table URL (...-tabelle/).
    The tabelle page shows ALL tested products, not just the top comparison."""
    return re.sub(r'-0/$', '-tabelle/', listing_url)


def parse_tabelle_page(soup):
    """
    Parse the full comparison table page (-tabelle/ URL).
    test.de shows ALL products here (e.g. all 448 smartphones).
    Each product row has a -detail/ link and an overall grade nearby.
    Returns list of {name, grade, price, url} dicts.
    """
    products = []
    seen = set()

    # The tabelle is a transposed HTML table: rows = criteria, columns = products.
    # BUT on the tabelle page products are often listed as rows in a product list.
    # Strategy: find every -detail/ link, then look for a grade in its closest ancestor row/cell.
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '-detail/' not in href:
            continue

        name = link.get_text(strip=True)
        if not name or len(name) < 3:
            continue

        url = href if href.startswith('http') else "https://www.test.de" + href
        if url in seen:
            continue
        seen.add(url)

        # Look for an overall grade near this link by walking up the DOM
        grade = None
        node = link.parent
        for _ in range(8):
            if node is None:
                break
            cell_text = node.get_text(" ", strip=True)
            # Remove soft hyphens before matching
            cell_text = cell_text.replace('\u00ad', '').replace('\xad', '')
            m = GRADE_RE.search(cell_text)
            if m:
                try:
                    grade = float(m.group(2).replace(',', '.'))
                    if 0.5 <= grade <= 5.9:
                        break
                    grade = None
                except ValueError:
                    pass
            # Stop climbing when we hit a large container (many products)
            if node.name in ('body', 'main', 'section', 'article') and len(cell_text) > 2000:
                break
            node = node.parent

        products.append({"name": name, "grade": grade, "price": None, "url": url})

    return products


def fetch_all_tabelle_pages(listing_url, cookies):
    """
    Fetch the -tabelle/ page and handle pagination if present.
    Returns merged list of all products found across all pages.
    """
    base_tabelle = tabelle_url(listing_url)
    all_products = {}

    for page in range(1, 30):  # max 30 pages (safety limit)
        url = base_tabelle if page == 1 else f"{base_tabelle}?page={page}"
        log.info(f"  Tabelle page {page}: {url}")
        html = fetch(url, cookies)
        if not html:
            break

        soup = BeautifulSoup(html, "html.parser")
        products = parse_tabelle_page(soup)

        if not products:
            log.info(f"  No products on page {page}, stopping pagination")
            break

        new_count = 0
        for p in products:
            if p["url"] not in all_products:
                all_products[p["url"]] = p
                new_count += 1

        log.info(f"  Tabelle page {page}: {len(products)} found, {new_count} new (total: {len(all_products)})")

        # Stop if this page had no new products (duplicate page = end of list)
        if new_count == 0:
            break

        # Check for a "next page" link
        has_next = bool(
            soup.find('a', rel='next') or
            soup.find('a', string=re.compile(r'Weiter|next|›|»', re.I)) or
            soup.find('link', rel='next')
        )
        if not has_next:
            break

        _sleep()

    return list(all_products.values())


def extract_products(html, base_url, cookies=None):
    """Combine all parsing strategies, deduplicate by URL."""
    soup = BeautifulSoup(html, "html.parser")

    # Strategy 1: Comparison teaser (top of page, has grades)
    teaser_products = parse_comparison_teaser(soup)
    log.info(f"  Teaser: {len(teaser_products)} products")

    # Strategy 2: Individual product cards (rest of page)
    card_products = parse_product_cards(soup)
    log.info(f"  Cards: {len(card_products)} products")

    # Strategy 3: All detail links on the page (fallback, no grades)
    all_links = parse_detail_links(soup)
    log.info(f"  Detail links: {len(all_links)} total on page")

    # Merge: teaser products first (have grades), then cards, then remaining links
    merged = {}

    for p in teaser_products:
        merged[p["url"]] = p

    for p in card_products:
        if p["url"] not in merged:
            merged[p["url"]] = p
        elif p.get("grade") and not merged[p["url"]].get("grade"):
            merged[p["url"]]["grade"] = p["grade"]

    for name, url in all_links:
        if url not in merged:
            merged[url] = {"name": name, "grade": None, "price": None, "url": url}

    products = list(merged.values())
    log.info(f"  Total unique: {len(products)} products")
    return products


# ── Detail page scraper ──────────────────────────────────────────────────────

# Sub-rating label → short English key. Covers both English and German
# (test.de serves English by default for many visitors).
SUB_RATING_LABELS = [
    # English labels (longest match first to avoid partial hits)
    ("quality judgment",                "overall"),
    ("protection against water damage", "water_protection"),
    ("environmental properties",        "environmental"),
    ("beverage preparation",            "beverage_prep"),
    ("temperature stability",           "temperature_stability"),
    ("energy efficiency",               "energy_efficiency"),
    ("power consumption",               "power_consumption"),
    ("phone calls",                     "phone"),
    ("endurance test",                  "endurance"),
    ("basic functions",                 "functions"),
    ("water protection",                "water_protection"),
    ("handling",                        "handling"),
    ("endurance",                       "endurance"),
    ("washing",                         "wash"),
    ("vacuuming",                       "vacuum"),
    ("vacuum",                          "vacuum"),
    ("battery",                         "battery"),
    ("display",                         "display"),
    ("camera",                          "camera"),
    ("picture",                         "picture"),
    ("sound",                           "sound"),
    ("noise",                           "noise"),
    ("durability",                      "durability"),
    ("pollutants",                      "pollutants"),
    ("stability",                       "stability"),
    ("baking",                          "baking"),
    ("grilling",                        "grilling"),
    ("cleaning",                        "cleaning"),
    ("safety",                          "safety"),
    ("rinsing",                         "wash_cycle"),
    ("rinse",                           "wash_cycle"),
    ("cooling",                         "cooling"),
    ("freezing",                        "freezing"),
    ("navigation",                      "navigation"),
    ("fitness",                         "fitness"),
    ("communication",                   "communication"),
    ("functions",                       "functions"),
    ("wash",                            "wash"),
    ("quality",                         "overall"),
    # German labels — general
    ("qualitätsurteil",                 "overall"),
    ("grundfunktionen",                 "functions"),
    ("waschen",                         "wash"),
    ("trocknen",                        "dry"),
    ("dauerprüfung",                    "endurance"),
    ("handhabung",                      "handling"),
    ("umwelteigenschaften",             "environmental"),
    ("schutz vor wasserschäden",        "water_protection"),
    ("saugen",                          "vacuum"),
    ("akku",                            "battery"),
    ("geräusch",                        "noise"),
    ("haltbarkeit",                     "durability"),
    ("schadstoffe",                     "pollutants"),
    ("kamera",                          "camera"),
    ("stabilität",                      "stability"),
    ("backen",                          "baking"),
    ("grillen",                         "grilling"),
    ("reinigung",                       "cleaning"),
    ("sicherheit",                      "safety"),
    ("spülen",                          "wash_cycle"),
    ("kühlen",                          "cooling"),
    ("gefrieren",                       "freezing"),
    ("energieeffizienz",                "energy_efficiency"),
    ("temperaturstabilität",            "temperature_stability"),
    ("stromverbrauch",                  "power_consumption"),
    ("getränkezubereitung",             "beverage_prep"),
    ("kommunikation",                   "communication"),
    # E-Bikes
    ("fahreigenschaften",               "ride_quality"),
    ("antrieb",                         "motor"),
    ("reichweite",                      "range"),
    ("bremsen",                         "brakes"),
    ("beleuchtung",                     "lighting"),
    ("motor",                           "motor"),
    ("fahrkomfort",                     "ride_quality"),
    ("lenkung",                         "steering"),
    ("ride quality",                    "ride_quality"),
    ("range",                           "range"),
    ("brakes",                          "brakes"),
    ("lighting",                        "lighting"),
    ("motor",                           "motor"),
    # Mattresses
    ("liegekomfort",                    "comfort"),
    ("hygiene",                         "hygiene"),
    ("verarbeitung",                    "build_quality"),
    ("schlafkomfort",                   "comfort"),
    ("comfort",                         "comfort"),
    ("build quality",                   "build_quality"),
    # Car seats / Baby
    ("crashtest",                       "crash_test"),
    ("bedienung",                       "ease_of_use"),
    ("ergonomie",                       "ergonomics"),
    ("crash test",                      "crash_test"),
    ("ease of use",                     "ease_of_use"),
    ("ergonomics",                      "ergonomics"),
    # Electric toothbrushes
    ("reinigungswirkung",               "cleaning_effect"),
    ("zahnfleisch",                     "gum_care"),
    ("cleaning effect",                 "cleaning_effect"),
    ("gum care",                        "gum_care"),
    # Blood pressure monitors
    ("messgenauigkeit",                 "accuracy"),
    ("measurement accuracy",            "accuracy"),
    ("accuracy",                        "accuracy"),
    # Air purifiers / Climate
    ("luftreinigung",                   "air_cleaning"),
    ("kühlung",                         "cooling"),
    ("air cleaning",                    "air_cleaning"),
    ("air quality",                     "air_cleaning"),
    # Robot lawnmowers
    ("mähen",                           "mowing"),
    ("mowing",                          "mowing"),
    ("navigation",                      "navigation"),
    ("rasenpflege",                     "mowing"),
    # Printers
    ("druckqualität",                   "print_quality"),
    ("druckkosten",                     "print_cost"),
    ("print quality",                   "print_quality"),
    ("print cost",                      "print_cost"),
    ("running costs",                   "print_cost"),
    # Shavers / Personal care
    ("rasierergebnis",                  "shave_quality"),
    ("hautverträglichkeit",             "skin_comfort"),
    ("shave quality",                   "shave_quality"),
    ("skin comfort",                    "skin_comfort"),
    # Sunscreen
    ("lichtschutz",                     "uv_protection"),
    ("uv-schutz",                       "uv_protection"),
    ("uv protection",                   "uv_protection"),
    ("verträglichkeit",                 "skin_tolerance"),
    ("skin tolerance",                  "skin_tolerance"),
    # Air fryers / cooking
    ("garergebnis",                     "cooking_quality"),
    ("cooking quality",                 "cooking_quality"),
    ("cooking",                         "cooking_quality"),
    ("frittierergebnis",                "cooking_quality"),
    # Mixers / Blenders
    ("mixen",                           "blending"),
    ("blending",                        "blending"),
    ("mixergebnis",                     "blending"),
    # Dashcams
    ("videoqualität",                   "video_quality"),
    ("aufnahme",                        "video_quality"),
    ("video quality",                   "video_quality"),
    ("night vision",                    "night_vision"),
    ("nachtsicht",                      "night_vision"),
]


def map_sub_rating_label(label_clean):
    """Map a cleaned label string to a short English key."""
    for de_label, en_key in SUB_RATING_LABELS:
        if de_label in label_clean:
            return en_key
    return label_clean  # fallback: use the label itself


def scrape_detail_page(url, cookies):
    """
    Fetch a product's detail page and extract sub-ratings, price, and metadata.
    Returns dict with: sub_ratings, price, test_program, similar_to
    """
    html = fetch(url, cookies)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    result = {"sub_ratings": {}, "price": None, "test_program": None, "similar_to": None}

    # Strip soft hyphens (\xad / &#173;) which test.de uses everywhere and break regexes
    raw_text = soup.get_text(" ", strip=True)
    text = raw_text.replace('\u00ad', '').replace('\xad', '').replace('\u202f', ' ')

    # ── Sub-ratings ──────────────────────────────────────────────────────────
    # test.de format (German): "Display 15 % befriedigend (3,4)"
    #   = <label> <weight>% <grade_word> (<grade_num>)
    # Also seen without weight, or with soft-hyphen in label (already stripped above).
    sub_pattern = re.compile(
        r'([\wäöüÄÖÜß]+(?:\s[\wäöüÄÖÜß]+){0,4}?)'   # label: 1-5 words
        r'\s*(?:\d+\s*%\s*)?'                          # optional weight "15 %"
        r'(sehr\s*gut|gut|befriedigend|ausreichend|mangelhaft'
        r'|very\s+good|good|satisfactory|sufficient|poor)\s*'
        r'\((\d[.,]\d)\)',                             # "(3,4)"
        re.IGNORECASE
    )

    for match in sub_pattern.finditer(text):
        label_clean = match.group(1).strip().lower()
        grade_word  = match.group(2).strip()
        grade_num_str = match.group(3).replace(',', '.')

        try:
            grade_num = float(grade_num_str)
        except ValueError:
            continue

        if not (0.5 <= grade_num <= 5.9):
            continue

        key = map_sub_rating_label(label_clean)
        result["sub_ratings"][key] = {
            "label": grade_word.lower(),
            "grade": grade_num,
            "stars": grade_to_stars(grade_num),
        }

    # ── Price ────────────────────────────────────────────────────────────────
    # German (after soft-hyphen removal): "Mittlerer Onlinepreis 135,00 Euro"
    # English: "Average online price 135.00 Euro"
    price_match = re.search(
        r'(?:Mittlerer\s+Onlinepreis|Average\s+[Oo]nline\s+[Pp]rice)\s+([\d.,]+)\s*Euro',
        text, re.IGNORECASE
    )
    if price_match:
        result["price"] = parse_price_eur(price_match.group(1) + " €")

    # ── Test program ─────────────────────────────────────────────────────────
    # German (after stripping): "Untersuchungsprogramm Fußnote: 3 Handys 06/2024 Online-Veröffentlichung"
    # We want the part after the keyword (and optional "Fußnote: N") up to "Online"
    prog_match = re.search(
        r'Untersuchungsprogramm\s+(?:Funote|Fu.note|Fußnote):\s*\d+\s+(.+?)\s+Online',
        text, re.IGNORECASE
    )
    if not prog_match:
        # Try without footnote
        prog_match = re.search(
            r'(?:Untersuchungsprogramm|Investigation\s+program)\s+(.{5,60}?)\s+(?:Online|Produkt|$)',
            text, re.IGNORECASE
        )
    if prog_match:
        result["test_program"] = prog_match.group(1).strip()

    # ── Similar / identical product ──────────────────────────────────────────
    sim_match = re.search(
        r'(?:Baugleich|Ähnlichkeit|Similarity)\s*:?\s*(.{3,150}?)\s*(?:Mittlerer|Average|Unter|$)',
        text, re.IGNORECASE
    )
    if sim_match:
        result["similar_to"] = sim_match.group(1).strip()[:200]

    return result


# ── Database ─────────────────────────────────────────────────────────────────

def ensure_columns(conn):
    """Add optional columns if they don't exist (safe to run repeatedly)."""
    for col, typedef in [
        ("details_json", "TEXT"),
        ("test_date",    "TEXT"),   # "YYYY-MM" or "YYYY" — when the test was conducted
    ]:
        try:
            conn.execute(f"SELECT {col} FROM products LIMIT 1")
        except sqlite3.OperationalError:
            conn.execute(f"ALTER TABLE products ADD COLUMN {col} {typedef}")
            conn.commit()
            log.info(f"Added column '{col}' to products table")


# ── Test-date extraction ──────────────────────────────────────────────────────

def extract_test_date(html: str):
    """
    Try several strategies to find when a test was conducted from a listing/tabelle page.
    Returns "YYYY-MM" if month known, "YYYY" if only year, or None.

    Sources tried in order of reliability:
      1. JSON-LD datePublished / dateModified
      2. <meta property="article:published_time">
      3. <time datetime="..."> elements
      4. "Stand: MM/JJJJ" text pattern (test.de's standard "as of" label)
      5. "MM/YYYY" near test-related keywords (Heft, Untersuchung, Test)
    """
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # 1. JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            for key in ("datePublished", "dateModified", "dateCreated"):
                val = data.get(key, "")
                if val:
                    m = re.match(r'(\d{4})-(\d{2})', val)
                    if m:
                        return f"{m.group(1)}-{m.group(2)}"
                    m = re.match(r'(\d{4})', val)
                    if m:
                        return m.group(1)
        except Exception:
            pass

    # 2. Meta tag
    for meta in soup.find_all("meta", property="article:published_time"):
        val = meta.get("content", "")
        m = re.match(r'(\d{4})-(\d{2})', val)
        if m:
            return f"{m.group(1)}-{m.group(2)}"

    # 3. <time> elements
    for t in soup.find_all("time", datetime=True):
        val = t["datetime"]
        m = re.match(r'(\d{4})-(\d{2})', val)
        if m:
            return f"{m.group(1)}-{m.group(2)}"

    # 4 & 5. Text patterns — work on stripped text (removes soft hyphens)
    text = html.replace('\u00ad', '').replace('\xad', '')

    # "Stand: 01/2024" or "Stand Januar 2024"
    m = re.search(r'Stand\s*:?\s*(\d{1,2})[./](\d{4})', text, re.I)
    if m:
        return f"{m.group(2)}-{m.group(1).zfill(2)}"

    m = re.search(
        r'Stand\s*:?\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+(\d{4})',
        text, re.I
    )
    if m:
        month_map = {"januar":1,"februar":2,"märz":3,"april":4,"mai":5,"juni":6,
                     "juli":7,"august":8,"september":9,"oktober":10,"november":11,"dezember":12}
        mn = month_map.get(m.group(1).lower(), 0)
        if mn:
            return f"{m.group(2)}-{str(mn).zfill(2)}"

    # "Heft 01/2024" or near "Untersuchungsprogramm" / "veröffentlicht"
    for keyword in (r'Heft', r'Untersuchung', r'ver.ffentlicht', r'getestet', r'Online'):
        m = re.search(rf'{keyword}.{{0,40}}?(\d{{1,2}})[./](\d{{4}})', text, re.I)
        if m:
            month, year = m.group(1), m.group(2)
            if 1 <= int(month) <= 12 and 1990 <= int(year) <= 2030:
                return f"{year}-{month.zfill(2)}"

    # Last resort: any MM/YYYY in a reasonable range
    for m in re.finditer(r'\b(\d{1,2})[./](\d{4})\b', text):
        month, year = int(m.group(1)), int(m.group(2))
        if 1 <= month <= 12 and 1990 <= year <= 2030:
            return f"{year}-{str(month).zfill(2)}"

    return None


def upsert(conn, products, category, main_category):
    """Insert/update products into the products table (same as all other scrapers)."""
    cur = conn.cursor()
    inserted = updated = 0

    for p in products:
        if not p.get("name") or not p.get("url"):
            continue
        try:
            stars = grade_to_stars(p["grade"]) if p.get("grade") else None
            recommend = grade_to_recommend(p["grade"]) if p.get("grade") else None
            details = json.dumps(p.get("details"), ensure_ascii=False) if p.get("details") else None

            # Check existence before upsert — changes() returns 1 for both
            # INSERT and ON CONFLICT UPDATE, so it can't distinguish them.
            exists = cur.execute(
                "SELECT 1 FROM products WHERE ProductURL = ?", (p["url"],)
            ).fetchone()

            cur.execute("""
                INSERT INTO products
                  (Name, Category, MainCategory, ProductURL,
                   Price_EUR, AvgStarRating, RecommendRate_pct,
                   source, country, currency, details_json, test_date)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(ProductURL) DO UPDATE SET
                  Name             = excluded.Name,
                  Price_EUR        = COALESCE(excluded.Price_EUR, products.Price_EUR),
                  AvgStarRating    = excluded.AvgStarRating,
                  RecommendRate_pct= excluded.RecommendRate_pct,
                  details_json     = COALESCE(excluded.details_json, products.details_json),
                  test_date        = COALESCE(excluded.test_date, products.test_date)
            """, (
                p["name"], category, main_category, p["url"],
                p.get("price"), stars, recommend,
                "warentest", "DE", "EUR", details, p.get("test_date"),
            ))
            if exists:
                updated += 1
            else:
                inserted += 1

            # Also write sub_ratings to dedicated table if present
            sub = (p.get("details") or {}).get("sub_ratings", {})
            for key, val in sub.items():
                try:
                    cur.execute("""
                        INSERT OR REPLACE INTO warentest_sub_ratings
                          (product_url, rating_key, rating_label, grade, stars)
                        VALUES (?,?,?,?,?)
                    """, (p["url"], key, val.get("label"), val.get("grade"), val.get("stars")))
                except Exception:
                    pass

        except Exception as e:
            log.error(f"  DB error: {e}")

    conn.commit()
    return inserted, updated


# ── Debug ────────────────────────────────────────────────────────────────────

def debug_page(html):
    """Print detailed HTML structure for debugging."""
    soup = BeautifulSoup(html, "html.parser")
    print("\n" + "="*80)
    print("HTML STRUCTURE ANALYSIS")
    print("="*80)

    title = soup.find('title')
    print(f"\nPage Title: {title.get_text() if title else 'N/A'}")

    # Tables
    tables = soup.find_all('table')
    print(f"\nTables: {len(tables)}")
    for i, t in enumerate(tables[:3]):
        rows = t.find_all('tr')
        print(f"  Table {i+1}: {len(rows)} rows")
        if rows:
            cells = rows[0].find_all(['th', 'td'])
            print(f"    Header cells: {len(cells)}")
            for j, c in enumerate(cells[:5]):
                print(f"      [{j}] {c.get_text(strip=True)[:60]}")
            if len(rows) > 1:
                cells2 = rows[1].find_all(['th', 'td'])
                print(f"    Row 2 cells: {len(cells2)}")
                for j, c in enumerate(cells2[:5]):
                    print(f"      [{j}] {c.get_text(strip=True)[:60]}")

    # Product divs
    pdivs = soup.find_all('div', class_=lambda c: c and 'product' in c.lower())
    print(f"\nProduct divs: {len(pdivs)}")
    for i, d in enumerate(pdivs[:3]):
        print(f"  Div {i+1} class={d.get('class')}")
        text = d.get_text(" ", strip=True)[:200]
        print(f"    Text: {text}")
        links = d.find_all('a', href=True)
        for a in links[:2]:
            print(f"    Link: {a['href'][:80]} → {a.get_text(strip=True)[:40]}")

    # Articles
    articles = soup.find_all('article')
    print(f"\nArticle tags: {len(articles)}")
    for i, a in enumerate(articles[:3]):
        print(f"  Article {i+1} class={a.get('class')}")
        text = a.get_text(" ", strip=True)[:200]
        print(f"    Text: {text}")

    # Any elements with grade text
    grade_keywords = ['sehr gut', 'gut (', 'befriedigend', 'ausreichend', 'mangelhaft']
    body_text = soup.get_text()
    print(f"\nGrade keyword occurrences:")
    for kw in grade_keywords:
        count = body_text.lower().count(kw)
        if count:
            print(f"  '{kw}': {count} times")

    print("="*80 + "\n")


def debug_full(html):
    """Dump first few product divs' full HTML."""
    soup = BeautifulSoup(html, "html.parser")
    pdivs = soup.find_all('div', class_=lambda c: c and 'product' in c.lower())
    print(f"\n{'='*80}\nFULL PRODUCT DIV HTML (first 3)\n{'='*80}\n")
    for i, d in enumerate(pdivs[:3]):
        print(f"--- DIV {i+1} ---")
        print(str(d)[:2000])
        print()


# ── Entry point ──────────────────────────────────────────────────────────────

def scrape_warentest(db_path=None):
    if db_path is None:
        db_path = DB_PATH

    debug = "--debug" in sys.argv
    debug_f = "--debug-full" in sys.argv
    detail_debug = "--detail-debug" in sys.argv
    with_details = "--details" in sys.argv or "--with-details" in sys.argv

    cookies = load_cookies()
    if not cookies:
        log.warning("No cookies — premium content may be inaccessible")

    # Debug: show detail page structure for first product
    if detail_debug:
        url, cat, _ = TEST_URLS[0]
        log.info(f"Detail debug: fetching listing {url}")
        html = fetch(url, cookies)
        if not html:
            return
        products = extract_products(html, url)
        if not products:
            log.warning("No products on listing page")
            return
        # Pick first product with a detail URL
        for p in products:
            if '-detail/' in p['url']:
                log.info(f"Fetching detail page: {p['url']}")
                detail = scrape_detail_page(p['url'], cookies)
                print(f"\nProduct: {p['name']}")
                print(f"URL: {p['url']}")
                if detail:
                    print(f"Price: {detail['price']}")
                    print(f"Test program: {detail['test_program']}")
                    print(f"Similar to: {detail['similar_to']}")
                    print(f"Sub-ratings ({len(detail['sub_ratings'])}):")
                    for key, val in detail['sub_ratings'].items():
                        print(f"  {key:25s}  {val.get('label','?'):15s} ({val['grade']})  → {val['stars']}★")
                    # Search page text for relevant keywords to debug missing fields
                    from bs4 import BeautifulSoup as _BS
                    _html = fetch(p['url'], cookies)
                    if _html:
                        _text = _BS(_html, "html.parser").get_text(" ", strip=True)
                        print(f"\n--- Page total: {len(_text)} chars ---")
                        for kw in ["very good","good","gut","sehr gut","Euro","price","Preis",
                                   "program","Programm","similar","baugleich","satisfactory","(1,","(2,"]:
                            idx = _text.lower().find(kw.lower())
                            if idx >= 0:
                                s = max(0, idx-60); e = min(len(_text), idx+150)
                                print(f"  [{kw}@{idx}]: ...{repr(_text[s:e])}...")
                        print("---")
                else:
                    print("  (no detail data extracted)")
                print()
                break
        return

    if debug or debug_f:
        url, cat, _ = TEST_URLS[0]
        log.info(f"Debug: fetching {url}")
        html = fetch(url, cookies)
        if html:
            if debug_f:
                debug_full(html)
            else:
                debug_page(html)

            # Also try parsing to show results
            products = extract_products(html, url)
            print(f"\nParsed {len(products)} products:")
            for p in products[:10]:
                g = f"Grade {p['grade']}" if p.get('grade') else "No grade"
                pr = f"{p['price']:.0f}€" if p.get('price') else "No price"
                print(f"  {p['name'][:50]:50s}  {g:15s}  {pr}")
        return

    do_discover          = "--discover"          in sys.argv
    do_discover_sitemap  = "--discover-sitemap"  in sys.argv or "--discover-all" in sys.argv
    do_discover_search   = "--discover-search"   in sys.argv or "--discover-all" in sys.argv
    do_discover_produkte = "--discover-produkte" in sys.argv or "--discover-all" in sys.argv
    do_discover         |= "--discover-all"      in sys.argv  # --discover-all implies --discover too
    discover_only        = "--discover-only"     in sys.argv  # skip scraping, just persist new URLs
    skip_preflight       = "--no-preflight"      in sys.argv  # bypass preflight (e.g. testing)

    # ── Pre-flight: verify connectivity and cookie auth ───────────────────────
    if not skip_preflight and not discover_only:
        if not preflight_check(cookies):
            log.error("Aborting — fix cookies or wait for IP block to lift, then retry.")
            sys.exit(1)

    conn = sqlite3.connect(db_path)
    ensure_columns(conn)
    total_ins = total_upd = 0
    all_products = []  # Collect for detail pass

    # ── Discovery pass ────────────────────────────────────────────────────────
    discovered = load_discovered_urls()
    added_new = 0

    def _merge_discovered(new_entries):
        nonlocal added_new
        for url, cat, main_cat in new_entries:
            if url not in discovered:
                discovered[url] = {
                    "category": cat,
                    "main_category": main_cat,
                    "discovered_at": time.strftime("%Y-%m-%d"),
                }
                log.info(f"  NEW: [{cat}]  {url}")
                added_new += 1

    if do_discover:
        log.info("=== Discovery: RSS feeds (recent tests) ===")
        _merge_discovered(discover_via_rss(cookies))

    if do_discover_sitemap:
        log.info("=== Discovery: sitemap crawl (all historical tests) ===")
        _merge_discovered(discover_via_sitemap(cookies))

    if do_discover_search:
        log.info("=== Discovery: search-based (older/archived tests) ===")
        _merge_discovered(discover_via_search(cookies))

    if do_discover_produkte:
        log.info("=== Discovery: products sitemap (individual product test pages) ===")
        _merge_discovered(discover_via_produkte(cookies))

    if do_discover or do_discover_sitemap or do_discover_search or do_discover_produkte:
        if added_new:
            save_discovered_urls(discovered)
            log.info(f"Discovery complete: {added_new} new URLs added (total stored: {len(discovered)})")
        else:
            log.info("Discovery complete: no new test URLs found")

    if discover_only:
        log.info("--discover-only: skipping scrape pass")
        conn.close()
        return 0, 0

    # Build combined URL list: hardcoded + discovered (skip known-empty ones)
    # discovered entries may have "empty_strikes": N — skip after 2 consecutive zero-product runs
    combined_urls: list[tuple[str, str, str]] = list(TEST_URLS)
    skipped_empty = 0
    for url, meta in discovered.items():
        if meta.get("empty_strikes", 0) >= 2:
            skipped_empty += 1
            continue
        combined_urls.append((url, meta["category"], meta["main_category"]))

    log.info(f"Total test categories to scrape: {len(combined_urls)} "
             f"({len(TEST_URLS)} hardcoded + {len(discovered) - skipped_empty} discovered, "
             f"{skipped_empty} skipped as persistently empty)")

    discovered_dirty = False  # track if we need to re-save discovered

    # Resume support: load previously completed URLs and skip them
    do_resume = "--resume" in sys.argv
    completed = load_progress() if do_resume else set()
    if completed:
        log.info(f"Resuming: {len(completed)} categories already done, skipping them")

    for url, category, main_cat in combined_urls:
        if url in completed:
            continue  # already done in a previous interrupted run

        log.info(f"Warentest  [{category}]")

        listing_html = None  # keep hold of listing HTML for date extraction

        # PRIMARY: tabelle URL → get ALL tested products (e.g. all 448 smartphones)
        products = fetch_all_tabelle_pages(url, cookies)

        if not products:
            # FALLBACK: listing page (only shows ~17 top products)
            log.warning(f"  Tabelle empty — falling back to listing page")
            listing_html = fetch(url, cookies)
            if not listing_html:
                # Network failure — don't penalise, just skip for now
                _sleep()
                continue
            products = extract_products(listing_html, url)

        if not products:
            log.warning(f"  No products found for [{category}]")
            # Increment empty_strikes for discovered URLs so we skip them faster next time
            if url in discovered:
                discovered[url]["empty_strikes"] = discovered[url].get("empty_strikes", 0) + 1
                discovered_dirty = True
            _sleep()
            continue

        # Extract test date from the listing page (fetch it if we only used tabelle)
        if listing_html is None:
            listing_html = fetch(url, cookies)
        test_date = extract_test_date(listing_html) if listing_html else None
        if test_date:
            log.info(f"  Test date: {test_date}")
            for p in products:
                p.setdefault("test_date", test_date)

        # Successful scrape — reset empty_strikes
        if url in discovered and discovered[url].get("empty_strikes", 0) > 0:
            discovered[url]["empty_strikes"] = 0
            discovered_dirty = True

        ins, upd = upsert(conn, products, category, main_cat)
        total_ins += ins
        total_upd += upd
        log.info(f"  [{category}] {len(products)} found → {ins} new, {upd} updated")

        if with_details:
            all_products.extend([(p, category, main_cat) for p in products])

        # Mark URL as done for resume support (save every 10 categories)
        completed.add(url)
        if len(completed) % 10 == 0:
            save_progress(completed)

        _sleep()

    # Persist any empty_strikes updates
    if discovered_dirty:
        save_discovered_urls(discovered)

    # Full run completed — clear progress checkpoint
    clear_progress()

    log.info(f"\nListing pass done: {total_ins} inserted, {total_upd} updated")

    # Second pass: fetch detail pages for sub-ratings & prices
    if with_details and all_products:
        detail_count = 0
        detail_products = [(p, cat, mc) for p, cat, mc in all_products if '-detail/' in p.get('url', '')]
        log.info(f"\nDetail pass: {len(detail_products)} product detail pages to scrape...")

        for p, category, main_cat in detail_products:
            log.info(f"  Detail: {p['name'][:40]}")
            detail = scrape_detail_page(p['url'], cookies)
            if detail:
                p['details'] = detail
                # Update price if we got one from detail page
                if detail.get('price') and not p.get('price'):
                    p['price'] = detail['price']
                # Re-upsert with details
                upsert(conn, [p], category, main_cat)
                detail_count += 1
            _sleep()

        log.info(f"Detail pass done: {detail_count} products enriched with sub-ratings")

    conn.close()
    log.info(f"\nWarentest finished: {total_ins} inserted, {total_upd} updated")
    return total_ins, total_upd


if __name__ == "__main__":
    scrape_warentest()
