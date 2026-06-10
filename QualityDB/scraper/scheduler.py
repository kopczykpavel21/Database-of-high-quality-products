#!/usr/bin/env python3
"""
QualityDB Master Scheduler — runs all scrapers on staggered schedules.
═══════════════════════════════════════════════════════════════════════════════════

Schedule overview (all scrapers weekly, weekday 0=Mon … 6=Sun):
  MONDAY     Heureka CZ, Zbozi.cz, Heureka SK              (CZ/SK market)
  TUESDAY    Amazon.de, Conrad.de                            (DE market)
  WEDNESDAY  Fnac.fr, Darty.fr + FR score linking            (FR market)
  THURSDAY   Otto.de, Geizhals.de, Ceneo.pl                  (DE + PL market)
  FRIDAY     Saturn.de, Digitec.ch, Testberichte.de          (DE/CH market)
  SATURDAY   Amazon.com (US)                                  (US market)
  SUNDAY     Coolblue.nl, Prisjakt.nu, PriceRunner.dk,
             PriceRunner.se                                   (NL + Scandinavia)
  1st of month  Warentest.de, Dtest.cz, Indice de réparabilité (FR official)

Staggered so the two Amazon scrapers never run on the same day.
Each day's scrapers run sequentially (one at a time) to avoid IP conflicts.

Removed (broken / inaccessible):
  MediaMarkt.de — Cloudflare blocks scraper
  Idealo.de     — blocks automated access
  CZC.cz        — Playwright session unreliable

Moved to monthly (content updates slowly):
  Warentest.de  — subscription-gated, changes ~monthly
  Dtest.cz      — subscription-gated, changes ~monthly

Usage:
    python3 scraper/scheduler.py              # start the scheduler daemon
    python3 scraper/scheduler.py --now        # run today's due scrapers immediately
    python3 scraper/scheduler.py --market CZ  # run all CZ scrapers now
    python3 scraper/scheduler.py --market DE  # run all DE scrapers now
    python3 scraper/scheduler.py --scraper "Heureka CZ"  # run one scraper by name
    python3 scraper/scheduler.py --list       # print the full schedule and exit

Each scraper run is logged to scraper/logs/<name>.log and recorded in the
scraper_runs table inside products.db for longitudinal diagnostics.
"""

from __future__ import annotations

import os
import sys
import time
import datetime
import logging
import sqlite3
import subprocess
import traceback
import argparse

# ── path setup ────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scraper.config import DB_PATH, SCHEDULE_HOUR, SCHEDULE_MINUTE, JOURNAL_MODE

LOG_DIR = os.path.join(BASE_DIR, "scraper", "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# ── master logger ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(LOG_DIR, "scheduler.log")),
    ],
)
log = logging.getLogger("scheduler")


# ══════════════════════════════════════════════════════════════════════════════
#  Run-tracking table
# ══════════════════════════════════════════════════════════════════════════════

def ensure_run_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS scraper_runs (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            scraper_name     TEXT    NOT NULL,
            market           TEXT    NOT NULL,
            started_at       TEXT    NOT NULL,
            finished_at      TEXT,
            status           TEXT    NOT NULL DEFAULT 'running',
            products_added   INTEGER DEFAULT 0,
            products_updated INTEGER DEFAULT 0,
            error_msg        TEXT,
            duration_sec     REAL
        );
        CREATE INDEX IF NOT EXISTS idx_runs_name_date
            ON scraper_runs(scraper_name, started_at);
    """)
    conn.commit()


def log_run_start(conn: sqlite3.Connection, name: str, market: str) -> int:
    cur = conn.execute(
        "INSERT INTO scraper_runs (scraper_name, market, started_at, status) VALUES (?,?,?,?)",
        (name, market, datetime.datetime.now().isoformat(), "running"),
    )
    conn.commit()
    return cur.lastrowid


def log_run_end(conn: sqlite3.Connection, run_id: int, status: str,
                added: int = 0, updated: int = 0,
                error: str = "", duration: float = 0.0) -> None:
    conn.execute(
        """UPDATE scraper_runs SET
               finished_at=?, status=?, products_added=?,
               products_updated=?, error_msg=?, duration_sec=?
           WHERE id=?""",
        (datetime.datetime.now().isoformat(), status, added, updated,
         error[:2000] if error else None, duration, run_id),
    )
    conn.commit()


def already_ran_today(conn: sqlite3.Connection, name: str) -> bool:
    """Return True if this scraper completed successfully today."""
    today = datetime.date.today().isoformat()
    row = conn.execute(
        "SELECT 1 FROM scraper_runs WHERE scraper_name=? AND status='ok' "
        "AND started_at LIKE ? LIMIT 1",
        (name, today + "%"),
    ).fetchone()
    return row is not None


# ══════════════════════════════════════════════════════════════════════════════
#  Scraper runner — wraps each callable with timing, retry, and tracking
# ══════════════════════════════════════════════════════════════════════════════

def _run_once(scraper: dict) -> tuple:
    """Call a scraper's entry point. Returns (success, stats_dict)."""
    fn    = scraper["fn"]
    stats = {"added": 0, "updated": 0}

    if callable(fn):
        result = fn()
        if isinstance(result, dict):
            stats["added"]   = result.get("total_added",   result.get("added",   0)) or 0
            stats["updated"] = result.get("total_updated", result.get("updated", 0)) or 0
        return True, stats

    if isinstance(fn, list):   # subprocess mode (Playwright / argparse scrapers)
        result = subprocess.run(fn, capture_output=True, text=True, timeout=7200)
        if result.returncode != 0:
            raise RuntimeError(
                f"subprocess exited {result.returncode}:\n{result.stderr[-1000:]}"
            )
        return True, stats

    raise TypeError(f"Unknown fn type for {scraper['name']}: {type(fn)}")


def run_scraper_safe(scraper: dict, conn: sqlite3.Connection,
                     retry: bool = True) -> bool:
    """Run a scraper with error handling, timing, and DB logging."""
    name   = scraper["name"]
    market = scraper["market"]
    log.info(f"▶  Starting  {name}  [{market}]")

    run_id = log_run_start(conn, name, market)
    t0     = time.time()

    for attempt in range(1, 3):
        try:
            ok, stats = _run_once(scraper)
            duration  = time.time() - t0
            log_run_end(conn, run_id, "ok",
                        added=stats["added"], updated=stats["updated"],
                        duration=duration)
            log.info(f"✓  Finished  {name}  "
                     f"(+{stats['added']} new, ~{stats['updated']} updated, "
                     f"{duration / 60:.1f} min)")
            return True

        except Exception as exc:
            err = traceback.format_exc()
            if attempt == 1 and retry:
                log.warning(f"⚠  {name} failed (attempt 1) — retrying in 10 min…\n"
                            f"   {exc}")
                time.sleep(600)
                continue
            duration = time.time() - t0
            log_run_end(conn, run_id, "error", error=err, duration=duration)
            log.error(f"✗  {name} failed after {attempt} attempt(s):\n{err}")
            return False

    return False


# ══════════════════════════════════════════════════════════════════════════════
#  Post-run hooks
# ══════════════════════════════════════════════════════════════════════════════

def run_fr_score_linking() -> None:
    """Link French durability scores onto CZ/DE products after Fnac scrape."""
    try:
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "link_french_scores",
            os.path.join(BASE_DIR, "link_french_scores.py"),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.run(DB_PATH)
        log.info("   ↳ FR score linking complete.")
    except Exception as exc:
        log.error(f"   ↳ FR score linking failed: {exc}")


# ══════════════════════════════════════════════════════════════════════════════
#  Scraper registry
# ══════════════════════════════════════════════════════════════════════════════

def _build_registry() -> list:
    scrapers = []

    def add(name, market, freq, fn, post=None, days=None, day_of_month=None):
        scrapers.append(dict(
            name=name, market=market, freq=freq,
            days=days or [], fn=fn, post=post,
            day_of_month=day_of_month,
        ))

    # ══════════════════════════════════════════════════════════════════════════
    #  ALL scrapers run ONCE per week, spread across days to avoid overload.
    #  Warentest + D-test stay monthly (see below).
    #
    #  Schedule layout (0=Mon … 6=Sun):
    #    MONDAY     Heureka CZ, Zbozi.cz, Heureka SK  (CZ/SK market)
    #    TUESDAY    Amazon.de, Conrad.de               (DE market)
    #    WEDNESDAY  Fnac.fr, Darty.fr + FR score link  (FR market)
    #    THURSDAY   Otto.de, Geizhals.de, Ceneo.pl     (DE + PL market)
    #    FRIDAY     Saturn.de                           (DE market)
    #    SATURDAY   Amazon.com (US)                     (US market)
    #    SUNDAY     Coolblue.nl                         (NL market)
    # ══════════════════════════════════════════════════════════════════════════

    # ── Monday — CZ / SK ─────────────────────────────────────────────────────
    try:
        from scraper.heureka_scraper import run_scraper as run_heureka
        add("Heureka CZ", "CZ", "weekly", run_heureka, days=[0])
    except ImportError as e:
        log.warning(f"Heureka CZ not available: {e}")

    try:
        from scraper.zbozi_scraper import run_scraper as run_zbozi
        add("Zbozi.cz", "CZ", "weekly", run_zbozi, days=[0])
    except ImportError as e:
        log.warning(f"Zbozi.cz not available: {e}")

    try:
        from scraper.heureka_sk_scraper import run_scraper as run_heureka_sk
        add("Heureka SK", "SK", "weekly", run_heureka_sk, days=[0])
    except ImportError as e:
        log.warning(f"Heureka SK not available: {e}")

    # ── Tuesday — DE (Amazon + Conrad) ────────────────────────────────────────
    try:
        from scraper.amazon_de_scraper import scrape_amazon_de
        add("Amazon.de", "DE", "weekly", scrape_amazon_de, days=[1])
    except ImportError as e:
        log.warning(f"Amazon.de not available: {e}")

    try:
        from scraper.conrad_scraper import run_scraper as run_conrad
        add("Conrad.de", "DE", "weekly", run_conrad, days=[1])
    except ImportError as e:
        log.warning(f"Conrad.de not available: {e}")

    # ── Wednesday — FR ────────────────────────────────────────────────────────
    try:
        from scraper.fnac_scraper import run_scraper as run_fnac
        add("Fnac.fr", "FR", "weekly", run_fnac, post=run_fr_score_linking, days=[2])
    except ImportError as e:
        log.warning(f"Fnac.fr not available: {e}")

    try:
        from scraper.darty_scraper import run as run_darty
        add("Darty.fr", "FR", "weekly", run_darty, days=[2])
    except ImportError as e:
        log.warning(f"Darty.fr not available: {e}")

    # ── Thursday — DE (Otto, Geizhals) + PL ───────────────────────────────────
    try:
        from scraper.otto_scraper_v2 import scrape_otto
        add("Otto.de", "DE", "weekly", scrape_otto, days=[3])
    except ImportError as e:
        log.warning(f"Otto.de not available: {e}")

    try:
        from scraper.geizhals_scraper import scrape_geizhals
        add("Geizhals.de", "DE", "weekly", scrape_geizhals, days=[3])
    except ImportError as e:
        log.warning(f"Geizhals.de not available: {e}")

    try:
        from scraper.ceneo_scraper import run_scraper as run_ceneo
        add("Ceneo.pl", "PL", "weekly", run_ceneo, days=[3])
    except ImportError as e:
        log.warning(f"Ceneo.pl not available: {e}")

    # ── Friday — DE (Saturn) + CH (Digitec) + DE (Testberichte) ──────────────
    try:
        from scraper.saturn_scraper import scrape_saturn
        add("Saturn.de", "DE", "weekly", scrape_saturn, days=[4])
    except ImportError as e:
        log.warning(f"Saturn.de not available: {e}")

    try:
        from scraper.digitec_scraper import run_scraper as run_digitec
        add("Digitec.ch", "CH", "weekly", run_digitec, days=[4])
    except ImportError as e:
        log.warning(f"Digitec.ch not available: {e}")

    try:
        from scraper.testberichte_scraper import run_scraper as run_testberichte
        add("Testberichte.de", "DE", "weekly", run_testberichte, days=[4])
    except ImportError as e:
        log.warning(f"Testberichte.de not available: {e}")

    # ── Saturday — US (separate from Amazon.de to avoid IP pattern detection) ──
    try:
        from scraper.amazon_scraper import run_scraper as run_amazon_us
        add("Amazon.com", "US", "weekly", run_amazon_us, days=[5])
    except ImportError as e:
        log.warning(f"Amazon.com not available: {e}")

    # ── Sunday — NL (Coolblue) + Scandinavia (Prisjakt, PriceRunner) ─────────
    try:
        from scraper.coolblue_scraper import run_scraper as run_coolblue
        add("Coolblue.nl", "NL", "weekly", run_coolblue, days=[6])
    except ImportError as e:
        log.warning(f"Coolblue.nl not available: {e}")

    try:
        from scraper.prisjakt_scraper import run_scraper as run_prisjakt
        add("Prisjakt.nu", "SE", "weekly", run_prisjakt, days=[6])
    except ImportError as e:
        log.warning(f"Prisjakt.nu not available: {e}")

    try:
        from scraper.pricerunner_scraper import run_scraper as run_pricerunner_dk
        add("PriceRunner.dk", "DK", "weekly", run_pricerunner_dk, days=[6])
    except ImportError as e:
        log.warning(f"PriceRunner.dk not available: {e}")

    try:
        from scraper.pricerunner_se_scraper import run_scraper as run_pricerunner_se
        add("PriceRunner.se", "SE", "weekly", run_pricerunner_se, days=[6])
    except ImportError as e:
        log.warning(f"PriceRunner.se not available: {e}")

    # ── Monthly — slow-changing quality / compliance data ─────────────────────
    # Warentest.de — subscription-gated German consumer tests, updates ~monthly
    try:
        from scraper.warentest_scraper import scrape_warentest
        add("Warentest.de", "DE", "monthly", scrape_warentest, day_of_month=1)
    except ImportError as e:
        log.warning(f"Warentest.de not available: {e}")

    # Dtest.cz — Czech consumer tests, uses argparse, updates ~monthly
    dtest_py = os.path.join(BASE_DIR, "scraper", "dtest_scraper.py")
    if os.path.exists(dtest_py):
        add("Dtest.cz", "CZ", "monthly",
            [sys.executable, dtest_py, "--skip-existing"], day_of_month=1)

    # Indice de réparabilité — official French government data (data.gouv.fr)
    # Downloads the consolidated CSV: ~2.2 MB, updated daily by the government.
    # We sync monthly — content is stable, EAN-linked, open licence (OL v2.0).
    try:
        from scraper.indicereparabilite_scraper import run_scraper as run_ir
        add("Indice Réparabilité FR", "FR", "monthly", run_ir, day_of_month=1)
    except ImportError as e:
        log.warning(f"Indice Réparabilité FR not available: {e}")

    return scrapers


# ══════════════════════════════════════════════════════════════════════════════
#  Schedule logic
# ══════════════════════════════════════════════════════════════════════════════

def due_today(scraper: dict) -> bool:
    today = datetime.date.today()
    freq  = scraper["freq"]
    if freq == "daily":
        return True
    if freq == "weekly":
        return today.weekday() in (scraper.get("days") or [])
    if freq == "monthly":
        # runs on the specified day-of-month (default: 1st)
        return today.day == scraper.get("day_of_month", 1)
    return False


def run_due_scrapers(registry: list, conn: sqlite3.Connection,
                     force: bool = False) -> None:
    due = [s for s in registry if force or due_today(s)]
    if not due:
        log.info("No scrapers due today.")
        return

    log.info(f"Scrapers due — {datetime.date.today()}: "
             f"{', '.join(s['name'] for s in due)}")

    for scraper in due:
        if not force and already_ran_today(conn, scraper["name"]):
            log.info(f"   ↷ {scraper['name']} already completed today — skipping.")
            continue

        ok = run_scraper_safe(scraper, conn)

        if ok and scraper.get("post"):
            try:
                scraper["post"]()
            except Exception as exc:
                log.error(f"Post-run hook for {scraper['name']} failed: {exc}")

        # Brief cooldown between scrapers so rate-limit windows reset
        time.sleep(30)


def run_market(registry: list, conn: sqlite3.Connection, market: str) -> None:
    market_scrapers = [s for s in registry
                       if s["market"].upper() == market.upper()]
    if not market_scrapers:
        valid = sorted({s["market"] for s in registry})
        log.error(f"No scrapers for market '{market}'. Valid: {valid}")
        return
    log.info(f"Running all {market.upper()} scrapers immediately…")
    for scraper in market_scrapers:
        run_scraper_safe(scraper, conn, retry=False)
        time.sleep(15)


def run_one(registry: list, conn: sqlite3.Connection, name: str) -> None:
    matches = [s for s in registry if s["name"].lower() == name.lower()]
    if not matches:
        close = [s["name"] for s in registry if name.lower() in s["name"].lower()]
        log.error(f"No scraper named '{name}'. "
                  f"Did you mean: {close or [s['name'] for s in registry]}")
        return
    run_scraper_safe(matches[0], conn, retry=False)


def print_schedule(registry: list) -> None:
    DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    print(f"\n{'Scraper':<28} {'Market':<7} {'Frequency':<10} When")
    print("─" * 62)
    for s in registry:
        freq = s["freq"]
        if freq == "daily":
            when = "every day"
        elif freq == "monthly":
            dom = s.get("day_of_month", 1)
            suffix = {1: "st", 2: "nd", 3: "rd"}.get(dom, "th")
            when = f"{dom}{suffix} of each month"
        else:
            when = ", ".join(DAYS[d] for d in sorted(s.get("days") or []))
        print(f"{s['name']:<28} {s['market']:<7} {freq:<10} {when}")
    print()


def seconds_until(hour: int, minute: int) -> float:
    now    = datetime.datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += datetime.timedelta(days=1)
    return (target - now).total_seconds()


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    ap = argparse.ArgumentParser(
        description="QualityDB Master Scheduler — runs all 16+ scrapers on schedule."
    )
    ap.add_argument("--now",     action="store_true",
                    help="Run today's due scrapers immediately instead of waiting for 03:00")
    ap.add_argument("--market",  metavar="MARKET",
                    help="Run all scrapers for a market now (e.g. CZ, DE, US, FR, PL, SK)")
    ap.add_argument("--scraper", metavar="NAME",
                    help='Run a single scraper by name now (e.g. "Heureka CZ")')
    ap.add_argument("--list",    action="store_true",
                    help="Print the full schedule and exit")
    args = ap.parse_args()

    log.info("QualityDB Master Scheduler starting…")

    registry = _build_registry()
    log.info(f"Loaded {len(registry)} scrapers.")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")
    conn.execute("PRAGMA synchronous=NORMAL")
    ensure_run_table(conn)

    # ── One-shot modes ────────────────────────────────────────────────────────
    if args.list:
        print_schedule(registry)
        return

    if args.market:
        run_market(registry, conn, args.market)
        return

    if args.scraper:
        run_one(registry, conn, args.scraper)
        return

    if args.now:
        log.info("--now flag: running today's due scrapers immediately.")
        run_due_scrapers(registry, conn, force=False)
        return

    # ── Daemon mode ───────────────────────────────────────────────────────────
    log.info(f"Daemon mode — daily wake-up at {SCHEDULE_HOUR:02d}:{SCHEDULE_MINUTE:02d}.")
    log.info("Press Ctrl+C to stop.\n")

    def _open_conn():
        """Open a fresh DB connection with the right pragmas."""
        c = sqlite3.connect(DB_PATH, timeout=60)
        c.execute(f"PRAGMA journal_mode={JOURNAL_MODE}")
        c.execute("PRAGMA synchronous=NORMAL")
        ensure_run_table(c)
        return c

    while True:
        wait   = seconds_until(SCHEDULE_HOUR, SCHEDULE_MINUTE)
        wakeup = datetime.datetime.now() + datetime.timedelta(seconds=wait)
        log.info(f"Sleeping until {wakeup.strftime('%Y-%m-%d %H:%M')} ({wait/3600:.1f} h)…")

        try:
            time.sleep(wait)
        except KeyboardInterrupt:
            log.info("Scheduler stopped by user.")
            break

        log.info(f"{'═' * 60}")
        log.info(f"Daily run — {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
        log.info(f"{'═' * 60}")

        # Retry up to 3 times on transient disk / DB errors, reconnecting each time
        for attempt in range(1, 4):
            try:
                run_due_scrapers(registry, conn)
                break   # success — exit retry loop
            except Exception as exc:
                log.error(f"Unexpected error in run_due_scrapers (attempt {attempt}/3): {exc}\n"
                          f"{traceback.format_exc()}")
                if attempt < 3:
                    retry_wait = attempt * 300   # 5 min, then 10 min
                    log.info(f"Retrying in {retry_wait // 60} min — reconnecting DB first…")
                    time.sleep(retry_wait)
                    try:
                        conn.close()
                    except Exception:
                        pass
                    try:
                        conn = _open_conn()
                        log.info("DB reconnected successfully.")
                    except Exception as conn_exc:
                        log.error(f"DB reconnect failed: {conn_exc}")

        # ── Daily digest — always fires after the nightly run, whether or not
        #    any scrapers were due or succeeded.  This guarantees you get a
        #    push notification every morning.
        try:
            from scraper.notify import run_daily_digest
            run_daily_digest()
        except Exception as exc:
            log.warning(f"Daily digest notification failed: {exc}")

        time.sleep(10)


if __name__ == "__main__":
    main()
