"""
notify.py — Daily digest for QualityDB scraper runs.

Sends a summary after each nightly scrape batch with:
  • Run stats per shop (new / updated / errors / duration)
  • Delisted products   — were seen last week, gone today
  • Price drops         — top-rated (≥85%) products that got ≥8% cheaper
  • Score risers        — products whose rating jumped ≥10 pts this week
  • New arrivals        — products that entered the DB for the first time today

Delivery:
  • macOS notification  — always (uses osascript, zero dependencies)
  • ntfy.sh push        — if NTFY_TOPIC env var is set
                          (free phone push: install ntfy app → subscribe to your topic)

Setup (phone notifications):
  1. Pick a unique topic name, e.g.  qualitydb-yourname42
  2. Add to your shell profile:  export NTFY_TOPIC=qualitydb-yourname42
  3. Install ntfy app on iPhone/Android → Subscribe → your topic URL

Usage:
  python3 -m scraper.notify          # standalone test / manual trigger
  # Or called automatically by scheduler after all daily scrapers finish.
"""

import os
import re
import sys
import json
import sqlite3
import datetime
import subprocess
import urllib.request
import urllib.parse
import logging
from typing import Optional

log = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────
_BASE       = os.path.dirname(os.path.dirname(__file__))
DB_PATH     = os.environ.get("DB_PATH",          os.path.join(_BASE, "products.db"))
SNAP_PATH   = os.environ.get("SNAPSHOTS_DB_PATH", os.path.join(_BASE, "snapshots.db"))
NTFY_TOPIC  = os.environ.get("NTFY_TOPIC", "")          # e.g. "qualitydb-yourname42"
NTFY_SERVER = os.environ.get("NTFY_SERVER", "https://ntfy.sh")

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_SCORE_FOR_PRICE_ALERT = 85.0   # only alert on highly-rated products
MIN_PRICE_DROP_PCT        = 8.0    # minimum % price drop to mention
MIN_SCORE_RISE_PTS        = 10.0   # minimum rating-point rise to mention
MAX_ITEMS_PER_SECTION     = 6      # keep notifications concise

SOURCE_FLAG = {
    "heureka": "🇨🇿", "zbozi": "🇨🇿", "heureka_sk": "🇸🇰",
    "amazon": "🇩🇪", "amazon_de": "🇩🇪", "otto": "🇩🇪", "saturn": "🇩🇪",
    "ceneo": "🇵🇱", "coolblue": "🇳🇱",
    "fnac": "🇫🇷", "darty": "🇫🇷",
    "amazon_us": "🇺🇸",
}


# ══════════════════════════════════════════════════════════════════════════════
#  Data queries
# ══════════════════════════════════════════════════════════════════════════════

def _open(path: str) -> Optional[sqlite3.Connection]:
    if not os.path.exists(path):
        return None
    c = sqlite3.connect(path, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def query_run_stats(conn: sqlite3.Connection) -> list[dict]:
    """Today's scraper_runs rows, most recent per scraper."""
    today = datetime.date.today().isoformat()
    rows = conn.execute(
        """
        SELECT scraper_name, market, status,
               products_added, products_updated, duration_sec, error_msg
        FROM   scraper_runs
        WHERE  started_at >= ?
        ORDER  BY started_at
        """,
        (today,),
    ).fetchall()
    return [dict(r) for r in rows]


def query_delisted(snap: sqlite3.Connection) -> list[dict]:
    """
    Products seen in a source's PREVIOUS scrape run that are absent from
    its MOST RECENT scrape run.

    Key fixes vs. old version:
    • Each source uses its OWN most-recent snapshot date as 'current',
      not a global 3-day window.  Weekly scrapers that ran 5–7 days ago
      were previously never included in 'current', so their entire
      catalogue appeared delisted every day they weren't re-scraped.
    • Sources not scraped in the last 14 days are excluded entirely
      (handles retired or broken scrapers).
    • If fewer than half the previous products are still present in the
      current run the result is suppressed — this guards against false
      alerts when a scraper returns unusually few results (bot block,
      pagination change, etc.) rather than real delisting.

    Returns [{source, delisted_count, retained_count, prev_count}]
    """
    rows = snap.execute(
        """
        WITH
        -- Each source's own most-recent snapshot date (within 14 days)
        latest_run AS (
            SELECT   source, MAX(snapshot_date) AS last_date
            FROM     product_snapshots
            GROUP    BY source
            HAVING   MAX(snapshot_date) >= date('now', '-14 days')
        ),
        -- Products seen in that most-recent run
        current_products AS (
            SELECT ps.product_url, ps.source
            FROM   product_snapshots ps
            JOIN   latest_run lr ON ps.source        = lr.source
                                AND ps.snapshot_date = lr.last_date
        ),
        -- Products seen in runs just before the most-recent one
        -- (within the 21-day window prior to last_date)
        previous_products AS (
            SELECT DISTINCT ps.product_url, ps.source
            FROM   product_snapshots ps
            JOIN   latest_run lr ON ps.source = lr.source
            WHERE  ps.snapshot_date <  lr.last_date
              AND  ps.snapshot_date >= date(lr.last_date, '-21 days')
        )
        SELECT  pw.source,
                COUNT(*)  AS prev_count,
                SUM(CASE WHEN c.product_url IS NOT NULL THEN 1 ELSE 0 END)
                          AS retained_count,
                SUM(CASE WHEN c.product_url IS NULL     THEN 1 ELSE 0 END)
                          AS delisted_count
        FROM    previous_products pw
        LEFT    JOIN current_products c
                ON  c.product_url = pw.product_url
                AND c.source      = pw.source
        GROUP   BY pw.source
        HAVING  delisted_count > 0
        ORDER   BY delisted_count DESC
        """
    ).fetchall()

    results = []
    for r in [dict(row) for row in rows]:
        prev_count = r.get("prev_count") or 0
        retained   = r.get("retained_count") or 0
        # Suppress sources where fewer than half the previous products are
        # still present — almost certainly a scraper health issue rather than
        # genuine product delisting (e.g. got blocked, returned fewer pages).
        if prev_count > 0 and retained < prev_count * 0.5:
            continue
        results.append(r)

    return results


def query_price_drops(snap: sqlite3.Connection, prod: sqlite3.Connection) -> list[dict]:
    """
    Top-rated products (≥85% recommend) whose price dropped ≥8% vs a week ago.
    Returns [{source, name, old_price, new_price, drop_pct, score, url}]
    """
    # We do this in Python to avoid complex cross-DB SQL
    recent = snap.execute(
        """
        SELECT product_url, source, recommend_pct,
               COALESCE(price_czk, price_eur) AS price
        FROM   product_snapshots
        WHERE  snapshot_date >= date('now', '-2 days')
          AND  recommend_pct >= ?
          AND  COALESCE(price_czk, price_eur) > 0
        """,
        (MIN_SCORE_FOR_PRICE_ALERT,),
    ).fetchall()

    old = snap.execute(
        """
        SELECT product_url, source,
               COALESCE(price_czk, price_eur) AS price
        FROM   product_snapshots
        WHERE  snapshot_date BETWEEN date('now', '-10 days')
                                 AND date('now', '-5 days')
          AND  COALESCE(price_czk, price_eur) > 0
        """
    ).fetchall()

    old_map = {}  # (url, source) → price
    for r in old:
        old_map[(r["product_url"], r["source"])] = r["price"]

    results = []
    for r in recent:
        key = (r["product_url"], r["source"])
        old_price = old_map.get(key)
        if not old_price or old_price <= 0:
            continue
        new_price = r["price"]
        drop_pct = (old_price - new_price) / old_price * 100
        if drop_pct < MIN_PRICE_DROP_PCT:
            continue

        # Look up product name
        name_row = prod.execute(
            "SELECT Name FROM products WHERE ProductURL=? LIMIT 1",
            (r["product_url"],),
        ).fetchone()
        name = name_row["Name"] if name_row else r["product_url"][:60]
        # Trim name to 50 chars
        if len(name) > 50:
            name = name[:47] + "…"

        results.append({
            "source":    r["source"],
            "name":      name,
            "score":     r["recommend_pct"],
            "old_price": old_price,
            "new_price": new_price,
            "drop_pct":  round(drop_pct, 1),
            "url":       r["product_url"],
        })

    results.sort(key=lambda x: x["drop_pct"], reverse=True)
    return results[:MAX_ITEMS_PER_SECTION]


def query_score_risers(snap: sqlite3.Connection, prod: sqlite3.Connection) -> list[dict]:
    """
    Products whose recommend_pct jumped ≥10 pts since last week.
    Returns [{source, name, old_score, new_score, rise, url}]
    """
    recent = snap.execute(
        """
        SELECT product_url, source, recommend_pct
        FROM   product_snapshots
        WHERE  snapshot_date >= date('now', '-2 days')
          AND  recommend_pct IS NOT NULL
        """
    ).fetchall()

    old = snap.execute(
        """
        SELECT product_url, source, recommend_pct
        FROM   product_snapshots
        WHERE  snapshot_date BETWEEN date('now', '-10 days')
                                 AND date('now', '-5 days')
          AND  recommend_pct IS NOT NULL
        """
    ).fetchall()

    old_map = {}
    for r in old:
        old_map[(r["product_url"], r["source"])] = r["recommend_pct"]

    results = []
    for r in recent:
        key = (r["product_url"], r["source"])
        old_score = old_map.get(key)
        if old_score is None:
            continue
        rise = r["recommend_pct"] - old_score
        if rise < MIN_SCORE_RISE_PTS:
            continue

        name_row = prod.execute(
            "SELECT Name FROM products WHERE ProductURL=? LIMIT 1",
            (r["product_url"],),
        ).fetchone()
        name = name_row["Name"] if name_row else r["product_url"][:60]
        if len(name) > 50:
            name = name[:47] + "…"

        results.append({
            "source":    r["source"],
            "name":      name,
            "old_score": round(old_score, 1),
            "new_score": round(r["recommend_pct"], 1),
            "rise":      round(rise, 1),
            "url":       r["product_url"],
        })

    results.sort(key=lambda x: x["rise"], reverse=True)
    return results[:MAX_ITEMS_PER_SECTION]


def query_new_arrivals(prod: sqlite3.Connection) -> dict[str, int]:
    """
    Products added to the DB today, grouped by source.
    Uses products table — looks for rows with no matching snapshot 2+ days ago.
    Falls back to scraper_runs products_added if available.
    """
    # Easier: just use scraper_runs.products_added for today
    today = datetime.date.today().isoformat()
    rows = prod.execute(
        """
        SELECT scraper_name, SUM(products_added) AS added
        FROM   scraper_runs
        WHERE  started_at >= ? AND status = 'ok'
        GROUP  BY scraper_name
        """,
        (today,),
    ).fetchall()
    return {r["scraper_name"]: r["added"] for r in rows if r["added"]}


# ══════════════════════════════════════════════════════════════════════════════
#  Report builder
# ══════════════════════════════════════════════════════════════════════════════

def build_report() -> dict:
    prod = _open(DB_PATH)
    snap = _open(SNAP_PATH)

    report = {
        "date":         datetime.date.today().isoformat(),
        "run_stats":    [],
        "delisted":     [],
        "price_drops":  [],
        "score_risers": [],
        "errors":       [],
    }

    if prod:
        report["run_stats"] = query_run_stats(prod)

    if snap and prod:
        try:
            report["delisted"]     = query_delisted(snap)
        except Exception as e:
            log.warning(f"delisted query failed: {e}")
        try:
            report["price_drops"]  = query_price_drops(snap, prod)
        except Exception as e:
            log.warning(f"price_drops query failed: {e}")
        try:
            report["score_risers"] = query_score_risers(snap, prod)
        except Exception as e:
            log.warning(f"score_risers query failed: {e}")

    if prod:
        prod.close()
    if snap:
        snap.close()

    return report


# ══════════════════════════════════════════════════════════════════════════════
#  Formatting
# ══════════════════════════════════════════════════════════════════════════════

def _flag(source: str) -> str:
    return SOURCE_FLAG.get(source.lower(), "🌐")


def format_short(report: dict) -> tuple[str, str]:
    """Returns (title, body) for a concise macOS notification."""
    stats  = report["run_stats"]
    ok     = [s for s in stats if s["status"] == "ok"]
    errors = [s for s in stats if s["status"] != "ok"]

    total_new = sum(s.get("products_added", 0) or 0 for s in ok)
    total_upd = sum(s.get("products_updated", 0) or 0 for s in ok)
    shops     = len(ok)

    title = f"QualityDB — {report['date']}"
    lines = [f"{shops} shops scraped • +{total_new:,} new • ~{total_upd:,} updated"]

    if errors:
        lines.append(f"⚠️ {len(errors)} error(s): {', '.join(e['scraper_name'] for e in errors)}")
    if report["price_drops"]:
        d = report["price_drops"][0]
        lines.append(f"📉 Price drop: {d['name']} −{d['drop_pct']}%")
    if report["score_risers"]:
        r = report["score_risers"][0]
        lines.append(f"📈 Rising: {r['name']} +{r['rise']} pts")

    return title, "\n".join(lines)


def format_long(report: dict) -> str:
    """Returns a detailed markdown-style message for ntfy.sh."""
    lines = [f"# 🗓 QualityDB Daily Digest — {report['date']}", ""]

    # ── Run stats ──────────────────────────────────────────────────────────────
    stats = report["run_stats"]
    if stats:
        lines.append("## 🏪 Today's scrapes")
        for s in stats:
            flag     = _flag(s["market"])
            status   = "✅" if s["status"] == "ok" else "❌"
            dur_min  = f"{s['duration_sec'] / 60:.0f}m" if s.get("duration_sec") else "?"
            added    = s.get("products_added", 0) or 0
            updated  = s.get("products_updated", 0) or 0
            name     = s["scraper_name"]
            lines.append(
                f"{status} {flag} **{name}** — +{added:,} new, ~{updated:,} updated ({dur_min})"
            )
            if s.get("error_msg"):
                snippet = s["error_msg"][:120].replace("\n", " ")
                lines.append(f"   ↳ `{snippet}`")
        lines.append("")

    # ── Delisted ───────────────────────────────────────────────────────────────
    if report["delisted"]:
        lines.append("## 🚫 Delisted this week")
        for d in report["delisted"]:
            flag = _flag(d["source"])
            count = d["delisted_count"]
            lines.append(f"  {flag} **{d['source']}** — {count:,} products no longer listed")
        lines.append("")

    # ── Price drops ───────────────────────────────────────────────────────────
    if report["price_drops"]:
        lines.append("## 📉 Price drops (top-rated products)")
        for d in report["price_drops"]:
            flag = _flag(d["source"])
            lines.append(
                f"  {flag} **{d['name']}** "
                f"({d['score']:.0f}% score) "
                f"−{d['drop_pct']}% "
                f"({d['old_price']:.0f} → {d['new_price']:.0f})"
            )
        lines.append("")

    # ── Score risers ──────────────────────────────────────────────────────────
    if report["score_risers"]:
        lines.append("## 📈 Rating risers")
        for r in report["score_risers"]:
            flag = _flag(r["source"])
            lines.append(
                f"  {flag} **{r['name']}** "
                f"{r['old_score']:.0f}% → {r['new_score']:.0f}% "
                f"(+{r['rise']:.0f} pts)"
            )
        lines.append("")

    if not any([stats, report["delisted"], report["price_drops"], report["score_risers"]]):
        lines.append("_No scrapers ran today or no data available._")

    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════════
#  Delivery
# ══════════════════════════════════════════════════════════════════════════════

def send_macos(title: str, body: str) -> None:
    """Send a macOS notification via osascript. Silent failure on non-Mac."""
    try:
        script = (
            f'display notification "{body}" '
            f'with title "{title}" '
            f'sound name "Glass"'
        )
        subprocess.run(
            ["osascript", "-e", script],
            timeout=5, capture_output=True,
        )
    except Exception as e:
        log.debug(f"macOS notification failed: {e}")


def send_ntfy(title: str, body: str, long_body: str) -> None:
    """
    Send to ntfy.sh (free push notifications — works on iOS + Android).

    Setup:
      1. Choose a unique topic:   export NTFY_TOPIC=qualitydb-yourname42
      2. Install ntfy app on phone
      3. Subscribe to:  ntfy.sh/qualitydb-yourname42
    """
    if not NTFY_TOPIC:
        return

    url = f"{NTFY_SERVER}/{NTFY_TOPIC}"
    payload = long_body.encode("utf-8")
    headers = {
        "Title":        urllib.parse.quote(title),
        "Priority":     "default",
        "Tags":         "chart_with_upwards_trend,shopping",
        "Content-Type": "text/plain; charset=utf-8",
    }

    try:
        req = urllib.request.Request(url, data=payload, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                log.info(f"ntfy.sh notification sent to topic '{NTFY_TOPIC}'")
            else:
                log.warning(f"ntfy.sh returned status {resp.status}")
    except Exception as e:
        log.warning(f"ntfy.sh send failed: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  Public entry point (called by scheduler)
# ══════════════════════════════════════════════════════════════════════════════

def run_daily_digest() -> None:
    """Build report and send notifications. Called by scheduler after each nightly batch."""
    log.info("Building daily digest…")
    report    = build_report()
    title, short = format_short(report)
    long_msg  = format_long(report)

    # Always print to log
    log.info(f"\n{long_msg}")

    # macOS desktop notification
    send_macos(title, short)

    # Phone push (if configured)
    send_ntfy(title, short, long_msg)

    log.info("Daily digest sent.")


# ══════════════════════════════════════════════════════════════════════════════
#  Standalone CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    report   = build_report()
    title, short = format_short(report)
    long_msg = format_long(report)

    print("\n" + "=" * 60)
    print(long_msg)
    print("=" * 60)
    print(f"\nmacOS notification: {title}")
    print(short)

    send_macos(title, short)

    if NTFY_TOPIC:
        send_ntfy(title, short, long_msg)
        print(f"\nntfy.sh notification sent to: {NTFY_SERVER}/{NTFY_TOPIC}")
    else:
        print("\nTip: set NTFY_TOPIC=qualitydb-yourname42 to get phone push notifications.")
        print("     Then install the free ntfy app and subscribe to that topic.")
