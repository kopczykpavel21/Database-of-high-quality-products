"""
User authentication and contribution management for QualityDB / Institut Kvality.

Stores users and staging contributions in a separate users.db file so that
personal data is isolated from the product catalogue.

Auth is passwordless: user enters email → receives 6-digit OTP → verified →
new users complete a short profile (country + 3 research questions).

Public API:
    from scraper.auth import (
        ensure_tables, request_verification_code, verify_code_and_login,
        complete_profile, get_user_by_token,
        record_contribution, get_contrib_stats, maybe_merge_staged
    )
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import secrets
import smtplib
import sqlite3
import time
from email.mime.text import MIMEText
from typing import Optional

from scraper.snapshots import ensure_snapshot_table, record_snapshot

DB_PATH = os.environ.get(
    "USERS_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "users.db"),
)

# ── Sources available per country ─────────────────────────────────────────────
COUNTRY_SOURCES: dict[str, list[dict]] = {
    "CZ": [
        {"key": "heureka",  "name": "Heureka.cz"},
        {"key": "zbozi",    "name": "Zbozi.cz"},
        {"key": "czc",      "name": "CZC.cz"},
    ],
    "SK": [
        {"key": "heureka_sk", "name": "Heureka.sk"},
    ],
    "DE": [
        {"key": "otto",       "name": "Otto.de"},
        {"key": "mediamarkt", "name": "MediaMarkt.de"},
        {"key": "amazon_de",  "name": "Amazon.de"},
        {"key": "idealo",     "name": "Idealo.de"},
    ],
    "AT": [
        {"key": "geizhals",   "name": "Geizhals.at"},
    ],
    "CH": [
        {"key": "geizhals",   "name": "Geizhals.at"},
    ],
    "FR": [
        {"key": "fnac",       "name": "Fnac.fr"},
        {"key": "darty",      "name": "Darty.fr"},
    ],
    "PL": [
        {"key": "ceneo",      "name": "Ceneo.pl"},
    ],
    "NL": [
        {"key": "coolblue",   "name": "Coolblue.nl"},
    ],
    "SE": [
        {"key": "prisjakt",       "name": "Prisjakt.nu"},
        {"key": "pricerunner_se", "name": "PriceRunner.se"},
    ],
    "DK": [
        {"key": "pricerunner",    "name": "PriceRunner.dk"},
    ],
    "NO": [
        {"key": "prisjakt",       "name": "Prisjakt.nu"},
    ],
    "FI": [
        {"key": "prisjakt",       "name": "Prisjakt.nu"},
    ],
    "US": [
        {"key": "amazon_de",      "name": "Amazon.de"},
    ],
    "GB": [
        {"key": "idealo",         "name": "Idealo.de"},
    ],
}

ALL_COUNTRIES = sorted(COUNTRY_SOURCES.keys())

# ── DB setup ──────────────────────────────────────────────────────────────────

def open_users_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_tables() -> None:
    conn = open_users_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            email        TEXT    UNIQUE NOT NULL,
            password_hash TEXT   NOT NULL,
            token        TEXT    UNIQUE NOT NULL,
            country      TEXT,
            q1           TEXT,
            q2           TEXT,
            q3           TEXT,
            created_at   TEXT    DEFAULT (datetime('now')),
            last_login   TEXT,
            contrib_count INTEGER DEFAULT 0,
            is_admin     INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS staging_products (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            product_url  TEXT    NOT NULL,
            name         TEXT,
            source       TEXT,
            country      TEXT,
            category     TEXT,
            main_category TEXT,
            recommend_pct REAL,
            reviews_count INTEGER,
            avg_star_rating REAL,
            price_czk    REAL,
            price_eur    REAL,
            currency     TEXT,
            user_id      INTEGER REFERENCES users(id),
            submitted_at TEXT    DEFAULT (datetime('now')),
            merged       INTEGER DEFAULT 0,
            UNIQUE(product_url, user_id)
        );

        CREATE INDEX IF NOT EXISTS idx_staging_url    ON staging_products(product_url);
        CREATE INDEX IF NOT EXISTS idx_staging_merged ON staging_products(merged);
        CREATE INDEX IF NOT EXISTS idx_staging_user   ON staging_products(user_id);

        CREATE TABLE IF NOT EXISTS verification_codes (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            email      TEXT    NOT NULL,
            code       TEXT    NOT NULL,
            expires_at REAL    NOT NULL,
            created_at REAL    NOT NULL,
            used       INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_vc_email ON verification_codes(email);

        CREATE TABLE IF NOT EXISTS pending_profiles (
            email       TEXT PRIMARY KEY,
            setup_token TEXT NOT NULL,
            expires_at  REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS contribution_sessions (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER REFERENCES users(id),
            source       TEXT,
            submitted_at TEXT DEFAULT (datetime('now')),
            product_count INTEGER DEFAULT 0,
            queued_count  INTEGER DEFAULT 0,
            merged_count  INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    conn.close()


# ── Password helpers ──────────────────────────────────────────────────────────

def _hash_password(password: str) -> str:
    salt = os.urandom(32)
    key  = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 260_000)
    return salt.hex() + ":" + key.hex()


def _verify_password(stored: str, password: str) -> bool:
    try:
        salt_hex, key_hex = stored.split(":", 1)
        key = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), bytes.fromhex(salt_hex), 260_000
        )
        return secrets.compare_digest(key.hex(), key_hex)
    except Exception:
        return False


# ── User CRUD ─────────────────────────────────────────────────────────────────

def register_user(email: str, password: str, country: str,
                  q1: str, q2: str, q3: str) -> dict:
    """
    Create a new account.  Returns {"ok": True, "token": ..., "user": {...}}
    or {"ok": False, "error": "..."}.
    """
    email = email.strip().lower()
    if not email or "@" not in email:
        return {"ok": False, "error": "Invalid email address."}
    if len(password) < 8:
        return {"ok": False, "error": "Password must be at least 8 characters."}
    if country.upper() not in COUNTRY_SOURCES:
        return {"ok": False, "error": f"Country '{country}' is not supported yet."}

    pw_hash = _hash_password(password)
    token   = secrets.token_urlsafe(32)

    conn = open_users_db()
    try:
        conn.execute(
            "INSERT INTO users (email, password_hash, token, country, q1, q2, q3) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (email, pw_hash, token, country.upper(), q1.strip(), q2.strip(), q3.strip()),
        )
        conn.commit()
        user = dict(conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone())
        return {"ok": True, "token": token, "user": _public_user(user)}
    except sqlite3.IntegrityError:
        return {"ok": False, "error": "An account with this email already exists."}
    finally:
        conn.close()


def login_user(email: str, password: str) -> dict:
    email = email.strip().lower()
    conn = open_users_db()
    row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    if not row or not _verify_password(row["password_hash"], password):
        conn.close()
        return {"ok": False, "error": "Invalid email or password."}
    conn.execute("UPDATE users SET last_login = datetime('now') WHERE id = ?", (row["id"],))
    conn.commit()
    conn.close()
    return {"ok": True, "token": row["token"], "user": _public_user(dict(row))}


# ── Passwordless OTP auth ─────────────────────────────────────────────────────

def _send_verification_email(to_email: str, code: str) -> None:
    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    smtp_from = os.environ.get("SMTP_FROM", smtp_user) or "noreply@institutkvality.cz"

    body = (
        f"Your Institut Kvality verification code:\n\n"
        f"  {code}\n\n"
        f"This code expires in 10 minutes.\n"
        f"If you didn't request this, you can safely ignore this email.\n\n"
        f"— Institut Kvality team\n"
    )

    if not smtp_host:
        # Dev mode — no SMTP configured. Print clearly so it's impossible to miss.
        print(f"\n{'='*50}\n  DEV MODE — verification code for {to_email}\n  CODE: {code}\n{'='*50}\n", flush=True)
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = f"Your verification code: {code}"
    msg["From"] = smtp_from
    msg["To"] = to_email
    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as srv:
            srv.ehlo()
            srv.starttls()
            srv.login(smtp_user, smtp_pass)
            srv.send_message(msg)
        logging.info(f"Verification email sent to {to_email}")
    except Exception as exc:
        logging.error(f"Failed to send verification email to {to_email}: {exc}")


def request_verification_code(email: str) -> dict:
    email = email.strip().lower()
    if not email or "@" not in email:
        return {"ok": False, "error": "Invalid email address."}

    conn = open_users_db()
    try:
        recent = conn.execute(
            "SELECT created_at FROM verification_codes WHERE email = ? ORDER BY created_at DESC LIMIT 1",
            (email,),
        ).fetchone()
        if recent and (time.time() - recent[0]) < 60:
            return {"ok": False, "error": "Please wait a minute before requesting another code."}

        code = f"{random.randint(0, 999999):06d}"
        now  = time.time()
        conn.execute("DELETE FROM verification_codes WHERE email = ?", (email,))
        conn.execute(
            "INSERT INTO verification_codes (email, code, expires_at, created_at) VALUES (?,?,?,?)",
            (email, code, now + 600, now),
        )
        conn.commit()
    finally:
        conn.close()

    dev_mode = not os.environ.get("SMTP_HOST", "")
    _send_verification_email(email, code)
    # In dev mode (no SMTP), return the code so the UI can show it directly
    return {"ok": True, **({"dev_code": code} if dev_mode else {})}


def verify_code_and_login(email: str, code: str) -> dict:
    """
    Verify the OTP. Returns one of:
      {"ok": True, "token": ..., "user": ..., "is_new": False}   — existing user, done
      {"ok": True, "needs_profile": True, "setup_token": ...}    — new user, needs profile
      {"ok": False, "error": "..."}
    """
    email = email.strip().lower()
    code  = code.strip()

    conn = open_users_db()
    try:
        row = conn.execute(
            "SELECT id, code, expires_at, used FROM verification_codes "
            "WHERE email = ? ORDER BY expires_at DESC LIMIT 1",
            (email,),
        ).fetchone()

        if not row:
            return {"ok": False, "error": "No code found — please request a new one."}
        if row["used"]:
            return {"ok": False, "error": "Code already used — please request a new one."}
        if time.time() > row["expires_at"]:
            return {"ok": False, "error": "Code expired — please request a new one."}
        if code != row["code"]:
            return {"ok": False, "error": "Invalid code."}

        conn.execute("UPDATE verification_codes SET used = 1 WHERE id = ?", (row["id"],))

        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if user_row:
            token = secrets.token_urlsafe(32)
            conn.execute(
                "UPDATE users SET token = ?, last_login = datetime('now') WHERE email = ?",
                (token, email),
            )
            conn.commit()
            user_dict = dict(user_row)
            user_dict["token"] = token
            return {"ok": True, "token": token, "user": _public_user(user_dict), "is_new": False}

        # New user — store a short-lived setup token
        setup_token = secrets.token_urlsafe(24)
        conn.execute(
            "INSERT OR REPLACE INTO pending_profiles (email, setup_token, expires_at) VALUES (?,?,?)",
            (email, setup_token, time.time() + 1800),
        )
        conn.commit()
        return {"ok": True, "needs_profile": True, "setup_token": setup_token}
    finally:
        conn.close()


def reset_password(email: str, code: str, new_password: str) -> dict:
    """Verify OTP then update password hash. Returns login token on success."""
    email = email.strip().lower()
    code  = code.strip()
    if len(new_password) < 8:
        return {"ok": False, "error": "Password must be at least 8 characters."}

    conn = open_users_db()
    try:
        row = conn.execute(
            "SELECT id, code, expires_at, used FROM verification_codes "
            "WHERE email = ? ORDER BY expires_at DESC LIMIT 1",
            (email,),
        ).fetchone()

        if not row:
            return {"ok": False, "error": "No code found — please request a new one."}
        if row["used"]:
            return {"ok": False, "error": "Code already used — please request a new one."}
        if time.time() > row["expires_at"]:
            return {"ok": False, "error": "Code expired — please request a new one."}
        if code != row["code"]:
            return {"ok": False, "error": "Invalid code."}

        # Mark code as used
        conn.execute("UPDATE verification_codes SET used = 1 WHERE id = ?", (row["id"],))

        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not user_row:
            return {"ok": False, "error": "No account found for this email."}

        new_hash = _hash_password(new_password)
        token    = secrets.token_urlsafe(32)
        conn.execute(
            "UPDATE users SET password_hash = ?, token = ?, last_login = datetime('now') WHERE email = ?",
            (new_hash, token, email),
        )
        conn.commit()
        user_dict = dict(user_row)
        user_dict["token"] = token
        return {"ok": True, "token": token, "user": _public_user(user_dict)}
    finally:
        conn.close()


def complete_profile(setup_token: str, country: str, q1: str, q2: str, q3: str) -> dict:
    conn = open_users_db()
    try:
        row = conn.execute(
            "SELECT email, expires_at FROM pending_profiles WHERE setup_token = ?",
            (setup_token,),
        ).fetchone()
        if not row:
            return {"ok": False, "error": "Invalid or expired setup link — please start again."}
        if time.time() > row["expires_at"]:
            return {"ok": False, "error": "Setup session expired — please start again."}
        if country.upper() not in COUNTRY_SOURCES:
            return {"ok": False, "error": f"Country '{country}' is not supported yet."}

        email = row["email"]
        token = secrets.token_urlsafe(32)
        try:
            conn.execute(
                "INSERT INTO users (email, password_hash, token, country, q1, q2, q3) "
                "VALUES (?, '', ?, ?, ?, ?, ?)",
                (email, token, country.upper(), q1.strip(), q2.strip(), q3.strip()),
            )
        except sqlite3.IntegrityError:
            # Race: user created between verify and complete — just refresh token
            conn.execute("UPDATE users SET token = ? WHERE email = ?", (token, email))

        conn.execute("DELETE FROM pending_profiles WHERE setup_token = ?", (setup_token,))
        conn.commit()

        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        return {"ok": True, "token": token, "user": _public_user(dict(user_row)), "is_new": True}
    finally:
        conn.close()


def google_auth(email: str, google_id: str = "", name: str = "") -> dict:
    """
    Sign in or sign up a user via Google OAuth.
    Email is already verified by Google, so no OTP is needed.

    Returns one of:
      {"ok": True, "token": ..., "user": ..., "is_new": False}   — existing user
      {"ok": True, "needs_profile": True, "setup_token": ..., "email": ...}  — new user
      {"ok": False, "error": "..."}
    """
    email = email.strip().lower()
    if not email or "@" not in email:
        return {"ok": False, "error": "Invalid email address."}

    conn = open_users_db()
    try:
        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if user_row:
            # Existing user — issue a fresh token and log them in
            token = secrets.token_urlsafe(32)
            conn.execute(
                "UPDATE users SET token = ?, last_login = datetime('now') WHERE email = ?",
                (token, email),
            )
            conn.commit()
            user_dict = dict(user_row)
            user_dict["token"] = token
            return {"ok": True, "token": token, "user": _public_user(user_dict), "is_new": False}

        # New user — store a short-lived setup token so they can complete their profile
        setup_token = secrets.token_urlsafe(24)
        conn.execute(
            "INSERT OR REPLACE INTO pending_profiles (email, setup_token, expires_at) VALUES (?,?,?)",
            (email, setup_token, time.time() + 1800),
        )
        conn.commit()
        return {"ok": True, "needs_profile": True, "setup_token": setup_token, "email": email}
    finally:
        conn.close()


def get_user_by_token(token: str) -> Optional[dict]:
    if not token:
        return None
    conn = open_users_db()
    row = conn.execute(
        "SELECT * FROM users WHERE token = ?", (token,)
    ).fetchone()
    conn.close()
    return _public_user(dict(row)) if row else None


def _public_user(u: dict) -> dict:
    """Strip sensitive fields before returning to client."""
    return {k: v for k, v in u.items()
            if k not in ("password_hash", "is_admin")}


# ── Contributions ─────────────────────────────────────────────────────────────

def record_contribution(user_id: int, source: str, products: list[dict]) -> dict:
    """
    Stage a batch of products from a contributor.
    Returns {"queued": N, "duplicate": M}.
    """
    conn = open_users_db()
    queued = 0
    duplicate = 0

    for p in products:
        url = (p.get("ProductURL") or p.get("product_url") or "").strip()
        if not url:
            continue
        try:
            conn.execute("""
                INSERT OR IGNORE INTO staging_products
                    (product_url, name, source, country, category, main_category,
                     recommend_pct, reviews_count, avg_star_rating,
                     price_czk, price_eur, currency, user_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                url,
                (p.get("Name") or p.get("name") or "")[:500],
                source,
                p.get("country", ""),
                (p.get("Category") or p.get("category") or ""),
                (p.get("MainCategory") or p.get("main_category") or ""),
                p.get("RecommendRate_pct") or p.get("recommend_pct"),
                p.get("ReviewsCount") or p.get("reviews_count"),
                p.get("AvgStarRating") or p.get("avg_star_rating"),
                p.get("Price_CZK") or p.get("price_czk"),
                p.get("Price_EUR") or p.get("price_eur"),
                p.get("currency", ""),
                user_id,
            ))
            if conn.execute("SELECT changes()").fetchone()[0]:
                queued += 1
            else:
                duplicate += 1
        except Exception:
            pass

    # Log the session
    conn.execute(
        "INSERT INTO contribution_sessions (user_id, source, product_count, queued_count) "
        "VALUES (?, ?, ?, ?)",
        (user_id, source, len(products), queued),
    )
    # Bump user contrib counter
    conn.execute(
        "UPDATE users SET contrib_count = contrib_count + ? WHERE id = ?",
        (queued, user_id),
    )
    conn.commit()
    conn.close()
    return {"queued": queued, "duplicate": duplicate}


def get_contrib_stats() -> dict:
    """Return public leaderboard and aggregate stats."""
    conn = open_users_db()
    total_users   = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    total_staged  = conn.execute(
        "SELECT COUNT(*) FROM staging_products WHERE merged = 0"
    ).fetchone()[0]
    total_merged  = conn.execute(
        "SELECT COUNT(*) FROM staging_products WHERE merged = 1"
    ).fetchone()[0]
    ready_to_merge = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT product_url FROM staging_products WHERE merged = 0
            GROUP BY product_url HAVING COUNT(DISTINCT user_id) >= 3
        )
    """).fetchone()[0]

    leaderboard = conn.execute("""
        SELECT u.email, u.country, u.contrib_count, u.created_at
        FROM users u
        WHERE u.contrib_count > 0
        ORDER BY u.contrib_count DESC
        LIMIT 20
    """).fetchall()

    conn.close()
    return {
        "total_contributors": total_users,
        "total_staged":   total_staged,
        "total_merged":   total_merged,
        "ready_to_merge": ready_to_merge,
        "leaderboard": [
            {
                "email":   row["email"].split("@")[0] + "@…",   # partial anonymise
                "country": row["country"],
                "contributions": row["contrib_count"],
                "joined":  row["created_at"][:10],
            }
            for row in leaderboard
        ],
    }


# ── Threshold merge ───────────────────────────────────────────────────────────
MERGE_THRESHOLD = 3          # distinct users required
MAX_REC_SPREAD  = 15.0       # max allowed spread in recommend_pct (percentage points)
MAX_PRICE_RATIO = 0.35       # max allowed price spread as fraction of median


def maybe_merge_staged(products_db_path: str) -> int:
    """
    Find staging_products groups that have MERGE_THRESHOLD distinct contributors
    and consistent data, then merge them into the main products.db.
    Returns number of new products merged.
    """
    users_conn = open_users_db()

    # Find product_urls with enough distinct contributors
    candidates = users_conn.execute("""
        SELECT product_url
        FROM staging_products
        WHERE merged = 0
        GROUP BY product_url
        HAVING COUNT(DISTINCT user_id) >= ?
    """, (MERGE_THRESHOLD,)).fetchall()

    merged_count = 0
    prod_conn = sqlite3.connect(products_db_path, timeout=30)
    ensure_snapshot_table(prod_conn)

    for (url,) in candidates:
        rows = users_conn.execute(
            "SELECT * FROM staging_products WHERE product_url = ? AND merged = 0",
            (url,),
        ).fetchall()
        rows = [dict(r) for r in rows]

        # ── Consistency check ─────────────────────────────────────────────
        recs    = [r["recommend_pct"]   for r in rows if r["recommend_pct"]   is not None]
        prices  = [r["price_czk"]       for r in rows if r["price_czk"]       is not None]

        if recs and (max(recs) - min(recs)) > MAX_REC_SPREAD:
            continue  # too noisy — skip for now

        if len(prices) >= 2:
            median_price = sorted(prices)[len(prices) // 2]
            if median_price > 0 and (max(prices) - min(prices)) / median_price > MAX_PRICE_RATIO:
                continue

        # ── Compute consensus values ──────────────────────────────────────
        def median(vals):
            s = sorted(v for v in vals if v is not None)
            return s[len(s) // 2] if s else None

        def most_common(vals):
            vals = [v for v in vals if v]
            return max(set(vals), key=vals.count) if vals else None

        name        = most_common([r["name"]          for r in rows])
        source      = rows[0]["source"]
        country     = most_common([r["country"]       for r in rows])
        category    = most_common([r["category"]      for r in rows])
        main_cat    = most_common([r["main_category"] for r in rows])
        rec_pct     = median([r["recommend_pct"]   for r in rows])
        reviews     = median([r["reviews_count"]   for r in rows])
        stars       = median([r["avg_star_rating"] for r in rows])
        price_czk   = median([r["price_czk"]       for r in rows])
        price_eur   = median([r["price_eur"]        for r in rows])
        currency    = most_common([r["currency"]   for r in rows])

        if not name:
            continue

        # ── Insert or update in products.db ──────────────────────────────
        try:
            existing = prod_conn.execute(
                "SELECT id FROM products WHERE ProductURL = ?", (url,)
            ).fetchone()
            if existing:
                prod_conn.execute("""
                    UPDATE products SET
                        RecommendRate_pct = COALESCE(?, RecommendRate_pct),
                        ReviewsCount      = COALESCE(?, ReviewsCount),
                        AvgStarRating     = COALESCE(?, AvgStarRating),
                        Price_CZK         = COALESCE(?, Price_CZK)
                    WHERE ProductURL = ?
                """, (rec_pct, reviews, stars, price_czk, url))
            else:
                prod_conn.execute("""
                    INSERT OR IGNORE INTO products
                        (Name, source, country, Category, MainCategory,
                         ProductURL, RecommendRate_pct, ReviewsCount, AvgStarRating,
                         Price_CZK, Price_EUR, currency)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (name, source, country, category, main_cat,
                      url, rec_pct, reviews, stars, price_czk, price_eur, currency))

            prod_conn.commit()

            # Record a snapshot so this price/recommend update shows up in the
            # "Has price history" filter and snapshot-deltas/movers over time.
            try:
                cur = prod_conn.execute(
                    "SELECT RecommendRate_pct, ReviewsCount, AvgStarRating, "
                    "Price_CZK, Price_EUR FROM products WHERE ProductURL = ?",
                    (url,),
                ).fetchone()
                if cur:
                    record_snapshot(
                        prod_conn, url, source,
                        {
                            "RecommendRate_pct": cur[0],
                            "ReviewsCount":      cur[1],
                            "AvgStarRating":     cur[2],
                            "Price_CZK":         cur[3],
                            "Price_EUR":         cur[4],
                        },
                        country=country,
                    )
            except Exception as _se:
                import logging
                logging.warning(f"snapshot failed for {url}: {_se}")

            # Mark all staging rows for this URL as merged
            users_conn.execute(
                "UPDATE staging_products SET merged = 1 WHERE product_url = ?", (url,)
            )
            merged_count += 1
        except Exception as e:
            import logging
            logging.warning(f"merge failed for {url}: {e}")
            continue

    users_conn.commit()
    users_conn.close()
    prod_conn.close()
    return merged_count


def force_merge_user_staged(user_id: int, products_db_path: str) -> dict:
    """
    Admin override: merge all staged products for a single user without
    requiring MERGE_THRESHOLD distinct contributors.

    Used for testing / bootstrapping by the account owner.
    Returns {"merged": N, "skipped": M, "already_exists": K}.
    """
    users_conn = open_users_db()
    rows = users_conn.execute(
        "SELECT * FROM staging_products WHERE user_id = ? AND merged = 0",
        (user_id,),
    ).fetchall()
    rows = [dict(r) for r in rows]

    if not rows:
        users_conn.close()
        return {"merged": 0, "skipped": 0, "already_exists": 0}

    prod_conn = sqlite3.connect(products_db_path, timeout=30)
    ensure_snapshot_table(prod_conn)
    merged = 0
    skipped = 0
    already = 0

    for r in rows:
        url  = r["product_url"]
        name = r["name"]
        if not url or not name:
            skipped += 1
            continue
        try:
            existing = prod_conn.execute(
                "SELECT id FROM products WHERE ProductURL = ?", (url,)
            ).fetchone()
            if existing:
                # Update stale fields only when we have better values
                prod_conn.execute("""
                    UPDATE products SET
                        RecommendRate_pct = COALESCE(?, RecommendRate_pct),
                        ReviewsCount      = COALESCE(?, ReviewsCount),
                        AvgStarRating     = COALESCE(?, AvgStarRating),
                        Price_CZK         = COALESCE(?, Price_CZK),
                        Price_EUR         = COALESCE(?, Price_EUR)
                    WHERE ProductURL = ?
                """, (r["recommend_pct"], r["reviews_count"], r["avg_star_rating"],
                      r["price_czk"], r["price_eur"], url))
                already += 1
            else:
                prod_conn.execute("""
                    INSERT OR IGNORE INTO products
                        (Name, source, country, Category, MainCategory,
                         ProductURL, RecommendRate_pct, ReviewsCount,
                         AvgStarRating, Price_CZK, Price_EUR, currency)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (name,
                      r["source"], r["country"],
                      r["category"], r["main_category"],
                      url,
                      r["recommend_pct"], r["reviews_count"],
                      r["avg_star_rating"], r["price_czk"], r["price_eur"],
                      r["currency"]))
                # Only count if the INSERT actually happened (not silently ignored)
                if prod_conn.execute("SELECT changes()").fetchone()[0]:
                    merged += 1
                else:
                    already += 1  # URL variant already in DB (trailing slash, case, etc.)
            prod_conn.commit()

            # Record a snapshot so this price/recommend update shows up in the
            # "Has price history" filter and snapshot-deltas/movers over time.
            try:
                cur = prod_conn.execute(
                    "SELECT RecommendRate_pct, ReviewsCount, AvgStarRating, "
                    "Price_CZK, Price_EUR FROM products WHERE ProductURL = ?",
                    (url,),
                ).fetchone()
                if cur:
                    record_snapshot(
                        prod_conn, url, r["source"],
                        {
                            "RecommendRate_pct": cur[0],
                            "ReviewsCount":      cur[1],
                            "AvgStarRating":     cur[2],
                            "Price_CZK":         cur[3],
                            "Price_EUR":         cur[4],
                        },
                        country=r["country"],
                    )
            except Exception as _se:
                import logging as _log
                _log.warning(f"force_merge: snapshot failed for {url}: {_se}")

            users_conn.execute(
                "UPDATE staging_products SET merged = 1 WHERE id = ?", (r["id"],)
            )
        except Exception as e:
            import logging as _log
            _log.warning(f"force_merge: failed for {url}: {e}")
            skipped += 1

    users_conn.commit()
    users_conn.close()
    prod_conn.close()
    return {"merged": merged, "skipped": skipped, "already_exists": already}
