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
import hmac
import json
import logging
import os
import secrets
import smtplib
import sqlite3
import time
from email.mime.text import MIMEText
from typing import Optional
from urllib.parse import urlparse

from scraper.snapshots import ensure_snapshot_table, record_snapshot

DB_PATH = os.environ.get(
    "USERS_DB_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "users.db"),
)

SESSION_TTL_SECONDS = int(os.environ.get("SESSION_TTL_SECONDS", str(30 * 24 * 60 * 60)))
SCANNER_TOKEN_TTL_SECONDS = int(
    os.environ.get("SCANNER_TOKEN_TTL_SECONDS", str(365 * 24 * 60 * 60))
)
OTP_MAX_ATTEMPTS = int(os.environ.get("OTP_MAX_ATTEMPTS", "5"))
MAX_CONTRIBUTION_PRODUCTS = int(os.environ.get("MAX_CONTRIBUTION_PRODUCTS", "200"))

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
            token_hash   TEXT    UNIQUE,
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

        CREATE TABLE IF NOT EXISTS scanner_tokens (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id      INTEGER NOT NULL REFERENCES users(id),
            token_hash   TEXT UNIQUE NOT NULL,
            created_at   REAL NOT NULL,
            expires_at   REAL NOT NULL,
            last_used_at REAL,
            revoked_at   REAL
        );
        CREATE INDEX IF NOT EXISTS idx_scanner_tokens_user
            ON scanner_tokens(user_id, revoked_at, expires_at);
    """)

    # Safe, additive migrations for databases created by earlier releases.
    user_cols = {row[1] for row in conn.execute("PRAGMA table_info(users)")}
    for column, typedef in [
        ("google_sub", "TEXT"),
        ("email_verified_at", "TEXT"),
        ("token_hash", "TEXT"),
        ("token_expires_at", "REAL"),
    ]:
        if column not in user_cols:
            conn.execute(f"ALTER TABLE users ADD COLUMN {column} {typedef}")

    verification_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(verification_codes)")
    }
    for column, typedef in [
        ("code_hash", "TEXT"),
        ("attempts", "INTEGER NOT NULL DEFAULT 0"),
    ]:
        if column not in verification_cols:
            conn.execute(f"ALTER TABLE verification_codes ADD COLUMN {column} {typedef}")

    pending_cols = {
        row[1] for row in conn.execute("PRAGMA table_info(pending_profiles)")
    }
    if "google_sub" not in pending_cols:
        conn.execute("ALTER TABLE pending_profiles ADD COLUMN google_sub TEXT")

    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_google_sub "
        "ON users(google_sub) WHERE google_sub IS NOT NULL"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_users_token_hash "
        "ON users(token_hash) WHERE token_hash IS NOT NULL"
    )

    # Old OTPs were stored in plaintext. Invalidate them instead of retaining a
    # credential-verification path that bypasses the new HMAC storage.
    conn.execute(
        "UPDATE verification_codes SET used = 1 "
        "WHERE code_hash IS NULL AND used = 0"
    )

    # Existing sessions get a short compatibility window so an installed legacy
    # IKOR bookmarklet does not stop at the instant of deployment. The website
    # creates a contribution-only scanner token on the next authenticated visit.
    legacy_grace = max(0, int(os.environ.get("LEGACY_SESSION_GRACE_SECONDS", "604800")))
    legacy_rows = conn.execute(
        "SELECT id, token FROM users WHERE token_hash IS NULL"
    ).fetchall()
    for legacy in legacy_rows:
        conn.execute(
            "UPDATE users SET token = ?, token_hash = ? WHERE id = ?",
            (
                "stored-" + secrets.token_urlsafe(24),
                hashlib.sha256(legacy["token"].encode("utf-8")).hexdigest(),
                legacy["id"],
            ),
        )
    conn.execute(
        "UPDATE users SET token_expires_at = ? "
        "WHERE token_expires_at IS NULL",
        (time.time() + legacy_grace,),
    )
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


def _issue_session_token(conn: sqlite3.Connection, user_id: int) -> str:
    """Rotate a website session token and attach an absolute expiry."""
    token = secrets.token_urlsafe(32)
    conn.execute(
        "UPDATE users SET token = ?, token_hash = ?, token_expires_at = ?, "
        "last_login = datetime('now') WHERE id = ?",
        (
            "stored-" + secrets.token_urlsafe(24),
            hashlib.sha256(token.encode("utf-8")).hexdigest(),
            time.time() + SESSION_TTL_SECONDS,
            user_id,
        ),
    )
    return token


def _is_development() -> bool:
    """Developer-only behavior must be explicitly enabled, never inferred."""
    return os.environ.get("APP_ENV", "production").strip().lower() in {
        "development", "dev", "local", "test",
    }


def _otp_pepper() -> Optional[bytes]:
    pepper = os.environ.get("OTP_PEPPER", "")
    if pepper:
        return pepper.encode("utf-8")
    if _is_development():
        return b"qualitydb-explicit-development-only"
    return None


def _hash_otp(email: str, code: str, pepper: bytes) -> str:
    message = f"{email.strip().lower()}:{code.strip()}".encode("utf-8")
    return hmac.new(pepper, message, hashlib.sha256).hexdigest()


def _consume_verification_code(
    conn: sqlite3.Connection, email: str, code: str
) -> Optional[str]:
    """Validate and consume an OTP, returning a user-safe error on failure."""
    row = conn.execute(
        "SELECT id, code_hash, expires_at, used, attempts "
        "FROM verification_codes WHERE email = ? "
        "ORDER BY expires_at DESC LIMIT 1",
        (email,),
    ).fetchone()
    if not row:
        return "No code found — please request a new one."
    if row["used"]:
        return "Code already used — please request a new one."
    if time.time() > row["expires_at"]:
        conn.execute("UPDATE verification_codes SET used = 1 WHERE id = ?", (row["id"],))
        return "Code expired — please request a new one."
    if row["attempts"] >= OTP_MAX_ATTEMPTS:
        conn.execute("UPDATE verification_codes SET used = 1 WHERE id = ?", (row["id"],))
        return "Too many attempts — please request a new code."

    pepper = _otp_pepper()
    if not pepper or not row["code_hash"]:
        conn.execute("UPDATE verification_codes SET used = 1 WHERE id = ?", (row["id"],))
        return "Code verification is unavailable — please request a new code."

    supplied_hash = _hash_otp(email, code, pepper)
    if not secrets.compare_digest(supplied_hash, row["code_hash"]):
        attempts = row["attempts"] + 1
        conn.execute(
            "UPDATE verification_codes SET attempts = ?, used = CASE WHEN ? >= ? THEN 1 ELSE used END "
            "WHERE id = ?",
            (attempts, attempts, OTP_MAX_ATTEMPTS, row["id"]),
        )
        if attempts >= OTP_MAX_ATTEMPTS:
            return "Too many attempts — please request a new code."
        return "Invalid code."

    conn.execute("UPDATE verification_codes SET used = 1 WHERE id = ?", (row["id"],))
    return None


# ── User CRUD ─────────────────────────────────────────────────────────────────

def register_user(email: str, password: str, country: str,
                  q1: str, q2: str, q3: str) -> dict:
    """
    Create a new account.  Returns {"ok": True, "token": ..., "user": {...}}
    or {"ok": False, "error": "..."}.
    """
    if not all(isinstance(value, str) for value in (email, password, country, q1, q2, q3)):
        return {"ok": False, "error": "Invalid registration data."}
    email = email.strip().lower()
    if not email or "@" not in email or len(email) > 320:
        return {"ok": False, "error": "Invalid email address."}
    if len(password) < 8 or len(password) > 1024:
        return {"ok": False, "error": "Password must be between 8 and 1024 characters."}
    if country.upper() not in COUNTRY_SOURCES:
        return {"ok": False, "error": f"Country '{country}' is not supported yet."}

    pw_hash = _hash_password(password)
    token   = secrets.token_urlsafe(32)
    token_expires_at = time.time() + SESSION_TTL_SECONDS
    stored_token = "stored-" + secrets.token_urlsafe(24)
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()

    conn = open_users_db()
    try:
        conn.execute(
            "INSERT INTO users (email, password_hash, token, token_hash, token_expires_at, "
            "country, q1, q2, q3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (email, pw_hash, stored_token, token_hash, token_expires_at, country.upper(),
             q1.strip()[:4000], q2.strip()[:4000], q3.strip()[:4000]),
        )
        conn.commit()
        user = dict(conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone())
        return {"ok": True, "token": token, "user": _public_user(user)}
    except sqlite3.IntegrityError:
        return {"ok": False, "error": "An account with this email already exists."}
    finally:
        conn.close()


def login_user(email: str, password: str) -> dict:
    if not isinstance(email, str) or not isinstance(password, str):
        return {"ok": False, "error": "Invalid email or password."}
    email = email.strip().lower()
    if len(email) > 320 or len(password) > 1024:
        return {"ok": False, "error": "Invalid email or password."}
    conn = open_users_db()
    row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    if not row or not _verify_password(row["password_hash"], password):
        conn.close()
        return {"ok": False, "error": "Invalid email or password."}
    token = _issue_session_token(conn, row["id"])
    conn.commit()
    user = dict(conn.execute("SELECT * FROM users WHERE id = ?", (row["id"],)).fetchone())
    conn.close()
    return {"ok": True, "token": token, "user": _public_user(user)}


# ── Passwordless OTP auth ─────────────────────────────────────────────────────

def _send_verification_email(to_email: str, code: str) -> bool:
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
        if not _is_development():
            logging.error("SMTP_HOST is missing; refusing to expose an OTP in production")
            return False
        print(
            f"\n{'='*50}\n  DEV MODE — verification code for {to_email}"
            f"\n  CODE: {code}\n{'='*50}\n",
            flush=True,
        )
        return True

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = "Your Institut Kvality verification code"
    msg["From"] = smtp_from
    msg["To"] = to_email
    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as srv:
            srv.ehlo()
            srv.starttls()
            srv.login(smtp_user, smtp_pass)
            srv.send_message(msg)
        logging.info(f"Verification email sent to {to_email}")
        return True
    except Exception as exc:
        logging.error(f"Failed to send verification email to {to_email}: {exc}")
        return False


def request_verification_code(email: str) -> dict:
    if not isinstance(email, str):
        return {"ok": False, "error": "Invalid email address."}
    email = email.strip().lower()
    if not email or "@" not in email or len(email) > 320:
        return {"ok": False, "error": "Invalid email address."}

    pepper = _otp_pepper()
    if not pepper:
        return {"ok": False, "error": "Email verification is not configured."}
    if not os.environ.get("SMTP_HOST", "") and not _is_development():
        return {"ok": False, "error": "Email verification is not configured."}

    conn = open_users_db()
    try:
        recent = conn.execute(
            "SELECT created_at FROM verification_codes WHERE email = ? ORDER BY created_at DESC LIMIT 1",
            (email,),
        ).fetchone()
        if recent and (time.time() - recent[0]) < 60:
            return {"ok": False, "error": "Please wait a minute before requesting another code."}

        code = f"{secrets.randbelow(1_000_000):06d}"
        now  = time.time()
        conn.execute("DELETE FROM verification_codes WHERE email = ?", (email,))
        conn.execute(
            "INSERT INTO verification_codes "
            "(email, code, code_hash, attempts, expires_at, created_at) "
            "VALUES (?, '', ?, 0, ?, ?)",
            (email, _hash_otp(email, code, pepper), now + 600, now),
        )
        conn.commit()
    finally:
        conn.close()

    if not _send_verification_email(email, code):
        conn = open_users_db()
        try:
            conn.execute("UPDATE verification_codes SET used = 1 WHERE email = ?", (email,))
            conn.commit()
        finally:
            conn.close()
        return {"ok": False, "error": "Could not send verification email. Please try again later."}

    # Only an explicitly selected development environment may receive the code.
    return {"ok": True, **({"dev_code": code} if _is_development() else {})}


def verify_code_and_login(email: str, code: str) -> dict:
    """
    Verify the OTP. Returns one of:
      {"ok": True, "token": ..., "user": ..., "is_new": False}   — existing user, done
      {"ok": True, "needs_profile": True, "setup_token": ...}    — new user, needs profile
      {"ok": False, "error": "..."}
    """
    if not isinstance(email, str) or not isinstance(code, str):
        return {"ok": False, "error": "Invalid verification data."}
    email = email.strip().lower()
    code  = code.strip()

    conn = open_users_db()
    try:
        error = _consume_verification_code(conn, email, code)
        if error:
            conn.commit()
            return {"ok": False, "error": error}

        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if user_row:
            conn.execute(
                "UPDATE users SET email_verified_at = COALESCE(email_verified_at, datetime('now')) "
                "WHERE id = ?",
                (user_row["id"],),
            )
            token = _issue_session_token(conn, user_row["id"])
            conn.commit()
            user_dict = dict(
                conn.execute("SELECT * FROM users WHERE id = ?", (user_row["id"],)).fetchone()
            )
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
    if not all(isinstance(value, str) for value in (email, code, new_password)):
        return {"ok": False, "error": "Invalid password reset data."}
    email = email.strip().lower()
    code  = code.strip()
    if len(new_password) < 8 or len(new_password) > 1024:
        return {"ok": False, "error": "Password must be between 8 and 1024 characters."}

    conn = open_users_db()
    try:
        error = _consume_verification_code(conn, email, code)
        if error:
            conn.commit()
            return {"ok": False, "error": error}

        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if not user_row:
            conn.commit()
            return {"ok": False, "error": "No account found for this email."}

        new_hash = _hash_password(new_password)
        token = _issue_session_token(conn, user_row["id"])
        conn.execute(
            "UPDATE users SET password_hash = ?, "
            "email_verified_at = COALESCE(email_verified_at, datetime('now')) WHERE id = ?",
            (new_hash, user_row["id"]),
        )
        conn.execute(
            "UPDATE scanner_tokens SET revoked_at = ? "
            "WHERE user_id = ? AND revoked_at IS NULL",
            (time.time(), user_row["id"]),
        )
        conn.commit()
        user_dict = dict(
            conn.execute("SELECT * FROM users WHERE id = ?", (user_row["id"],)).fetchone()
        )
        return {"ok": True, "token": token, "user": _public_user(user_dict)}
    finally:
        conn.close()


def complete_profile(setup_token: str, country: str, q1: str, q2: str, q3: str) -> dict:
    if (not all(isinstance(value, str) for value in (setup_token, country, q1, q2, q3))
            or len(setup_token) > 256):
        return {"ok": False, "error": "Invalid or expired setup link — please start again."}
    conn = open_users_db()
    try:
        row = conn.execute(
            "SELECT email, expires_at, google_sub FROM pending_profiles WHERE setup_token = ?",
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
        stored_token = "stored-" + secrets.token_urlsafe(24)
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        token_expires_at = time.time() + SESSION_TTL_SECONDS
        try:
            conn.execute(
                "INSERT INTO users (email, password_hash, token, token_hash, token_expires_at, "
                "google_sub, email_verified_at, country, q1, q2, q3) "
                "VALUES (?, '', ?, ?, ?, ?, datetime('now'), ?, ?, ?, ?)",
                (email, stored_token, token_hash, token_expires_at,
                 row["google_sub"], country.upper(),
                 q1.strip()[:4000], q2.strip()[:4000], q3.strip()[:4000]),
            )
        except sqlite3.IntegrityError:
            # Race: user created between verify and complete — just refresh token
            existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
            if not existing:
                raise
            token = _issue_session_token(conn, existing["id"])

        conn.execute("DELETE FROM pending_profiles WHERE setup_token = ?", (setup_token,))
        conn.commit()

        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        return {"ok": True, "token": token, "user": _public_user(dict(user_row)), "is_new": True}
    finally:
        conn.close()


def verify_google_credential(credential: str, audience: str) -> dict:
    """Cryptographically verify a Google ID token and return its claims."""
    if not isinstance(credential, str) or not credential:
        raise ValueError("Google credential is required.")
    if not audience:
        raise ValueError("Google sign-in is not configured.")
    try:
        from google.auth.transport import requests as google_requests
        from google.oauth2 import id_token
    except ImportError as exc:
        raise RuntimeError("Google token verification dependency is unavailable.") from exc
    return id_token.verify_oauth2_token(
        credential,
        google_requests.Request(),
        audience,
    )


def google_auth(credential: str) -> dict:
    """
    Sign in or sign up a user via Google OAuth.
    The server verifies the signed Google ID token itself. Client-supplied
    profile fields are never trusted as proof of identity.

    Returns one of:
      {"ok": True, "token": ..., "user": ..., "is_new": False}   — existing user
      {"ok": True, "needs_profile": True, "setup_token": ..., "email": ...}  — new user
      {"ok": False, "error": "..."}
    """
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
    try:
        claims = verify_google_credential(credential, client_id)
    except Exception as exc:
        logging.warning("Rejected Google credential: %s", exc)
        return {"ok": False, "error": "Invalid Google sign-in credential."}

    email = str(claims.get("email", "")).strip().lower()
    google_sub = str(claims.get("sub", "")).strip()
    email_verified = claims.get("email_verified") is True or claims.get("email_verified") == "true"
    if (not email or "@" not in email or len(email) > 320
            or not google_sub or len(google_sub) > 255 or not email_verified):
        return {"ok": False, "error": "Google did not provide a verified email address."}

    conn = open_users_db()
    try:
        sub_owner = conn.execute(
            "SELECT email FROM users WHERE google_sub = ?", (google_sub,)
        ).fetchone()
        if sub_owner and sub_owner["email"] != email:
            return {"ok": False, "error": "This Google account is linked to a different user."}
        user_row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
        if user_row:
            if user_row["google_sub"] and user_row["google_sub"] != google_sub:
                return {"ok": False, "error": "This email is linked to a different Google account."}
            if not user_row["google_sub"] and not user_row["email_verified_at"]:
                return {
                    "ok": False,
                    "error": "Verify this account's email before linking Google sign-in.",
                }
            # Link only after the signed token proves control of the same email.
            conn.execute(
                "UPDATE users SET google_sub = ? WHERE id = ?",
                (google_sub, user_row["id"]),
            )
            token = _issue_session_token(conn, user_row["id"])
            conn.commit()
            user_dict = dict(
                conn.execute("SELECT * FROM users WHERE id = ?", (user_row["id"],)).fetchone()
            )
            return {"ok": True, "token": token, "user": _public_user(user_dict), "is_new": False}

        # New user — store a short-lived setup token so they can complete their profile
        setup_token = secrets.token_urlsafe(24)
        conn.execute(
            "INSERT OR REPLACE INTO pending_profiles "
            "(email, setup_token, expires_at, google_sub) VALUES (?,?,?,?)",
            (email, setup_token, time.time() + 1800, google_sub),
        )
        conn.commit()
        return {"ok": True, "needs_profile": True, "setup_token": setup_token, "email": email}
    finally:
        conn.close()


def get_user_by_token(token: str) -> Optional[dict]:
    if not token:
        return None
    conn = open_users_db()
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    row = conn.execute(
        "SELECT * FROM users WHERE token_hash = ? AND token_expires_at > ?",
        (token_hash, time.time()),
    ).fetchone()
    conn.close()
    return _public_user(dict(row)) if row else None


def get_internal_user_by_token(token: str) -> Optional[dict]:
    """Return the complete user record for server-side authorization only."""
    if not token:
        return None
    conn = open_users_db()
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    row = conn.execute(
        "SELECT * FROM users WHERE token_hash = ? AND token_expires_at > ?",
        (token_hash, time.time()),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def revoke_session_token(token: str) -> bool:
    if not token:
        return False
    conn = open_users_db()
    try:
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        cur = conn.execute(
            "UPDATE users SET token = ?, token_hash = NULL, token_expires_at = 0 "
            "WHERE token_hash = ?",
            ("stored-" + secrets.token_urlsafe(24), token_hash),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def create_scanner_token(user_id: int) -> dict:
    """Create a contribution-only token. Only its SHA-256 hash is persisted."""
    raw_token = secrets.token_urlsafe(32)
    now = time.time()
    conn = open_users_db()
    try:
        user = conn.execute("SELECT id FROM users WHERE id = ?", (user_id,)).fetchone()
        if not user:
            return {"ok": False, "error": "User not found."}
        conn.execute(
            "DELETE FROM scanner_tokens WHERE expires_at <= ? "
            "OR (revoked_at IS NOT NULL AND revoked_at <= ?)",
            (now, now - 30 * 24 * 60 * 60),
        )
        conn.execute(
            "INSERT INTO scanner_tokens "
            "(user_id, token_hash, created_at, expires_at) VALUES (?,?,?,?)",
            (user_id, hashlib.sha256(raw_token.encode("utf-8")).hexdigest(),
             now, now + SCANNER_TOKEN_TTL_SECONDS),
        )
        conn.commit()
        return {
            "ok": True,
            "scanner_token": raw_token,
            "expires_at": now + SCANNER_TOKEN_TTL_SECONDS,
        }
    finally:
        conn.close()


def get_user_by_scanner_token(token: str) -> Optional[dict]:
    if not token:
        return None
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    conn = open_users_db()
    try:
        row = conn.execute(
            "SELECT u.* FROM scanner_tokens st "
            "JOIN users u ON u.id = st.user_id "
            "WHERE st.token_hash = ? AND st.revoked_at IS NULL AND st.expires_at > ?",
            (token_hash, time.time()),
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE scanner_tokens SET last_used_at = ? WHERE token_hash = ?",
                (time.time(), token_hash),
            )
            conn.commit()
        return _public_user(dict(row)) if row else None
    finally:
        conn.close()


def revoke_scanner_tokens(user_id: int) -> int:
    conn = open_users_db()
    try:
        cur = conn.execute(
            "UPDATE scanner_tokens SET revoked_at = ? "
            "WHERE user_id = ? AND revoked_at IS NULL",
            (time.time(), user_id),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def _public_user(u: dict) -> dict:
    """Strip sensitive fields before returning to client."""
    public = {
        k: v for k, v in u.items()
        if k not in (
            "password_hash", "token", "token_hash", "token_expires_at",
            "google_sub", "email_verified_at",
        )
    }
    public["is_admin"] = bool(public.get("is_admin", False))
    public["email_verified"] = bool(u.get("email_verified_at"))
    return public


# ── Contributions ─────────────────────────────────────────────────────────────

SOURCE_HOST_SUFFIXES = {
    "heureka": ("heureka.cz",),
    "heureka_sk": ("heureka.sk",),
    "alza": ("alza.cz",),
    "zbozi": ("zbozi.cz",),
    "coolblue": ("coolblue.nl",),
    "idealo": ("idealo.de",),
    "idealo_de": ("idealo.de",),
    "fnac": ("fnac.com", "fnac.fr"),
    "czc": ("czc.cz",),
    "otto": ("otto.de",),
    "mediamarkt": ("mediamarkt.de",),
    "amazon_de": ("amazon.de",),
    "geizhals": ("geizhals.at",),
    "darty": ("darty.com",),
    "ceneo": ("ceneo.pl",),
    "prisjakt": ("prisjakt.nu",),
    "pricerunner": ("pricerunner.dk",),
    "pricerunner_se": ("pricerunner.se",),
}


def _normalise_contribution_source(source: object) -> str:
    if not isinstance(source, str):
        return ""
    value = source.strip().lower()[:64]
    return value if value in SOURCE_HOST_SUFFIXES else ""


def _valid_contribution_url(url: object, source: str) -> bool:
    if not isinstance(url, str) or len(url) > 2048:
        return False
    try:
        parsed = urlparse(url.strip())
    except ValueError:
        return False
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username:
        return False
    host = parsed.hostname.lower().rstrip(".")
    return any(
        host == suffix or host.endswith("." + suffix)
        for suffix in SOURCE_HOST_SUFFIXES.get(source, ())
    )


def _scalar(value: object) -> object:
    return value if value is None or isinstance(value, (str, int, float)) else None


def record_contribution(user_id: int, source: str, products: list[dict]) -> dict:
    """
    Stage a batch of products from a contributor.
    Returns {"queued": N, "duplicate": M}.
    """
    if not isinstance(products, list):
        raise ValueError("products must be a list.")
    if len(products) > MAX_CONTRIBUTION_PRODUCTS:
        raise ValueError(f"A contribution may contain at most {MAX_CONTRIBUTION_PRODUCTS} products.")

    conn = open_users_db()
    queued = 0
    duplicate = 0
    rejected = 0
    session_sources: set[str] = set()

    for p in products:
        if not isinstance(p, dict):
            rejected += 1
            continue
        product_source = _normalise_contribution_source(source or p.get("source", ""))
        raw_url = p.get("ProductURL") or p.get("product_url") or ""
        url = raw_url.strip() if isinstance(raw_url, str) else ""
        if not product_source or not _valid_contribution_url(url, product_source):
            rejected += 1
            continue
        session_sources.add(product_source)
        try:
            conn.execute("""
                INSERT OR IGNORE INTO staging_products
                    (product_url, name, source, country, category, main_category,
                     recommend_pct, reviews_count, avg_star_rating,
                     price_czk, price_eur, currency, user_id)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                url,
                str(p.get("Name") or p.get("name") or "")[:500],
                product_source,
                str(p.get("country") or "")[:8],
                str(p.get("Category") or p.get("category") or "")[:300],
                str(p.get("MainCategory") or p.get("main_category") or "")[:300],
                _scalar(p.get("RecommendRate_pct") or p.get("recommend_pct")),
                _scalar(p.get("ReviewsCount") or p.get("reviews_count")),
                _scalar(p.get("AvgStarRating") or p.get("avg_star_rating")),
                _scalar(p.get("Price_CZK") or p.get("price_czk")),
                _scalar(p.get("Price_EUR") or p.get("price_eur")),
                str(p.get("currency") or "")[:8],
                user_id,
            ))
            if conn.execute("SELECT changes()").fetchone()[0]:
                queued += 1
            else:
                duplicate += 1
        except (sqlite3.Error, TypeError, ValueError):
            rejected += 1

    # Log the session
    conn.execute(
        "INSERT INTO contribution_sessions (user_id, source, product_count, queued_count) "
        "VALUES (?, ?, ?, ?)",
        (user_id, ",".join(sorted(session_sources))[:200], len(products), queued),
    )
    # Bump user contrib counter
    conn.execute(
        "UPDATE users SET contrib_count = contrib_count + ? WHERE id = ?",
        (queued, user_id),
    )
    conn.commit()
    conn.close()
    return {"queued": queued, "duplicate": duplicate, "rejected": rejected}


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
