import hashlib
import json
import sqlite3
import threading
import urllib.error
import urllib.request
from http.server import HTTPServer

import pytest

import server
from scraper import auth


@pytest.fixture
def auth_db(tmp_path, monkeypatch):
    db_path = tmp_path / "users.db"
    monkeypatch.setattr(auth, "DB_PATH", str(db_path))
    monkeypatch.setenv("APP_ENV", "test")
    monkeypatch.setenv("OTP_PEPPER", "test-only-pepper")
    monkeypatch.delenv("SMTP_HOST", raising=False)
    auth.ensure_tables()
    return db_path


def _register(email="person@example.com"):
    result = auth.register_user(email, "correct horse battery", "CZ", "q1", "q2", "q3")
    assert result["ok"]
    return result


def test_sessions_are_hashed_rotated_and_expire(auth_db):
    created = _register()
    raw_token = created["token"]

    conn = sqlite3.connect(auth_db)
    stored_token, token_hash = conn.execute(
        "SELECT token, token_hash FROM users WHERE email = 'person@example.com'"
    ).fetchone()
    assert stored_token != raw_token
    assert token_hash == hashlib.sha256(raw_token.encode()).hexdigest()

    login = auth.login_user("person@example.com", "correct horse battery")
    assert login["ok"] and login["token"] != raw_token
    assert auth.get_user_by_token(raw_token) is None
    assert auth.get_user_by_token(login["token"])["email"] == "person@example.com"

    conn.execute("UPDATE users SET token_expires_at = 0")
    conn.commit()
    conn.close()
    assert auth.get_user_by_token(login["token"]) is None


def test_legacy_bookmark_session_is_migrated_without_storing_its_raw_value(auth_db):
    created = _register()
    legacy_token = created["token"]
    conn = sqlite3.connect(auth_db)
    conn.execute(
        "UPDATE users SET token = ?, token_hash = NULL, token_expires_at = NULL",
        (legacy_token,),
    )
    conn.commit()
    conn.close()

    auth.ensure_tables()
    assert auth.get_user_by_token(legacy_token)["email"] == "person@example.com"

    conn = sqlite3.connect(auth_db)
    stored, stored_hash, expiry = conn.execute(
        "SELECT token, token_hash, token_expires_at FROM users"
    ).fetchone()
    conn.close()
    assert stored != legacy_token
    assert stored_hash == hashlib.sha256(legacy_token.encode()).hexdigest()
    assert expiry > 0


def test_otp_is_hashed_and_locked_after_five_failures(auth_db):
    requested = auth.request_verification_code("otp@example.com")
    assert requested["ok"] and len(requested["dev_code"]) == 6

    conn = sqlite3.connect(auth_db)
    code, code_hash = conn.execute(
        "SELECT code, code_hash FROM verification_codes WHERE email = 'otp@example.com'"
    ).fetchone()
    conn.close()
    assert code == ""
    assert code_hash and requested["dev_code"] not in code_hash

    for _ in range(4):
        assert auth.verify_code_and_login("otp@example.com", "999999")["error"] == "Invalid code."
    locked = auth.verify_code_and_login("otp@example.com", "999999")
    assert "Too many attempts" in locked["error"]
    assert not auth.verify_code_and_login("otp@example.com", requested["dev_code"])["ok"]


def test_otp_fails_closed_without_production_email_configuration(auth_db, monkeypatch):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("SMTP_HOST", raising=False)
    result = auth.request_verification_code("production@example.com")
    assert result == {"ok": False, "error": "Email verification is not configured."}


def test_successful_otp_profile_marks_email_verified(auth_db):
    requested = auth.request_verification_code("new-person@example.com")
    verified = auth.verify_code_and_login("new-person@example.com", requested["dev_code"])
    assert verified["ok"] and verified["needs_profile"]

    completed = auth.complete_profile(
        verified["setup_token"], "CZ", "research", "electronics", "reviews"
    )
    assert completed["ok"]
    assert completed["user"]["email_verified"] is True
    assert auth.get_user_by_token(completed["token"])["email"] == "new-person@example.com"


def test_google_login_uses_verified_claims_not_caller_email(auth_db, monkeypatch):
    registered = _register("google@example.com")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "client.apps.googleusercontent.com")
    monkeypatch.setattr(
        auth,
        "verify_google_credential",
        lambda credential, audience: {
            "sub": "google-subject-123",
            "email": "google@example.com",
            "email_verified": True,
        },
    )

    # A matching local email is linkable only after QualityDB has independently
    # verified that address (direct registration alone does not prove ownership).
    conn = sqlite3.connect(auth_db)
    conn.execute(
        "UPDATE users SET email_verified_at = datetime('now') WHERE email = 'google@example.com'"
    )
    conn.commit()
    conn.close()

    result = auth.google_auth("signed-google-id-token")
    assert result["ok"]
    assert result["token"] != registered["token"]

    conn = sqlite3.connect(auth_db)
    assert conn.execute(
        "SELECT google_sub FROM users WHERE email = 'google@example.com'"
    ).fetchone()[0] == "google-subject-123"
    conn.close()

    monkeypatch.setattr(
        auth,
        "verify_google_credential",
        lambda credential, audience: (_ for _ in ()).throw(ValueError("bad signature")),
    )
    assert not auth.google_auth("attacker-controlled-value")["ok"]


def test_google_does_not_link_an_unverified_pre_registered_email(auth_db, monkeypatch):
    _register("victim@example.com")
    monkeypatch.setenv("GOOGLE_CLIENT_ID", "client.apps.googleusercontent.com")
    monkeypatch.setattr(
        auth,
        "verify_google_credential",
        lambda credential, audience: {
            "sub": "victims-real-google-subject",
            "email": "victim@example.com",
            "email_verified": True,
        },
    )
    result = auth.google_auth("valid-token-from-the-real-victim")
    assert not result["ok"]
    assert "Verify this account's email" in result["error"]


def test_scanner_token_is_scoped_hashed_and_revocable(auth_db):
    user = _register()["user"]
    created = auth.create_scanner_token(user["id"])
    scanner_token = created["scanner_token"]

    conn = sqlite3.connect(auth_db)
    stored_hash = conn.execute("SELECT token_hash FROM scanner_tokens").fetchone()[0]
    conn.close()
    assert stored_hash == hashlib.sha256(scanner_token.encode()).hexdigest()
    assert auth.get_user_by_scanner_token(scanner_token)["id"] == user["id"]
    assert auth.get_user_by_token(scanner_token) is None

    assert auth.revoke_scanner_tokens(user["id"]) == 1
    assert auth.get_user_by_scanner_token(scanner_token) is None


def test_password_reset_revokes_old_sessions_and_scanner_tokens(auth_db):
    registered = _register()
    scanner_token = auth.create_scanner_token(registered["user"]["id"])["scanner_token"]
    requested = auth.request_verification_code("person@example.com")

    reset = auth.reset_password(
        "person@example.com", requested["dev_code"], "a new secure password"
    )
    assert reset["ok"]
    assert auth.get_user_by_token(registered["token"]) is None
    assert auth.get_user_by_scanner_token(scanner_token) is None
    assert auth.get_user_by_token(reset["token"])["email"] == "person@example.com"


def test_contribution_uses_each_scanned_products_source_and_validates_host(auth_db):
    user = _register()["user"]
    result = auth.record_contribution(user["id"], "", [
        {
            "Name": "Good product",
            "ProductURL": "https://www.alza.cz/good-product-d123.htm",
            "source": "alza",
            "country": "CZ",
        },
        {
            "Name": "Forged product",
            "ProductURL": "https://attacker.example/product",
            "source": "alza",
            "country": "CZ",
        },
    ])
    assert result == {"queued": 1, "duplicate": 0, "rejected": 1}

    conn = sqlite3.connect(auth_db)
    assert conn.execute("SELECT source FROM staging_products").fetchone()[0] == "alza"
    conn.close()


@pytest.fixture
def security_server(auth_db):
    httpd = HTTPServer(("127.0.0.1", 0), server.Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{httpd.server_port}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def _request(base, path, method="GET", body=None, headers=None):
    payload = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        base + path,
        data=payload,
        method=method,
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            raw = response.read()
            return response.status, dict(response.headers), json.loads(raw) if raw else None
    except urllib.error.HTTPError as error:
        raw = error.read()
        return error.code, dict(error.headers), json.loads(raw) if raw else None


def test_ikor_scanner_contributes_cross_origin_but_cannot_access_account_or_admin(
    security_server,
):
    registered = _register()
    scanner_token = auth.create_scanner_token(registered["user"]["id"])["scanner_token"]
    scanner_headers = {
        "Authorization": f"Scanner {scanner_token}",
        "Origin": "https://www.alza.cz",
    }

    status, headers, _ = _request(
        security_server,
        "/api/contribute",
        method="OPTIONS",
        headers={"Origin": "https://www.alza.cz"},
    )
    assert status == 204
    assert headers["Access-Control-Allow-Origin"] == "https://www.alza.cz"

    status, headers, result = _request(
        security_server,
        "/api/contribute",
        method="POST",
        headers=scanner_headers,
        body={"products": [{
            "Name": "IKOR product",
            "ProductURL": "https://www.alza.cz/ikor-product-d456.htm",
            "source": "alza",
            "country": "CZ",
        }]},
    )
    assert status == 200 and result["queued"] == 1
    assert headers["Access-Control-Allow-Origin"] == "https://www.alza.cz"

    assert _request(
        security_server, "/api/me", headers={"Authorization": f"Scanner {scanner_token}"}
    )[0] == 401
    context_status, _, context = _request(
        security_server,
        "/api/scanner-context",
        headers={"Authorization": f"Scanner {scanner_token}"},
    )
    assert context_status == 200 and context["country"] == "CZ"
    assert "email" not in context and "id" not in context
    assert _request(
        security_server,
        "/api/admin/restart-scheduler",
        method="POST",
        body={},
        headers={"Authorization": f"Scanner {scanner_token}"},
    )[0] == 401


def test_admin_endpoints_reject_normal_users(security_server):
    token = _register()["token"]
    status, _, result = _request(
        security_server,
        "/api/admin/restart-scheduler",
        method="POST",
        body={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert status == 403
    assert result["error"] == "Administrator access required."


def test_admin_role_also_requires_verified_email(security_server, auth_db):
    registered = _register()
    conn = sqlite3.connect(auth_db)
    conn.execute("UPDATE users SET is_admin = 1 WHERE id = ?", (registered["user"]["id"],))
    conn.commit()
    conn.close()

    headers = {"Authorization": f"Bearer {registered['token']}"}
    status, _, result = _request(
        security_server, "/api/admin/not-a-real-route", method="POST", body={}, headers=headers
    )
    assert status == 403 and "Verify" in result["error"]

    conn = sqlite3.connect(auth_db)
    conn.execute(
        "UPDATE users SET email_verified_at = datetime('now') WHERE id = ?",
        (registered["user"]["id"],),
    )
    conn.commit()
    conn.close()
    assert _request(
        security_server, "/api/admin/not-a-real-route", method="POST", body={}, headers=headers
    )[0] == 404


def test_scraper_key_fails_closed(monkeypatch):
    monkeypatch.delenv("SCRAPER_KEY", raising=False)
    assert not server._valid_scraper_key("")
    assert not server._valid_scraper_key("attacker")
    monkeypatch.setenv("SCRAPER_KEY", "expected-secret")
    assert server._valid_scraper_key("expected-secret")
    assert not server._valid_scraper_key("wrong-secret")


def test_static_file_resolution_blocks_directory_and_encoded_traversal():
    safe_path = server._safe_static_path("/static/app.js")
    assert safe_path and safe_path.endswith("/QualityDB/static/app.js")
    assert server._safe_static_path("/static/../server.py") is None
    assert server._safe_static_path("/static/%2e%2e/server.py") is None


@pytest.mark.parametrize(
    ("origin", "allowed"),
    [
        ("https://www.heureka.cz", True),
        ("https://m.alza.cz", True),
        ("https://www.fnac.com", True),
        ("http://www.alza.cz", False),
        ("https://alza.cz.attacker.example", False),
        ("https://attacker.example/?next=alza.cz", False),
    ],
)
def test_scanner_origin_allowlist(origin, allowed):
    assert server.is_scanner_origin_allowed(origin) is allowed
