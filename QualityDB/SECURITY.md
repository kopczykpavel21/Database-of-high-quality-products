# QualityDB security configuration

The application now fails closed: privileged APIs need an administrator session,
machine ingestion needs `SCRAPER_KEY`, production OTP delivery needs SMTP plus an
OTP pepper, and Google sign-in accepts only a signed Google ID token verified by
the server.

## Required production secrets

Generate independent random values; do not reuse a login password or commit the
values to Git:

```sh
fly secrets set \
  SCRAPER_KEY="<at least 32 random bytes>" \
  OTP_PEPPER="<a different random value>" \
  GOOGLE_CLIENT_ID="<Google OAuth web client ID>" \
  SMTP_HOST="<SMTP host>" \
  SMTP_USER="<SMTP user>" \
  SMTP_PASS="<SMTP password>" \
  SMTP_FROM="<verified sender address>"
```

`SCRAPER_KEY` is also required in the environment of local upload jobs such as
`scrape_alza_local.py`. If it is absent on either side, ingestion is rejected.

If a separately hosted frontend calls the Fly API, list its exact HTTPS origin:

```toml
[env]
  FRONTEND_ORIGINS = "https://quality.example.com,https://preview.example.com"
```

Do not add `*`. The built-in Fly site and the current Vercel site already have
safe defaults. Marketplace origins used by IKOR Skener are separately limited
in code.

## Administrator accounts

Admin access is a database role (`users.is_admin = 1`), not merely a logged-in
account. Promote only the intended owner directly in `/data/users.db`, then log
in again so the UI reloads the role. The account email must also be verified
(using the email-code/password-reset flow if it predates this migration).
Ordinary users and scanner tokens receive HTTP 403/401 from admin endpoints.

## IKOR Skener migration

New bookmarklets automatically use a contribution-only `Scanner` token. The
server stores only its SHA-256 hash, and it cannot call `/api/me`, contribution
history, scheduler, or admin APIs.

Existing bookmarklets used the website login token. On the first deployment,
those old sessions receive a seven-day compatibility window so the feature does
not fail immediately. Users should log in once and drag **IKOR Skener** to the
bookmarks bar again during that window. New scanner tokens last one year by
default and are independent of website logout/session rotation.
Password reset revokes all scanner tokens; after a reset, reinstall the
bookmarklet so a compromised account cannot retain a contribution credential.

After the migration window, set the following to remove the legacy contribution
path entirely and deploy again:

```toml
[env]
  ALLOW_LEGACY_BOOKMARKLET_BEARER = "false"
  LEGACY_SESSION_GRACE_SECONDS = "0"
```

## Operational notes

- Website sessions rotate on login and expire after 30 days. Only their hashes
  are stored in the database.
- OTPs expire after 10 minutes, are stored as keyed hashes, and lock after five
  failed attempts. Development codes are exposed only when `APP_ENV` is
  explicitly `development`, `dev`, `local`, or `test`.
- Google sign-in requires the `google-auth` dependency in `requirements.txt` and
  the exact OAuth Web client ID in `GOOGLE_CLIENT_ID`.
- JSON request bodies are capped at 2 MiB; contribution batches are capped at
  200 products and machine-ingest batches at 1,000 products.
- Rotate `SCRAPER_KEY`, `OTP_PEPPER`, SMTP credentials, and the Google OAuth
  client secret immediately if an old value was ever committed or exposed.
