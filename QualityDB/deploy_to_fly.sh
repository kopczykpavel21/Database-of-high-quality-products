#!/bin/bash
# deploy_to_fly.sh — Upload the local databases to Fly.io after a scraper run.
#
# Usage:
#   ./deploy_to_fly.sh                  # uploads products.db AND snapshots.db
#   ./deploy_to_fly.sh --restart        # also restarts the Fly.io machine after upload
#   ./deploy_to_fly.sh --products-only  # skip the snapshots upload
#
# Prerequisites:
#   fly CLI installed: https://fly.io/docs/hands-on/install-flyctl/
#   Logged in:         fly auth login
#   App name set:      edit FLY_APP below, or set FLY_APP env var
#
# How the workflow looks:
#   1. Run scrapers locally (python3 scraper/scheduler.py --now)
#   2. Wait for them to finish
#   3. Run this script to upload the fresh databases to Fly.io
#   4. The Fly.io server picks up the new files on the next request (cache TTL)
#
# Why both databases?
#   Price history lives in a SEPARATE snapshots.db, not in products.db.  This
#   script used to upload only products.db, so every deploy silently shipped
#   stale price history — the history strips and the has_history / price_drop
#   filters read snapshots.db exclusively.  fly.toml points the server at
#   SNAPSHOTS_DB_PATH=/data/snapshots.db.
#
# Why run scrapers locally?
#   • Your Mac is faster for I/O-heavy scraping than a shared-cpu-1x Fly instance
#   • No scraper competes with the HTTP server for CPU
#   • Fly.io free tier has limited CPU credits; scraping burns them fast
#   • You can inspect/debug the DB locally before publishing

set -euo pipefail

FLY_APP="${FLY_APP:-database-of-high-quality-products}"

# Canonical data root — the same pair the server and scheduler read.
# Override with QUALITYDB_DATA_ROOT, or point at the filtered deploy copies
# produced by prepare_db_for_deploy.py.
DATA_ROOT="${QUALITYDB_DATA_ROOT:-$HOME/QualityData/qualitydb}"
LOCAL_DB="${DB_PATH:-$DATA_ROOT/products.db}"
LOCAL_SNP="${SNAPSHOTS_DB_PATH:-$DATA_ROOT/snapshots.db}"

REMOTE_DB="/data/products.db"
REMOTE_SNP="/data/snapshots.db"

RESTART=0
PRODUCTS_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --restart)        RESTART=1 ;;
    --products-only)  PRODUCTS_ONLY=1 ;;
    *) echo "Unknown option: $arg"; exit 2 ;;
  esac
done

if [ ! -f "$LOCAL_DB" ]; then
  echo "ERROR: $LOCAL_DB not found"
  exit 1
fi

SIZE=$(du -sh "$LOCAL_DB" | cut -f1)
echo "products.db   $LOCAL_DB ($SIZE) → fly://$FLY_APP$REMOTE_DB"

if [ "$PRODUCTS_ONLY" -eq 0 ]; then
  if [ ! -f "$LOCAL_SNP" ]; then
    echo "ERROR: $LOCAL_SNP not found — price history would go stale on the"
    echo "       live site.  Pass --products-only to upload products.db alone."
    exit 1
  fi
  SNP_SIZE=$(du -sh "$LOCAL_SNP" | cut -f1)
  # Report what is actually being shipped, so a stale snapshots.db is visible
  # before the upload rather than after someone notices empty charts.
  SNP_INFO=$(sqlite3 "$LOCAL_SNP" \
    "SELECT COUNT(*) || ' rows, latest ' || MAX(snapshot_date) FROM product_snapshots;" \
    2>/dev/null || echo "unreadable")
  echo "snapshots.db  $LOCAL_SNP ($SNP_SIZE; $SNP_INFO) → fly://$FLY_APP$REMOTE_SNP"
fi

echo

# fly's sftp PUT refuses to overwrite an existing path ("file exists on VM"),
# and `fly sftp shell` exits 0 even when every put inside it failed — so a
# broken deploy used to print "Upload complete" and change nothing.  Upload to
# a temporary name, check it landed at the right size, then swap it in with an
# atomic mv over SSH.  The running server reopens the DB per request, so the
# rename is safe while it serves traffic.
FLY_SSH=(fly ssh console -a "$FLY_APP" -C)

upload() {
  local src="$1" dest="$2" tmp="$2.new"
  local expect actual out

  expect=$(/usr/bin/stat -f%z "$src")
  echo "  uploading $(basename "$src") ($expect bytes) → $tmp"

  "${FLY_SSH[@]}" "rm -f $tmp" >/dev/null 2>&1 || true

  # flyctl reports sftp problems on stdout and still exits 0, so inspect the
  # text rather than trusting the status.
  out=$(fly sftp put -a "$FLY_APP" "$src" "$tmp" 2>&1)
  echo "$out" | sed 's/^/    /'
  if echo "$out" | grep -qiE "file exists on VM|error|failed|unrecognized"; then
    echo "ERROR: upload of $src failed (see above)" >&2
    return 1
  fi

  # Confirm the bytes really arrived before destroying the live file.
  actual=$("${FLY_SSH[@]}" "stat -c%s $tmp" 2>/dev/null | tr -dc '0-9')
  if [ "$actual" != "$expect" ]; then
    echo "ERROR: $tmp is ${actual:-0} bytes on the VM, expected $expect — not swapping in." >&2
    "${FLY_SSH[@]}" "rm -f $tmp" >/dev/null 2>&1 || true
    return 1
  fi

  "${FLY_SSH[@]}" "mv $tmp $dest" >/dev/null || {
    echo "ERROR: could not swap $tmp into place" >&2; return 1; }
  echo "    ok — $dest is now $actual bytes"
}

upload "$LOCAL_DB" "$REMOTE_DB" || exit 1
if [ "$PRODUCTS_ONLY" -eq 0 ]; then
  upload "$LOCAL_SNP" "$REMOTE_SNP" || exit 1
fi

echo "Upload complete."

if [ "$RESTART" -eq 1 ]; then
  echo "Restarting Fly.io machines to pick up new DBs..."
  # Non-interactive runs must name the machine explicitly, otherwise flyctl
  # errors with "a machine ID must be specified when not running interactively".
  for mid in $(fly machines list -a "$FLY_APP" --json | /usr/bin/python3 -c \
        'import sys,json; print(" ".join(m["id"] for m in json.load(sys.stdin)))'); do
    echo "  restarting $mid"
    fly machine restart "$mid" -a "$FLY_APP"
  done
  echo "Done."
else
  echo "Tip: run with --restart to also restart the Fly.io machine."
  echo "     Without restart, the running server picks up the new files within cache TTL (5 min)."
fi
