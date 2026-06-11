#!/bin/bash
# deploy_to_fly.sh — Upload the local products.db to Fly.io after a scraper run.
#
# Usage:
#   ./deploy_to_fly.sh                  # uploads products.db
#   ./deploy_to_fly.sh --restart        # also restarts the Fly.io machine after upload
#
# Prerequisites:
#   fly CLI installed: https://fly.io/docs/hands-on/install-flyctl/
#   Logged in:         fly auth login
#   App name set:      edit FLY_APP below, or set FLY_APP env var
#
# How the workflow looks:
#   1. Run scrapers locally (python3 scraper/scheduler.py --now)
#   2. Wait for them to finish
#   3. Run this script to upload the fresh products.db to Fly.io
#   4. The Fly.io server picks up the new file on the next request (cache TTL)
#
# Why run scrapers locally?
#   • Your Mac is faster for I/O-heavy scraping than a shared-cpu-1x Fly instance
#   • No scraper competes with the HTTP server for CPU
#   • Fly.io free tier has limited CPU credits; scraping burns them fast
#   • You can inspect/debug the DB locally before publishing

set -euo pipefail

FLY_APP="${FLY_APP:-database-of-high-quality-products}"
LOCAL_DB="$(dirname "$0")/products.db"
REMOTE_PATH="/data/products.db"   # path inside the Fly.io volume (set DB_PATH env var to match)

if [ ! -f "$LOCAL_DB" ]; then
  echo "ERROR: $LOCAL_DB not found"
  exit 1
fi

SIZE=$(du -sh "$LOCAL_DB" | cut -f1)
echo "Uploading $LOCAL_DB ($SIZE) → fly://$FLY_APP$REMOTE_PATH"

# Upload via fly sftp
fly sftp shell -a "$FLY_APP" <<SFTP
put $LOCAL_DB $REMOTE_PATH
exit
SFTP

echo "Upload complete."

if [[ "${1:-}" == "--restart" ]]; then
  echo "Restarting Fly.io machines to pick up new DB..."
  fly machine restart -a "$FLY_APP"
  echo "Done."
else
  echo "Tip: run with --restart to also restart the Fly.io machine."
  echo "     Without restart, the running server picks up the new file within cache TTL (5 min)."
fi
