#!/bin/sh
# Seed the Fly volume with the bundled DB on first boot.
# On subsequent deploys the volume already has data — don't overwrite it.
VOLUME_DB="${DB_PATH:-/data/products.db}"
SEED_DB="/app/QualityDB/products.db"

if [ ! -f "$VOLUME_DB" ]; then
    echo "[entrypoint] Volume DB not found — seeding from image..."
    cp "$SEED_DB" "$VOLUME_DB"
    echo "[entrypoint] Seed complete."
else
    echo "[entrypoint] Volume DB exists, skipping seed."
fi

exec python3 /app/QualityDB/server.py
