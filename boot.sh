#!/usr/bin/env bash
set -euo pipefail

APP_DIR=/opt/render/project/src
DB="$APP_DIR/finance.db"
SEED="$APP_DIR/seed/finance.db.gz"
CSV_BOOTSTRAP="${CSV_BOOTSTRAP:-0}"     # 0 = off by default
PORT="${PORT:-5056}"

echo "BOOT: starting — $(date)"

# --- restore DB ---
if [ -f /etc/secrets/FINANCE_DB_GZ ]; then
  echo "BOOT: restoring DB from Render Secret"
  gunzip -c /etc/secrets/FINANCE_DB_GZ > "$DB"
elif [ -f "$SEED" ]; then
  echo "BOOT: restoring DB from repo seed"
  gunzip -c "$SEED" > "$DB"
else
  echo "BOOT: no seed provided — leaving DB as-is (may be empty)"
  : > "$DB"
fi

# --- apply schema + migrations (idempotent) ---
echo "BOOT: applying schema.sql"
sqlite3 "$DB" < "$APP_DIR/schema.sql" || true
for f in "$APP_DIR"/migrations/*.sql; do
  [ -f "$f" ] && echo "BOOT: applying migration $f" && sqlite3 "$DB" < "$f" || true
done

# --- optional CSV bootstrap (off unless CSV_BOOTSTRAP=1) ---
if [ "$CSV_BOOTSTRAP" = "1" ]; then
  CSV="${CSV:-$APP_DIR/The New Wholy Grail - CLEAN.csv}"
  if [ -f "$CSV" ]; then
    echo "BOOT: CSV bootstrap from $CSV"
    python3 "$APP_DIR/grail_loader.py" --csv "$CSV" || echo "BOOT: grail_loader returned non-zero"
  else
    echo "BOOT: CSV_BOOTSTRAP=1 but CSV not found at $CSV"
  fi
else
  echo "BOOT: Skip CSV import (CSV_BOOTSTRAP=$CSV_BOOTSTRAP)"
fi

# --- diagnostics ---
echo "BOOT: tables now:" $(sqlite3 "$DB" "SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1;")
echo "BOOT: transactions rows:" $(sqlite3 "$DB" "SELECT COUNT(*) FROM transactions;") || true

# --- start app ---
exec gunicorn wsgi:application --workers 2 --bind 0.0.0.0:$PORT --timeout 120
