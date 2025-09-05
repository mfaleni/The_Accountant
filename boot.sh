#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/render/project/src"
DB="$APP_DIR/finance.db"
SEED="$APP_DIR/seed/finance.db.gz"
CSV="${CSV:-$APP_DIR/The New Wholy Grail - CLEAN.csv}"
CSV_BOOTSTRAP="${CSV_BOOTSTRAP:-0}"
FORCE_DB_RESTORE="${FORCE_DB_RESTORE:-0}"

echo "BOOT: starting — $(date)"

mkdir -p "$APP_DIR"

# Restore DB from repo seed if present (or if forced)
if [ -f "$SEED" ]; then
  if [ ! -s "$DB" ] || [ "$FORCE_DB_RESTORE" = "1" ]; then
    echo "BOOT: restoring DB from $SEED"
    rm -f "$DB"
    gunzip -c "$SEED" > "$DB" || echo "BOOT: seed restore failed; continuing"
  else
    echo "BOOT: DB exists; skipping seed restore (FORCE_DB_RESTORE=$FORCE_DB_RESTORE)"
  fi
else
  echo "BOOT: no seed file found at $SEED"
fi

# Always apply schema + migrations
echo "BOOT: applying schema.sql"
sqlite3 "$DB" < "$APP_DIR/schema.sql" || true
for f in "$APP_DIR"/migrations/*.sql; do
  [ -f "$f" ] && echo "BOOT: applying migration $f" && sqlite3 "$DB" < "$f" || true
done

# Row count after schema/migrations
ROWS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM transactions;" 2>/dev/null || echo 0)
echo "BOOT: transactions rows: ${ROWS}"

# Only import CSV if explicitly enabled
if [ "$CSV_BOOTSTRAP" = "1" ] && [ "${ROWS:-0}" -eq 0 ] && [ -f "$CSV" ]; then
  echo "BOOT: DB empty; loading from CSV with grail_loader.py"
  python3 "$APP_DIR/grail_loader.py" --csv "$CSV" || echo "BOOT: grail_loader returned non-zero"
  echo "BOOT: rows after CSV: $(sqlite3 "$DB" "SELECT COUNT(*) FROM transactions;")"
else
  echo "BOOT: Skip CSV import (ROWS=${ROWS:-na}, CSV_BOOTSTRAP=${CSV_BOOTSTRAP})"
fi

# Final table dump
echo "BOOT: tables now: $(sqlite3 "$DB" ".tables")"
echo "BOOT: transactions rows: $(sqlite3 "$DB" "SELECT COUNT(*) FROM transactions;")"

exec gunicorn wsgi:application --workers 2 --bind 0.0.0.0:$PORT --timeout 120
