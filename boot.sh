#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/render/project/src"
DB="$APP_DIR/finance.db"
SEED="$APP_DIR/seed/finance.db.gz"
CSV_CANDIDATE_1="$APP_DIR/The New Wholy Grail - CLEAN.csv"
CSV_CANDIDATE_2="$APP_DIR/The Wholy Grail.csv"
if [[ -f "$CSV_CANDIDATE_1" ]]; then CSV="$CSV_CANDIDATE_1"; elif [[ -f "$CSV_CANDIDATE_2" ]]; then CSV="$CSV_CANDIDATE_2"; else CSV=""; fi

echo "BOOT: starting — $(date)"

# --- Restore DB from seed if requested or DB missing/empty ---
if [[ "${FORCE_DB_RESTORE:-0}" == "1" && -f "$SEED" ]]; then
  echo "BOOT: FORCE_DB_RESTORE=1 — restoring DB from seed"
  rm -f "$DB"
  gunzip -c "$SEED" > "$DB" || echo "BOOT: seed restore failed; continuing"
elif [[ ! -s "$DB" && -f "$SEED" ]]; then
  echo "BOOT: DB missing/empty — restoring DB from seed"
  gunzip -c "$SEED" > "$DB" || echo "BOOT: seed restore failed; continuing"
else
  echo "BOOT: No seed provided — leaving DB as-is (may be empty)."
fi

# --- Apply schema and migrations (idempotent) ---
if [[ -f "$APP_DIR/schema.sql" ]]; then
  echo "BOOT: applying schema.sql"
  sqlite3 "$DB" < "$APP_DIR/schema.sql" || true
fi

if [[ -d "$APP_DIR/migrations" ]]; then
  for f in "$APP_DIR"/migrations/*.sql; do
    [[ -f "$f" ]] || continue
    echo "BOOT: applying migration $f"
    sqlite3 "$DB" < "$f" || true
  done
fi

# --- If DB still empty and CSV present, load once via grail_loader ---
ROWS=$(sqlite3 "$DB" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='transactions';")
if [[ "${ROWS:-0}" -gt 0 ]]; then
  COUNT=$(sqlite3 "$DB" "SELECT COUNT(*) FROM transactions;")
else
  COUNT=0
fi

if [[ "$COUNT" -eq 0 && -f "$CSV" ]]; then
  echo "BOOT: DB empty; loading from CSV with grail_loader.py"
  python3 "$APP_DIR/grail_loader.py" --csv "$CSV" || echo "BOOT: grail_loader returned non-zero"
  echo "BOOT: rows after CSV: $(sqlite3 "$DB" "SELECT COUNT(*) FROM transactions;")"
else
  echo "BOOT: Skip CSV import (rows=$COUNT; csv_exists=$( [[ -f "$CSV" ]] && echo yes || echo no ))"
fi

# --- Quick diagnostics (pure Python; no shell inside) ---
python3 - <<'PY'
import sqlite3, os
db="/opt/render/project/src/finance.db"
con=sqlite3.connect(db); c=con.cursor()
tables=[r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
print("BOOT: tables now:", tables)
if "transactions" in tables:
    n=c.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
else:
    n=0
print("BOOT: transactions rows:", n)
con.close()
PY

# --- Run app ---
exec gunicorn wsgi:application --workers 2 --bind 0.0.0.0:$PORT --timeout 120
