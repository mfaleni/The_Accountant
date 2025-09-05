#!/usr/bin/env bash
set -euo pipefail

APP_DIR=/opt/render/project/src
DB_TARGET="$APP_DIR/finance.db"
SECR="/etc/secrets/FINANCE_DB_GZ"

echo "BOOT: starting — $(date)"
echo "BOOT: looking for secret at $SECR"
ls -l /etc/secrets 2>/dev/null || true
ls -l "$APP_DIR" 2>/dev/null || true

needs_restore=1
if [ -s "$DB_TARGET" ]; then
  if command -v sqlite3 >/dev/null 2>&1; then
    if sqlite3 "$DB_TARGET" "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;" | grep -q .; then
       needs_restore=0
       echo "BOOT: DB already has schema — skipping restore"
    else
       echo "BOOT: DB exists but has NO schema — will restore"
    fi
  else
    echo "BOOT: sqlite3 not available to check schema — will restore if secret present"
  fi
fi

# Allow forcing a restore even if schema exists
if [ "${FORCE_DB_RESTORE:-0}" = "1" ]; then
  echo "BOOT: FORCE_DB_RESTORE=1 — will restore regardless of current DB"
  needs_restore=1
fi

if [ "$needs_restore" -ne 0 ]; then
  if [ -f "$SECR" ]; then
    echo "BOOT: Restoring DB from secret…"
    rm -f "$DB_TARGET"
    gunzip -c "$SECR" > "$DB_TARGET"
    echo "BOOT: restore complete: $(ls -lh "$DB_TARGET")"
  elif [ -n "${DB_BOOTSTRAP_URL:-}" ]; then
    echo "BOOT: Downloading DB from DB_BOOTSTRAP_URL…"
    rm -f "$DB_TARGET"
    curl -fsSL "$DB_BOOTSTRAP_URL" | gunzip > "$DB_TARGET"
    echo "BOOT: download+restore complete: $(ls -lh "$DB_TARGET")"
  else
    echo "BOOT: No seed provided — leaving DB as-is (may be empty)."
  fi
fi

# Ensure minimum schema exists (no-op if already there)
python - <<'PY'
import sqlite3
db="/opt/render/project/src/finance.db"
con=sqlite3.connect(db); c=con.cursor()
c.executescript("""
CREATE TABLE IF NOT EXISTS accounts (
  id INTEGER PRIMARY KEY,
  name TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS transactions (
  id INTEGER PRIMARY KEY,
  transaction_date TEXT,
  original_description TEXT,
  cleaned_description TEXT,
  amount REAL,
  category TEXT,
  sub_category TEXT,
  account_id INTEGER REFERENCES accounts(id),
  transaction_id TEXT,
  unique_hash TEXT
);
CREATE TABLE IF NOT EXISTS category_rules (
  id INTEGER PRIMARY KEY,
  merchant_pattern TEXT UNIQUE,
  category TEXT
);
""")
con.commit()
print("BOOT: tables now:",
      [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")])
print("BOOT: transactions rows:",
      c.execute("SELECT COUNT(*) FROM transactions").fetchone()[0])
con.close()
PY

exec gunicorn wsgi:application --workers 2 --bind 0.0.0.0:$PORT --timeout 120

# --- staged-import migration (idempotent) ---
if [ -f "/opt/render/project/src/migrations/20250905_staged_import.sql" ]; then
  echo "BOOT: applying staged-import migration"
  sqlite3 /opt/render/project/src/finance.db < /opt/render/project/src/migrations/20250905_staged_import.sql || true
fi
