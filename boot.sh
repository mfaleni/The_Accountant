#!/usr/bin/env bash
set -euo pipefail
APP_DIR=/opt/render/project/src
DB_TARGET="$APP_DIR/finance.db"

if [ ! -s "$DB_TARGET" ]; then
  if [ -f /etc/secrets/FINANCE_DB_GZ ]; then
    echo "Restoring DB from Render Secret File..."
    gunzip -c /etc/secrets/FINANCE_DB_GZ > "$DB_TARGET"
  elif [ -n "${DB_BOOTSTRAP_URL:-}" ]; then
    echo "Downloading DB from DB_BOOTSTRAP_URL..."
    curl -fsSL "$DB_BOOTSTRAP_URL" | gunzip > "$DB_TARGET"
  else
    echo "No DB seed provided; starting empty."
  fi
fi

exec gunicorn wsgi:application --workers 2 --bind 0.0.0.0:$PORT --timeout 120
