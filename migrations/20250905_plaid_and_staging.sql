PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS plaid_items (
  item_id TEXT PRIMARY KEY,
  access_token TEXT NOT NULL,
  institution_name TEXT,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS import_batches (
  id INTEGER PRIMARY KEY,
  source TEXT NOT NULL,
  item_id TEXT,
  account_ids TEXT,
  start_date TEXT,
  end_date TEXT,
  status TEXT DEFAULT 'raw',
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS import_raw (
  id INTEGER PRIMARY KEY,
  batch_id INTEGER NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
  plaid_txn_id TEXT UNIQUE,
  account_id TEXT,
  date TEXT,
  authorized_date TEXT,
  name TEXT,
  merchant_name TEXT,
  amount REAL,
  pending INTEGER,
  currency TEXT,
  original_json TEXT
);

CREATE TABLE IF NOT EXISTS import_suggestions (
  id INTEGER PRIMARY KEY,
  batch_id INTEGER NOT NULL REFERENCES import_batches(id) ON DELETE CASCADE,
  raw_id INTEGER NOT NULL REFERENCES import_raw(id) ON DELETE CASCADE,
  suggested_merchant TEXT,
  suggested_category TEXT,
  suggested_subcategory TEXT,
  confidence REAL,
  source TEXT,
  locked INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_import_raw_batch ON import_raw(batch_id);
CREATE INDEX IF NOT EXISTS idx_import_suggestions_batch ON import_suggestions(batch_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_import_suggestions_raw ON import_suggestions(raw_id);
