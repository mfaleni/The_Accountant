-- schema.sql
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    transaction_date TEXT NOT NULL,
    original_description TEXT,
    cleaned_description TEXT,
    amount REAL NOT NULL,
    category TEXT DEFAULT 'Uncategorized',
    account_id INTEGER NOT NULL,
    unique_hash TEXT UNIQUE, -- Corrected from unique_id for consistency with python scripts
    FOREIGN KEY (account_id) REFERENCES accounts (id)
);

CREATE TABLE IF NOT EXISTS category_rules (
    merchant_pattern TEXT PRIMARY KEY, -- Made merchant_pattern the primary key directly
    category         TEXT NOT NULL
);

-- New table for storing budget limits --
CREATE TABLE IF NOT EXISTS budgets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT NOT NULL UNIQUE,
    limit_amount REAL NOT NULL DEFAULT 0
);