# database.py — canonical DB layer for The_305_Accountant
# -------------------------------------------------------
# Single source of truth: finance.db
# - transaction_id: external, stable, UNIQUE
# - merchant: canonical merchant name (heuristic or from your corrections)
# - ai_category / ai_subcategory: model or rules suggestions (non-binding)
# - category / subcategory: your final selections (binding)
#
# This module exposes:
#   Connection & schema:
#     - get_db_connection(), initialize_database(), apply_v1_compat_migrations()
#   Accounts:
#     - get_or_create_account(conn, name), list_accounts()
#   Imports / corrections:
#     - add_transactions_df(df, account_name)
#     - import_corrections_from_rows(conn, rows)
#   Rules:
#     - apply_rules_to_ai_fields(conn=None)
#     - apply_category_rules(conn=None, overwrite=False)
#   Queries / updates:
#     - fetch_transactions(...), fetch_summary(...), fetch_category_summary(...)
#     - update_transaction_category_by_txid(txid, new_category, new_subcategory=None)
#   Profile & budgets:
#     - get_user_profile(), set_user_profile(...)
#     - upsert_budget(...), list_budgets(), estimate_budgets_from_history(months=3)
#     - recompute_tracking_for_month(ym), get_budget_status(start_date, end_date)
#     - normalize_amount_signs()
#   Aliases for backward compatibility:
#     - get_all_budgets() -> list_budgets()
#     - update_budget(category, limit_amount) -> upsert_budget(...)
#
# Notes:
#   - We keep both a suggestion-path (ai_*) and a final decision path (category/subcategory).
#   - By default, imports populate ai_* via rules and heuristics; finals are only set via corrections/your edits.
#   - If you want to standardize existing rows later, call apply_category_rules(..., overwrite=True).

from __future__ import annotations

import os
import re
import sqlite3
import hashlib
from typing import List, Dict, Optional, Tuple
from datetime import datetime, date, timedelta
from parser import extract_zelle_to_from, extract_to_from_party

import pandas as pd
from dateutil.relativedelta import relativedelta

# -------------------------------------------------------------------
# Paths / connection
# -------------------------------------------------------------------

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_DIR, "finance.db")


def get_db_connection():
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

# --- Stronger normalization for global fingerprinting ---
_REF_TOKEN_RE   = re.compile(r"(?i)\bref(?:erence)?\s*#?\s*[\w-]+\b")
_MASKED_RE      = re.compile(r"(?i)\bX{2,}\d+\b|\bx{2,}\d+\b")  # XXXXXX4311, xxx1234
_DATE_TAIL_RE   = re.compile(r"(?i)\bon\s+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b.*$")
_ACC_TAIL_RE    = re.compile(r"(?i)\b(?:account|acct|ending|number|no\.)\b.*$")
_FP_MULTI_WS    = re.compile(r"\s{2,}")

def _normalized_event_for_fp(desc: str) -> str:
    """
    Normalize the description so visually-dupe rows collide:
      - Prefer deterministic parsers (Zelle/Transfer) when they fire.
      - Else scrub one-off tokens (REF#, masked acct tails, dates), collapse whitespace,
        and uppercase for stability.
    This is used for ALL transactions, not just transfers.
    """
    s = _as_text(desc)
    if not s:
        return ""

    # Prefer deterministic event forms if available
    try:
        z = extract_zelle_to_from(s)   # e.g., "Zelle To John Doe"
        if z:
            return z.upper()
    except Exception:
        pass
    try:
        t = extract_to_from_party(s)   # e.g., "TRANSFER TO PERSONAL LINE OF CREDIT"
        if t:
            return t.upper()
    except Exception:
        pass

    # Light scrub for all other types
    s = _REF_TOKEN_RE.sub("", s)
    s = _MASKED_RE.sub("", s)
    s = _DATE_TAIL_RE.sub("", s)
    s = _ACC_TAIL_RE.sub("", s)

    # Normalize a bit of wording & whitespace
    s = re.sub(r"(?i)\brecurr?ing\b", "", s)
    s = _FP_MULTI_WS.sub(" ", s).strip(" -:.,\t")
    return s.upper()



# -------------------------------------------------------------------
# Schema / migrations
# -------------------------------------------------------------------

def initialize_database():
    """
    Create tables if they don't exist.
    transaction_id is the canonical unique key for transactions.
    We store both ai_* suggestions and final category/subcategory.
    """
    conn = get_db_connection()
    try:
        conn.executescript(
            """
            PRAGMA foreign_keys=ON;

            CREATE TABLE IF NOT EXISTS accounts (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,  -- internal row id (not used externally)
                transaction_id TEXT NOT NULL UNIQUE,   -- canonical external key

                transaction_date TEXT NOT NULL,
                account_id INTEGER NOT NULL,

                original_description TEXT,
                cleaned_description  TEXT,
                merchant TEXT,                          -- canonical merchant name

                amount REAL NOT NULL,                   -- normalized signs

                ai_category TEXT,                       -- suggestions only
                ai_subcategory TEXT,

                category TEXT,                          -- your final choice
                subcategory TEXT,

                unique_fingerprint TEXT,                -- deterministic fallback when txid missing

                FOREIGN KEY (account_id) REFERENCES accounts(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS category_rules (
                merchant_pattern TEXT PRIMARY KEY,      -- lower(cleaned/merchant) key (<= 64 chars)
                category    TEXT NOT NULL,
                subcategory TEXT,
                merchant_canonical TEXT
            );

            CREATE TABLE IF NOT EXISTS user_profile (
                id INTEGER PRIMARY KEY CHECK (id=1),
                annual_after_tax_income REAL,
                household_size INTEGER DEFAULT 1,
                currency TEXT DEFAULT 'USD',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS budgets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT NOT NULL UNIQUE,
                limit_amount REAL NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS budget_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                budget_id INTEGER NOT NULL,
                month TEXT NOT NULL,
                spent REAL DEFAULT 0,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (budget_id, month),
                FOREIGN KEY (budget_id) REFERENCES budgets(id) ON DELETE CASCADE
            );

            
            CREATE INDEX IF NOT EXISTS ix_txn_date ON transactions(transaction_date);
            """
        )
        conn.commit()
        print("Database schema created/verified successfully (transaction_id is UNIQUE).")
    finally:
        conn.close()


def apply_v1_compat_migrations():
    conn = get_db_connection()
    try:
        def has_col(table: str, col: str) -> bool:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            return any(r["name"] == col for r in rows)

        needed = [
            ("transactions", "merchant", "ALTER TABLE transactions ADD COLUMN merchant TEXT"),
            ("transactions", "ai_category", "ALTER TABLE transactions ADD COLUMN ai_category TEXT"),
            ("transactions", "ai_subcategory", "ALTER TABLE transactions ADD COLUMN ai_subcategory TEXT"),
            ("transactions", "subcategory", "ALTER TABLE transactions ADD COLUMN subcategory TEXT"),
            ("transactions", "unique_fingerprint", "ALTER TABLE transactions ADD COLUMN unique_fingerprint TEXT"),
        ]
        for table, col, ddl in needed:
            if not has_col(table, col):
                conn.execute(ddl)

        # Rules carry canonical merchant (if missing, add it)
        rows = conn.execute("PRAGMA table_info(category_rules)").fetchall()
        if not any(r["name"] == "merchant_canonical" for r in rows):
            conn.execute("ALTER TABLE category_rules ADD COLUMN merchant_canonical TEXT")

        conn.commit()

        # One-time cleanup: drop duplicates by existing fingerprint (keep lowest id)
        conn.execute("""
            DELETE FROM transactions
            WHERE id NOT IN (
              SELECT MIN(id) FROM transactions
              WHERE unique_fingerprint IS NOT NULL AND TRIM(unique_fingerprint) <> ''
              GROUP BY unique_fingerprint
            )
            AND unique_fingerprint IN (
              SELECT unique_fingerprint FROM transactions
              WHERE unique_fingerprint IS NOT NULL AND TRIM(unique_fingerprint) <> ''
              GROUP BY unique_fingerprint HAVING COUNT(*) > 1
            )
        """)
        conn.commit()

        # Enforce uniqueness going forward (raw-only usage; business fields unaffected)
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_fingerprint
            ON transactions(unique_fingerprint)
            WHERE unique_fingerprint IS NOT NULL AND TRIM(unique_fingerprint) <> ''
        """)
        conn.commit()
    finally:
        conn.close()


def ensure_unique_fp_index() -> dict:
    """
    Try to enforce uniqueness on unique_fingerprint for future inserts.
    Will fail if duplicates still exist.
    """
    conn = get_db_connection()
    try:
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_unique_fp ON transactions(unique_fingerprint)")
            conn.commit()
            return {"created_or_exists": True}
        except Exception as e:
            # This can fail if duplicates still exist at index-creation time
            return {"created_or_exists": False, "error": str(e)}
    finally:
        conn.close()


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
# ---- unique_fingerprint index helpers ----
def _drop_unique_fingerprint_index(conn):
    conn.execute("DROP INDEX IF EXISTS ux_transactions_unique_fingerprint")

def _create_unique_fingerprint_index(conn):
    # partial unique index so NULLs are allowed
    conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_transactions_unique_fingerprint
        ON transactions(unique_fingerprint)
        WHERE unique_fingerprint IS NOT NULL
    """)

def _fp_index_name() -> str:
    return "ux_transactions_unique_fp"

def _unique_fp_index_exists(conn: sqlite3.Connection) -> bool:
    rows = conn.execute("PRAGMA index_list(transactions)").fetchall()
    for r in rows:
        nm = r["name"] if isinstance(r, sqlite3.Row) else r[1]
        if nm == _fp_index_name():
            return True
    return False

def _drop_unique_fp_index(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute(f"DROP INDEX IF EXISTS {_fp_index_name()}")
        return True
    except Exception:
        return False

def _create_unique_fp_index(conn: sqlite3.Connection) -> bool:
    try:
        conn.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {_fp_index_name()} "
            f"ON transactions(unique_fingerprint)"
        )
        return True
    except Exception:
        return False


def _to_iso_date(s) -> Optional[str]:
    """Best-effort convert to YYYY-MM-DD."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        d = pd.to_datetime(s, errors="coerce")
        return None if pd.isna(d) else d.strftime("%Y-%m-%d")
    except Exception:
        return None


def _fingerprint(account_id: int, date_s: str, desc: str, amount: float) -> str:
    """
    Global fingerprint: account|ISO date|amount|normalized_event.
    IMPORTANT: pass a COMBINED description (original + cleaned) so reimports
    with slightly different text still collide.
    """
    iso = _to_iso_date(date_s) or (str(date_s) if date_s else "")
    event = _normalized_event_for_fp(desc)
    basis = f"{account_id}|{iso}|{amount:.2f}|{event}"
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:24]


# Normalize signs: credits positive by keywords; otherwise assume expenses negative.
CREDIT_KEYWORDS = (
    "payment", "thank you", "refund", "reversal", "credit", "deposit",
    "interest", "cashback", "direct deposit", "transfer in"
)

GENERIC_P2P_MERCHANTS = {
    "", "unknown", "zelle", "venmo", "cash app", "cashapp", "square cash",
    "paypal", "apple cash", "google pay", "gpay", "google wallet"
}

def _as_text(v) -> str:
    """
    Coerce any CSV cell into a clean string we can safely .strip().
    - tuples/lists -> space-joined string
    - None/NaN     -> ""
    - everything else -> str(v)
    """
    if v is None:
        return ""
    # pandas NA guard
    try:
        import pandas as _pd
        if _pd.isna(v):
            return ""
    except Exception:
        pass
    if isinstance(v, (list, tuple)):
        return " ".join(str(x) for x in v if x is not None).strip()
    return str(v).strip()

def _apply_signs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize signs: credits positive by keywords; otherwise assume expenses negative.
    NOTE: Use BOTH cleaned_description and original_description so 'direct deposit'
    in the raw text is always detected even if cleaned text lost the keyword.
    """
    df = df.copy()

    # Build a combined description field (cleaned + original) for robust keyword matching
    desc_clean = df.get("cleaned_description")
    desc_orig  = df.get("original_description")
    if desc_clean is None:
        desc_clean = pd.Series([""] * len(df), index=df.index)
    else:
        desc_clean = desc_clean.astype(str)
    if desc_orig is None:
        desc_orig = pd.Series([""] * len(df), index=df.index)
    else:
        desc_orig = desc_orig.astype(str)

    desc = (desc_clean.fillna("") + " " + desc_orig.fillna("")).str.lower()

    amt = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)

    # Include a few extra safe credit indicators on top of CREDIT_KEYWORDS
    extra_credit = {"payroll", "ach credit", "zelle from", "incoming"}
    credit_terms = set(CREDIT_KEYWORDS) | extra_credit

    credit_mask = pd.Series(False, index=df.index)
    for kw in credit_terms:
        if kw:
            credit_mask = credit_mask | desc.str.contains(re.escape(kw))

    # Force credited items positive
    amt = amt.where(~credit_mask, amt.abs())

    # For the rest, many banks export expenses as positive; flip those to negative
    non_credit = amt[~credit_mask]
    if len(non_credit) and (non_credit > 0).mean() >= 0.5:
        amt = amt.where(credit_mask, -amt.abs())

    df["amount"] = amt.astype(float)
    return df

def _caps(s: Optional[str]) -> Optional[str]:
    """Uppercase helper that keeps None as None."""
    if s is None:
        return None
    s = str(s).strip()
    return s.upper() if s else None


def normalize_all_transaction_dates(conn: sqlite3.Connection | None = None) -> dict:
    own = False
    if conn is None:
        conn = get_db_connection()
        own = True
    try:
        rows = conn.execute("SELECT id, transaction_date FROM transactions").fetchall()
        changed = bad = 0
        for r in rows:
            raw = r["transaction_date"]
            iso = _to_iso_date(raw)
            if not iso:
                bad += 1
                continue
            if iso != raw:
                conn.execute("UPDATE transactions SET transaction_date=? WHERE id=?", (iso, r["id"]))
                changed += 1
        conn.commit()
        return {"changed": changed, "bad": bad, "total": len(rows)}
    finally:
        if own:
            conn.close()


# Merchant heuristics
_CANON = {
    "amzn mktp us": "Amazon", "amazon.com": "Amazon", "amazon": "Amazon",
    "uber trip": "Uber", "uber": "Uber", "lyft": "Lyft",
    "dd doordash": "DoorDash", "doordash": "DoorDash",
    "wholefd market": "Whole Foods Market", "whole foods": "Whole Foods Market",
    "trader joes": "Trader Joe's", "trader joe": "Trader Joe's",
    "walmart": "Walmart", "target": "Target", "costco": "Costco",
    "home depot": "Home Depot", "lowes": "Lowe's",
    "square *": "Square", "sq *": "Square", "stripe": "Stripe",
    "zelle": "Zelle", "venmo": "Venmo", "cash app": "Cash App",
    "airbnb": "Airbnb", "booking.com": "Booking.com", "marriott": "Marriott", "hilton": "Hilton",
}
_RE_MULTI_WS = re.compile(r"\s+")
_RE_PUNCT = re.compile(r"[^\w\s&'\.-]+")
_RE_DIGITS = re.compile(r"\b\d{2,}\b")
_RE_STATES = re.compile(r"\b(AL|AK|AS|AZ|AR|CA|CO|CT|DC|DE|FL|GA|GU|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MP|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|PR|RI|SC|SD|TN|TX|UM|UT|VA|VI|VT|WA|WI|WV|WY)\b", re.I)

def _merchant_from_desc(text: str) -> str:
    s = (text or "").strip().lower()
    if not s:
        return ""
    s = _RE_PUNCT.sub(" ", s)
    s = _RE_DIGITS.sub(" ", s)
    s = _RE_STATES.sub(" ", s)
    s = _RE_MULTI_WS.sub(" ", s).strip()
    for key, canon in _CANON.items():
        if key in s:
            return canon
    tokens = s.split()
    if len(tokens) > 5:
        s = " ".join(tokens[:5])
    return s.title()

def rebuild_fingerprints_and_dedupe(dry_run: bool = False) -> dict:
    """
    Recompute per-row unique_fingerprint for all transactions and delete duplicates.
    Strategy:
      1) Compute new fingerprints for every row from (account_id, date, amount, text).
      2) Group by the NEW fingerprint and pick a single "winner" per group.
         - Prefer a row that already has that fingerprint (old_fp == new_fp) to minimize writes.
         - Otherwise fall back to the smallest id.
      3) In a single transaction (if not dry_run):
           a) DELETE all "loser" rows first (so no collisions remain).
           b) For winners whose fingerprint will change, STAGE them to a unique placeholder value
              (e.g., "__stage__<id>__") so the UNIQUE index never sees interim collisions.
           c) Set the winners' final fingerprints to their new values.
    Returns summary stats and whether a unique index is currently present.
    """
    import sqlite3

    conn = get_db_connection()
    conn.row_factory = sqlite3.Row

    # --- helpers -------------------------------------------------------------
    def _index_on_unique_fingerprint_exists(c: sqlite3.Connection) -> bool:
        # Detect any UNIQUE index (auto or named) that covers 'unique_fingerprint'
        try:
            idx_rows = c.execute("PRAGMA index_list('transactions')").fetchall()
            for idx in idx_rows:
                # columns: seq, name, unique, origin, partial
                if not idx["unique"]:
                    continue
                name = idx["name"]
                cols = c.execute(f"PRAGMA index_info('{name}')").fetchall()
                if any(col["name"] == "unique_fingerprint" for col in cols):
                    return True
        except Exception:
            pass
        return False

    def _pick_column(existing_cols: set, candidates: list[str], required: bool = True) -> str | None:
        for c in candidates:
            if c in existing_cols:
                return c
        if required:
            raise RuntimeError(f"Required column not found. Tried: {candidates}")
        return None

    # Discover available columns so we can be schema-tolerant for 'orig'/'clean'
    cols = {r["name"] for r in conn.execute("PRAGMA table_info('transactions')")}

    id_col = _pick_column(cols, ["id"])
    acct_col = _pick_column(cols, ["account_id", "acct_id", "account"])
    date_col = _pick_column(cols, ["transaction_date", "date", "posted_date", "dt"])
    amt_col = _pick_column(cols, ["amount", "amt", "value"])
    fp_col = _pick_column(cols, ["unique_fingerprint", "fingerprint", "global_fingerprint"])  # 'unique_fingerprint' should exist

    # Best-effort options for original/cleaned text columns.
    # If none exist, we SELECT '' so the fingerprint still works.
    orig_candidate = _pick_column(
        cols,
        ["original_description", "orig_description", "raw_description", "bank_memo", "memo", "description_raw"],
        required=False,
    )
    clean_candidate = _pick_column(
        cols,
        ["clean_description", "normalized_description", "description", "desc_clean", "payee_clean"],
        required=False,
    )

    select_text_bits = []
    if orig_candidate:
        select_text_bits.append(f"{orig_candidate} AS orig")
    else:
        select_text_bits.append("'' AS orig")
    if clean_candidate:
        select_text_bits.append(f"{clean_candidate} AS clean")
    else:
        select_text_bits.append("'' AS clean")

    sql = f"""
        SELECT
            {id_col} AS id,
            {acct_col} AS account_id,
            {date_col} AS transaction_date,
            {amt_col} AS amount,
            {fp_col}  AS old_fp,
            {", ".join(select_text_bits)}
        FROM transactions
    """

    rows = conn.execute(sql).fetchall()
    rows_scanned = len(rows)
    index_present = _index_on_unique_fingerprint_exists(conn)

    # Build quick lookup maps
    old_fp_by_id: dict[int, str] = {int(r["id"]): (r["old_fp"] or "") for r in rows}

    # Compute new fingerprints
    computed: list[tuple[int, str]] = []
    for r in rows:
        rid = int(r["id"])
        combined = f"{(r['orig'] or '').strip()} {(r['clean'] or '').strip()}".strip()
        # IMPORTANT: use the same _fingerprint function your codebase already uses
        # signature assumed: _fingerprint(account_id, transaction_date, text, amount) -> str
        new_fp = _fingerprint(r["account_id"], r["transaction_date"], combined, float(r["amount"]))
        computed.append((rid, new_fp))

    # Group by NEW fingerprint
    by_fp: dict[str, list[int]] = {}
    for rid, new_fp in computed:
        by_fp.setdefault(new_fp, []).append(rid)

    # Decide winners & losers per new fingerprint
    winners: dict[str, int] = {}
    losers: list[int] = []
    for new_fp, id_list in by_fp.items():
        # Prefer a row that already has this fp (minimize writes)
        already = [rid for rid in id_list if old_fp_by_id.get(rid, "") == new_fp]
        if already:
            # If multiple already-have rows exist, pick the smallest id as the canonical keeper
            winner = min(already)
        else:
            winner = min(id_list)
        winners[new_fp] = winner
        losers.extend([rid for rid in id_list if rid != winner])

    rows_to_delete = losers[:]  # losers are the ones to physically delete
    rows_with_dupe_fp_groups = len(rows_to_delete)

    # Determine which winners require an UPDATE (old_fp != new_fp)
    new_fp_by_id = {rid: fp for (rid, fp) in computed}
    winners_to_change: list[tuple[int, str]] = []
    for new_fp, winner_id in winners.items():
        if old_fp_by_id.get(winner_id, "") != new_fp:
            winners_to_change.append((winner_id, new_fp))

    # Summary when dry-run
    if dry_run:
        return {
            "dry_run": True,
            "impl": "rebuild_fingerprints_v4",
            "index_present": index_present,
            "rows_deleted": 0,
            "rows_scanned": rows_scanned,
            "rows_to_delete": len(rows_to_delete),
            "rows_with_dupe_fp_groups": rows_with_dupe_fp_groups,
        }

    # --- WRITE phase (delete-first + placeholder stage) ----------------------
    rows_deleted = 0
    try:
        conn.execute("BEGIN")

        # 1) Delete all losers first (eliminates any immediate UNIQUE collisions)
        if rows_to_delete:
            conn.executemany(
                "DELETE FROM transactions WHERE id=?",
                [(rid,) for rid in rows_to_delete],
            )
            rows_deleted = len(rows_to_delete)

        # 2) Stage winners that will change to unique placeholder values
        if winners_to_change:
            conn.executemany(
                "UPDATE transactions SET unique_fingerprint=? WHERE id=?",
                [(f"__stage__{rid}__", rid) for (rid, _new_fp) in winners_to_change],
            )

        # 3) Set final fingerprints on winners
        if winners_to_change:
            conn.executemany(
                "UPDATE transactions SET unique_fingerprint=? WHERE id=?",
                [(new_fp, rid) for (rid, new_fp) in winners_to_change],
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        # Re-check whether the unique index is present (for completeness)
        index_present = _index_on_unique_fingerprint_exists(conn)

    return {
        "dry_run": False,
        "impl": "rebuild_fingerprints_v4",
        "index_present": index_present,
        "rows_deleted": rows_deleted,
        "rows_scanned": rows_scanned,
        "rows_to_delete": len(rows_to_delete),
        "rows_with_dupe_fp_groups": rows_with_dupe_fp_groups,
    }




# -------------------------------------------------------------------
# Accounts
# -------------------------------------------------------------------

def get_or_create_account(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT id FROM accounts WHERE name=?", (name,)).fetchone()
    if row:
        return int(row["id"])
    cur = conn.execute("INSERT INTO accounts(name) VALUES(?)", (name,))
    conn.commit()
    return int(cur.lastrowid)


def list_accounts() -> List[Dict]:
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT id, name FROM accounts ORDER BY name").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


# -------------------------------------------------------------------
# Rules application
# -------------------------------------------------------------------

def apply_rules_to_ai_fields(conn: Optional[sqlite3.Connection] = None):
    own = False
    if conn is None:
        conn = get_db_connection()
        own = True
    try:
        rules = conn.execute(
            "SELECT merchant_pattern, category, COALESCE(subcategory,'') AS subcategory, merchant_canonical "
            "FROM category_rules"
        ).fetchall()

        for r in rules:
            pat = f"%{(r['merchant_pattern'] or '').lower().strip()}%"
                        # if rule has a canonical merchant, fill it only if current merchant is null/empty
            if r["subcategory"]:
                conn.execute(
                    "UPDATE transactions "
                    "SET ai_category=?, ai_subcategory=?, "
                    "    merchant = CASE WHEN (merchant IS NULL OR TRIM(merchant)='') AND ? IS NOT NULL THEN UPPER(?) ELSE merchant END "
                    "WHERE (ai_category IS NULL OR ai_category='') "
                    "  AND lower(COALESCE(merchant, cleaned_description)) LIKE ?",
                    (r["category"], r["subcategory"], r["merchant_canonical"], r["merchant_canonical"], pat)
                )
            else:
                conn.execute(
                    "UPDATE transactions "
                    "SET ai_category=?, "
                    "    merchant = CASE WHEN (merchant IS NULL OR TRIM(merchant)='') AND ? IS NOT NULL THEN UPPER(?) ELSE merchant END "
                    "WHERE (ai_category IS NULL OR ai_category='') "
                    "  AND lower(COALESCE(merchant, cleaned_description)) LIKE ?",
                    (r["category"], r["merchant_canonical"], r["merchant_canonical"], pat)
                )
        conn.commit()
    finally:
        if own:
            conn.close()


def apply_category_rules(conn: Optional[sqlite3.Connection] = None, overwrite: bool = False):
    """
    Final rule application:
      - overwrite=False: fill category/subcategory/merchant only where empty or 'Uncategorized'
      - overwrite=True: standardize ALL matching rows to the rule (careful!)
    """
    own = False
    if conn is None:
        conn = get_db_connection()
        own = True
    try:
        # always ensure ai_* are filled as well
        apply_rules_to_ai_fields(conn)

        rules = conn.execute(
            "SELECT merchant_pattern, category, COALESCE(subcategory,'') AS subcategory, merchant_canonical "
            "FROM category_rules"
        ).fetchall()

        for r in rules:
            pat = f"%{(r['merchant_pattern'] or '').lower().strip()}%"
            cat, sub, mcanon = r["category"], (r["subcategory"] or None), (r["merchant_canonical"] or None)

            if overwrite:
                if sub is not None:
                    conn.execute(
                        "UPDATE transactions "
                        "SET category=?, subcategory=?, merchant=COALESCE(UPPER(?), merchant) "
                        "WHERE lower(COALESCE(merchant, cleaned_description)) LIKE ?",
                        (cat, sub, mcanon, pat)
                    )
                else:
                    conn.execute(
                        "UPDATE transactions "
                        "SET category=?, merchant=COALESCE(UPPER(?), merchant) "
                        "WHERE lower(COALESCE(merchant, cleaned_description)) LIKE ?",
                        (cat, mcanon, pat)
                    )

        else:
            if sub is not None:
                conn.execute(
                    "UPDATE transactions "
                    "SET category=CASE WHEN (category IS NULL OR category='' OR category='Uncategorized') THEN ? ELSE category END, "
                    "    subcategory=CASE WHEN (subcategory IS NULL OR subcategory='') THEN ? ELSE subcategory END, "
                    "    merchant=CASE WHEN (merchant IS NULL OR TRIM(merchant)='') AND ? IS NOT NULL THEN UPPER(?) ELSE merchant END "
                    "WHERE lower(COALESCE(merchant, cleaned_description)) LIKE ?",
                    (cat, sub, mcanon, mcanon, pat)
                )
            else:
                conn.execute(
                    "UPDATE transactions "
                    "SET category=CASE WHEN (category IS NULL OR category='' OR category='Uncategorized') THEN ? ELSE category END, "
                    "    merchant=CASE WHEN (merchant IS NULL OR TRIM(merchant)='') AND ? IS NOT NULL THEN UPPER(?) ELSE merchant END "
                    "WHERE lower(COALESCE(merchant, cleaned_description)) LIKE ?",
                    (cat, mcanon, mcanon, pat)
                )
        conn.commit()
    finally:
        if own:
            conn.close()

# --- Auto-fill subcategory from rules given merchant+category ---
def autofill_subcategory_for_tx(transaction_id: str, category: Optional[str] = None) -> Optional[str]:
    """
    If we know the merchant (or cleaned_description) and the final category,
    set subcategory automatically based on a learned rule:
      rule: (merchant_pattern, category) -> subcategory
    Returns the subcategory that was applied (or None if no match).
    """
    conn = get_db_connection()
    try:
        tx = conn.execute(
            "SELECT merchant, cleaned_description, category, ai_category FROM transactions WHERE transaction_id=?",
            (str(transaction_id),)
        ).fetchone()
        if not tx:
            return None

        chosen_category = (category or tx["category"] or tx["ai_category"] or "").strip()
        if not chosen_category:
            return None

        merchant_text = (tx["merchant"] or tx["cleaned_description"] or "").strip().lower()
        if not merchant_text:
            return None

        # Strict match: same merchant pattern AND same category; take the strongest (longest) pattern first
        rule = conn.execute(
            """
            SELECT category, subcategory, merchant_canonical
            FROM category_rules
            WHERE subcategory IS NOT NULL
              AND ? LIKE '%' || merchant_pattern || '%'
              AND category = ?
            ORDER BY LENGTH(merchant_pattern) DESC
            LIMIT 1
            """,
            (merchant_text, chosen_category)
        ).fetchone()

        if not rule:
            return None

        sub = (rule["subcategory"] or "").strip()
        if not sub:
            return None

        # Apply subcategory; also backfill merchant if empty
        conn.execute(
            """
            UPDATE transactions
            SET subcategory = ?,
                merchant = CASE
                                WHEN (merchant IS NULL OR TRIM(merchant)='') AND ? IS NOT NULL
                                THEN UPPER(?)
                                ELSE merchant
                            END
            WHERE transaction_id = ?
            """,
            (sub, rule["merchant_canonical"], rule["merchant_canonical"], str(transaction_id))
        )
        conn.commit()
        return sub
    finally:
        conn.close()



# -------------------------------------------------------------------
# Import / corrections
# -------------------------------------------------------------------

def add_transactions_df(df: pd.DataFrame, account_name: str) -> Tuple[int, int]:
    """
    Insert a DataFrame as transactions. Dedupe by unique_fingerprint.
    - We keep OUR own transaction_id (numeric fallback), never the bank's.
    - We use RAW text ONLY to compute the fingerprint for dedupe.
    - Cleaned/merchant in the DB remain the business-facing fields.
    """
    if not isinstance(df, pd.DataFrame) or df.empty:
        return (0, 0)

    # Normalize required columns
    if "transaction_date" not in df.columns and "date" in df.columns:
        df = df.rename(columns={"date": "transaction_date"})
    if "cleaned_description" not in df.columns:
        base = df.get("original_description") if "original_description" in df.columns else df.get("description")
        df["cleaned_description"] = base if base is not None else ""

    # Normalize signs; use both cleaned and original for keyword detection
    df = _apply_signs(df)

    conn = get_db_connection()
    try:
        account_id = get_or_create_account(conn, account_name)

        # Current max numeric transaction_id (we own this space)
        row = conn.execute(
            """
            SELECT COALESCE(MAX(CAST(transaction_id AS INTEGER)), 0) AS max_id
            FROM transactions
            WHERE TRIM(COALESCE(transaction_id,'')) != ''
              AND transaction_id GLOB '[0-9]*'
            """
        ).fetchone()
        next_txid = int(row["max_id"] or 0) + 1

        added = skipped = 0

        sql = (
            "INSERT OR IGNORE INTO transactions "
            "(transaction_id, transaction_date, account_id, "
            " original_description, cleaned_description, merchant, amount, "
            " ai_category, ai_subcategory, category, subcategory, unique_fingerprint) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, ?)"
        )

        for r in df.to_dict(orient="records"):
            tdate = _to_iso_date(r.get("transaction_date"))
            if not tdate:
                skipped += 1
                continue

            try:
                amt = float(r.get("amount"))
            except Exception:
                skipped += 1
                continue

            # RAW (import-only / dedupe-only) and CLEAN (business-facing) text
            orig  = _as_text(r.get("original_description") or r.get("description"))
            clean = _as_text(r.get("cleaned_description") or orig)

            # OUR transaction id: prefer provided numeric-ish id; otherwise allocate next numeric id
            txid = _as_text(
                r.get("transaction_id")
                or r.get("Transaction ID")
                or r.get("ID")
                or r.get("id")
            )
            if not txid:
                txid = str(next_txid)
                next_txid += 1

            # Suggestions (optional)
            ai_cat = _as_text(r.get("ai_category") or r.get("category")) or None
            ai_sub = _as_text(r.get("ai_subcategory") or r.get("ai_sub")) or None

            # Merchant: keep your existing logic (Zelle/transfer extract, else heuristic)
            desc_for_extract = f"{orig} {clean}".strip()
            zelle_pick = extract_zelle_to_from(desc_for_extract)
            xfer_pick  = extract_to_from_party(desc_for_extract)

            mer = (r.get("merchant") or "").strip()
            if not mer:
                mer = zelle_pick or xfer_pick or _merchant_from_desc(clean) or ""

            # Never persist literal 'Unknown'
            if mer:
                s = mer.strip().strip('"').strip("'")
                mer = None if (not s or s.lower() == "unknown") else s
            else:
                mer = None

            # Store business-facing text in uppercase (keeps UI consistent)
            clean_to_store = (clean or "").upper()
            mer_to_store   = _caps(mer)

            # DEDUPE KEY: use the RAWEST text we have (orig preferred) + account + date + amount
            desc_for_fp = orig or clean
            desc_for_extract = f"{orig} {clean}".strip()
            fp = _fingerprint(account_id, tdate, desc_for_extract, amt)


            cur = conn.execute(
                sql,
                (txid, tdate, account_id, orig, clean_to_store, mer_to_store, amt, ai_cat, ai_sub, fp),
            )
            if cur.rowcount == 1:
                added += 1
            else:
                # Duplicate (same fingerprint) — ignore silently
                skipped += 1

        # Fill ai_* suggestions via rules for any remaining blanks
        apply_rules_to_ai_fields(conn)
        conn.commit()
        return added, skipped
    finally:
        conn.close()



def import_corrections_from_rows(conn: sqlite3.Connection, rows: List[Dict]) -> Tuple[int, int]:
    """
    Apply your spreadsheet corrections. Expected columns (case-insensitive):
      - transaction_id (required)
      - new_category (final category)
      - Sub_category / sub_category / sub category (final subcategory)
      - new_description (merchant canonical name)
      - (optionally) description/original_description to form a new rule key if you prefer that
    Writes finals into transactions and upserts a category_rule using new_description as key.
    """
    updated = sk = 0
    for r in rows:
        txid = r.get("transaction_id") or r.get("Transaction ID") or r.get("ID") or r.get("id")
        if txid in (None, ""):
            sk += 1
            continue
        txid = str(txid).strip()

        new_cat = r.get("new_category")
        # Accept a few common header variants for subcategory
        new_sub = (
            r.get("Sub_category")
            or r.get("sub_category")
            or r.get("sub category")
            or r.get("Sub Category")
            or r.get("Sub-Category")
        )
        new_mer = r.get("new_description")

        # 1) Learn/refresh a rule if we have merchant + category
        if new_mer and new_cat:
            conn.execute(
                "INSERT OR REPLACE INTO category_rules(merchant_pattern, category, subcategory, merchant_canonical) "
                "VALUES (?,?,?,?)",
                (str(new_mer).lower().strip()[:64], str(new_cat).strip(), (str(new_sub).strip() or None), str(new_mer).strip()),
            )

        # 2) Update finals on the row
        sets, args = [], []
        if new_cat:
            sets.append("category=?")
            args.append(str(new_cat).strip())
        if new_sub:
            sets.append("subcategory=?")
            args.append(str(new_sub).strip())
        if new_mer:
            sets.append("merchant=?")
            args.append(str(new_mer).strip())

        if not sets:
            sk += 1
            continue

        args.append(txid)
        cur = conn.execute(f"UPDATE transactions SET {', '.join(sets)} WHERE transaction_id=?", args)
        updated += int(cur.rowcount == 1)

    conn.commit()
    return updated, sk


# -------------------------------------------------------------------
# Queries / updates
# -------------------------------------------------------------------

def fetch_transactions(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    account_id: Optional[int] = None,
) -> List[Dict]:
    conn = get_db_connection()
    try:
        q = (
            "SELECT t.*, a.name AS account_name "
            "FROM transactions t JOIN accounts a ON a.id = t.account_id WHERE 1=1"
        )
        args: List = []
        if start_date:
            q += " AND t.transaction_date >= ?"
            args.append(start_date)
        if end_date:
            q += " AND t.transaction_date <= ?"
            args.append(end_date)
        if account_id:
            q += " AND t.account_id = ?"
            args.append(account_id)
        q += " ORDER BY t.transaction_date DESC, t.id DESC"
        return [dict(r) for r in conn.execute(q, args).fetchall()]
    finally:
        conn.close()


def fetch_summary(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
    """
    Top-line P&L:
      - income  = ONLY rows tagged category='Income' (true income)
      - expenses = sum of negatives (all expense categories)
      - balance = income + expenses
    """
    conn = get_db_connection()
    try:
        base = "FROM transactions WHERE 1=1"
        args: List = []
        if start_date:
            base += " AND transaction_date >= ?"
            args.append(start_date)
        if end_date:
            base += " AND transaction_date <= ?"
            args.append(end_date)

        # True income: only the 'Income' category (credits/other positives excluded)
        income = conn.execute(
            f"SELECT COALESCE(SUM(amount),0) {base} AND category = 'Income'"
            , args
        ).fetchone()[0] or 0.0

        # Expenses remain all negatives
        expenses = conn.execute(
            f"SELECT COALESCE(SUM(amount),0) {base} AND amount < 0"
            , args
        ).fetchone()[0] or 0.0

        return {"income": float(income), "expenses": float(expenses), "balance": float(income + expenses)}
    finally:
        conn.close()


def fetch_category_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    account_id: Optional[int] = None,
) -> List[Dict]:
    conn = get_db_connection()
    try:
        q = (
            "SELECT COALESCE(category,'Uncategorized') AS category, "
            "       COALESCE(subcategory,'') AS subcategory, "
            "       SUM(amount) AS total "
            "FROM transactions WHERE 1=1"
        )
        args: List = []
        if start_date:
            q += " AND transaction_date >= ?"
            args.append(start_date)
        if end_date:
            q += " AND transaction_date <= ?"
            args.append(end_date)
        if account_id:
            q += " AND account_id = ?"
            args.append(account_id)
        q += " GROUP BY category, subcategory ORDER BY ABS(SUM(amount)) DESC"
        return [dict(r) for r in conn.execute(q, args).fetchall()]
    finally:
        conn.close()


def update_transaction_category_by_txid(
    txid: str,
    new_category: str,
    new_subcategory: Optional[str] = None,
):
    """Update final category/subcategory for a specific external transaction_id."""
    conn = get_db_connection()
    try:
        if new_subcategory is None:
            conn.execute("UPDATE transactions SET category=? WHERE transaction_id=?", (new_category, txid))
        else:
            conn.execute(
                "UPDATE transactions SET category=?, subcategory=? WHERE transaction_id=?",
                (new_category, new_subcategory, txid),
            )
        conn.commit()
    finally:
        conn.close()


# -------------------------------------------------------------------
# Profile & budgets
# -------------------------------------------------------------------

def get_user_profile() -> Dict:
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT * FROM user_profile WHERE id=1").fetchone()
        return dict(row) if row else {"annual_after_tax_income": None, "household_size": 1, "currency": "USD"}
    finally:
        conn.close()


def set_user_profile(annual_after_tax_income: Optional[float], household_size: int = 1, currency: str = "USD"):
    conn = get_db_connection()
    try:
        conn.execute(
            """
            INSERT INTO user_profile(id, annual_after_tax_income, household_size, currency)
            VALUES (1,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
              annual_after_tax_income=excluded.annual_after_tax_income,
              household_size=excluded.household_size,
              currency=excluded.currency,
              updated_at=CURRENT_TIMESTAMP
            """,
            (annual_after_tax_income, household_size, currency),
        )
        conn.commit()
    finally:
        conn.close()


def upsert_budget(category: str, limit_amount: float):
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT INTO budgets(category, limit_amount) VALUES (?, ?) "
            "ON CONFLICT(category) DO UPDATE SET limit_amount=excluded.limit_amount",
            (category, float(limit_amount)),
        )
        conn.commit()
    finally:
        conn.close()


def list_budgets() -> List[Dict]:
    conn = get_db_connection()
    try:
        rows = conn.execute("SELECT id, category, limit_amount FROM budgets ORDER BY category").fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def estimate_budgets_from_history(months: int = 3) -> Dict[str, float]:
    """
    Average monthly spend by category across the last N months.
    """
    conn = get_db_connection()
    try:
        start = (date.today().replace(day=1) - relativedelta(months=months - 1)).strftime("%Y-%m-01")
        rows = conn.execute(
            """
            SELECT category, AVG(abs(month_sum)) AS avg_spend
            FROM (
                SELECT category,
                       strftime('%Y-%m', transaction_date) AS ym,
                       SUM(CASE WHEN amount<0 THEN amount ELSE 0 END) AS month_sum
                FROM transactions
                WHERE transaction_date >= ?
                GROUP BY category, ym
            )
            GROUP BY category
            """,
            (start,),
        ).fetchall()
        return {r["category"]: float(r["avg_spend"] or 0.0) for r in rows if r["category"]}
    finally:
        conn.close()


def _month_bounds(ym: str) -> Tuple[str, str]:
    start = f"{ym}-01"
    dt = datetime.strptime(start, "%Y-%m-%d").date()
    end = ((dt.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)).strftime("%Y-%m-%d")
    return start, end


def recompute_tracking_for_month(ym: str):
    conn = get_db_connection()
    try:
        start, end = _month_bounds(ym)
        budgets = conn.execute("SELECT id, category FROM budgets").fetchall()
        for b in budgets:
            spent = abs(
                conn.execute(
                    "SELECT COALESCE(SUM(amount),0) "
                    "FROM transactions WHERE category=? AND amount<0 AND transaction_date BETWEEN ? AND ?",
                    (b["category"], start, end),
                ).fetchone()[0]
                or 0.0
            )
            conn.execute(
                "INSERT INTO budget_tracking(budget_id, month, spent, updated_at) "
                "VALUES (?,?,?,CURRENT_TIMESTAMP) "
                "ON CONFLICT(budget_id, month) DO UPDATE SET spent=excluded.spent, updated_at=CURRENT_TIMESTAMP",
                (b["id"], ym, spent),
            )
        conn.commit()
    finally:
        conn.close()


def get_budget_status(start_date: str, end_date: str) -> List[Dict]:
    """
    Scale monthly budget limits across the selected range and compare with actual spend (amount<0).
    """
    conn = get_db_connection()
    try:
        s = datetime.strptime(start_date, "%Y-%m-%d").date()
        e = datetime.strptime(end_date, "%Y-%m-%d").date()
        months = (e.year - s.year) * 12 + (e.month - s.month) + 1
        months = max(months, 1)

        budgets = conn.execute("SELECT category, limit_amount FROM budgets").fetchall()
        out = []
        for b in budgets:
            period_limit = float(b["limit_amount"]) * months
            spent = abs(
                conn.execute(
                    "SELECT COALESCE(SUM(amount),0) FROM transactions "
                    "WHERE category=? AND amount<0 AND transaction_date BETWEEN ? AND ?",
                    (b["category"], start_date, end_date),
                ).fetchone()[0]
                or 0.0
            )
            out.append(
                {
                    "category": b["category"],
                    "limit_amount": round(period_limit, 2),
                    "spent": round(spent, 2),
                    "remaining": round(period_limit - spent, 2),
                }
            )
        return out
    finally:
        conn.close()


def normalize_amount_signs():
    """
    Normalize special cases where some credits might have been imported as negatives.
    Leaves user categories untouched; this is a hygiene step.
    """
    conn = get_db_connection()
    try:
        conn.execute(
            "UPDATE transactions SET amount = ABS(amount) "
            "WHERE amount < 0 AND lower(COALESCE(category,'')) IN ('income','card payment','transfer')"
        )
        conn.commit()
    finally:
        conn.close()

def uppercase_existing_transactions(conn=None):
    """
    One-time maintenance: make all existing cleaned_description and merchant ALL CAPS.
    Safe to re-run; it doesn't touch original_description.
    """
    own = False
    if conn is None:
        conn = get_db_connection()
        own = True
    try:
        conn.execute("""
          UPDATE transactions
             SET cleaned_description = UPPER(COALESCE(cleaned_description,'')),
                 merchant = CASE WHEN merchant IS NOT NULL THEN UPPER(merchant) ELSE merchant END
        """)
        conn.commit()
    finally:
        if own:
            conn.close()


# -------------------------------------------------------------------
# Back-compat aliases (do not remove)
# -------------------------------------------------------------------

def get_all_budgets() -> List[Dict]:
    """Alias for older app code."""
    return list_budgets()


def update_budget(category: str, limit_amount: float):
    """Alias for older app code."""
    return upsert_budget(category, limit_amount)
