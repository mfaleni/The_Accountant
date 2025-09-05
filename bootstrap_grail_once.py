#!/usr/bin/env python3
"""
bootstrap_grail_once.py  —  DESTRUCTIVE fresh start from The Wholy Grail.csv

This script is for development resets only. It will:

  1) DELETE database files:
        - finance.db
        - finance.db-wal
        - finance.db-shm
     (optionally, ALL *.db files in the project if --delete-all is set)

  2) Recreate schema using database.initialize_database()

  3) Import 'The Wholy Grail.csv' as the source of truth:
        - Preserves transaction_id (UNIQUE)
        - Sets FINAL category/subcategory/merchant
        - Mirrors original 'category' into ai_category (if present)
        - (NEW) Prefers deterministic P2P parsing from raw text for Zelle/Venmo/etc.
               so merchants look like "Zelle To Jane Doe" / "Zelle From John Roe".

  4) Seed/refresh category_rules from the Grail

  5) (Optional) Learn rules from imported history (most-frequent mapping)

Usage:
  python bootstrap_grail_once.py
  python bootstrap_grail_once.py --csv "/path/to/The Wholy Grail.csv"
  python bootstrap_grail_once.py --delete-all
  python bootstrap_grail_once.py --min-count 2
  python bootstrap_grail_once.py --no-history-rules
  python bootstrap_grail_once.py --self-destruct
"""

import argparse
import csv
import glob
import os
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from typing import Dict, List, Tuple, Optional
import json  # (new)
try:
    import requests  # (new) used to call your Flask endpoint
except Exception:
    requests = None


from database import (
    initialize_database,
    get_db_connection,
    add_transactions_df,          # not used here, but kept for back-compat import side-effects
    apply_v1_compat_migrations,
)

# ---------- Project paths ----------
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_MAIN     = os.path.join(PROJECT_DIR, "finance.db")
DEFAULT_CSV = os.path.join(PROJECT_DIR, "The Wholy Grail.csv")

# ---------- DB file deletion (destructive) ----------
def _rm(path: str):
    try:
        os.remove(path)
        print(f"Deleted: {path}")
    except FileNotFoundError:
        pass
    except PermissionError as e:
        print(f"PermissionError deleting {path}: {e}", file=sys.stderr)
    except OSError as e:
        print(f"OSError deleting {path}: {e}", file=sys.stderr)

def delete_db_files(delete_all: bool = False):
    """
    Remove primary DB files. If delete_all=True, remove every *.db (and their -wal/-shm) in the project.
    """
    targets = [
        DB_MAIN,
        DB_MAIN + "-wal",
        DB_MAIN + "-shm",
    ]
    if delete_all:
        for path in glob.glob(os.path.join(PROJECT_DIR, "*.db")):
            targets.append(path)
            targets.append(path + "-wal")
            targets.append(path + "-shm")

    # De-dup while preserving order
    seen = set()
    ordered = []
    for t in targets:
        if t not in seen:
            seen.add(t)
            ordered.append(t)

    print("Deleting database files...")
    for t in ordered:
        _rm(t)

# ---------- SQLite helpers ----------
def get_conn() -> sqlite3.Connection:
    """
    Open a connection with pragmatic settings after we recreate the DB.
    """
    conn = sqlite3.connect(DB_MAIN, timeout=30.0, isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA busy_timeout=15000;")
    return conn

def introspect_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cols = conn.execute(f"PRAGMA table_info({table});").fetchall()
    return [c["name"] for c in cols]

# ---------- Recreate schema via your module ----------
def initialize_schema():
    initialize_database()
    apply_v1_compat_migrations()
    print("Schema initialized (via database.initialize_database).")

# ---------- Accounts ----------
def get_or_create_account(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute("SELECT id FROM accounts WHERE name=?", (name,)).fetchone()
    if row:
        return int(row["id"])
    cur = conn.execute("INSERT INTO accounts(name) VALUES(?)", (name,))
    return int(cur.lastrowid)

# ---------- CSV helpers ----------
def norm(s: Optional[str]) -> str: return (s or "").strip()
def lower(s: Optional[str]) -> str: return norm(s).lower()

def pick_first(d: dict, *keys, default: str = "") -> str:
    for k in keys:
        if k in d and str(d[k]).strip() != "":
            return str(d[k]).strip()
    return default

def read_grail_rows(csv_path: str) -> List[dict]:
    if not os.path.exists(csv_path):
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(2)
    rows: List[dict] = []
    with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
        rdr = csv.DictReader(f)
        for r in rdr:
            rows.append({(k or "").strip(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()})
    if not rows:
        print("ERROR: The Grail CSV appears empty.", file=sys.stderr)
        sys.exit(2)
    print(f"Loaded {len(rows)} rows from: {csv_path}")
    return rows

# ---------- (NEW) Deterministic P2P parsing ----------
_P2P_PROVIDERS = [
    ("Zelle",       [r"zelle"]),
    ("Venmo",       [r"\bvenmo\b"]),
    ("Cash App",    [r"cash\s*app", r"\bcashapp\b", r"square\s*cash"]),
    ("PayPal",      [r"\bpaypal\b", r"\bpypl\b"]),
    ("Apple Cash",  [r"apple\s*cash", r"apple\s*pay(?:\s*cash)?"]),
    ("Google Pay",  [r"google\s*pay", r"\bgpay\b", r"google\s*wallet"]),
]
_P2P_NAMES_LOWER = {p[0].lower() for p in _P2P_PROVIDERS}

_RE_MULTI_WS  = re.compile(r"\s+")
_RE_NUMBERS   = re.compile(r"\b\d{2,}\b")
_RE_JUNK_TOK  = re.compile(r"(payment|transfer|online|mobile|memo|note|id|ref|reference|confirmation|conf|auth|trace|txn|xfer|p2p|pos|debit|credit)", re.I)
_RE_TRAILERS  = re.compile(r"[-–—:,;]?\s*(id|ref|reference|confirmation|conf|auth|trace|txn)\b.*$", re.I)
_RE_HANDLE    = re.compile(r"@([A-Za-z0-9_\.]{2,40})")
_RE_EMAIL     = re.compile(r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b")

def _title_person(s: str) -> str:
    s = _RE_MULTI_WS.sub(" ", (s or "").strip())
    if not s:
        return ""
    return " ".join(p.capitalize() for p in s.split())

def _detect_provider(text: str) -> Optional[str]:
    s = (text or "").lower()
    for name, pats in _P2P_PROVIDERS:
        if any(re.search(p, s, re.I) for p in pats):
            return name
    return None

def _extract_counterparty(text: str, provider: str) -> Tuple[Optional[str], Optional[str]]:
    s = " " + (text or "") + " "

    m = re.search(r"\bto[:\s]+(.+?)\b(?:from|via|with|memo|note|id|ref|reference|conf|auth|trace|txn)\b", s, re.I)
    if m:
        who = _title_person(_RE_JUNK_TOK.sub(" ", _RE_NUMBERS.sub(" ", _RE_TRAILERS.sub("", m.group(1)))))
        return "To", (who or None)

    m = re.search(r"\bfrom[:\s]+(.+?)\b(?:to|via|with|memo|note|id|ref|reference|conf|auth|trace|txn)\b", s, re.I)
    if m:
        who = _title_person(_RE_JUNK_TOK.sub(" ", _RE_NUMBERS.sub(" ", _RE_TRAILERS.sub("", m.group(1)))))
        return "From", (who or None)

    h = _RE_HANDLE.search(s)
    if h:
        return None, "@" + h.group(1)
    e = _RE_EMAIL.search(s)
    if e:
        return None, e.group(1)

    prov = re.escape(provider)
    m = re.search(rf"{prov}\s+(to|from)?\s*([A-Za-z][A-Za-z\s'.\-]{{1,80}})", s, re.I)
    if m:
        direction = m.group(1)
        name = _title_person(_RE_JUNK_TOK.sub(" ", _RE_NUMBERS.sub(" ", _RE_TRAILERS.sub("", m.group(2)))))
        return (direction.capitalize() if direction else None), (name or None)

    return None, None

def p2p_prefill(text: str) -> Optional[str]:
    provider = _detect_provider(text)
    if not provider:
        return None
    direction, who = _extract_counterparty(text, provider)
    if direction and who:
        return f"{provider} {direction} {who}"
    if who:
        who_fmt = who if who.startswith("@") or "@" in who else _title_person(who)
        return f"{provider} {who_fmt}"
    return provider

# ---------- Insert transactions ----------
def insert_transactions(conn: sqlite3.Connection, rows: List[dict]) -> Tuple[int, int]:
    """
    Insert from Grail, preserving transaction_id as UNIQUE.
    (NEW) Prefer deterministic P2P merchant from raw text if present.
    Returns (added, skipped).
    """
    tcols = set(introspect_columns(conn, "transactions"))
    required = {"transaction_id", "transaction_date", "amount", "account_id"}
    if not required.issubset(tcols):
        print("ERROR: transactions table missing required columns.", file=sys.stderr)
        sys.exit(2)

    fields = [
        c for c in [
            "transaction_id", "transaction_date",
            "original_description", "cleaned_description", "merchant",
            "amount",
            "category", "subcategory",
            "ai_category", "ai_subcategory",
            "account_id"
        ] if c in tcols
    ]
    placeholders = ",".join("?" for _ in fields)
    sql = f"INSERT OR IGNORE INTO transactions ({','.join(fields)}) VALUES ({placeholders})"

    added = 0
    skipped = 0
    p2p_overrides = 0
    acct_cache: Dict[str, int] = {}
    cur = conn.cursor()

    for r in rows:
        txid = pick_first(r, "transaction_id", "Transaction ID")
        if not txid:
            skipped += 1
            continue

        date = pick_first(r, "date", "transaction_date", "Date")
        amt_s = pick_first(r, "amount", "Amount", default="0")
        try:
            amount = float(amt_s.replace(",", "")) if amt_s else 0.0
        except Exception:
            skipped += 1
            continue

        account = pick_first(r, "account", "Account", default="Grail")
        if account not in acct_cache:
            acct_cache[account] = get_or_create_account(conn, account)
        account_id = acct_cache[account]

        # Source text for P2P detection (use the rawest available)
        original_description = pick_first(r, "description", "Description")
        cleaned_description  = pick_first(r, "cleaned_description", "Cleaned Description")
        source_text = original_description or cleaned_description or pick_first(r, "name", "memo", "narrative")

        # Merchant from CSV (final/canonical if provided)
        merchant_csv = pick_first(r, "new_description", "merchant", "Merchant", "cleaned_description", "description", "Description")

        # (NEW) Deterministic P2P override from raw text
        merchant_final = merchant_csv
        p2p = p2p_prefill(source_text or "")
        if p2p:
            merchant_final = p2p
                
        final_cat = pick_first(r, "new_category", "New_Category", "Final Category", default="")
        final_sub = pick_first(r, "Sub_category", "sub_category", "subcategory", "Subcategory", default="")
        ai_cat    = pick_first(r, "category", "Category", default="")
        ai_sub    = pick_first(r, "ai_subcategory", "AI Subcategory", default="")

        payload = {
            "transaction_id":   str(txid),
            "transaction_date": date,
            "original_description": (original_description or None),
            "cleaned_description":  (cleaned_description or merchant_csv or None),
            "merchant":             (merchant_final or None),
            "amount":               amount,
            "category":             (final_cat or None),
            "subcategory":          (final_sub or None),
            "ai_category":          (ai_cat or None),
            "ai_subcategory":       (ai_sub or None),
            "account_id":           account_id,
        }
        values = [payload[c] for c in fields]

        # Retry once on lock
        try:
            cur.execute(sql, values)
            if cur.rowcount: added += 1
            else:            skipped += 1
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower():
                time.sleep(0.2)
                cur.execute(sql, values)
                if cur.rowcount: added += 1
                else:            skipped += 1
            else:
                raise

    conn.commit()
    if p2p_overrides:
        print(f"P2P (raw) merchant overrides during import: {p2p_overrides}")
    return added, skipped

# ---------- (NEW) Post-import P2P fix (for any leftovers) ----------
def post_import_fix_p2p(conn: sqlite3.Connection) -> int:
    """
    Force "Provider To/From Name" into transactions.merchant whenever a P2P
    provider is detectable from the raw text.
    """
    rows = conn.execute(
        """
        SELECT id, merchant,
               COALESCE(NULLIF(cleaned_description,''), original_description) AS text
        FROM transactions
        WHERE
            lower(COALESCE(cleaned_description,'')) LIKE '%zelle%'
         OR lower(COALESCE(original_description,''))  LIKE '%zelle%'
         OR lower(COALESCE(cleaned_description,'')) LIKE '%venmo%'
         OR lower(COALESCE(original_description,''))  LIKE '%venmo%'
         OR lower(COALESCE(cleaned_description,'')) LIKE '%cash app%'
         OR lower(COALESCE(original_description,''))  LIKE '%cash app%'
         OR lower(COALESCE(cleaned_description,'')) LIKE '%paypal%'
         OR lower(COALESCE(original_description,''))  LIKE '%paypal%'
         OR lower(COALESCE(cleaned_description,'')) LIKE '%apple cash%'
         OR lower(COALESCE(original_description,''))  LIKE '%apple cash%'
         OR lower(COALESCE(cleaned_description,'')) LIKE '%google pay%'
         OR lower(COALESCE(original_description,''))  LIKE '%google pay%'
        """
    ).fetchall()

    updated = 0
    for r in rows:
        text = r["text"] or ""
        candidate = p2p_prefill(text)
        if not candidate:
            continue
        current = (r["merchant"] or "").strip()
        if current != candidate:
            conn.execute("UPDATE transactions SET merchant=? WHERE id=?", (candidate, r["id"]))
            updated += 1
    conn.commit()
    return updated

# ---------- Rules ----------
def upsert_rule(conn: sqlite3.Connection, pattern: str, category: str, subcategory: Optional[str], merchant_canonical: Optional[str]):
    if not pattern or not category:
        return
    conn.execute(
        """
        INSERT INTO category_rules (merchant_pattern, category, subcategory, merchant_canonical)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(merchant_pattern)
        DO UPDATE SET category=excluded.category,
                      subcategory=COALESCE(excluded.subcategory, category_rules.subcategory),
                      merchant_canonical=COALESCE(excluded.merchant_canonical, category_rules.merchant_canonical)
        """,
        (pattern[:64].lower(), norm(category), (norm(subcategory) or None), (norm(merchant_canonical) or None))
    )

def seed_rules_from_grail(conn: sqlite3.Connection, rows: List[dict]) -> int:
    count = 0
    for r in rows:
        merchant_canonical = pick_first(r, "new_description", "merchant", "cleaned_description", "description")
        pattern = lower(merchant_canonical)
        final_cat = pick_first(r, "new_category", "Final Category")
        final_sub = pick_first(r, "Sub_category", "subcategory")
        if not pattern or not final_cat:
            continue
        upsert_rule(conn, pattern, final_cat, final_sub or None, merchant_canonical)
        count += 1
    conn.commit()
    return count

def learn_rules_from_history(conn: sqlite3.Connection, min_count: int = 1) -> int:
    """
    Build rules from imported transactions (most-frequent category per merchant key).
    """
    rows = conn.execute(
        """
        SELECT
            COALESCE(NULLIF(LOWER(merchant),''), LOWER(cleaned_description)) AS key,
            category, subcategory,
            COALESCE(merchant, cleaned_description) AS merchant_canonical
        FROM transactions
        WHERE key IS NOT NULL AND TRIM(key) != ''
          AND category IS NOT NULL AND TRIM(category) != ''
        """
    ).fetchall()

    buckets = defaultdict(list)
    for r in rows:
        k = r["key"]
        if not k:
            continue
        buckets[k].append((r["category"], r["subcategory"], r["merchant_canonical"]))

    wrote = 0
    for k, items in buckets.items():
        if len(items) < min_count:
            continue
        cat_counts = Counter([c for c, _, _ in items if norm(c)])
        if not cat_counts:
            continue
        cat = cat_counts.most_common(1)[0][0]

        sub_counts = Counter([s for c, s, _ in items if c == cat and norm(s)])
        sub = sub_counts.most_common(1)[0][0] if sub_counts else None
        mc = next((m for _, _, m in items if norm(m)), None)

        upsert_rule(conn, k, cat, sub, mc)
        wrote += 1

    conn.commit()
    return wrote

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="DESTRUCTIVE: reset DB and bootstrap from The Wholy Grail.")
    ap.add_argument("--csv", dest="csv_path", default=DEFAULT_CSV, help="Path to The Wholy Grail.csv")
    ap.add_argument("--delete-all", action="store_true", help="Delete ALL *.db files in the project (plus -wal/-shm)")
    ap.add_argument("--min-count", type=int, default=1, help="Min occurrences when learning rules from history")
    ap.add_argument("--no-history-rules", action="store_true", help="Skip the history-derived rule pass")
    ap.add_argument("--self-destruct", action="store_true", help="Delete this script after successful run")
    args = ap.parse_args()

    # 1) Delete DB files
    delete_db_files(delete_all=args.delete_all)

    # 2) Recreate schema
    initialize_schema()

    # 3) Re-open DB and import Grail
    conn = get_conn()
    try:
        rows = read_grail_rows(args.csv_path)
        print("Importing transactions from Grail...")
        added, skipped = insert_transactions(conn, rows)
        print(f"Bootstrap: added {added}, skipped {skipped} (duplicate transaction_id or invalid rows).")

        # (NEW) Post-import P2P fix to catch any generic leftovers
        fixed = post_import_fix_p2p(conn)
        if fixed:
            print(f"P2P post-import fixes applied: {fixed}")

        print("Seeding rules from Grail...")
        seeded = seed_rules_from_grail(conn, rows)
        print(f"Seeded/updated {seeded} rules from Grail.")

        if not args.no_history_rules:
            print(f"Learning rules from history (min_count={args.min_count})...")
            wrote = learn_rules_from_history(conn, min_count=args.min_count)
            print(f"Learned/updated {wrote} rules from history.")
    finally:
        conn.close()
    # --- Optional: call API to fix P2P merchants (Zelle/Venmo/etc.) ---
    if requests is not None:
        try:
            url = "http://127.0.0.1:5056/api/fix-p2p-merchants"
            resp = requests.post(url, params={"force": 1, "limit": 5000}, timeout=12)
            if resp.ok:
                data = resp.json()
                print(
                    f"P2P fix via API -> updated={data.get('updated')}, "
                    f"scanned={data.get('scanned')}, skipped={data.get('skipped')}"
                )
            else:
                print(f"P2P fix skipped: HTTP {resp.status_code} from {url}")
        except Exception as e:
            print(f"P2P fix skipped (server not running?): {e}")
    else:
        print('P2P fix skipped (no "requests" lib). Run manually if needed:')
        print('  curl -s -X POST "http://127.0.0.1:5056/api/fix-p2p-merchants?force=1&limit=5000" | jq .')

    print("Done. Restart the Flask server (or refresh if you started it after this).")

    # 4) Self-destruct if asked
    if args.self_destruct:
        me = os.path.abspath(__file__)
        try:
            os.remove(me)
            print(f"Self-destructed: {me}")
        except Exception as e:
            print(f"Could not self-destruct ({me}): {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
