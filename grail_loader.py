# grail_loader.py (v6 — robust CSV mapping; DB-aware inserts)
import argparse
import hashlib
import re
import sqlite3
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd

from database import get_db_connection, get_or_create_account

CSV_DEFAULT = "The New Wholy Grail - CLEAN.csv"

# ----------------------------
# CSV header normalization
# ----------------------------
def _canon(s: str) -> str:
    """Normalize a header: lowercase, collapse non-alphanum to underscores."""
    return re.sub(r"[^a-z0-9]+", "_", (s or "").strip().lower()).strip("_")

# Canonical field -> list of acceptable header aliases (normalized via _canon)
CSV_ALIASES: Dict[str, list] = {
    "transaction_id": [
        "transaction_id","transactionid","txn_id","id","reference","reference_number","ref_no","trace_number","trace_id"
    ],
    "date": [
        "date","transaction_date","posted_date","post_date","statement_date"
    ],
    "account": [
        "account","account_name","account_id","acct_id","card","card_name","card_last4","account_number"
    ],
    "merchant": [
        "merchant","description","payee","name"
    ],
    "original_description": [
        "original_description","description_raw","bank_memo","memo"
    ],
    "cleaned_description": [
        "cleaned_description","clean_description","normalized_description","desc_clean"
    ],
    "new_description": [
        "new_description","final_description","merchant_canonical"
    ],
    "amount": [
        "amount","transaction_amount","value","amt","amount_usd","debit","credit"
    ],
    # CLEAN file categorization
    "new_category": [
        "new_category","final_category","category_final"
    ],
    "subcategory": [
        "sub_category","subcategory","subcat","sub_category_final"
    ],
}

def map_headers(df_columns) -> Dict[str, Optional[str]]:
    """Return mapping canonical_field -> actual CSV column (or None if not present)."""
    norm_map = {_canon(col): col for col in df_columns}
    out: Dict[str, Optional[str]] = {}
    for want, aliases in CSV_ALIASES.items():
        found = None
        for alias in aliases:
            a = _canon(alias)
            if a in norm_map:
                found = norm_map[a]
                break
        out[want] = found
    return out

# ----------------------------
# DB schema detection
# ----------------------------
def _detect_txn_columns(conn: sqlite3.Connection) -> Dict[str, bool]:
    cols = {r[1].lower(): r for r in conn.execute("PRAGMA table_info('transactions')")}
    return {
        "has_unique_fingerprint": "unique_fingerprint" in cols,   # <- fixed typo
        "has_unique_hash":       "unique_hash" in cols,
        "has_subcategory":       "subcategory" in cols,
        "has_sub_category":      "sub_category" in cols,
        "has_original_description": "original_description" in cols,
        "has_cleaned_description":  "cleaned_description" in cols,
        "has_merchant":             "merchant" in cols,
        "has_transaction_id":       "transaction_id" in cols,
        "has_ai_category":          "ai_category" in cols,
        "has_ai_subcategory":       "ai_subcategory" in cols,
    }

# ----------------------------
# Normalizers
# ----------------------------
def _std_date_to_ymd(s: str) -> str:
    """
    Accepts mm/dd/yyyy or yyyy-mm-dd (and common variants); returns yyyy-mm-dd.
    """
    s = (s or "").strip()
    if not s:
        return ""
    # try strict mm/dd/yyyy first (your CLEAN file uses this)
    m = pd.to_datetime(s, errors="coerce", format="%m/%d/%Y")
    if pd.isna(m):
        m = pd.to_datetime(s, errors="coerce")
    return "" if pd.isna(m) else m.strftime("%Y-%m-%d")

def _to_float_amount(x) -> float:
    s = str(x).replace(",", "").strip()
    if s == "":
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0

def _fingerprint(account_id: int, date_ymd: str, clean_desc: str, amount: float, txn_id: Optional[str]) -> str:
    basis = f"{account_id}|{date_ymd}|{(clean_desc or '').strip().lower()}|{amount:.2f}|{(txn_id or '').strip()}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()

# ----------------------------
# Loader
# ----------------------------
def run_grail_load(csv_path: str, wipe: bool = True):
    """
    Load 'The New Wholy Grail - CLEAN.csv' into the DB, using Accountant 6's schema:
      - categories come EXACTLY from 'new_category'
      - subcategory from 'Sub_category'/'subcategory' if present
      - accounts created via get_or_create_account
      - optional wipe (transactions + category_rules) before load
      - DB-aware insert: only uses columns that actually exist in 'transactions'
    """
    csv_file = Path(csv_path).expanduser().resolve()
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV not found: {csv_file}")

    print(f"--- Grail Load starting: '{csv_file.name}' ---")
    conn = get_db_connection()
    try:
        cur = conn.cursor()

        # Read CSV
        print("Reading CSV...")
        df = pd.read_csv(csv_file, dtype=str, keep_default_na=False, na_filter=False)
        print(f"Rows in file: {len(df)}")

        # Header mapping (tolerant)
        hdr = map_headers(df.columns)
        print("CSV column mapping (detected):", {k: v for k, v in hdr.items() if v})

        # Required minimal inputs
        required = ["date", "amount", "account"]
        missing = [k for k in required if not hdr.get(k)]
        if missing:
            raise ValueError(
                f"Missing required CSV columns (any alias): {missing}. "
                f"Available headers: {list(df.columns)}"
            )

        # Resolve source columns (None if not present)
        def col(key: str) -> Optional[str]:
            return hdr.get(key) or None

        date_col   = col("date")
        acct_col   = col("account")
        amt_col    = col("amount")
        merch_col  = col("merchant")
        txid_col   = col("transaction_id")
        orig_col   = col("original_description")
        clean_col  = col("new_description") or col("cleaned_description") or merch_col
        cat_col    = col("new_category") or "category"   # prefer new_category; tolerate legacy 'category'
        subcat_col = col("subcategory")  # may be None

        # Normalize essentials
        print("Normalizing dates and amounts...")
        df["date_std"] = df[date_col].apply(_std_date_to_ymd)
        df["amount_float"] = df[amt_col].apply(_to_float_amount)

        # Descriptions
        if orig_col:
            df["orig_desc_final"] = df[orig_col].astype(str)
        else:
            df["orig_desc_final"] = df[merch_col].astype(str) if merch_col else ""

        if clean_col:
            df["clean_desc_final"] = df[clean_col].astype(str)
        else:
            # fallback cascade
            if col("cleaned_description"):
                df["clean_desc_final"] = df[col("cleaned_description")].astype(str)
            elif merch_col:
                df["clean_desc_final"] = df[merch_col].astype(str)
            else:
                df["clean_desc_final"] = ""

        # Category/subcategory (exact from CLEAN)
        df["category_final"] = df[cat_col].astype(str) if cat_col in df.columns else ""
        df["subcategory_final"] = df[subcat_col].astype(str) if subcat_col in df.columns else ""

        # Accounts sync
        print("Syncing accounts...")
        accounts_map: Dict[str, int] = {}
        for acc_name in sorted(df[acct_col].unique()):
            acc_id = get_or_create_account(conn, acc_name)
            accounts_map[acc_name] = acc_id
        conn.commit()
        print(f"Synchronized {len(accounts_map)} accounts.")

        # DB schema switches
        tx_cols = _detect_txn_columns(conn)

        # Optional wipe (data only)
        if wipe:
            print("Clearing existing transactions and rules (keeping schema)...")
            try:
                cur.execute("DELETE FROM transactions")
            except Exception as e:
                print("Note: could not clear transactions:", e)
            try:
                cur.execute("DELETE FROM category_rules")
            except Exception:
                pass
            try:
                cur.execute("DELETE FROM sqlite_sequence WHERE name IN ('transactions','category_rules')")
            except Exception:
                pass
            conn.commit()
            print("Old data cleared.")

        # Build INSERT based on actual DB columns
        base_cols = ["transaction_date", "amount", "account_id", "category"]
        extra_cols = []

        subcat_db_col = None
        if tx_cols["has_subcategory"]:
            subcat_db_col = "subcategory"
        elif tx_cols["has_sub_category"]:
            subcat_db_col = "sub_category"
        if subcat_db_col:
            extra_cols.append(subcat_db_col)

        if tx_cols["has_original_description"]:
            extra_cols.append("original_description")
        if tx_cols["has_cleaned_description"]:
            extra_cols.append("cleaned_description")
        if tx_cols["has_merchant"]:
            extra_cols.append("merchant")
        if tx_cols["has_transaction_id"]:
            extra_cols.append("transaction_id")

        fp_db_col = None
        if tx_cols["has_unique_fingerprint"]:
            fp_db_col = "unique_fingerprint"
        elif tx_cols["has_unique_hash"]:
            fp_db_col = "unique_hash"
        if fp_db_col:
            extra_cols.append(fp_db_col)

        if tx_cols["has_ai_category"]:
            extra_cols.append("ai_category")
        if tx_cols["has_ai_subcategory"]:
            extra_cols.append("ai_subcategory")

        insert_cols = base_cols + extra_cols
        placeholders = ", ".join("?" for _ in insert_cols)
        sql = f"INSERT INTO transactions ({', '.join(insert_cols)}) VALUES ({placeholders})"

        # Insert rows
        print("Inserting transactions...")
        inserted = 0
        skipped = 0

        for _, row in df.iterrows():
            account_id = accounts_map[row[acct_col]]
            t_date = row["date_std"]
            t_amount = float(row["amount_float"])
            t_category = row["category_final"]

            values = [t_date, t_amount, account_id, t_category]

            for name in extra_cols:
                if name == subcat_db_col:
                    values.append(row["subcategory_final"])
                elif name == "original_description":
                    values.append(row["orig_desc_final"])
                elif name == "cleaned_description":
                    values.append(row["clean_desc_final"])
                elif name == "merchant":
                    values.append(row[merch_col] if merch_col else "")
                elif name == "transaction_id":
                    values.append(row[txid_col] if txid_col else "")
                elif name in ("unique_fingerprint", "unique_hash"):
                    fp = _fingerprint(
                        account_id=account_id,
                        date_ymd=t_date,
                        clean_desc=row["clean_desc_final"],
                        amount=t_amount,
                        txn_id=row[txid_col] if txid_col else "",
                    )
                    values.append(fp)
                elif name in ("ai_category", "ai_subcategory"):
                    values.append(None)
                else:
                    values.append(None)

            try:
                cur.execute(sql, values)
                inserted += 1
            except Exception as e:
                # Unique constraint collision or other row-specific failure
                skipped += 1
                # Uncomment for debugging:
                # print(f"Skip row due to error: {e}")

        conn.commit()
        print(f"✅ Inserted: {inserted}   Skipped: {skipped}")

        # Rebuild category rules strictly from CLEAN file
        try:
            if "new_description" in df.columns or "cleaned_description" in df.columns:
                rules_src = df.copy()
                # prefer new_description for pattern; else cleaned_description
                if "new_description" in rules_src.columns:
                    rules_src["pattern"] = rules_src["new_description"].astype(str)
                elif "cleaned_description" in rules_src.columns:
                    rules_src["pattern"] = rules_src["cleaned_description"].astype(str)
                else:
                    rules_src["pattern"] = ""

                rules_src["pattern"] = rules_src["pattern"].str.strip().str.lower()
                rules_src["cat"] = df["category_final"].astype(str).str.strip()
                rules_src = rules_src[(rules_src["pattern"] != "") & (rules_src["cat"] != "")]
                rules_df = rules_src[["pattern", "cat"]].drop_duplicates()

                cur.executemany(
                    "INSERT OR REPLACE INTO category_rules (merchant_pattern, category) VALUES (?, ?)",
                    list(map(tuple, rules_df.to_numpy()))
                )
                conn.commit()
                print(f"✅ Saved {len(rules_df)} category rules.")
        except Exception as e:
            print("Note: could not save category_rules (table may not exist):", e)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
        print("--- Grail Load Complete ---")

# ----------------------------
# CLI
# ----------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load 'The New Wholy Grail - CLEAN.csv' into Accountant 6 DB.")
    ap.add_argument("--csv", default=CSV_DEFAULT, help="Path to The New Wholy Grail - CLEAN.csv")
    ap.add_argument("--no-wipe", action="store_true", help="Do not clear existing transactions/category_rules first")
    args = ap.parse_args()
    run_grail_load(csv_path=args.csv, wipe=(not args.no_wipe))
