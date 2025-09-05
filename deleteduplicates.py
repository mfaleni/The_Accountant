#!/usr/bin/env python3
import argparse, csv, datetime as dt, os, re, sqlite3, sys
from typing import List, Dict, Tuple, Optional
import pandas as pd
import numpy as np
from pathlib import Path

# Try to reuse your app's fingerprint if available, else fall back to a safe local one.
try:
    from database import _fingerprint as app_fingerprint  # type: ignore
except Exception:
    def app_fingerprint(account: str, date_ymd: str, description: str, amount: float) -> str:
        # Fallback fingerprint: deterministic, case/space/format insensitive.
        desc = re.sub(r"\s+", " ", (description or "").strip().lower())
        date = (date_ymd or "").strip()
        cents = int(round(float(amount) * 100)) if amount not in (None, "") else 0
        key = f"{(account or '').strip().lower()}|{date}|{desc}|{cents}"
        return re.sub(r"[^a-z0-9|]", "", key)

DATE_CANDS = ["transaction_date","date","posted_date","post_date","dt","trans_date","statement_date"]
AMOUNT_CANDS = ["amount","amt","value","transaction_amount","debit","credit","amount_usd"]
MERCHANT_CANDS = ["merchant","description","desc","memo","payee","name","original_description","cleaned_description"]
REFERENCE_CANDS = ["reference","ref","ref_number","reference_number","check_number","transaction_id","external_id","post_id","ref_no","trace_number","trace_id"]
ACCOUNT_CANDS = ["account","account_id","acct_id","account_name","card","card_last4","account_number"]

# -------------------------
# Downloads helpers
# -------------------------
def get_downloads_dir() -> Path:
    """
    Return a best-guess Downloads directory across macOS/Linux/Windows.
    macOS: ~/Downloads is standard.
    """
    # 1) Respect explicit env var if present
    env = os.environ.get("DOWNLOADS_DIR")
    if env:
        p = Path(env).expanduser()
        p.mkdir(parents=True, exist_ok=True)
        return p

    # 2) Common default: ~/Downloads
    p = Path.home() / "Downloads"
    try:
        p.mkdir(parents=True, exist_ok=True)
        return p
    except Exception:
        pass

    # 3) Fallback: home directory
    p = Path.home()
    p.mkdir(parents=True, exist_ok=True)
    return p

def resolve_out_path(in_csv: str, out_arg: Optional[str], to_downloads: bool, suffix: str = " - CLEAN.csv") -> Path:
    """
    Decide where to write the clean CSV.
    Priority:
      a) --out if provided (as-is)
      b) --to-downloads -> ~/Downloads/<basename><suffix>
      c) next to input file -> <basename><suffix>
    """
    if out_arg:
        return Path(out_arg).expanduser().resolve()

    base = Path(in_csv).expanduser().resolve()
    target_name = f"{base.stem}{suffix}"
    if to_downloads:
        return (get_downloads_dir() / target_name).resolve()
    return (base.parent / target_name).resolve()

def resolve_dupes_path(in_csv: str, dupes_arg: Optional[str], to_downloads: bool) -> Optional[Path]:
    """
    If --dupes-out is given, respect it. If not given, return None.
    If --dupes-out is not given BUT --to-downloads is true, we still keep None
    (we only write duplicates when user explicitly asks for it).
    """
    if not dupes_arg:
        return None
    p = Path(dupes_arg).expanduser()
    if to_downloads and not p.is_absolute():
        # If user gave a relative name AND wants Downloads, put it under Downloads
        p = get_downloads_dir() / p
    return p.resolve()

# -------------------------

def norm_text(x:str)->str:
    if x is None: return ""
    return re.sub(r"\s+"," ",str(x).strip().lower())
def norm_ref(x:str)->str:
    return re.sub(r"[^a-z0-9]","",norm_text(x)) if x is not None else ""
def parse_date_any(s: str) -> Optional[dt.datetime]:
    if s is None or str(s).strip()=="":
        return None
    fmts = ["%m/%d/%Y","%Y-%m-%d","%d/%m/%Y","%m/%d/%y","%d-%b-%Y","%b %d %Y","%b %d, %Y"]
    for f in fmts:
        try: return dt.datetime.strptime(str(s).strip(), f)
        except: pass
    try:
        return pd.to_datetime(s, errors="coerce").to_pydatetime()
    except: return None
def mmddyyyy(x:str)->str:
    d=parse_date_any(x); return d.strftime("%m/%d/%Y") if d else ""
def ymd(x:str)->str:
    d=parse_date_any(x); return d.strftime("%Y-%m-%d") if d else ""
def to_float(x)->Optional[float]:
    if x is None or str(x).strip()=="": return None
    try: return float(str(x).replace(",",""))
    except: return None
def to_cents(x:Optional[float])->Optional[int]:
    if x is None: return None
    return int(round(x*100))

def find_col(cols: List[str], cands: List[str]) -> Optional[str]:
    lower_map = {c.lower():c for c in cols}
    for c in cands:
        if c in lower_map: return lower_map[c]
    # soft contains
    for c in cols:
        if any(k in c.lower() for k in cands): return c
    return None

def choose_winner(g: pd.DataFrame) -> pd.Series:
    # earliest date, then longest merchant text
    cols, asc = [], []
    if "norm_date_ymd" in g.columns: cols.append("norm_date_ymd"); asc.append(True)
    if "merchant_len" in g.columns: cols.append("merchant_len"); asc.append(False)
    if cols:
        return g.sort_values(cols, ascending=asc, kind="stable").iloc[0]
    return g.iloc[0]

def dedupe_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str,str]]:
    orig_cols = list(df.columns)
    date_col     = find_col(orig_cols, [c.lower() for c in DATE_CANDS])
    amount_col   = find_col(orig_cols, [c.lower() for c in AMOUNT_CANDS])
    merchant_col = find_col(orig_cols, [c.lower() for c in MERCHANT_CANDS])
    ref_col      = find_col(orig_cols, [c.lower() for c in REFERENCE_CANDS])
    acct_col     = find_col(orig_cols, [c.lower() for c in ACCOUNT_CANDS])

    info = {
        "date_col": date_col or "",
        "amount_col": amount_col or "",
        "merchant_col": merchant_col or "",
        "reference_col": ref_col or "",
        "account_col": acct_col or "",
    }

    df = df.copy()
    df["_src_row"] = np.arange(len(df))
    df["norm_merchant"] = df[merchant_col].map(norm_text) if merchant_col else ""
    df["merchant_len"]  = df["norm_merchant"].str.len() if merchant_col else 0
    df["norm_date_ymd"] = df[date_col].map(ymd) if date_col else ""
    df["norm_date_mmddyyyy"] = df[date_col].map(mmddyyyy) if date_col else ""
    df["norm_amount_cents"] = df[amount_col].map(to_float).map(to_cents) if amount_col else None
    df["norm_reference"] = df[ref_col].map(norm_ref) if ref_col else ""
    # Fallback fingerprint for visibility/debug
    df["fp"] = df.apply(
        lambda r: app_fingerprint(
            (str(r.get(acct_col,"")) if acct_col else ""),
            r["norm_date_ymd"],
            r["norm_merchant"],
            (r["norm_amount_cents"]/100.0) if r["norm_amount_cents"] is not None else 0.0
        ),
        axis=1
    )
    df["mda_key"] = (
        df["norm_merchant"].fillna("") + "|" +
        df["norm_date_ymd"].fillna("") + "|" +
        df["norm_amount_cents"].astype("Int64").astype(str).replace("<NA>","")
    )
    df["ref_key"] = df["norm_reference"]

    # Stage 1: reference-based
    has_ref = df["ref_key"].astype(bool)
    df["_keep_ref"] = True
    if has_ref.any():
        winners_ref = df[has_ref].groupby("ref_key", dropna=False).apply(choose_winner).reset_index(drop=True)
        keep_idx = set(winners_ref["_src_row"].tolist())
        df["_keep_ref"] = df["_src_row"].isin(keep_idx)
    df["_dup_stage1"] = has_ref & (~df["_keep_ref"])

    # Stage 2: merchant+date+amount
    remaining = df[~df["_dup_stage1"]].copy()
    has_mda = remaining["mda_key"].astype(bool)
    remaining["_keep_mda"] = True
    if has_mda.any():
        winners_mda = remaining[has_mda].groupby("mda_key", dropna=False).apply(choose_winner).reset_index(drop=True)
        keep_idx_mda = set(winners_mda["_src_row"].tolist())
        remaining["_keep_mda"] = remaining["_src_row"].isin(keep_idx_mda)
    remaining["_dup_stage2"] = has_mda & (~remaining["_keep_mda"])

    dup_idx = set(df.loc[df["_dup_stage1"], "_src_row"].tolist()) | set(remaining.loc[remaining["_dup_stage2"], "_src_row"].tolist())
    df["_is_duplicate"] = df["_src_row"].isin(dup_idx)

    clean = df[~df["_is_duplicate"]].copy()
    dups  = df[df["_is_duplicate"]].copy()

    # normalize transaction_date column (create if missing)
    out_date_col = date_col or "transaction_date"
    if date_col:
        clean[date_col] = clean["norm_date_mmddyyyy"]
    else:
        clean[out_date_col] = clean["norm_date_mmddyyyy"]

    # final cols: original order + ensured date col
    helper = {"_src_row","norm_merchant","merchant_len","norm_date_mmddyyyy","norm_date_ymd","norm_amount_cents",
              "norm_reference","mda_key","ref_key","_keep_ref","_dup_stage1","_keep_mda","_dup_stage2","_is_duplicate","fp"}
    final_cols = [c for c in orig_cols if c in clean.columns and c not in helper]
    if out_date_col not in final_cols: final_cols.append(out_date_col)
    return clean[final_cols], dups[orig_cols], info

def dedupe_csv(in_csv: str, out_csv: Path, dupes_csv: Optional[Path] = None):
    df = pd.read_csv(in_csv, dtype=str, keep_default_na=False, na_filter=False)
    clean, dups, info = dedupe_df(df)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    clean.to_csv(out_csv, index=False)
    if dupes_csv:
        dupes_csv.parent.mkdir(parents=True, exist_ok=True)
        dups.to_csv(dupes_csv, index=False)
    print(f"[CSV] rows_in={len(df)} rows_out={len(clean)} removed={len(df)-len(clean)}")
    print(f"[CSV] wrote clean -> {out_csv}")
    if dupes_csv:
        print(f"[CSV] wrote duplicates -> {dupes_csv}")
    print(f"[CSV] columns used → date={info['date_col']} amount={info['amount_col']} merchant={info['merchant_col']} ref={info['reference_col']} account={info['account_col']}")

def get_conn(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def export_ids_to_csv(conn: sqlite3.Connection, ids: List[int], path: str):
    if not ids: return
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
    cols = [r["name"] for r in conn.execute("PRAGMA table_info('transactions')")]
    with open(path,"w",newline="") as f:
        w = csv.writer(f); w.writerow(cols)
        for i in range(0,len(ids),900):
            chunk=ids[i:i+900]
            q = f"SELECT * FROM transactions WHERE id IN ({','.join('?' for _ in chunk)})"
            for row in conn.execute(q, chunk):
                w.writerow([row[c] for c in cols])

def ensure_backup_table(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS deleted_transactions AS
        SELECT * FROM transactions WHERE 0
    """)

def copy_rows_to_backup_table(conn: sqlite3.Connection, ids: List[int]) -> int:
    if not ids: return 0
    ensure_backup_table(conn)
    total=0
    for i in range(0,len(ids),900):
        chunk=ids[i:i+900]
        conn.execute(f"INSERT INTO deleted_transactions SELECT * FROM transactions WHERE id IN ({','.join('?' for _ in chunk)})", chunk)
        total += len(chunk)
    return total

def delete_rows(conn: sqlite3.Connection, ids: List[int]) -> int:
    if not ids: return 0
    total=0
    for i in range(0,len(ids),900):
        chunk=ids[i:i+900]
        conn.execute(f"DELETE FROM transactions WHERE id IN ({','.join('?' for _ in chunk)})", chunk)
        total += len(chunk)
    return total

def from_db_to_df(conn: sqlite3.Connection) -> pd.DataFrame:
    # Pull everything (stringify), for robust pandas processing
    df = pd.read_sql_query("SELECT * FROM transactions", conn)
    for c in df.columns:
        df[c] = df[c].astype(str).fillna("")
    return df

def main():
    ap = argparse.ArgumentParser(description="Nuclear duplicate killer for CSV or SQLite DB.")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--csv", help="Input CSV path to clean")
    g.add_argument("--db", help="Path to SQLite DB to clean in-place (transactions table)")

    ap.add_argument("--out", help="Output path for cleaned CSV (when --csv used)")
    ap.add_argument("--dupes-out", help="Optional path to write duplicates CSV (when --csv used)")
    ap.add_argument("--to-downloads", action="store_true", help="Write outputs into your Downloads folder")
    ap.add_argument("--backup-csv", help="(DB mode) Export rows-to-delete to this CSV")
    ap.add_argument("--backup-table", action="store_true", help="(DB mode) Copy rows to deleted_transactions before delete")
    ap.add_argument("--confirm", action="store_true", help="(DB mode) Actually delete from DB; otherwise dry-run")
    ap.add_argument("--show", type=int, default=10, help="(DB mode) Show up to N sample duplicate groups")
    args = ap.parse_args()

    if args.csv:
        out_path   = resolve_out_path(args.csv, args.out, args.to_downloads, suffix=" - CLEAN.csv")
        dupes_path = resolve_dupes_path(args.csv, args.dupes_out, args.to_downloads)
        dedupe_csv(args.csv, out_path, dupes_path)
        return

    # DB mode
    conn = get_conn(args.db)
    try:
        df = from_db_to_df(conn)
        clean, dups, info = dedupe_df(df)

        # Map cleaned rows back to IDs to delete
        keep_mask = ~df.index.isin(dups.index)
        losers_ids = df.loc[~keep_mask, "id"].astype(int).tolist()

        print(f"[DB] scanned={len(df)} keep={keep_mask.sum()} delete={len(losers_ids)}")
        print(f"[DB] columns used → date={info['date_col']} amount={info['amount_col']} merchant={info['merchant_col']} ref={info['reference_col']} account={info['account_col']}")

        if args.show and len(losers_ids):
            print("Sample of IDs to delete:", losers_ids[:args.show])

        if args.backup_csv and losers_ids:
            # If user gave relative path + wants Downloads, put it there.
            bkp = Path(args.backup_csv).expanduser()
            if args.to_downloads and not bkp.is_absolute():
                bkp = get_downloads_dir() / bkp
            export_ids_to_csv(conn, losers_ids, str(bkp))
            print(f"Wrote backup CSV: {bkp}")

        if not args.confirm:
            print("DRY RUN — no DB changes made. Use --confirm to apply.")
            return

        # destructive path (with optional table backup)
        conn.execute("BEGIN")
        try:
            if args.backup_table and losers_ids:
                backed = copy_rows_to_backup_table(conn, losers_ids)
                print(f"Backed up {backed} rows to deleted_transactions.")
            deleted = delete_rows(conn, losers_ids)
            conn.commit()
            print(f"Deleted {deleted} rows from DB.")
        except Exception:
            conn.rollback()
            print("ERROR: Rolled back DB changes.")
            raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
