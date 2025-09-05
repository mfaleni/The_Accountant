# parser.py — universal CSV → normalized DataFrame
# Output columns: transaction_date (YYYY-MM-DD), original_description, cleaned_description, amount, category

from __future__ import annotations
import io, re, csv
from datetime import datetime
from typing import Optional, List, Tuple
import pandas as pd

# ---------------- helpers ----------------

_DATE_PATTERNS = (
    "%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y", "%d/%m/%y",
    "%b %d, %Y", "%d %b %Y"
)

def _parse_date_any(s: str) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    if not s:
        return None
    for fmt in _DATE_PATTERNS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        # pandas fallback (handles a lot of oddities)
        d = pd.to_datetime(s, errors="coerce")
        if pd.isna(d):
            return None
        return d.strftime("%Y-%m-%d")
    except Exception:
        return None

_money_cleaner = re.compile(r"[,$\s]")

def _to_amount(x) -> Optional[float]:
    """Handle $, commas, parentheses neg, CR/DR flags, and strings."""
    if pd.isna(x):
        return None
    s = str(x).strip()
    if not s:
        return None

    # trailing CR/DR flags
    crdr = 1.0
    if s.upper().endswith(" CR"):
        crdr = 1.0
        s = s[:-3].strip()
    elif s.upper().endswith(" DR"):
        crdr = -1.0
        s = s[:-3].strip()

    # parentheses negatives
    neg = 1.0
    if s.startswith("(") and s.endswith(")"):
        neg = -1.0
        s = s[1:-1]

    s = _money_cleaner.sub("", s)
    if not s:
        return None
    try:
        return float(s) * neg * crdr
    except Exception:
        return None

def _is_date_series(ser: pd.Series) -> bool:
    ok = 0
    n = min(len(ser), 50)
    for v in ser.head(n):
        if _parse_date_any(str(v)) is not None:
            ok += 1
    return ok >= max(3, int(n * 0.6))

def _is_amount_series(ser: pd.Series) -> bool:
    ok = 0
    n = min(len(ser), 50)
    for v in ser.head(n):
        if _to_amount(v) is not None:
            ok += 1
    return ok >= max(3, int(n * 0.6))
PROVIDERS = [
    "zelle","venmo","cash app","cashapp","paypal","apple cash","google pay",
    "ach","wire","transfer","online transfer","external transfer","p2p"
]

def extract_to_from_party(text: str) -> tuple[str|None, str|None, str|None]:
    """
    Returns (provider, direction, counterparty) or (None,None,None).
    Direction is 'to' or 'from'. Provider is one of the words above if present,
    else 'transfer' when a transfer with to/from is detected.
    """
    if not text:
        return (None, None, None)

    s = str(text)
    slow = s.lower()

    provider = None
    for p in PROVIDERS:
        if p in slow:
            provider = "zelle" if "zelle" in p else p
            break

    # unified to/from counterparty pattern
    m = re.search(r"(?i)\b(to|from)\b\s*[:\-]?\s*([A-Za-z][\w .,&'`-]{2,})", s)
    if m:
        direction = m.group(1).strip().lower()  # 'to' or 'from'
        name = re.sub(r"\s{2,}", " ", m.group(2)).strip(" -:.,")
        # strip trailing refs/emails/ids
        name = re.sub(r"\b(?:acct|account|ending|x{2,}\d+|#\d+).*$", "", name, flags=re.I).strip()
        name = re.sub(r"\b(?:id|ref|conf|confirmation)\s*[:#]?\s*\w+.*$", "", name, flags=re.I).strip()
        if name:
            if not provider:
                # if we saw 'transfer' terms but no branded provider, tag as generic transfer
                if "transfer" in slow or "p2p" in slow or "ach" in slow or "wire" in slow:
                    provider = "transfer"
            return (provider or "transfer", direction, name)

    # special fallback: keep plain 'Zelle' if detected with no counterparty
    if provider == "zelle":
        return ("zelle", None, None)

    return (None, None, None)


def extract_zelle_to_from(text: str) -> str | None:
    """
    Try to produce canonical 'Zelle To X' or 'Zelle From Y' from a raw bank line.
    Handles many banks' phrasings. Returns None if not sure.
    """
    if not text:
        return None
    s = str(text)
    if "zelle" not in s.lower():
        return None

    # Common patterns
    m = re.search(r"(?i)zelle(?:\s+payment|\s+transfer|\s+credit|\s+debit|)\s*(to|from)\s*[:\-]?\s*([A-Za-z][\w .,&'`-]{2,})", s)
    if m:
        direction = m.group(1).strip().title()  # To / From
        name = re.sub(r"\s{2,}", " ", m.group(2)).strip(" -:.,")
        # Trim trailing ids/emails if they snuck in
        name = re.sub(r"\b(?:acct|account|ending|x{2,}\d+|#\d+).*$", "", name, flags=re.I).strip()
        name = re.sub(r"\b(?:id|ref|conf|confirmation)\s*[:#]?\s*\w+.*$", "", name, flags=re.I).strip()
        return f"Zelle {direction} {name}" if name else f"Zelle {direction}"

    # Fallbacks where bank emits separate tokens
    m2 = re.search(r"(?i)(?:to|from)\s+([A-Za-z][\w .,&'`-]{2,}).*zelle", s)
    if m2:
        direction = "To" if " to " in s.lower() else "From" if " from " in s.lower() else ""
        name = re.sub(r"\s{2,}", " ", m2.group(1)).strip(" -:.,")
        if direction and name:
            return f"Zelle {direction} {name}"

    # Generic: keep at least 'Zelle' if we can't find a counterparty
    return "Zelle"

# --- Helpers to sanitize the tail after "to/from" ---
__RE_MULTI_WS   = re.compile(r"\s{2,}")
__RE_MASKED_AC  = re.compile(r"(?i)\b(?:x{2,}|[*#]{2,})\d{2,}\b")  # XXXXXX4311, ****1234, ###9876
__RE_TRAIL_META = re.compile(
    r"(?i)\b(?:ref(?:erence)?|id|trace|conf(?:irmation)?|confirmation|txn|trans(?:action)?)\s*[:#]?\s*[\w-]+.*$"
)
__RE_TRAIL_DATE = re.compile(
    r"(?i)\bon\s+\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?(?:\b|$)"
)

def _strip_tofrom_tail(s: str) -> str:
    if not s:
        return ""
    # remove trailing metadata like "REF #...", "ID ABC123", "... ON 08/12/25" etc.
    s = __RE_TRAIL_META.sub("", s)
    s = __RE_TRAIL_DATE.sub("", s)
    # drop masked account digits (keep the account name)
    s = __RE_MASKED_AC.sub("", s)
    # common noise around account hints
    s = re.sub(r"(?i)\b(?:acct|account|ending|number)\b[:#]?\s*", "", s)
    s = s.strip(" -:.,")
    s = __RE_MULTI_WS.sub(" ", s)
    return s

def extract_to_from_party(text: str) -> str | None:
    """
    Generic transfer extractor (non-Zelle):
    Returns 'Transfer To X' or 'Transfer From Y' for Online/ACH/External/Internal/Wire transfer lines,
    avoiding REF/ID tokens and dates. Example:
      'ONLINE TRANSFER REF #IB0THMKLQP FROM PERSONAL LINE OF CREDIT XXXXXX4311 ON 08/12/25'
        -> 'Transfer From Personal Line Of Credit'
    """
    if not text:
        return None
    s = str(text)

    # only attempt if line looks like a transfer/payment/etc.
    if not re.search(r"(?i)\b(transfer|payment|pmt|xfer)\b", s):
        return None

    # Prefer the token AFTER 'to|from'
    m = re.search(r"(?i)\b(to|from)\b\s*[:#-]?\s*(.+)", s)
    if not m:
        return None

    direction = m.group(1).strip().title()  # To / From
    tail = _strip_tofrom_tail(m.group(2))

    # If the tail is still empty or clearly a stray code, bail
    if not tail or re.fullmatch(r"(?i)(ref|id|conf|trace|txn)[\s:#-]*\w+", tail):
        return None

    # Normalize common account phrases (keeps the account name)
    tail = tail.title()
    # Clean small leftover artifacts
    tail = tail.replace("  ", " ").strip(" -:.,")
    if not tail:
        return None

    return f"Transfer {direction} {tail}"
# ---------------- header mapping ----------------

DATE_HEADERS = {"date","transaction date","posted date","posting date","transaction_date","posteddate"}
DESC_HEADERS = {"description","memo","details","payee","original description","name","narrative","cleaned_description"}
AMOUNT_HEADERS = {"amount","transaction amount","debit","credit","value","amount (usd)","amt"}
TYPE_HEADERS = {"type","dr/cr","credit/debit"}
CATEGORY_HEADERS = {"category","merchant category","mcc"}

def _best_header_match(columns: List[str]) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    cols_lower = [c.strip().lower() for c in columns]
    def find_from(cands):
        for i, c in enumerate(cols_lower):
            if c in cands:
                return columns[i]
        return None
    return (
        find_from(DATE_HEADERS),
        find_from(DESC_HEADERS),
        find_from(AMOUNT_HEADERS),
        find_from(TYPE_HEADERS),
    )

# ---------------- core CSV loading ----------------

def _read_csv_try(file_bytes: bytes, delimiter: Optional[str], header: Optional[int]) -> Optional[pd.DataFrame]:
    try:
        return pd.read_csv(
            io.BytesIO(file_bytes),
            sep=delimiter if delimiter else None,
            engine="python",
            header=header,
            dtype=str,
            encoding="utf-8",
            skip_blank_lines=True
        )
    except Exception:
        try:
            return pd.read_csv(
                io.BytesIO(file_bytes),
                sep=delimiter if delimiter else None,
                engine="python",
                header=header,
                dtype=str,
                encoding="latin-1",
                skip_blank_lines=True
            )
        except Exception:
            return None

def _sniff_delimiter(head: str) -> Optional[str]:
    try:
        dialect = csv.Sniffer().sniff(head, delimiters=[",",";","|","\t"])
        return dialect.delimiter
    except Exception:
        return None

# ---------------- public API ----------------

def intelligent_parser(file_stream: io.BytesIO) -> Optional[pd.DataFrame]:
    """
    Normalize arbitrary bank CSVs into:
      transaction_date (YYYY-MM-DD), original_description, cleaned_description, amount, category
    Returns a DataFrame with at least (transaction_date, original_description, cleaned_description, amount),
    or None if nothing usable is found.
    """
    raw = file_stream.read()
    if not raw:
        return None

    # Try to sniff delimiter from first chunk
    head = raw[:8192].decode("utf-8", errors="ignore")
    delimiter = _sniff_delimiter(head)

    # Pass 1: header on row 0
    df = _read_csv_try(raw, delimiter, header=0)
    if df is None or df.empty:
        # Pass 2: headerless
        df = _read_csv_try(raw, delimiter, header=None)
        if df is not None and not df.empty:
            df.columns = [f"col{i+1}" for i in range(df.shape[1])]

    if df is None or df.empty:
        return None

    # Trim headers & cell whitespace
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip()

    # Header-based mapping
    date_col, desc_col, amount_col, type_col = _best_header_match(list(df.columns))

    # Infer by content if needed
    if not date_col or not _is_date_series(df[date_col]):
        date_col = None
        for c in df.columns:
            if _is_date_series(df[c]):
                date_col = c
                break

    if not amount_col or not _is_amount_series(df[amount_col]):
        amount_col = None
        # Handle debit/credit split files by synthesizing a single amount
        debit_cols, credit_cols = [], []
        for c in df.columns:
            lc = c.strip().lower()
            if lc in {"debit","withdrawal","debits"}:
                debit_cols.append(c)
            if lc in {"credit","deposit","credits"}:
                credit_cols.append(c)
        if debit_cols or credit_cols:
            amt = pd.Series(0.0, index=df.index, dtype=float)
            for c in credit_cols:
                amt = amt + pd.to_numeric(df[c].map(_to_amount), errors="coerce").fillna(0.0)
            for c in debit_cols:
                amt = amt - pd.to_numeric(df[c].map(_to_amount), errors="coerce").fillna(0.0)
            df["_amount_synth"] = amt
            amount_col = "_amount_synth"
        else:
            for c in df.columns:
                if _is_amount_series(df[c]):
                    amount_col = c
                    break

    if not desc_col:
        # choose the wordiest column as description
        desc_col = max(df.columns, key=lambda c: df[c].astype(str).str.len().mean())

    # If essentials still missing, bail
    if not date_col or not amount_col or not desc_col:
        return None

    # Build normalized frame
    out = pd.DataFrame()
    out["transaction_date"] = df[date_col].map(_parse_date_any)
    out["original_description"] = df[desc_col].astype(str)
    out["cleaned_description"]  = out["original_description"].str.strip()

    if amount_col == "_amount_synth":
        out["amount"] = df["_amount_synth"]
    else:
        out["amount"] = df[amount_col].map(_to_amount)

    # Use type column if it clearly signals debit/credit
    if type_col and type_col in df.columns:
        t = df[type_col].astype(str).str.lower()
        # If this column looks categorical, flip amounts where needed
        if t.isin(["debit","debits","dr"]).mean() >= 0.6:
            out.loc[t.isin(["debit","debits","dr"]) & (out["amount"] > 0), "amount"] *= -1

    # Optional category passthrough
    cat_col = None
    for c in df.columns:
        if c.strip().lower() in CATEGORY_HEADERS:
            cat_col = c
            break
    out["category"] = (df[cat_col].astype(str).str.strip() if cat_col else "Uncategorized")

    # Keep only valid rows
    out = out.dropna(subset=["transaction_date", "amount"])
    out = out[out["transaction_date"].astype(str).str.len() > 0]
    out = out[pd.to_numeric(out["amount"], errors="coerce").notna()]

    if out.empty:
        return None

    # Final coercions
    out["amount"] = out["amount"].astype(float)

    # ------------------------ ADDED: merchant enrichment (Zelle) ------------------------
    # Ensure merchant column exists
    out["merchant"] = ""

    # Detect Zelle To/From for merchant where applicable, based on original_description
    zmask = out["original_description"].astype(str).str.contains("zelle", case=False, na=False)
    if zmask.any():
        out.loc[zmask, "merchant"] = out.loc[zmask, "original_description"].apply(extract_zelle_to_from).fillna("Zelle")

    # If cleaned_description is blank, fall back to merchant to keep UI readable
    blank_clean = out["cleaned_description"].astype(str).str.strip().eq("")
    if blank_clean.any():
        out.loc[blank_clean, "cleaned_description"] = out.loc[blank_clean, "merchant"]

    # ---------------------- end of merchant enrichment (Zelle) -------------------------

    # Standard column order  (ADDED merchant at the end)
    cols = ["transaction_date","original_description","cleaned_description","amount","category","merchant"]
    return out[cols]
