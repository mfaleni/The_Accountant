#!/usr/bin/env python3
"""
ai_merchant_extractor.py

Drop-in merchant extractor:
- Batch-calls OpenAI to extract ONLY the merchant/trade name for each transaction line.
- Handles Zelle/Venmo/etc per strict system instructions.
- Falls back to single-call retries on batch failure.
- Can be imported from app.py:  from ai_merchant_extractor import extract_merchant_names
- Can be run as a CLI over a CSV:  python ai_merchant_extractor.py --input file.csv --output out.csv
"""

import argparse
import json
import os
import sys
import time
import re  # <-- (NEW) needed for deterministic P2P parsing
from typing import List, Tuple
from typing import Union
import pandas as pd

_RAW_COLS_DEFAULT = [
    "original_description", "description", "cleaned_description",
    "details", "narrative", "memo", "payee", "name", "transaction_description"
]

# --- helpers to sanitize bank "transfer to/from" names ---
_REF_TOKEN_RE   = re.compile(r"(?i)\bref(?:erence)?\s*#?\s*[\w-]+\b")
_MASKED_RE      = re.compile(r"(?i)\bX{2,}\d+\b|\bx{2,}\d+\b")       # XXXXXX4311, xxx1234
_DATE_TRAIL_RE  = re.compile(r"(?i)\bon\s+\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b.*$")
_ACC_TAIL_RE    = re.compile(r"(?i)\b(?:account|acct|ending|number|no\.)\b.*$")
_WS_RE          = re.compile(r"\s{2,}")

def _clean_transfer_name(name: str) -> str:
    # Remove REF # tokens, masked acct digits, and trailing date/account chatter
    name = _REF_TOKEN_RE.sub("", name)
    name = _MASKED_RE.sub("", name)
    name = _DATE_TRAIL_RE.sub("", name)
    name = _ACC_TAIL_RE.sub("", name)
    name = name.replace("#", "")
    name = _WS_RE.sub(" ", name).strip(" -:.,\t")

    # Normalize common account labels
    name = re.sub(r"(?i)\bpersonal\s+line\s+of\s+credit\b.*", "Personal Line Of Credit", name)
    name = re.sub(r"(?i)\bline\s+of\s+credit\b.*", "Line Of Credit", name)
    name = re.sub(r"(?i)\bchk(?:g|ing)?\b|\bchecking\b", "Checking", name)
    name = re.sub(r"(?i)\bsav(?:ings?)?\b", "Savings", name)
    return name

def _extract_transfer_to_from(text: str) -> str | None:
    """
    Detect generic bank transfers and return an ALL-CAPS merchant like:
      'TRANSFER TO PERSONAL LINE OF CREDIT' or 'TRANSFER FROM CHECKING'
    """
    if not text:
        return None
    s = str(text)
    if "transfer" not in s.lower():
        return None

    m = re.search(r"(?i)\btransfer\b.*\b(to|from)\b\s+(.+)$", s)
    if not m:
        return None

    direction = m.group(1).lower()         # 'to' | 'from'
    name      = _clean_transfer_name(m.group(2))
    if not name:
        final = f"TRANSFER {direction.upper()}"
    else:
        final = f"TRANSFER {direction.upper()} {name.upper()}"
    return final


def _row_to_raw_text(row: Union[dict, pd.Series], use_columns: List[str] | None = None) -> str:
    """
    Build a robust raw text from a row by concatenating typical bank-export columns.
    We keep this minimal: only read fields if they exist and are non-empty.
    """
    cols = use_columns or _RAW_COLS_DEFAULT
    parts: List[str] = []
    getter = row.get if isinstance(row, dict) else (lambda k: row[k] if k in row and pd.notna(row[k]) else None)
    for k in cols:
        v = getter(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            parts.append(s)
    # use " | " as a weak delimiter; the P2P parser ignores these anyway
    return " | ".join(parts) if parts else ""


def extract_merchants_from_dataframe(
    df: pd.DataFrame,
    use_columns: List[str] | None = None,
    model: str = "gpt-4o",
    batch_size: int = 40,
    temperature: float = 0.0,
    max_retries: int = 3,
    disable_progress: bool = False
) -> List[str]:
    """
    RAW-FIRST merchant extraction for DataFrames:
    1) Build a raw text per row by joining typical bank fields (memo/details/etc. included).
    2) Run deterministic P2P detection on that raw text (Zelle/Venmo/...).
    3) Only unresolved rows go to the model, using the SAME raw text.

    Returns a list of merchant strings aligned with df index.
    """
    # Step 1: build raw texts
    raw_texts = [_row_to_raw_text(row, use_columns) for _, row in df.iterrows()]
    n = len(raw_texts)

    # Step 2: deterministic P2P prefill on RAW text
    prefilled: List[str | None] = [None] * n
    for i, raw in enumerate(raw_texts):
        try:
            m = _extract_transfer_to_from(raw) or _p2p_merchant(raw)
        except Exception:
            m = None
        if m:
            prefilled[i] = clean_merchant_name(m)

    # If no API key, return only deterministic wins (leave others blank for DB heuristics)
    if not os.getenv("OPENAI_API_KEY"):
        out: list[str] = []
        for i, raw in enumerate(raw_texts):
            if prefilled[i]:
                out.append(prefilled[i])
                continue
            s = str(raw or "").strip().strip('"').strip("'")
            out.append("" if (not s or s.lower() == "unknown") else s)
        print("WARNING: OPENAI_API_KEY not set. Returned originals (with transfer/P2P prefill), sanitized 'Unknown'.", file=sys.stderr)
        return out

    # Step 3: model for the unresolved rows ONLY, using RAW text
    client = OpenAI()
    extracted = [""] * n
    chunks = chunk_indices(n, batch_size)

    def assign_back(slice_start: int, slice_end: int, preds_for_unresolved: List[str], prefill_slice: List[str | None]):
        """Merge preds with prefilled for a slice."""
        merged: List[str] = []
        it = iter(preds_for_unresolved)
        for v in prefill_slice:
            if v:
                merged.append(clean_merchant_name(v))
            else:
                merged.append(clean_merchant_name(next(it, "Unknown")))
        extracted[slice_start:slice_end] = merged

    print(f"Processing {n} rows (RAW-first) to extract merchant names...")
    for start, end in tqdm(chunks, desc="merchant-extract(raw)", disable=disable_progress):
        batch_raw = raw_texts[start:end]
        batch_prefill = prefilled[start:end]
        unresolved_idx = [i for i, v in enumerate(batch_prefill) if not v]
        if not unresolved_idx:
            # everything in this slice is already prefilled
            extracted[start:end] = [clean_merchant_name(v) for v in batch_prefill]  # type: ignore
            continue

        attempt = 0
        while True:
            try:
                to_ai_texts = [batch_raw[i] for i in unresolved_idx]
                ai_results = call_openai_batch(client, model, to_ai_texts, temperature=temperature)
                assign_back(start, end, ai_results, batch_prefill)
                break
            except Exception as e:
                print(f"API Error on batch ({start}-{end}), attempt {attempt+1}: {e}", file=sys.stderr)
                attempt += 1
                if attempt > max_retries:
                    print(f"Batch ({start}-{end}) failed after {max_retries} retries. Falling back to per-item.", file=sys.stderr)
                    per_item: List[str] = []
                    for j, original_raw in enumerate([batch_raw[k] for k in unresolved_idx]):
                        try:
                            per_item.append(call_openai_single(client, model, original_raw, temperature=temperature))
                        except Exception as single_e:
                            print(f"Single item {start + j} failed: {single_e}", file=sys.stderr)
                            per_item.append("Unknown")
                    assign_back(start, end, per_item, batch_prefill)
                    break
                backoff_sleep(attempt)

    final_names = [clean_merchant_name(s) if s else "Unknown" for s in extracted]
    return final_names

# Optional tqdm; if missing, no progress bar
try:
    from tqdm import tqdm
except Exception:
    def tqdm(x, *a, **k):  # type: ignore
        return x

# Optional dotenv for local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# OpenAI 1.x SDK
try:
    from openai import OpenAI
except Exception:
    print("ERROR: The 'openai' package is not installed. Run: pip install -U openai", file=sys.stderr)
    raise

# ----------------- Heuristics / helpers -----------------

LIKELY_COLS = [
    "original_description", "description", "transaction_description",
    "details", "narrative", "memo", "payee", "name",
    "cleaned_description", "merchant_name", "merchant"
]

def auto_pick_source_column(df: pd.DataFrame, fallback: str = None) -> str:
    """Pick the best text column to extract merchants from."""
    if fallback and fallback in df.columns:
        return fallback
    for c in LIKELY_COLS:
        if c in df.columns:
            return c
    obj_cols = df.select_dtypes(include=["object"]).columns.tolist()
    if obj_cols:
        return obj_cols[0]
    # Last resort: join all columns into one text column
    df["__source_text__"] = df.astype(str).agg(" | ".join, axis=1)
    return "__source_text__"

def chunk_indices(n_rows: int, chunk_size: int) -> List[Tuple[int, int]]:
    """Yield (start, end) pairs for batching."""
    if chunk_size <= 0:
        chunk_size = 40
    return [(i, min(i + chunk_size, n_rows)) for i in range(0, n_rows, chunk_size)]

def clean_merchant_name(s: str) -> str:
    """Light post-process: trim and normalize whitespace/quotes."""
    if not isinstance(s, str):
        return "Unknown"
    s = s.strip().strip('"').strip("'")
    s = " ".join(s.split())
    return s or "Unknown"

def _coerce_len(merchants, n: int) -> list[str]:
    """Ensure list length equals n; pad with 'Unknown' or truncate."""
    arr = merchants if isinstance(merchants, list) else []
    out = []
    for x in arr:
        out.append(clean_merchant_name(x) if isinstance(x, str) else "Unknown")
    if len(out) < n:
        out += ["Unknown"] * (n - len(out))
    elif len(out) > n:
        out = out[:n]
    return out

# ----------------- (NEW) Deterministic P2P parsing -----------------
# Keep this minimal and self-contained; it only pre-fills obvious P2P cases.
_P2P_PROVIDERS = [
("Zelle", [
        r"\bzelle\b", r"ach\s*zelle", r"web\s*zelle", r"zelle\s*payment", r"zelle\s*xfer"
    ]),
    ("Venmo", [
        r"\bvenmo\b", r"venmo\s*payment", r"web\s*venmo", r"xfer\s*venmo"
    ]),
    ("Cash App", [
        r"cash\s*app", r"\bcashapp\b", r"square\s*cash", r"\bsq\s*cash\b"
    ]),
    ("PayPal", [
        r"\bpaypal\b", r"\bpypl\b", r"\bpp\*?\b", r"paypal\s*inst\s*xfer", r"pypl\*"
    ]),
    ("Apple Cash", [
        r"apple\s*cash", r"apple\s*pay\s*cash", r"apple\s*pay"
    ]),
    ("Google Pay", [
        r"google\s*pay", r"\bgpay\b", r"google\s*wallet"
    ]),
]

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

def _detect_provider(text: str) -> str | None:
    s = (text or "").lower()
    for name, pats in _P2P_PROVIDERS:
        if any(re.search(p, s, re.I) for p in pats):
            return name
    return None

def _extract_counterparty(text: str, provider: str) -> Tuple[str | None, str | None]:
    """
    Try hard to pull a 'To/From Name' or @handle/email from noisy bank text.
    Works for patterns like:
      - "Zelle Payment To JOHN SMITH on 08/24 Conf# 12345"
      - "Zelle From Jane-Doe Ref 9999"
      - "VENMO PAYMENT 123... @jsmith note: rent"
    """
    s_raw = " " + (text or "") + " "
    s = _RE_TRAILERS.sub("", s_raw)  # drop obvious trailing ref chunks
    s = _RE_NUMBERS.sub(" ", s)      # remove long numbers
    s = _RE_MULTI_WS.sub(" ", s)     # normalize whitespace

    # 1) explicit handle / email (strongest)
    h = _RE_HANDLE.search(s)
    if h:
        return None, "@" + h.group(1)
    e = _RE_EMAIL.search(s)
    if e:
        return None, e.group(1)

    # 2) "to X ..." or "from Y ..." anywhere (bounded by common markers)
    m = re.search(
        r"\b(to|from)\b[:\s]+([A-Za-z][A-Za-z\s'.\-]{1,80})(?=\b(?:on|for|via|with|memo|note|id|ref|reference|conf|confirmation|auth|trace|txn|payment|transfer)\b|$)",
        s, re.I
    )
    if m:
        direction = m.group(1).capitalize()
        who = _title_person(_RE_JUNK_TOK.sub(" ", m.group(2)))
        return direction, (who or None)

    # 3) provider-adjacent "Zelle to X", "Venmo from Y", "Cash App X"
    prov = re.escape(provider)
    m = re.search(
        rf"{prov}\s*(?:payment|transfer)?\s*(?:to|from)?\s*([A-Za-z@][A-Za-z0-9_.\-\s']{{1,80}})",
        s, re.I
    )
    if m:
        tail = m.group(1)
        # Try to split out a leading direction if present
        dm = re.match(r"^(to|from)\s+(.*)$", tail, re.I)
        direction = None
        name_part = tail
        if dm:
            direction = dm.group(1).capitalize()
            name_part = dm.group(2)

        name_part = _RE_TRAILERS.sub("", name_part)
        name_part = _RE_NUMBERS.sub(" ", name_part)
        name_part = _RE_JUNK_TOK.sub(" ", name_part)
        name_part = _RE_MULTI_WS.sub(" ", name_part).strip()

        # Prefer handle if present in this slice
        hh = _RE_HANDLE.search(name_part)
        if hh:
            return direction, "@" + hh.group(1)
        ee = _RE_EMAIL.search(name_part)
        if ee:
            return direction, ee.group(1)

        who = _title_person(name_part)
        return direction, (who or None)

    return None, None

def _p2p_merchant(text: str) -> str | None:
    provider = _detect_provider(text)
    if not provider:
        return None
    direction, who = _extract_counterparty(text, provider)
    if direction and who:
        return f"{provider} {direction} {who}"
    if who:
        who_fmt = who if who.startswith("@") or "@" in who else _title_person(who)
        return f"{provider} {who_fmt}"
    # If nothing better found, at least return provider (caller decides whether to overwrite)
    return provider

def debug_parse_p2p(texts: List[str]) -> List[dict]:
    """
    Returns structured info so you can see what the deterministic P2P parser
    thinks for each input string (provider, direction, counterparty, and the
    prefilled merchant string it would produce).
    This does NOT call the OpenAI API.
    """
    out = []
    for t in texts:
        s = str(t or "")
        provider = _detect_provider(s)
        direction, who = (None, None)
        if provider:
            direction, who = _extract_counterparty(s, provider)
        prefill = _p2p_merchant(s)
        out.append({
            "input": s,
            "provider": provider,
            "direction": direction,
            "counterparty": who,
            "prefill_merchant": clean_merchant_name(prefill) if prefill else None,
        })
    return out

# --- convenience wrapper for dataframes (do not modify other code) ---
def extract_merchants_from_dataframe(
    df: pd.DataFrame,
    use_columns: list[str] | None = None,
    model: str = "gpt-4o",
    batch_size: int = 40,
    temperature: float = 0.0,
    max_retries: int = 3,
    disable_progress: bool = True,
) -> list[str]:
    """
    Return a list of merchant strings aligned to df rows.
    RAW-first: build a robust raw string per row from typical bank columns,
    then run extract_merchant_names on that list.
    """
    if df is None or df.empty:
        return []

    # Build RAW text per row (uses all relevant columns, not just one)
    texts = [_row_to_raw_text(row, use_columns) for _, row in df.iterrows()]

    names = extract_merchant_names(
        texts,
        model=model,
        batch_size=batch_size,
        temperature=temperature,
        max_retries=max_retries,
        disable_progress=disable_progress,
    )

    # Guard: never return literal "Unknown" up to callers (let DB heuristics/rules kick in)
    out: list[str] = []
    for n in names:
        s = (n or "").strip().strip('"').strip("'")
        out.append("" if not s or s.lower() == "unknown" else s)
    return out

# ----------------- OpenAI prompts -----------------

SYS_INSTRUCTIONS = (
    "You are a precision data-extraction assistant for financial transactions. "
    "For each transaction string provided, extract ONLY the merchant/trade name that a human "
    "would recognize on a receipt (brand or business name). Follow rules strictly:\n"
    " - Return ONLY the merchant/trade name; no categories, no locations, no states, no ZIP codes.\n"
    " - For Zelle transactions, the output format MUST BE 'Zelle To [Recipient Name]' or 'Zelle From [Sender Name]'. Extract the person's name accurately.\n"
    " - For other peer-to-peer platforms (e.g., Venmo, Cash App), return the counterparty's name/handle if available; otherwise, return the platform name.\n"
    " - Remove words like: payment, purchase, debit/credit, transfer, POS, order/invoice/ref IDs.\n"
    " - Remove city/state, store numbers, suite/unit numbers, phone numbers, dates, and URLs.\n"
    " - Normalize variants (e.g., 'AMZN Mktp', 'Amazon Prime' -> 'Amazon').\n"
    " - If truly unknown after careful reading, return 'Unknown'.\n"
    "Output must strictly be valid JSON as requested."
)

def build_schema(batch_len: int) -> dict:
    return {
        "name": "merchant_list_schema",
        "schema": {
            "type": "object",
            "properties": {
                "merchants": {
                    "type": "array",
                    "minItems": batch_len,
                    "maxItems": batch_len,
                    "items": { "type": "string" }
                }
            },
            "required": ["merchants"],
            "additionalProperties": False
        },
        "strict": True
    }

# ----------------- API callers -----------------

def responses_batch(client: OpenAI, model: str, tx_texts: List[str], temperature: float = 0.0) -> List[str]:
    """Use the Responses API with JSON schema for robust structure."""
    schema = build_schema(len(tx_texts))
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(tx_texts))
    user_prompt = (
        "Extract ONLY the merchant/trade name for each transaction line below. "
        "Return an array of strings called 'merchants' aligned by index. "
        "Output valid JSON only, matching the provided schema.\n\n"
        f"TRANSACTIONS:\n{numbered}"
    )
    resp = client.responses.create(
        model=model,
        instructions=SYS_INSTRUCTIONS,
        input=[{"role": "user", "content": [{"type": "input_text", "text": user_prompt}]}],
        response_format={"type": "json_schema", "json_schema": schema},
        temperature=temperature,
    )
    # Be defensive about odd outputs; never raise on length mismatch.
    try:
        data = json.loads(resp.output_text)
        merchants = data.get("merchants", [])
    except Exception:
        merchants = []
    return _coerce_len(merchants, len(tx_texts))

def chat_batch(client: OpenAI, model: str, tx_texts: List[str], temperature: float = 0.0) -> List[str]:
    """Fallback to Chat Completions with JSON object."""
    numbered = "\n".join(f"{i+1}. {t}" for i, t in enumerate(tx_texts))
    user_prompt = (
        "Extract ONLY the merchant/trade name for each transaction line below. "
        "Return a JSON object: {\"merchants\": [<merchant for #1>, <merchant for #2>, ...] }. "
        "The array length MUST equal the number of lines. No prose.\n\n"
        f"TRANSACTIONS:\n{numbered}"
    )
    resp = client.chat.completions.create(
        model=model,
        temperature=temperature,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYS_INSTRUCTIONS},
            {"role": "user", "content": user_prompt},
        ],
    )
    # Be defensive about odd outputs; never raise on length mismatch.
    try:
        content = resp.choices[0].message.content
        data = json.loads(content)
        merchants = data.get("merchants", [])
    except Exception:
        merchants = []
    return _coerce_len(merchants, len(tx_texts))

def call_openai_batch(client: OpenAI, model: str, tx_texts: List[str], temperature: float = 0.0) -> List[str]:
    """Prefer Responses API; if not usable in environment, fallback to Chat."""
    try:
        return responses_batch(client, model, tx_texts, temperature=temperature)
    except TypeError:
        return chat_batch(client, model, tx_texts, temperature=temperature)

def call_openai_single(client: OpenAI, model: str, text: str, temperature: float = 0.0) -> str:
    """Single-row extraction for fallback when a batch fails."""
    try:
        schema = {
            "name": "merchant_schema",
            "schema": {
                "type": "object",
                "properties": { "merchant": { "type": "string" } },
                "required": ["merchant"],
                "additionalProperties": False
            },
            "strict": True
        }
        user_prompt = f"Extract ONLY the merchant/trade name from this transaction string:\n\n{text}"
        resp = client.responses.create(
            model=model,
            instructions=SYS_INSTRUCTIONS,
            input=[{"role": "user", "content": [{"type": "input_text", "text": user_prompt}]}],
            response_format={"type": "json_schema", "json_schema": schema},
            temperature=temperature,
        )
        data = json.loads(resp.output_text)
        return clean_merchant_name(data.get("merchant", "Unknown"))
    except TypeError:
        user_prompt = (
            "Extract ONLY the merchant/trade name from this transaction string and return: "
            "{\"merchant\": \"<name>\"}. No prose.\n\n" + str(text)
        )
        resp = client.chat.completions.create(
            model=model,
            temperature=temperature,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": SYS_INSTRUCTIONS},
                {"role": "user", "content": user_prompt},
            ],
        )
        data = json.loads(resp.choices[0].message.content)
        return clean_merchant_name(data.get("merchant", "Unknown"))

def backoff_sleep(attempt: int, base: float = 1.6, cap: float = 30.0):
    import random
    sleep_s = min(cap, (base ** attempt) + random.uniform(0, 1.0))
    time.sleep(sleep_s)

# ----------------- Public API (used by app.py) -----------------

def extract_merchant_names(
    descriptions: List[str],
    model: str = "gpt-4o",
    batch_size: int = 40,
    temperature: float = 0.0,
    max_retries: int = 3,
    disable_progress: bool = False
) -> List[str]:
    """
    Extract merchant names for a list of transaction description strings.

    Returns a list of strings, aligned to the input order.
    If OPENAI_API_KEY is missing, returns the original descriptions unchanged (graceful fallback).
    """
    # (NEW) Pre-fill obvious P2P cases deterministically BEFORE any model calls
    n = len(descriptions)
    prefilled = [None] * n
    for i, raw in enumerate(descriptions):
        txt = str(raw or "")

        # 1) generic bank transfers (prefer this over P2P if present)
        try:
            t = _extract_transfer_to_from(txt)
        except Exception:
            t = None
        if t:
            prefilled[i] = clean_merchant_name(t)
            continue  # already decided

        # 2) peer-to-peer (Zelle/Venmo/etc.)
        try:
            m = _p2p_merchant(txt)
        except Exception:
            m = None
        if m:
            prefilled[i] = clean_merchant_name(m)

    if not os.getenv("OPENAI_API_KEY"):
        # Keep your original behavior: no API -> return inputs unchanged,
        # but preserve any prefilled P2P wins we got for free.
        out = [str(x or "") for x in descriptions]
        for i, v in enumerate(prefilled):
            if v:  # only overwrite when we have a deterministic P2P result
                out[i] = v
        print("WARNING: OPENAI_API_KEY not set. Returned originals (with P2P prefill where possible).", file=sys.stderr)
        return out

    client = OpenAI()
    extracted = [""] * n
    chunks = chunk_indices(n, batch_size)

    print(f"Processing {n} descriptions to extract merchant names...")
    for start, end in tqdm(chunks, desc="merchant-extract", disable=disable_progress):
        batch_descriptions = [str(x or "") for x in descriptions[start:end]]

        # split batch: prefilled vs unresolved
        batch_prefill = prefilled[start:end]
        to_ai_idx = [i for i, v in enumerate(batch_prefill) if not v]
        if not to_ai_idx:
            # everything in this slice already decided by pre-parser
            extracted[start:end] = [clean_merchant_name(v) for v in batch_prefill]  # type: ignore
            continue

        def assign_back(preds: List[str]):
            merged = []
            it = iter(preds)
            for i, v in enumerate(batch_prefill):
                if v:
                    merged.append(clean_merchant_name(v))
                else:
                    merged.append(clean_merchant_name(next(it, "Unknown")))
            extracted[start:end] = merged

        attempt = 0
        while True:
            try:
                to_ai_texts = [batch_descriptions[i] for i in to_ai_idx]
                ai_results = call_openai_batch(client, model, to_ai_texts, temperature=temperature)
                assign_back(ai_results)
                break
            except Exception as e:
                print(f"API Error on batch ({start}-{end}), attempt {attempt+1}: {e}", file=sys.stderr)
                attempt += 1
                if attempt > max_retries:
                    print(f"Batch ({start}-{end}) failed after {max_retries} retries. Falling back to per-item.", file=sys.stderr)
                    per_item = []
                    for i, original_desc in enumerate([batch_descriptions[j] for j in to_ai_idx]):
                        try:
                            per_item.append(call_openai_single(client, model, original_desc, temperature=temperature))
                        except Exception as single_e:
                            print(f"Single item {start + i} failed: {single_e}", file=sys.stderr)
                            per_item.append("Unknown")
                    assign_back(per_item)
                    break
                backoff_sleep(attempt)

    # Merge any prefills for slices that never passed through the batching loop (n==0 edge)
    for i, v in enumerate(prefilled):
        if v and not extracted[i]:
            extracted[i] = v

    final_names = [clean_merchant_name(s) if s else "Unknown" for s in extracted]
    return final_names

# ----------------- CLI -----------------

def main():
    parser = argparse.ArgumentParser(description="Extract merchant names with OpenAI.")
    parser.add_argument("--input", required=True, help="Input CSV path")
    parser.add_argument("--output", default=None, help="Output CSV path (default: <input>_with_new_description_AI.csv)")
    parser.add_argument("--source-col", default=None, help="Column to read descriptions from (auto-detect if omitted)")
    parser.add_argument("--model", default="gpt-4o", help="OpenAI model ID (default: gpt-4o)")
    parser.add_argument("--batch-size", type=int, default=40, help="Batch size for API calls")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries per batch before fallback to single")
    parser.add_argument("--temperature", type=float, default=0.0, help="Sampling temperature")
    parser.add_argument("--no-progress", action="store_true", help="Disable progress bar")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"ERROR: file not found: {args.input}", file=sys.stderr)
        sys.exit(2)

    try:
        df = pd.read_csv(args.input)
    except Exception as e:
        print(f"ERROR: failed reading CSV: {e}", file=sys.stderr)
        sys.exit(2)

    src_col = auto_pick_source_column(df, args.source_col)
    if src_col not in df.columns:
        print(f"ERROR: Source column '{src_col}' not found.", file=sys.stderr)
        sys.exit(2)

    texts = df[src_col].astype(str).fillna("").tolist()

    new_desc = extract_merchant_names(
        texts,
        model=args.model,
        batch_size=args.batch_size,
        temperature=args.temperature,
        max_retries=args.max_retries,
        disable_progress=args.no_progress
    )

    df["new_description"] = new_desc
    out_path = args.output or os.path.splitext(args.input)[0] + "_with_new_description_AI.csv"
    try:
        df.to_csv(out_path, index=False)
    except Exception as e:
        print(f"ERROR: failed writing output CSV: {e}", file=sys.stderr)
        sys.exit(2)

    unknowns = sum(1 for x in new_desc if (not x) or str(x).strip().lower() == "unknown")
    print(f"Done. Wrote: {out_path}")
    print(f"Unknown rows: {unknowns} / {len(df)}")

if __name__ == "__main__":
    main()
