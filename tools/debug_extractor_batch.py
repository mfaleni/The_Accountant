#!/usr/bin/env python3
"""
tools/debug_extractor_batch.py

Local tester for the merchant-extractor pipeline.
- Reads a CSV (e.g. The Wholy Grail.csv)
- Builds a raw text line from common description columns
- Runs LOCAL heuristics (incl. transfers + parser.extract_zelle_to_from) to see what we'd set
- Optionally calls a server endpoint for comparison (/api/debug/extract-merchants)
- Writes a report to /tmp/extractor_debug_report.csv
"""

import os
import re
import sys
import json
import argparse
import requests
import pandas as pd

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# --- Optional imports (robust) ---
# 1) Zelle detector from your parser.py
try:
    from parser import extract_zelle_to_from
except Exception:
    def extract_zelle_to_from(_s: str) -> str | None:
        return None

# 2) Transfer detector from ai_merchant_extractor.py (may return str OR dict)
try:
    from ai_merchant_extractor import _extract_transfer_to_from
except Exception:
    def _extract_transfer_to_from(_s: str):
        return None

LIKELY_DESC_COLS = [
    "original_description", "description", "cleaned_description",
    "details", "narrative", "memo", "payee", "name", "transaction_description"
]

LIKELY_PREFILL_MERCHANT_COLS = [
    "merchant", "new_description", "cleaned_description"
]

def row_to_raw(r: pd.Series) -> str:
    """Concatenate best-guess description columns into a single raw string."""
    parts = []
    for c in LIKELY_DESC_COLS:
        try:
            if c in r and pd.notna(r[c]):
                s = str(r[c]).strip()
                if s:
                    parts.append(s)
        except Exception:
            pass
    return " | ".join(parts)

def pick_prefill_merchant(r: pd.Series) -> str:
    """If the CSV already has a merchant-ish column, surface it for comparison."""
    for c in LIKELY_PREFILL_MERCHANT_COLS:
        try:
            if c in r and pd.notna(r[c]) and str(r[c]).strip():
                return str(r[c]).strip()
        except Exception:
            continue
    return ""

# ---------- Local (non-AI) inference helpers ----------

_PHONE_RE = re.compile(r"\b(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b")
_NUMTOKEN_RE = re.compile(r"\b(?:\d{2,}|\w*\d\w*)\b")
_JUNKWORDS_RE = re.compile(
    r"(?i)\b(?:online\s+payment|payment,\s*thank\s*you|online\s+transfer|pos\s+purchase|auth|ref|reference|"
    r"card\s*(?:payment|purchase)|checking\s+acct|acct|account|transfer|debit|credit|withdrawal|deposit|"
    r"purchase|statement|transaction|fee|interest|charge|conf(?:irmation)?)\b"
)

def _fallback_merchant_guess(s: str) -> str:
    """
    A very light deterministic guess: strip phones, junk tokens, numbers,
    then take the first 3-4 alphabetic-ish tokens as a brand-ish name.
    This is ONLY for debugging/local preview (no DB writes).
    """
    if not s:
        return ""
    txt = str(s)
    txt = _PHONE_RE.sub(" ", txt)
    txt = _JUNKWORDS_RE.sub(" ", txt)
    txt = _NUMTOKEN_RE.sub(" ", txt)
    txt = re.sub(r"[^A-Za-z&'`.,\-()/\s]", " ", txt)  # keep common punctuation
    txt = re.sub(r"\s{2,}", " ", txt).strip()
    if not txt:
        return ""
    tokens = [t for t in re.split(r"\s+", txt) if re.search(r"[A-Za-z]", t)]
    if not tokens:
        return ""
    guess = " ".join(tokens[:4])
    return re.sub(r"\s{2,}", " ", guess).strip().title()

# --- Transfer result normalizer: supports string OR dict returns ---
def _normalize_transfer_result(ret) -> dict | None:
    """
    Accepts either:
      - a string, e.g. "TRANSFER TO PERSONAL LINE OF CREDIT"
      - a dict,  e.g. {"final_decision": "...", "direction": "to", "counterparty": "X", ...}
    Returns a standard dict or None if unusable.
    """
    if ret is None:
        return None

    if isinstance(ret, dict):
        final = (ret.get("final_decision") or "").strip()
        if not final:
            return None
        # Try to extract direction/counterparty from dict if not provided explicitly
        direction = (ret.get("direction") or "").strip().lower()
        counterparty = (ret.get("counterparty") or "").strip()
        if not direction:
            m = re.search(r"(?i)\bTRANSFER\s+(TO|FROM)\b(?:\s+(.*))?$", final)
            if m:
                direction = m.group(1).lower()
                counterparty = counterparty or (m.group(2) or "").strip()
        return {
            "local_provider": "transfer",
            "local_direction": direction,
            "local_counterparty": counterparty,
            "local_pick_merchant": final,
            "local_is_unknown": 0 if final else 1,
        }

    # String case
    if isinstance(ret, str):
        final = ret.strip()
        if not final:
            return None
        m = re.search(r"(?i)\bTRANSFER\s+(TO|FROM)\b(?:\s+(.*))?$", final)
        direction = m.group(1).lower() if m else ""
        counterparty = (m.group(2) or "").strip() if m else ""
        return {
            "local_provider": "transfer",
            "local_direction": direction,
            "local_counterparty": counterparty,
            "local_pick_merchant": final,
            "local_is_unknown": 0 if final else 1,
        }

    return None

def local_infer(raw_text: str):
    """
    Determine a merchant-like string locally (no AI):
      1) Try generic bank transfers (TO/FROM ...)
      2) Try Zelle-specific detector
      3) Fallback brand-ish guess
    """
    s = raw_text or ""

    # 1) Transfers (string OR dict)
    try:
        t = _extract_transfer_to_from(s)
    except Exception:
        t = None
    t_norm = _normalize_transfer_result(t)
    if t_norm and t_norm.get("local_pick_merchant"):
        return t_norm

    # 2) Zelle-specific string like "Zelle To Jane Doe"
    z = None
    try:
        z = extract_zelle_to_from(s)
    except Exception:
        z = None
    if z:
        z = z.strip()
        m = re.match(r"(?i)\s*zelle\s+(to|from)\s+(.*)$", z)
        direction = ""
        counterparty = ""
        if m:
            direction = m.group(1).lower()
            counterparty = (m.group(2) or "").strip()
        pick = f"Zelle {direction.title()} {counterparty}".strip() if direction and counterparty else "Zelle"
        return {
            "local_provider": "zelle",
            "local_direction": direction,
            "local_counterparty": counterparty,
            "local_pick_merchant": pick,
            "local_is_unknown": 0 if pick else 1,
        }

    # 3) Fallback
    guess = _fallback_merchant_guess(s)
    return {
        "local_provider": "unknown",
        "local_direction": "",
        "local_counterparty": "",
        "local_pick_merchant": guess if guess else "Unknown",
        "local_is_unknown": 0 if guess else 1,
    }

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description="Debug merchant extractor (local + optional server compare).")
    ap.add_argument("input", help="Path to CSV (e.g., 'The Wholy Grail.csv')")
    ap.add_argument("--limit", type=int, default=200, help="Max rows to sample")
    ap.add_argument("--server", default="", help="Server base URL (e.g., http://127.0.0.1:5056). If provided, will call /api/debug/extract-merchants.")
    ap.add_argument("--timeout", type=int, default=120, help="Server request timeout (seconds)")
    ap.add_argument("--local-only", action="store_true", help="Skip server call; run local inference only.")
    args = ap.parse_args()

    path = args.input
    if not os.path.isfile(path):
        print(f"ERROR: File not found: {path}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(path)
    if df.empty:
        print("CSV is empty.")
        return

    # Sample top N
    df = df.head(args.limit).copy()
    df["__raw__"] = df.apply(row_to_raw, axis=1)
    df["__prefill__"] = df.apply(pick_prefill_merchant, axis=1)

    out_rows = []

    # Optional server compare
    server_items = []
    if args.server and not args.local_only:
        try:
            url = args.server.rstrip("/") + "/api/debug/extract-merchants"
            payload = {"texts": df["__raw__"].tolist()}
            r = requests.post(url, json=payload, timeout=args.timeout)
            r.raise_for_status()
            server_items = r.json().get("items", [])
        except Exception as e:
            print(f"WARNING: Server compare failed: {e}", file=sys.stderr)
            server_items = []

    for idx, r in df.iterrows():
        raw_text = r["__raw__"]
        local = local_infer(raw_text)
        server = server_items[idx] if idx < len(server_items) else {}

        # Decide a "final_decision" for the row based on what we have (local preferred if not Unknown)
        final_decision = local.get("local_pick_merchant") or "Unknown"
        source = "local"
        if not args.local_only and server:
            srv_final = (server.get("final_decision") or server.get("ai_merchant") or "").strip()
            if srv_final and final_decision.strip().lower() in ("", "unknown"):
                final_decision = srv_final
                source = "server"

        out_rows.append({
            "source_text": raw_text,
            "original_description": r.get("original_description", ""),
            "cleaned_description": r.get("cleaned_description", ""),
            "prefill_merchant_in_csv": r.get("__prefill__", ""),
            # local inference columns
            "local_provider": local.get("local_provider", ""),
            "local_direction": local.get("local_direction", ""),
            "local_counterparty": local.get("local_counterparty", ""),
            "local_pick_merchant": local.get("local_pick_merchant", ""),
            "local_is_unknown": local.get("local_is_unknown", 1),
            # server (optional)
            "server_provider": server.get("provider", ""),
            "server_direction": server.get("direction", ""),
            "server_counterparty": server.get("counterparty", ""),
            "server_prefill_merchant": server.get("prefill_merchant", ""),
            "server_ai_merchant": server.get("ai_merchant", ""),
            "server_final_decision": server.get("final_decision", ""),
            # final blended view
            "final_decision": final_decision,
            "decision_source": source,
        })

    rep = pd.DataFrame(out_rows)

    print("\n=== Sample ===")
    if not rep.empty:
        print(rep.head(20).to_string(index=False))
    else:
        print("(no rows)")

    unknowns = (rep["final_decision"].fillna("Unknown").str.strip().str.lower() == "unknown").sum()
    total = len(rep)
    print(f"\nUnknown final decisions: {unknowns} / {total}")

    if "local_provider" in rep.columns:
        print("\nProvider counts (local):")
        print(rep["local_provider"].fillna("unknown").value_counts())

    out_csv = "/tmp/extractor_debug_report.csv"
    rep.to_csv(out_csv, index=False)
    print(f"\nWrote report: {out_csv}")

if __name__ == "__main__":
    main()
