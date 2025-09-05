#!/usr/bin/env python3
# TESTS/debug_parser_extractor.py
# Normalizes with parser.intelligent_parser, then runs ai_merchant_extractor on the normalized text.
# Writes a CSV report to /tmp/parser_extractor_report.csv and prints a small summary.

import os, sys, pandas as pd

# ensure imports resolve when run from repo root
sys.path.insert(0, os.getcwd())

from parser import intelligent_parser
from ai_merchant_extractor import extract_merchant_names, debug_parse_p2p

def _pick_source_text(df: pd.DataFrame) -> list[str]:
    # Prefer cleaned_description if non-empty, else original_description
    texts = []
    for _, r in df.iterrows():
        cd = str(r.get("cleaned_description", "") or "").strip()
        od = str(r.get("original_description", "") or "").strip()
        texts.append(cd if cd else od)
    return texts

def main():
    if len(sys.argv) < 2:
        print("Usage: python TESTS/debug_parser_extractor.py <CSV_PATH> [LIMIT]", file=sys.stderr)
        sys.exit(2)
    csv_path = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 300

    if not os.path.exists(csv_path):
        print(f"ERROR: file not found: {csv_path}", file=sys.stderr)
        sys.exit(2)

    # 1) Normalize with your universal parser
    with open(csv_path, "rb") as fh:
        df_norm = intelligent_parser(fh)

    if df_norm is None or df_norm.empty:
        print("Parser produced no rows. Check the CSV headers/format.", file=sys.stderr)
        sys.exit(2)

    df_norm = df_norm.head(limit).copy()

    # 2) Build source texts & run deterministic P2P debug (so we can see prefill)
    texts = _pick_source_text(df_norm)
    p2p_info = debug_parse_p2p(texts)  # returns provider/direction/counterparty/prefill_merchant

    # 3) Run the AI merchant extractor on the same texts
    names = extract_merchant_names(texts, batch_size=40, disable_progress=True)

    # 4) Merge results into a report
    rep = pd.DataFrame({
        "transaction_date": df_norm["transaction_date"],
        "amount": df_norm["amount"],
        "original_description": df_norm["original_description"],
        "cleaned_description": df_norm["cleaned_description"],
        "source_text": texts,
        "provider": [x.get("provider") for x in p2p_info],
        "direction": [x.get("direction") for x in p2p_info],
        "counterparty": [x.get("counterparty") for x in p2p_info],
        "prefill_merchant": [x.get("prefill_merchant") for x in p2p_info],
        "ai_merchant": names,
    })

    # final_decision: prefer ai_merchant when not Unknown/blank, else prefill_merchant, else "unknown"
    def decide(ai, pre):
        a = (ai or "").strip()
        if a and a.lower() != "unknown":
            return a
        p = (pre or "").strip()
        if p:
            return p
        return "unknown"

    rep["final_decision"] = [decide(a, p) for a, p in zip(rep["ai_merchant"], rep["prefill_merchant"])]

    # 5) Print small sample + counts and write file
    print("\n=== Sample ===")
    print(rep[["source_text","provider","direction","counterparty","prefill_merchant","ai_merchant","final_decision"]].head(20).to_string(index=False))

    counts = rep["final_decision"].str.lower().value_counts().head(15)
    print("\nCounts:")
    print(counts.to_string())

    out_path = "/tmp/parser_extractor_report.csv"
    rep.to_csv(out_path, index=False)
    print(f"\nWrote report: {out_path}")

if __name__ == "__main__":
    main()
