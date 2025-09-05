#!/usr/bin/env python3
import argparse, glob, io, os, sys
import pandas as pd

# local imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)) + "/..")
from parser import intelligent_parser                         # your normalizer
from ai_merchant_extractor import extract_merchant_names, debug_parse_p2p

OUT_ALL = "/tmp/corpus_extractor_report.csv"
OUT_UNK = "/tmp/unknowns_corpus.csv"

def _best_text(df: pd.DataFrame) -> pd.Series:
    # prefer cleaned_description, else original_description
    if "cleaned_description" in df.columns:
        cd = df["cleaned_description"].astype(str)
    else:
        cd = pd.Series([""] * len(df))
    od = df["original_description"].astype(str) if "original_description" in df.columns else pd.Series([""] * len(df))
    use = cd.where(cd.str.strip() != "", od)
    return use.fillna("")

def process_file(path: str, limit: int | None) -> pd.DataFrame:
    with open(path, "rb") as f:
        buf = io.BytesIO(f.read())
    norm = intelligent_parser(buf)
    if norm is None or norm.empty:
        return pd.DataFrame()

    if limit:
        norm = norm.head(limit)

    # text to extract from
    text = _best_text(norm).tolist()

    # deterministic P2P debug (provider/direction/counterparty/prefill)
    p2p = debug_parse_p2p(text)
    p2p_df = pd.DataFrame(p2p)

    # AI extraction
    ai = extract_merchant_names(text, disable_progress=True)
    ai_df = pd.DataFrame({"ai_merchant": ai})

    out = pd.concat([norm.reset_index(drop=True), p2p_df, ai_df], axis=1)
    # final decision: prefer prefill_merchant, else ai_merchant, else Unknown
    def choose(row):
        a = (row.get("prefill_merchant") or "").strip()
        b = (row.get("ai_merchant") or "").strip()
        return a if a else (b if b else "Unknown")

    out["final_decision"] = out.apply(choose, axis=1)
    out["source_file"] = os.path.basename(path)
    return out

def main():
    ap = argparse.ArgumentParser(description="Normalize then extract merchants over many CSVs.")
    ap.add_argument("--glob", required=True, help='e.g. "imports/*.csv"')
    ap.add_argument("--limit-per-file", type=int, default=None, help="Max rows per file (optional)")
    args = ap.parse_args()

    frames = []
    paths = sorted(glob.glob(args.glob))
    if not paths:
        print("No files matched your glob.")
        return

    for p in paths:
        print(f"• Processing {p} ...")
        try:
            df = process_file(p, args.limit_per_file)
            if not df.empty:
                frames.append(df)
        except Exception as e:
            print(f"  ! Skipped {p}: {e}")

    if not frames:
        print("Nothing processed.")
        return

    all_df = pd.concat(frames, ignore_index=True)
    # write all
    all_df.to_csv(OUT_ALL, index=False)

    # unknowns
    unk_df = all_df[all_df["final_decision"].str.strip().str.lower().eq("unknown")].copy()
    # Keep fields for later DB matching
    keep_cols = [
        "source_file",
        "transaction_date",
        "original_description",
        "cleaned_description",
        "amount",
        "provider","direction","counterparty",
    ]
    for c in keep_cols:
        if c not in unk_df.columns:
            unk_df[c] = None
    unk_df = unk_df[keep_cols]
    unk_df.to_csv(OUT_UNK, index=False)

    print("\nCounts:")
    print(all_df["final_decision"].str.lower().value_counts().head(30))
    print(f"\nWrote report: {OUT_ALL}")
    print(f"Wrote unknowns: {OUT_UNK}")

if __name__ == "__main__":
    main()
