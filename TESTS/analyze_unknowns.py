#!/usr/bin/env python3
# TESTS/analyze_unknowns.py
import os, sys, re, csv
import pandas as pd

DEFAULT_INPUT = os.environ.get("UNKNOWN_CSV", "/tmp/unknowns_corpus.csv")
OUT_RULES = "/tmp/transfer_learning_suggestions.csv"
OUT_CLUSTERS = "/tmp/unknown_clusters.csv"

P2P_WORDS = r"(zelle|venmo|cash\s*app|paypal|apple\s*cash|google\s*pay|gpay|google\s*wallet)"
RE_P2P = re.compile(P2P_WORDS, re.I)

RE_TRANSFER = re.compile(
    r"\b(online|recurring)\s+transfer\s+(to|from)\s+([A-Za-z][A-Za-z\s\.'-]{1,60})\b", re.I
)

RE_TOFROM = re.compile(
    r"\b(?:to|from)\s+([A-Za-z][A-Za-z\s\.'-]{1,60})\b", re.I
)

def title_person(s: str) -> str:
    s = " ".join(s.strip().split())
    return " ".join(p.capitalize() for p in s.split())

def read_unknowns(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        print(f"ERROR: file not found: {path}")
        sys.exit(2)
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    # Pick best text column available
    txt = df.get("cleaned_description", "")
    if not isinstance(txt, pd.Series) or txt.eq("").all():
        txt = df.get("original_description", "")
    df["text"] = txt.fillna("").astype(str)
    return df

def extract_transfer_candidates(df: pd.DataFrame) -> pd.DataFrame:
    names = []
    for t in df["text"]:
        m = RE_TRANSFER.search(t)
        if m:
            direction = m.group(2).capitalize()
            who = title_person(m.group(3))
            names.append((direction, who))
        else:
            # fallback: any "to|from NAME" pattern
            m2 = RE_TOFROM.search(t)
            if m2:
                names.append((None, title_person(m2.group(1))))
            else:
                names.append((None, None))
    out = pd.DataFrame(names, columns=["direction","who"])
    out["who_norm"] = out["who"].fillna("").str.strip()
    out = out[out["who_norm"] != ""]
    return out

def extract_p2p_candidates(df: pd.DataFrame) -> pd.DataFrame:
    prov = []
    who_list = []
    for t in df["text"]:
        if not t: 
            prov.append(None); who_list.append(None); continue
        has = RE_P2P.search(t)
        if not has:
            prov.append(None); who_list.append(None); continue
        provider = has.group(1)
        # try nearest to/from
        m = RE_TOFROM.search(t)
        who = title_person(m.group(1)) if m else None
        prov.append(provider.title())
        who_list.append(who)
    out = pd.DataFrame({"provider": prov, "counterparty": who_list})
    out["counterparty_norm"] = out["counterparty"].fillna("").str.strip()
    return out[(out["provider"].notna()) & (out["counterparty_norm"] != "")]

def main():
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT
    df = read_unknowns(path)
    n_all = len(df)
    print(f"Loaded {n_all} unknown rows from: {path}")

    # 1) Transfer-style candidates
    transfers = extract_transfer_candidates(df)
    top_transfers = (
        transfers.groupby("who_norm")
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    # 2) P2P-style candidates
    p2p = extract_p2p_candidates(df)
    top_p2p = (
        p2p.groupby(["provider","counterparty_norm"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    # 3) Quick token clusters for the rest (brands hidden inside “unknown” rows)
    #    crude heuristic: keep alphabetic tokens of length >= 3
    def tokenize(s: str):
        return [w for w in re.findall(r"[A-Za-z][A-Za-z\-]{2,}", s)]
    tokens = []
    for t in df["text"]:
        for tok in tokenize(t):
            tokens.append(tok.lower())
    tok_series = pd.Series(tokens)
    top_tokens = (
        tok_series[tok_series.str.len() >= 3]
        .value_counts()
        .reset_index()
        .rename(columns={"index":"token", 0:"count"})
        .head(200)
    )

    # Write rule suggestions (transfers + p2p)
    rules_rows = []

    # transfers → Suggest “Transfer To/From X”
    for _, r in top_transfers.iterrows():
        who = r["who_norm"]
        cnt = int(r["count"])
        rules_rows.append({
            "pattern_type": "TRANSFER",
            "direction": "To/From",
            "provider": "",
            "counterparty": who,
            "suggested_merchant": f"Transfer {who}",
            "support_count": cnt
        })

    # p2p → Suggest “Zelle To X” etc.
    for _, r in top_p2p.iterrows():
        prov = r["provider"]
        who = r["counterparty_norm"]
        cnt = int(r["count"])
        rules_rows.append({
            "pattern_type": "P2P",
            "direction": "To/From",
            "provider": prov,
            "counterparty": who,
            "suggested_merchant": f"{prov} {who}",
            "support_count": cnt
        })

    rules_df = pd.DataFrame(rules_rows, columns=[
        "pattern_type","direction","provider","counterparty",
        "suggested_merchant","support_count"
    ]).sort_values(["support_count","pattern_type"], ascending=[False, True])

    # Write clusters file with top tokens (for manual inspection/ideas)
    clusters_df = top_tokens

    # Save files
    rules_df.to_csv(OUT_RULES, index=False, quoting=csv.QUOTE_MINIMAL)
    clusters_df.to_csv(OUT_CLUSTERS, index=False, quoting=csv.QUOTE_MINIMAL)

    # Console summary
    print("\n=== Summary ===")
    print(f"Unknown rows analyzed: {n_all}")
    print("\nTop candidates (Transfers):")
    print(top_transfers.head(15).to_string(index=False) if not top_transfers.empty else "(none)")
    print("\nTop candidates (P2P):")
    print(top_p2p.head(15).to_string(index=False) if not top_p2p.empty else "(none)")
    print(f"\nWrote rule suggestions: {OUT_RULES}")
    print(f"Wrote token clusters:    {OUT_CLUSTERS}")

if __name__ == "__main__":
    main()
