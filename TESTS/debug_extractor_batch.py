#!/usr/bin/env python3
import sys, json, requests, pandas as pd

RAW_COLS = [
    "original_description","description","cleaned_description",
    "details","narrative","memo","payee","name","transaction_description"
]

def row_to_raw(r):
    parts=[]
    for c in RAW_COLS:
        if c in r and pd.notna(r[c]):
            s=str(r[c]).strip()
            if s: parts.append(s)
    return " | ".join(parts)

def main():
    if len(sys.argv)<2:
        print("Usage: python tools/debug_extractor_batch.py /path/to/batch.csv [limit]")
        sys.exit(2)
    path=sys.argv[1]
    limit=int(sys.argv[2]) if len(sys.argv)>2 else 200
    df=pd.read_csv(path)
    if df.empty: 
        print("CSV empty."); return
    df=df.head(limit).copy()
    df["__raw__"]=df.apply(row_to_raw, axis=1)

    url="http://127.0.0.1:5056/api/debug/extract-merchants"
    out_rows=[]
    B=40
    for i in range(0, len(df), B):
        chunk=df.iloc[i:i+B]
        payload={"texts": chunk["__raw__"].tolist()}
        r=requests.post(url, json=payload, timeout=120)
        r.raise_for_status()
        data=r.json()
        for src,item in zip(chunk["__raw__"].tolist(), data.get("items",[])):
            out_rows.append({
                "source_text": src,
                "provider": item.get("provider"),
                "direction": item.get("direction"),
                "counterparty": item.get("counterparty"),
                "prefill_merchant": item.get("prefill_merchant"),
                "ai_merchant": item.get("ai_merchant"),
                "final_decision": item.get("final_decision"),
            })

    rep=pd.DataFrame(out_rows)
    print("\n=== Sample ===")
    print(rep.head(20).to_string(index=False))
    print("\nCounts:")
    print(rep["final_decision"].fillna("Unknown").str.strip().str.lower().value_counts().head(10))
    out_csv="/tmp/extractor_debug_report.csv"
    rep.to_csv(out_csv, index=False)
    print(f"\nWrote report: {out_csv}")

if __name__=="__main__":
    main()
