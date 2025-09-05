# tests/test_database_add_df.py
import pandas as pd
import database as db

def test_add_transactions_df_uses_merchant_from_df_when_present(temp_db, monkeypatch):
    # Build a frame like intelligent_parser would return (no cleaned_description provided)
    df = pd.DataFrame([
        {"transaction_id":"m1","transaction_date":"2025-08-10","original_description":"ZELLE TO JOHN","amount":-50.00,"merchant":"Zelle To John"},
        {"transaction_id":"m2","transaction_date":"2025-08-10","original_description":"OPENAI","amount":-20.00,"merchant":"OpenAI"},
    ])
    added, skipped = db.add_transactions_df(df, "Account A")
    assert added == 2 and skipped == 0

    rows = db.fetch_transactions()
    by_id = {r["transaction_id"]: r for r in rows}
    assert by_id["m1"]["merchant"] == "Zelle To John"
    assert by_id["m2"]["merchant"] == "OpenAI"

def test_add_transactions_df_generates_txid_when_missing(temp_db):
    df = pd.DataFrame([
        {"transaction_date":"2025-08-09","original_description":"PUBLIX SOMETHING","amount":50.00}
    ])
    added, skipped = db.add_transactions_df(df, "Account B")
    assert added == 1 and skipped == 0
    rows = db.fetch_transactions()
    assert any(r["original_description"].startswith("PUBLIX") for r in rows)
