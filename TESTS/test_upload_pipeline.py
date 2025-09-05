# tests/test_upload_pipeline.py
import io
import csv
import sqlite3
import pandas as pd

def _csv_bytes(rows, header):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=header)
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue().encode("utf-8")

def test_upload_raw_first_extractor_applied(app_client):
    """
    /api/upload should:
      - call RAW-first merchant extractor
      - insert rows with those merchants
      - not write literal 'Unknown' as merchant
    """
    rows = [
        {"Account":"Fam Wells","transaction_id":"t1","date":"2025-08-08","original_description":"ZELLE TO JOHN DOE REF 123","amount":"-50.00"},
        {"Account":"Fam Wells","transaction_id":"t2","date":"2025-08-09","original_description":"VENMO FROM @maria pizza","amount":"25.00"},
        {"Account":"Fam Wells","transaction_id":"t3","date":"2025-08-10","original_description":"OPENAI SAN FRANCISCO","amount":"-20.00"},
    ]
    payload = {
        "account_id": "csv",
        "file": (io.BytesIO(_csv_bytes(rows, ["Account","transaction_id","date","original_description","amount"])), "batch.csv")
    }
    resp = app_client.post("/api/upload", data=payload, content_type="multipart/form-data")
    assert resp.status_code == 200, resp.get_data(as_text=True)

    # verify merchants were written (and not the raw 'Zelle' only)
    # check via API
    tx = app_client.get("/api/transactions").get_json()
    by_id = {t["transaction_id"]: t for t in tx}
    assert by_id["t1"]["merchant"].startswith("Zelle To "), by_id["t1"]
    assert by_id["t2"]["merchant"].startswith("Venmo"), by_id["t2"]
    assert by_id["t3"]["merchant"] == "OpenAI", by_id["t3"]
    # cleaned_description can still be original at this point (db’s job is merchant)

def test_unknown_guard_never_persists_unknown(app_client):
    """
    If AI returns 'Unknown', we should NOT store 'Unknown' in transactions.merchant.
    """
    # Craft a row that our fake extractor will return Unknown for
    rows = [
        {"Account":"Fam Wells","transaction_id":"u1","date":"2025-08-11","original_description":"UNSEEN THING 12345","amount":"-9.99"},
    ]
    payload = {
        "account_id": "csv",
        "file": (io.BytesIO(_csv_bytes(rows, ["Account","transaction_id","date","original_description","amount"])), "unk.csv")
    }
    resp = app_client.post("/api/upload", data=payload, content_type="multipart/form-data")
    assert resp.status_code == 200, resp.get_data(as_text=True)

    tx = app_client.get("/api/transactions").get_json()
    row = next(t for t in tx if t["transaction_id"] == "u1")
    # Either merchant is NULL (not present in JSON) or empty/absent key;
    # the API returns None as null in JSON, so just assert not literal 'Unknown'
    assert row.get("merchant") != "Unknown"

def test_date_normalization_and_signs(app_client):
    """
    Dates should be normalized to YYYY-MM-DD and amounts signed consistently.
    """
    rows = [
        {"Account":"Fam Wells","transaction_id":"d1","date":"08/11/2025","original_description":"PAYROLL DIRECT DEPOSIT","amount":"1000.00"},
        {"Account":"Fam Wells","transaction_id":"d2","date":"8/12/25","original_description":"PUBLIX SOMEWHERE","amount":"50.00"},
    ]
    payload = {
        "account_id": "csv",
        "file": (io.BytesIO(_csv_bytes(rows, ["Account","transaction_id","date","original_description","amount"])), "dates.csv")
    }
    resp = app_client.post("/api/upload", data=payload, content_type="multipart/form-data")
    assert resp.status_code == 200

    # Verify via API
    tx = app_client.get("/api/transactions").get_json()
    by_id = {t["transaction_id"]: t for t in tx}
    assert by_id["d1"]["transaction_date"] == "2025-08-11"
    assert by_id["d1"]["amount"] >= 0  # direct deposit → positive
    assert by_id["d2"]["transaction_date"] == "2025-08-12"

def test_p2p_fixer_endpoint_updates_cleaned_and_merchant(app_client):
    """
    Ensure /api/fix-p2p-merchants applies 'Zelle To/From ...' and also updates cleaned_description.
    """
    rows = [
        {"Account":"Fam Wells","transaction_id":"p2p1","date":"2025-08-13","original_description":"ZELLE TO JANE ROE REF abc","amount":"-42.00"},
        {"Account":"Fam Wells","transaction_id":"p2p2","date":"2025-08-13","original_description":"ZELLE FROM JOHN SMITH REF xyz","amount":"42.00"},
    ]
    payload = {
        "account_id": "csv",
        "file": (io.BytesIO(_csv_bytes(rows, ["Account","transaction_id","date","original_description","amount"])), "p2p.csv")
    }
    assert app_client.post("/api/upload", data=payload, content_type="multipart/form-data").status_code == 200

    # Now force the fixer
    resp = app_client.post("/api/fix-p2p-merchants?force=1&limit=5000")
    assert resp.status_code == 200

    # Check
    tx = app_client.get("/api/transactions").get_json()
    by_id = {t["transaction_id"]: t for t in tx}
    assert by_id["p2p1"]["merchant"].startswith("Zelle To ")
    assert by_id["p2p1"]["cleaned_description"].startswith("Zelle To ")
    assert by_id["p2p2"]["merchant"].startswith("Zelle From ")
    assert by_id["p2p2"]["cleaned_description"].startswith("Zelle From ")
