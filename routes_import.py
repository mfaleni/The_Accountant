import io, csv, json, hashlib, os
import sqlite3
from flask import Blueprint, request, jsonify, send_file
import os, sqlite3

# Try to import only get_or_create_account; provide a fallback if it is missing.
try:
    from database import get_or_create_account  # type: ignore
except Exception:
    def get_or_create_account(cur, name, external_id=None):
        # Minimal fallback: ensure an account row exists by name and return its id.
        row = cur.execute("SELECT id FROM accounts WHERE name=? LIMIT 1", (name,)).fetchone()
        if row:
            try:
                return row["id"]
            except Exception:
                return row[0]
        cur.execute("INSERT INTO accounts(name) VALUES(?)", (name,))
        return cur.lastrowid

def get_db():
    # Resolve DB path from env or default to finance.db next to app.
    db = os.getenv("DATABASE_URL") or os.getenv("FINANCE_DB")
    if not db:
        from pathlib import Path as _P
        db = str(_P(__file__).with_name("finance.db"))
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    return con
  # your helpers
from plaid_integration import transactions_get_by_date

import_bp = Blueprint("staged_import", __name__)

def _rule_suggest(cat_rules, desc: str):
    d = (desc or "").lower().strip()
    for pat, cat in cat_rules:
        if pat and pat.lower() in d:
            return {"merchant": "", "category": cat, "sub_category": ""}
    return None

def _ai_suggest(desc, amount):
    if not os.getenv("ENABLE_AI") or os.getenv("ENABLE_AI") == "0":
        return None
    try:
        from openai import OpenAI
        client = OpenAI()
        prompt = (
            "Merchant+category suggestion.\n"
            f"Description: {desc}\n"
            f"Amount: {amount}\n"
            "Return JSON like {\"merchant\":\"\",\"category\":\"\",\"sub_category\":\"\"}."
        )
        msg = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role":"user","content":prompt}],
            temperature=0.1
        )
        data = json.loads(msg.choices[0].message.content.strip())
        return {"merchant": data.get("merchant",""), "category": data.get("category",""), "sub_category": data.get("sub_category","")}
    except Exception:
        return None

@import_bp.post("/import/plaid/start")
def import_plaid_start():
    p = request.get_json(force=True)
    item_id = p["item_id"]; start = p["start_date"]; end = p["end_date"]
    account_ids = p.get("account_ids")

    got = transactions_get_by_date(item_id, start, end, account_ids)
    if "error" in got: return jsonify(got), 400
    txns = got["transactions"]

    con = get_db(); cur = con.cursor()
    cur.execute("""INSERT INTO import_batches (source,item_id,account_ids,start_date,end_date,status)
                   VALUES ('plaid',?,?,?,?, 'raw')""",
                (item_id, json.dumps(account_ids or []), start, end))
    batch_id = cur.lastrowid

    ins = 0
    for t in txns:
        try:
            cur.execute("""
            INSERT OR IGNORE INTO import_raw
            (batch_id, plaid_txn_id, account_id, date, authorized_date, name, merchant_name, amount, pending, currency, original_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                batch_id, t.get("transaction_id"), t.get("account_id"),
                t.get("date"), t.get("authorized_date"),
                t.get("name"), t.get("merchant_name"),
                float(t.get("amount") or 0.0), 1 if t.get("pending") else 0,
                (t.get("iso_currency_code") or t.get("unofficial_currency_code") or "USD"),
                json.dumps(t)
            ))
            ins += cur.rowcount
        except Exception:
            pass
    con.commit(); con.close()
    return jsonify({"batch_id": batch_id, "raw_inserted": ins})

@import_bp.post("/import/suggest/<int:batch_id>")
def import_suggest(batch_id):
    con = get_db(); cur = con.cursor()
    rows = cur.execute("SELECT id,name,merchant_name,amount FROM import_raw WHERE batch_id=?", (batch_id,)).fetchall()
    rules = cur.execute("SELECT merchant_pattern, category FROM category_rules").fetchall()

    made = 0
    for rid, name, merch, amt in rows:
        s = _rule_suggest(rules, name or merch or "")
        source = "rule" if s else None
        if not s:
            s_ai = _ai_suggest(name or merch or "", amt)
            if s_ai:
                s = {"merchant": s_ai["merchant"], "category": s_ai["category"], "sub_category": s_ai.get("sub_category","")}
                source = "ai"
        if s:
            cur.execute("""
            INSERT OR REPLACE INTO import_suggestions
            (batch_id, raw_id, suggested_merchant, suggested_category, suggested_subcategory, confidence, source)
            VALUES (?,?,?,?,?,?,?)
            """, (batch_id, rid, s.get("merchant",""), s.get("category",""), s.get("sub_category",""),
                  0.9 if source=="rule" else 0.6, source))
            made += 1
    cur.execute("UPDATE import_batches SET status='suggested' WHERE id=?", (batch_id,))
    con.commit(); con.close()
    return jsonify({"batch_id": batch_id, "suggested": made})

@import_bp.get("/import/review/<int:batch_id>.json")
def import_review_json(batch_id):
    con = get_db(); cur = con.cursor()
    q = """
    SELECT r.id as raw_id, r.date, r.name, r.merchant_name, r.amount, r.pending,
           COALESCE(s.suggested_merchant,'') AS merchant,
           COALESCE(s.suggested_category,'') AS category,
           COALESCE(s.suggested_subcategory,'') AS sub_category,
           COALESCE(s.source,'') AS source
    FROM import_raw r
    LEFT JOIN import_suggestions s ON s.raw_id=r.id
    WHERE r.batch_id=?
    ORDER BY r.date, r.amount DESC
    """
    cols = [d[0] for d in cur.execute(q,(batch_id,)).description]
    rows = [dict(zip(cols, r)) for r in cur.execute(q,(batch_id,)).fetchall()]
    con.close()
    return jsonify({"batch_id": batch_id, "rows": rows})

@import_bp.get("/import/review/<int:batch_id>.csv")
def import_review_csv(batch_id):
    con = get_db(); cur = con.cursor()
    q = """
    SELECT r.date, r.name, r.merchant_name, r.amount, r.pending,
           COALESCE(s.suggested_merchant,''), COALESCE(s.suggested_category,''), COALESCE(s.suggested_subcategory,'')
    FROM import_raw r LEFT JOIN import_suggestions s ON s.raw_id=r.id
    WHERE r.batch_id=? ORDER BY r.date, r.amount DESC
    """
    rows = cur.execute(q,(batch_id,)).fetchall()
    con.close()
    out = io.StringIO(); w = csv.writer(out)
    w.writerow(["date","name","merchant_name","amount","pending","merchant","category","sub_category"])
    for r in rows: w.writerow(r)
    mem = io.BytesIO(out.getvalue().encode("utf-8")); mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=f"batch_{batch_id}_review.csv")

@import_bp.post("/import/commit/<int:batch_id>")
def import_commit(batch_id):
    p = request.get_json(force=True) if request.data else {}
    overrides = {int(o["raw_id"]): o for o in p.get("overrides", [])}
    dup_policy = (p.get("duplicate_policy") or "skip").lower()

    con = get_db(); cur = con.cursor()
    q = """
    SELECT r.id, r.date, r.name, r.merchant_name, r.amount, r.account_id, r.plaid_txn_id,
           COALESCE(s.suggested_merchant,''), COALESCE(s.suggested_category,''), COALESCE(s.suggested_subcategory,'')
    FROM import_raw r LEFT JOIN import_suggestions s ON s.raw_id=r.id
    WHERE r.batch_id=?
    """
    rows = cur.execute(q,(batch_id,)).fetchall()

    inserted = 0; skipped = 0
    for (raw_id, dt, nm, merch, amt, plaid_acct, plaid_txn_id, sug_merch, sug_cat, sug_sub) in rows:
        ov = overrides.get(int(raw_id))
        merchant = (ov and ov.get("merchant")) or (sug_merch or merch or nm or "")
        category = (ov and ov.get("category")) or (sug_cat or "")
        subcat   = (ov and ov.get("sub_category")) or (sug_sub or "")
        local_acct_id = get_or_create_account(con, plaid_acct or "Plaid Account")

        if dup_policy == "skip":
            exists = cur.execute("""
              SELECT 1 FROM transactions
              WHERE (plaid_txn_id = ? AND plaid_txn_id IS NOT NULL)
                 OR (transaction_date=? AND amount=? AND original_description=? AND account_id=?)
              LIMIT 1
            """, (plaid_txn_id, dt, amt, nm, local_acct_id)).fetchone()
            if exists:
                skipped += 1
                continue

        cur.execute("""
        INSERT INTO transactions
        (transaction_date, original_description, cleaned_description, amount, category, sub_category, account_id, transaction_id, unique_hash, plaid_txn_id)
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (dt, nm, merchant, float(amt), category, subcat, local_acct_id, plaid_txn_id,
              hashlib.sha256(f"{local_acct_id}|{dt}|{(nm or '').lower()}|{float(amt):.2f}|{plaid_txn_id or ''}".encode()).hexdigest(),
              plaid_txn_id))
        inserted += 1

    cur.execute("UPDATE import_batches SET status='committed' WHERE id=?", (batch_id,))
    con.commit(); con.close()
    return jsonify({"batch_id": batch_id, "inserted": inserted, "skipped": skipped})

@import_bp.post("/import/discard/<int:batch_id>")
def import_discard(batch_id):
    con = get_db(); cur = con.cursor()
    cur.execute("DELETE FROM import_suggestions WHERE batch_id=?", (batch_id,))
    cur.execute("DELETE FROM import_raw WHERE batch_id=?", (batch_id,))
    cur.execute("UPDATE import_batches SET status='discarded' WHERE id=?", (batch_id,))
    con.commit(); con.close()
    return jsonify({"ok": True})
