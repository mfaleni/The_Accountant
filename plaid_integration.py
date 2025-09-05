from fernet_util import FERNET
import hashlib
import base64

def _make_fernet():
    # Prefer FERNET_KEY (must be 32 url-safe base64-encoded bytes / 44 chars)
    fk = os.getenv("FERNET_KEY", "").strip()
    if fk:
        return Fernet(fk.encode("utf-8"))
    # Else derive from APP_SECRET deterministically
    sec = os.getenv("APP_SECRET", "").encode("utf-8")
    if sec:
        key = base64.urlsafe_b64encode(digest) # 44-char base64
        return Fernet(key)
    # Last resort (ephemeral, not suitable for persisted tokens)
# plaid_integration.py
import os, hashlib
from typing import Optional, Dict, Any, List
from cryptography.fernet import Fernet
import sqlite3
from datetime import datetime

from plaid import Configuration, ApiClient
from plaid.api import plaid_api
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.model.transactions_sync_request import TransactionsSyncRequest

APP_SECRET = (os.getenv("APP_SECRET") or "dev-secret").encode()
# NOTE: For a proper static key: set APP_SECRET to a 32+ char random value and replace the above with Fernet(APP_SECRET32)

def get_plaid_client() -> plaid_api.PlaidApi:
    env = (os.getenv("PLAID_ENV") or "sandbox").lower()
    host = {
        "sandbox": "https://sandbox.plaid.com",
        "development": "https://development.plaid.com",
        "production": "https://production.plaid.com",
    }[env]
    config = Configuration(
        host=host,
        api_key={
            "clientId": os.getenv("PLAID_CLIENT_ID"),
            "secret": os.getenv("PLAID_SECRET"),
        },
        api_key_prefix={},
    )
    client = plaid_api.PlaidApi(ApiClient(config))
    return client

def get_db() -> sqlite3.Connection:
    import database
    return database.get_db_connection()

def encrypt(s: str) -> bytes:
    return FERNET.encrypt(s.encode())

def decrypt(b: bytes) -> str:
    return FERNET.decrypt(b).decode()

def create_link_token(user_id: str) -> str:
    client = get_plaid_client()
    req = LinkTokenCreateRequest(
        user=LinkTokenCreateRequestUser(client_user_id=user_id),
        client_name="The Accountant",
        products=[Products("transactions")],
        language="en",
        country_codes=[CountryCode("US")],
        redirect_uri=os.getenv("PLAID_REDIRECT_URI") or None,
    )
    resp = client.link_token_create(req)
    return resp.to_dict()["link_token"]

def exchange_public_token(public_token: str) -> Dict[str, Any]:
    client = get_plaid_client()
    resp = client.item_public_token_exchange(ItemPublicTokenExchangeRequest(public_token=public_token)).to_dict()
    # resp has access_token, item_id
    access_token = resp["access_token"]
    item_id = resp["item_id"]

    con = get_db(); cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO plaid_items(item_id, access_token, status) VALUES (?,?,?)",
        (item_id, encrypt(access_token), "active"),
    )
    con.commit()

    # pull accounts now
    accs = client.accounts_get(AccountsGetRequest(access_token=access_token)).to_dict()["accounts"]
    for a in accs:
        cur.execute("""
            INSERT OR REPLACE INTO plaid_accounts(plaid_account_id, item_id, name, official_name, mask, subtype)
            VALUES (?,?,?,?,?,?)
        """, (a["account_id"], item_id, a.get("name"), a.get("official_name"), a.get("mask"), a.get("subtype")))
    con.commit(); con.close()
    return {"item_id": item_id, "accounts": accs}

def _find_or_create_local_account_id(con: sqlite3.Connection, display_name: str) -> int:
    cur = con.cursor()
    from database import get_or_create_account
    return get_or_create_account(con, display_name)

def transactions_sync(item_id: str, backfill_days: int = 730) -> Dict[str, Any]:
    con = get_db(); cur = con.cursor()
    row = cur.execute("SELECT access_token, COALESCE(next_cursor,'') FROM plaid_items WHERE item_id=?", (item_id,)).fetchone()
    if not row: 
        con.close()
        return {"error": "unknown item_id"}
    access_token = decrypt(row[0])
    cursor = row[1] or None

    client = get_plaid_client()
    added: List[Dict[str, Any]] = []
    removed: List[str] = []
    has_more = True
    while has_more:
        req = TransactionsSyncRequest(access_token=access_token, cursor=cursor)
        resp = client.transactions_sync(req).to_dict()
        added.extend(resp.get("added", []))
        removed.extend([r.get("transaction_id") for r in resp.get("removed", []) if r.get("transaction_id")])
        cursor = resp.get("next_cursor")
        has_more = resp.get("has_more", False)

    # store new cursor
    cur.execute("UPDATE plaid_items SET next_cursor=? WHERE item_id=?", (cursor, item_id))

    # write transactions (idempotent by plaid_txn_id)
    ins = 0
    for t in added:
        date = t.get("date") or t.get("authorized_date")
        amt = float(t.get("amount") or 0)
        name_raw = (t.get("name") or "").strip()
        acc_id_plaid = t["account_id"]
        # map to your local accounts table
        acct_name = f"{t.get('account_owner') or t.get('account_id')}"
        local_acct_id = _find_or_create_local_account_id(con, acct_name)

        # category: if Plaid supplies categories, we do NOT write them (your rule: respect CSV & manual only)
        cur.execute("""
            INSERT OR IGNORE INTO transactions 
            (transaction_date, original_description, cleaned_description, amount, category, sub_category, account_id, transaction_id, unique_hash, plaid_txn_id)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            date, name_raw, "", amt, "", "", local_acct_id, t["transaction_id"],
            hashlib.sha256(f"{local_acct_id}|{date}|{name_raw.lower()}|{amt:.2f}|{t['transaction_id']}".encode()).hexdigest(),
            t["transaction_id"]
        ))
        ins += cur.rowcount

    con.commit(); con.close()
    return {"added": len(added), "inserted": ins, "removed": len(removed), "next_cursor": cursor}

from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions

def transactions_get_by_date(item_id: str, start: str, end: str, account_ids=None) -> dict:
  import database
  con = database.get_db_connection(); cur = con.cursor()
  row = cur.execute("SELECT access_token FROM plaid_items WHERE item_id=?", (item_id,)).fetchone()
  if not row:
    con.close(); return {"error":"unknown item_id"}
  access_token = row[0]
  client = get_plaid_client()
  all_txns = []; count = 500; offset = 0
  while True:
    opts = TransactionsGetRequestOptions(account_ids=account_ids or None, count=count, offset=offset, include_personal_finance_category=False)
    req  = TransactionsGetRequest(access_token=access_token, start_date=start, end_date=end, options=opts)
    resp = client.transactions_get(req).to_dict()
    all_txns.extend(resp.get("transactions", []))
    total = resp.get("total_transactions", 0)
    offset += count
    if len(all_txns) >= total: break
  con.close()
  return {"transactions": all_txns, "total": len(all_txns)}

FERNET = _make_fernet()
