import os, json, sqlite3
from typing import Optional, List, Dict
from fernet_util import encrypt, decrypt

# DB helper
try:
    from database import get_db  # if present in your project
except Exception:
    def get_db():
        return sqlite3.connect(os.getenv("DB_PATH", "finance.db"))

# Plaid client
from plaid.api import plaid_api
from plaid import Configuration, ApiClient
from plaid.model.country_code import CountryCode
from plaid.model.products import Products
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions

def get_plaid_client():
    cfg = Configuration(
        host = {
            "sandbox":"https://sandbox.plaid.com",
            "development":"https://development.plaid.com",
            "production":"https://production.plaid.com"
        }[os.getenv("PLAID_ENV","sandbox").lower()],
        api_key = {
            "clientId": os.getenv("PLAID_CLIENT_ID",""),
            "secret": os.getenv("PLAID_SECRET",""),
        }
    )
    return plaid_api.PlaidApi(ApiClient(cfg))

# ---- Link flow ----
def create_link_token(user_id: str) -> str:
    client = get_plaid_client()
    req = LinkTokenCreateRequest(
        user=LinkTokenCreateRequestUser(client_user_id=user_id),
        client_name="The Accountant",
        products=[Products("transactions")],
        country_codes=[CountryCode("US")],
        language="en",
        redirect_uri=os.getenv("PLAID_REDIRECT_URI", None)
    )
    resp = client.link_token_create(req)
    return resp.to_dict()["link_token"]

def exchange_public_token(public_token: str) -> Dict:
    client = get_plaid_client()
    ex = client.item_public_token_exchange(
        ItemPublicTokenExchangeRequest(public_token=public_token)
    ).to_dict()
    access_token = ex["access_token"]
    item_id = ex["item_id"]

    con = get_db(); cur = con.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS plaid_items(
        id INTEGER PRIMARY KEY, item_id TEXT UNIQUE, access_token_enc TEXT, institution_name TEXT, created_at TEXT DEFAULT (datetime('now'))
      )
    """)
    cur.execute("INSERT OR REPLACE INTO plaid_items(item_id, access_token_enc) VALUES (?,?)",
                (item_id, encrypt(access_token)))
    con.commit(); con.close()
    return {"item_id": item_id}

# ---- Transactions fetches ----
def _get_access_token(item_id: str) -> Optional[str]:
    con = get_db(); cur = con.cursor()
    row = cur.execute("SELECT access_token_enc FROM plaid_items WHERE item_id=?", (item_id,)).fetchone()
    con.close()
    if not row: return None
    return decrypt(row[0])

def transactions_get_by_date(item_id: str, start: str, end: str, account_ids: Optional[List[str]]=None) -> Dict:
    token = _get_access_token(item_id)
    if not token: return {"error":"unknown item_id"}

    client = get_plaid_client()
    all_txns = []
    count, offset = 500, 0
    while True:
        opts = TransactionsGetRequestOptions(
            account_ids=account_ids or None,
            count=count, offset=offset,
            include_personal_finance_category=False
        )
        req = TransactionsGetRequest(access_token=token, start_date=start, end_date=end, options=opts)
        resp = client.transactions_get(req).to_dict()
        all_txns.extend(resp.get("transactions", []))
        total = resp.get("total_transactions", 0)
        offset += count
        if len(all_txns) >= total: break
    return {"transactions": all_txns, "total": len(all_txns)}
