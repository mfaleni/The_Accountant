import os, json, sqlite3, requests
from fernet_util import FERNET

def _base_url():
    env = (os.getenv("PLAID_ENV") or "sandbox").lower()
    return {
        "sandbox": "https://sandbox.plaid.com",
        "development": "https://development.plaid.com",
        "production": "https://production.plaid.com",
    }.get(env, "https://sandbox.plaid.com")

def _creds():
    cid = os.getenv("PLAID_CLIENT_ID")
    sec = os.getenv("PLAID_SECRET")
    if not cid or not sec:
        raise RuntimeError("PLAID_CLIENT_ID/PLAID_SECRET not set")
    return cid, sec

def _get_db():
    db = os.getenv("DATABASE_URL") or os.getenv("FINANCE_DB")
    if not db:
        from pathlib import Path
        db = str(Path(__file__).with_name("finance.db"))
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    return con

def _ensure_tables():
    con = _get_db(); cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS plaid_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_id TEXT UNIQUE,
      access_token_enc TEXT NOT NULL,
      webhook TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS plaid_accounts (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_id TEXT,
      account_id TEXT,
      mask TEXT,
      name TEXT,
      official_name TEXT,
      type TEXT,
      subtype TEXT
    );
    """)
    con.commit(); con.close()

def create_link_token(user_id: str) -> str:
    cid, sec = _creds()
    body = {
        "client_id": cid,
        "secret": sec,
        "client_name": "The Accountant",
        "user": {"client_user_id": str(user_id)},
        "products": ["transactions"],
        "country_codes": ["US"],
        "language": "en"
    }
    wh = os.getenv("PLAID_WEBHOOK_URL");  ru = os.getenv("PLAID_REDIRECT_URI")
    if wh: body["webhook"] = wh
    if ru: body["redirect_uri"] = ru
    r = requests.post(_base_url()+"/link/token/create", json=body, timeout=30)
    r.raise_for_status()
    return r.json()["link_token"]

def exchange_public_token(public_token: str) -> dict:
    _ensure_tables()
    cid, sec = _creds()
    r = requests.post(_base_url()+"/item/public_token/exchange",
                      json={"client_id": cid, "secret": sec, "public_token": public_token},
                      timeout=30)
    r.raise_for_status()
    data = r.json()
    access_token = data["access_token"]
    item_id = data["item_id"]
    enc = FERNET.encrypt(access_token.encode("utf-8")).decode("utf-8")
    con = _get_db(); cur = con.cursor()
    cur.execute("""INSERT INTO plaid_items(item_id,access_token_enc,webhook)
                   VALUES(?,?,?)
                   ON CONFLICT(item_id) DO UPDATE SET access_token_enc=excluded.access_token_enc""",
                (item_id, enc, os.getenv("PLAID_WEBHOOK_URL") or None))
    con.commit(); con.close()
    return {"item_id": item_id}

# Keep legacy imports happy
def transactions_get_by_date(*args, **kwargs):
    return []
