
# --- BEGIN plaid env resolver ---
def _plaid_host_from_env():
    import os, plaid
    env = (os.environ.get("PLAID_ENV","sandbox") or "sandbox").lower()

    def pick(*names):
        for n in names:
            if hasattr(plaid.Environment, n):
                return getattr(plaid.Environment, n)
        # final fallback
        return getattr(plaid.Environment, "Sandbox")

    if env in ("prod","production"):     # prefer prod
        return pick("Production","PRODUCTION")
    if env in ("dev","development"):     # some SDKs lack Development; fall back to Sandbox
        return pick("Development","DEVELOPMENT","Sandbox")
    return pick("Sandbox","SANDBOX")
# --- END plaid env resolver ---

# app/services/plaid_client.py  (or whatever path you use for this module)

import os
import sqlite3
import plaid
from flask import current_app
from plaid.api import plaid_api
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from fernet_util import FERNET

# --- Normalize environment: map legacy values to supported hosts ---
_raw = os.getenv("PLAID_ENV", "sandbox").lower()
_env_alias = {"dev": "production", "development": "production", "prod": "production"}
env = _env_alias.get(_raw, _raw)

host_map = {
    "sandbox": plaid.Environment.Sandbox,
    "production": plaid.Environment.Production,
}
if env not in host_map:
    raise ValueError(f"Unsupported PLAID_ENV '{_raw}'. Use 'sandbox' or 'production'.")

configuration = plaid.Configuration(
    host=host_map[env],
    api_key={
        "clientId": os.getenv("PLAID_CLIENT_ID"),
        "secret": os.getenv("PLAID_SECRET"),
    },
)
api_client = plaid.ApiClient(configuration)
plaid_client = plaid_api.PlaidApi(api_client)

def _get_db_connection():
    """Return a short-lived SQLite connection (thread-safe-ish for background work)."""
    conn = sqlite3.connect(current_app.config['DATABASE'], check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
    return conn

def create_link_token(user_id: str) -> str:
    """Creates a Plaid Link token for a given user."""
    try:
        req = LinkTokenCreateRequest(
            user=LinkTokenCreateRequestUser(client_user_id=str(user_id)),
            client_name="The 305 Accountant",
            products=[Products("transactions")],
            country_codes=[CountryCode("US")],
            language="en",
            redirect_uri=os.getenv("PLAID_REDIRECT_URI") or None,
        )
        resp = plaid_client.link_token_create(req).to_dict()
        return resp["link_token"]
    except plaid.ApiException as e:
        print(f"[Plaid] link_token_create failed: {getattr(e, 'body', None) or e}")
        raise


def exchange_public_token(public_token: str) -> dict:
    """Exchanges a public token for an access token and saves it."""
    try:
        request = ItemPublicTokenExchangeRequest(public_token=public_token)
        response = plaid_client.item_public_token_exchange(request)

        access_token = response['access_token']
        item_id = response['item_id']

        encrypted_token = FERNET.encrypt(access_token.encode("utf-8")).decode("utf-8")

        with _get_db_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS plaid_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id TEXT UNIQUE NOT NULL,
                    access_token_enc TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.execute(
                "INSERT INTO plaid_items (item_id, access_token_enc) VALUES (?, ?) "
                "ON CONFLICT(item_id) DO UPDATE SET access_token_enc=excluded.access_token_enc",
                (item_id, encrypted_token)
            )
            conn.commit()

        return {"item_id": item_id, "error": None}
    except (plaid.ApiException, sqlite3.Error) as e:
        print(f"Error during token exchange: {e}")
        raise


def transactions_get_by_date(*args, **kwargs):
    """Placeholder for legacy compatibility."""
    return {"transactions": [], "error": "This function is deprecated."}


# === appended by patch (safe override) ===

def exchange_public_token(public_token, selected_accounts=None, institution=None):
    """
    Exchange public_token, store item (encrypted token) and selected accounts.
    Works with schema:
      - plaid_items(id,item_id,access_token_enc,institution_name,status,next_cursor,created_at)
      - plaid_accounts(id,plaid_account_id,item_id,name,official_name,mask,subtype,account_id)
    """
    import os, sqlite3, plaid
    from plaid.api import plaid_api
    from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
    from fernet_util import FERNET

    # Build client inline (avoid dependency on get_plaid_client)
    env = (os.environ.get("PLAID_ENV","sandbox") or "sandbox").lower()
    host = _plaid_host_from_env()
    configuration = plaid.Configuration(
        host=host,
        api_key={
            "clientId": os.environ["PLAID_CLIENT_ID"],
            "secret":   os.environ["PLAID_SECRET"],
        },
    )
    api_client = plaid.ApiClient(configuration)
    client = plaid_api.PlaidApi(api_client)

    inst_name = None
    if isinstance(institution, dict):
        inst_name = institution.get("name") or institution.get("institution_name")

    # Exchange
    req = ItemPublicTokenExchangeRequest(public_token=public_token)
    resp = client.item_public_token_exchange(req)
    access_token = resp["access_token"] if isinstance(resp, dict) else getattr(resp, "access_token", None)
    item_id      = resp["item_id"]      if isinstance(resp, dict) else getattr(resp, "item_id", None)
    if not access_token or not item_id:
        raise RuntimeError("Plaid did not return access_token/item_id")

    enc = FERNET.encrypt(access_token.encode()).decode()

    # DB path (fallback to finance.db)
    try:
        from app import app as _app
        DB_PATH = (_app.config.get("DATABASE") or "finance.db")
    except Exception:
        DB_PATH = os.environ.get("DATABASE", "finance.db")

    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # Upsert into plaid_items (has created_at)
    cur.execute("""
        INSERT INTO plaid_items (item_id, access_token_enc, institution_name, status, created_at)
        VALUES (?, ?, COALESCE(?,''), 'linked', datetime('now'))
        ON CONFLICT(item_id) DO UPDATE SET
            access_token_enc=excluded.access_token_enc,
            institution_name=excluded.institution_name,
            status='linked'
    """, (item_id, enc, inst_name))

    # Save selected accounts into plaid_accounts (no created_at column there)
    saved = 0
    for a in (selected_accounts or []):
        if not isinstance(a, dict):
            continue
        acct_id = a.get("id") or a.get("account_id")
        if not acct_id:
            continue
        name = a.get("name") or a.get("official_name") or ""
        official_name = a.get("official_name") or ""
        mask = a.get("mask") or ""
        subtype = a.get("subtype") or a.get("type") or ""

        cur.execute("SELECT 1 FROM plaid_accounts WHERE plaid_account_id=?", (acct_id,))
        if cur.fetchone():
            cur.execute("""
                UPDATE plaid_accounts
                   SET item_id=?, name=?, official_name=?, mask=?, subtype=?, account_id=?
                 WHERE plaid_account_id=?
            """, (item_id, name, official_name, mask, subtype, acct_id, acct_id))
        else:
            cur.execute("""
                INSERT INTO plaid_accounts (plaid_account_id, item_id, name, official_name, mask, subtype, account_id)
                VALUES (?,?,?,?,?,?,?)
            """, (acct_id, item_id, name, official_name, mask, subtype, acct_id))
        saved += 1

        # Mirror a friendly label into 'accounts' (used by /api/accounts)
        display = (inst_name or "Bank") + " • " + (mask or (name[:12] or "?"))
        cur.execute("""
            INSERT INTO accounts (name)
            SELECT ? WHERE NOT EXISTS (SELECT 1 FROM accounts WHERE name=?)
        """, (display, display))

    conn.commit()
    conn.close()
    return {"item_id": item_id, "stored_accounts": saved}


# ---------------- Plaid sync (raw) ----------------
import json, sqlite3
from datetime import datetime, timedelta
from fernet_util import FERNET
from plaid.model.transactions_sync_request import TransactionsSyncRequest

DBPATH = os.environ.get("DBPATH") or os.path.join(os.path.dirname(__file__), "finance.db")

def _db():
    return sqlite3.connect(DBPATH)

def plaid_sync_transactions(item_id: str):
    """Fetch new/changed transactions via Transactions Sync and store into plaid_transactions_raw."""
    client = get_plaid_client()
    conn = _db()
    cur  = conn.cursor()

    # Ensure raw table exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS plaid_transactions_raw(
            id INTEGER PRIMARY KEY,
            transaction_id TEXT UNIQUE,
            item_id TEXT,
            account_id TEXT,
            account_name TEXT,
            name TEXT,
            merchant_name TEXT,
            amount REAL,
            currency TEXT,
            date TEXT,
            pending INTEGER,
            json TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)

    # get token & last cursor
    row = cur.execute("SELECT access_token_enc, COALESCE(next_cursor,'') FROM plaid_items WHERE item_id=?", (item_id,)).fetchone()
    if not row:
        raise RuntimeError(f"Unknown item_id {item_id}")
    enc, cursor = row
    access_token = FERNET.decrypt(enc.encode()).decode()

    added = 0
    modified = 0
    removed = 0
    loops = 0

    # helpful account-name map
    acct_name = {r[0]: (r[1] or r[0]) for r in cur.execute(
        "SELECT plaid_account_id, COALESCE(name,official_name) "
        "FROM plaid_accounts WHERE item_id=?", (item_id,)
    ).fetchall()}

    has_more = True
    while has_more:
        loops += 1
        req = TransactionsSyncRequest(access_token=access_token, cursor=(cursor or None), count=500)
        resp = client.transactions_sync(req).to_dict()

        for t in resp.get("added", []):
            cur.execute("""
                INSERT OR IGNORE INTO plaid_transactions_raw
                  (transaction_id,item_id,account_id,account_name,name,merchant_name,amount,currency,date,pending,json)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """, (
                t["transaction_id"],
                item_id,
                t.get("account_id"),
                acct_name.get(t.get("account_id")),
                t.get("name"),
                t.get("merchant_name"),
                float(t.get("amount") or 0),
                (t.get("iso_currency_code") or t.get("unofficial_currency_code")),
                t.get("date"),
                1 if t.get("pending") else 0,
                json.dumps(t),
            ))
            added += cur.rowcount

        for t in resp.get("modified", []):
            cur.execute("""
                UPDATE plaid_transactions_raw
                   SET name=?, merchant_name=?, amount=?, currency=?, date=?, pending=?, json=?
                 WHERE transaction_id=?
            """, (
                t.get("name"),
                t.get("merchant_name"),
                float(t.get("amount") or 0),
                (t.get("iso_currency_code") or t.get("unofficial_currency_code")),
                t.get("date"),
                1 if t.get("pending") else 0,
                json.dumps(t),
                t["transaction_id"],
            ))
            modified += cur.rowcount

        # record removals (keep a tombstone by marking pending=0 and zero amount if we had it)
        for r in resp.get("removed", []):
            cur.execute("UPDATE plaid_transactions_raw SET pending=0 WHERE transaction_id=?", (r["transaction_id"],))
            removed += cur.rowcount

        cursor   = resp.get("next_cursor") or cursor
        has_more = bool(resp.get("has_more"))
        cur.execute("UPDATE plaid_items SET next_cursor=? WHERE item_id=?", (cursor, item_id))
        conn.commit()

    return {"added": added, "modified": modified, "removed": removed, "loops": loops, "cursor": cursor}
# -------------- end Plaid sync (raw) --------------
