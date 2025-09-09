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
    """Gets a database connection using the app context."""
    return sqlite3.connect(current_app.config["DATABASE"])

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
    conn = None
    try:
        resp = plaid_client.item_public_token_exchange(
            ItemPublicTokenExchangeRequest(public_token=public_token)
        ).to_dict()

        access_token = resp["access_token"]
        item_id = resp["item_id"]

        encrypted_token = FERNET.encrypt(access_token.encode("utf-8")).decode("utf-8")
        conn = _get_db_connection()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS plaid_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT UNIQUE NOT NULL,
                access_token_enc TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.execute(
            """INSERT INTO plaid_items (item_id, access_token_enc)
               VALUES (?, ?)
               ON CONFLICT(item_id) DO UPDATE SET access_token_enc=excluded.access_token_enc""",
            (item_id, encrypted_token),
        )
        conn.commit()
        return {"item_id": item_id, "error": None}
    except (plaid.ApiException, sqlite3.Error) as e:
        print(f"[Plaid] token exchange failed: {getattr(e, 'body', None) or e}")
        raise
    finally:
        if conn:
            conn.close()

def transactions_get_by_date(*args, **kwargs):
    """Placeholder for legacy compatibility."""
    return {"transactions": [], "error": "This function is deprecated."}
