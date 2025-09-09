import os
import plaid
from plaid.api import plaid_api
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.products import Products
from plaid.model.country_code import CountryCode
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from fernet_util import FERNET
import sqlite3
from flask import current_app

# Configure the Plaid client using the official library
plaid_env = os.getenv('PLAID_ENV', 'sandbox').lower()
host = plaid.Environment.Sandbox  # Default
if plaid_env == 'development':
    host = plaid.Environment.Development
elif plaid_env == 'production':
    host = plaid.Environment.Production

configuration = plaid.Configuration(
    host=host,
    api_key={
        'clientId': os.getenv('PLAID_CLIENT_ID'),
        'secret': os.getenv('PLAID_SECRET'),
    }
)
api_client = plaid.ApiClient(configuration)
plaid_client = plaid_api.PlaidApi(api_client)

def _get_db_connection():
    conn = sqlite3.connect(current_app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def create_link_token(user_id: str) -> str:
    try:
        request = LinkTokenCreateRequest(
            user=LinkTokenCreateRequestUser(client_user_id=str(user_id)),
            client_name="The 305 Accountant",
            products=[Products('transactions')],
            country_codes=[CountryCode('US')],
            language='en'
        )
        if os.getenv("PLAID_REDIRECT_URI"):
            request.redirect_uri = os.getenv("PLAID_REDIRECT_URI")

        response = plaid_client.link_token_create(request)
        return response['link_token']
    except plaid.ApiException as e:
        print(f"Plaid API Error creating link token: {e.body}")
        raise

def exchange_public_token(public_token: str) -> dict:
    try:
        request = ItemPublicTokenExchangeRequest(public_token=public_token)
        response = plaid_client.item_public_token_exchange(request)
        
        access_token = response['access_token']
        item_id = response['item_id']
        
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
            "INSERT INTO plaid_items (item_id, access_token_enc) VALUES (?, ?) ON CONFLICT(item_id) DO UPDATE SET access_token_enc=excluded.access_token_enc",
            (item_id, encrypted_token)
        )
        conn.commit()
        
        return {"item_id": item_id, "error": None}
    except plaid.ApiException as e:
        print(f"Plaid API Error exchanging token: {e.body}")
        return {"item_id": None, "error": "Plaid API error during token exchange."}
    except sqlite3.Error as e:
        print(f"Database Error: {e}")
        return {"item_id": None, "error": "Could not save Plaid item to database."}

def transactions_get_by_date(*args, **kwargs):
    return {"transactions": [], "error": "This function is deprecated."}

