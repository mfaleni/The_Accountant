import pandas as pd
import sqlite3
import os
import hashlib
from collections import defaultdict
from database import initialize_database

# --- Configuration ---
DATABASE_PATH = os.path.join(os.path.dirname(__file__), 'finance.db')
CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), 'Cat_Final_Load_Updated_Cleaned.csv')

def get_db_connection():
    """Establishes a connection to the database."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def _make_unique_hash(account_id: int, date_s: str, cleaned_desc: str, amount_f: float) -> str:
    """Generates a consistent hash for a transaction."""
    cleaned_desc = cleaned_desc or ""
    basis = f"{account_id}|{date_s}|{cleaned_desc.lower().strip()}|{amount_f:.2f}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()

def load_data():
    """Loads data from the cleaned CSV into the database, creating it if necessary."""
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Error: The file '{os.path.basename(CSV_FILE_PATH)}' was not found.")
        return

    if not os.path.exists(DATABASE_PATH):
        print("Database not found. Initializing a new one from schema.sql...")
        initialize_database()
        print("Database initialized successfully.")

    print("--- Starting Manual Data Load (v3.1 - Syntax Fix) ---")
    conn = get_db_connection()
    cursor = conn.cursor()

    # 1. Get or create accounts
    cursor.execute("SELECT id, name FROM accounts")
    accounts_map = {row['name'].strip().lower(): row['id'] for row in cursor.fetchall()}
    print(f"Found existing accounts: {list(accounts_map.keys())}")
    
    try:
        df_acc = pd.read_csv(CSV_FILE_PATH)
        unique_accounts = df_acc['account'].unique()
        new_accounts_found = False
        for acc_name in unique_accounts:
            if acc_name.strip().lower() not in accounts_map:
                cursor.execute("INSERT OR IGNORE INTO accounts (name) VALUES (?)", (acc_name,))
                new_accounts_found = True
        if new_accounts_found:
            conn.commit()
            print("New accounts from CSV added to database.")
            # Re-fetch the accounts map
            cursor.execute("SELECT id, name FROM accounts")
            accounts_map = {row['name'].strip().lower(): row['id'] for row in cursor.fetchall()}
    except Exception as e:
        print(f"CRITICAL ERROR: Could not read or create accounts. Error: {e}")
        conn.close()
        return

    # 2. Read the transaction data
    df = df_acc
    df['description'] = df['new_description']
    df['category'] = df['new_category']
    print(f"Read {len(df)} rows from '{os.path.basename(CSV_FILE_PATH)}'.")

    # 3. Clear existing transactions for a clean load
    cursor.execute("DELETE FROM transactions")
    print("Cleared the 'transactions' table for a fresh load.")

    # 4. Insert transactions
    inserted_count = 0
    skipped_count = 0
    
    for _, row in df.iterrows():
        account_name_in_csv = row['account'].strip().lower()
        account_id = accounts_map.get(account_name_in_csv)

        if account_id:
            # CORRECTED INDENTATION FOR TRY/EXCEPT BLOCK
            try:
                transaction_date = pd.to_datetime(row['date']).strftime('%Y-%m-%d')
                amount = row['amount']
                original_description = row['description']
                cleaned_description = row['description']
                unique_hash = _make_unique_hash(account_id, transaction_date, cleaned_description, amount)

                cursor.execute(
                    """
                    INSERT INTO transactions (transaction_date, original_description, cleaned_description, amount, category, account_id, unique_hash)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (transaction_date, original_description, cleaned_description, amount, row['category'], account_id, unique_hash)
                )
                inserted_count += 1
            except sqlite3.IntegrityError:
                skipped_count += 1
            except Exception as e:
                print(f"Could not insert row: {row.to_dict()}. Error: {e}")
                skipped_count += 1
        else:
            print(f"Warning: Skipping row because account '{row['account']}' was not found.")
            skipped_count += 1

    conn.commit()
    conn.close()

    print("\n--- Manual Load Complete ---")
    print(f"✅ Successfully inserted: {inserted_count} transactions.")
    print(f"❌ Skipped: {skipped_count} transactions.")

if __name__ == '__main__':
    load_data()