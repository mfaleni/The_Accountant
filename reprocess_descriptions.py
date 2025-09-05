# reprocess_descriptions.py (v2 - Corrected column name)
from database import get_db_connection
from ai_merchant_extractor import extract_merchant_names

def run_reprocessing():
    """
    Fetches all transactions, re-processes their descriptions using the AI
    name extractor, and updates them in the database.
    """
    print("--- Starting to re-process all existing transaction descriptions ---")
    conn = get_db_connection()
    
    try:
        print("Fetching all transactions from the database...")
        # CORRECTED: Changed 'description' to 'original_description' to match the schema
        transactions = conn.execute("SELECT id, original_description FROM transactions").fetchall()
        
        if not transactions:
            print("No transactions found to re-process.")
            return

        print(f"Found {len(transactions)} transactions to process.")
        
        # Create a list of just the descriptions to send to the AI
        descriptions_list = [t['original_description'] for t in transactions]
        
        # Call the AI extractor to get the new, improved names
        new_cleaned_names = extract_merchant_names(descriptions_list)
        
        # Prepare the data for a bulk update
        update_data = []
        for i, transaction in enumerate(transactions):
            update_data.append((new_cleaned_names[i], transaction['id']))

        print("Updating database with new descriptions...")
        cursor = conn.cursor()
        cursor.executemany(
            "UPDATE transactions SET cleaned_description = ? WHERE id = ?",
            update_data
        )
        conn.commit()
        print(f"✅ Successfully updated {cursor.rowcount} transactions.")

    except Exception as e:
        print(f"❌ An error occurred during reprocessing: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("--- Reprocessing complete ---")

if __name__ == '__main__':
    run_reprocessing()