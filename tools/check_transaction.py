# check_transaction.py
from database import get_db_connection

def find_transaction(search_term: str):
    """
    Connects to the database and prints the details for any transaction
    whose description contains the given search term (case-insensitive).
    """
    print(f"--- Searching for transactions containing '{search_term}' ---")
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = """
            SELECT id, transaction_date, cleaned_description, amount, category, sub_category
            FROM transactions 
            WHERE cleaned_description LIKE ?
        """
        # The '%' are wildcards, so it finds the term anywhere in the description
        params = (f'%{search_term}%',)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()

        if not rows:
            print("No matching transactions found in the database.")
            return

        print(f"Found {len(rows)} matching transaction(s):")
        for row in rows:
            print("-" * 20)
            print(f"  Transaction ID: {row['id']}")
            print(f"  Date: {row['transaction_date']}")
            print(f"  Description: {row['cleaned_description']}")
            print(f"  Amount: {row['amount']}")
            print(f"  Category: {row['category']}") # This is the crucial line
            print(f"  Sub-Category: {row['sub_category']}")
        print("-" * 20)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    # You can change 'Sunpass' to any other term you want to check
    find_transaction('Sunpass')