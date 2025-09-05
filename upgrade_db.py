# upgrade_db.py (v2 - Now creates the full schema)
from database import get_db_connection, initialize_database # Import the initializer

def setup_database():
    """
    Ensures the database is fully created from the schema and then applies
    any necessary upgrades like adding new columns.
    """
    print("--- Running Database Setup & Upgrade ---")
    
    # 1. First, ensure all tables are created as per the latest schema
    #    This will build the database if it's empty.
    print("Initializing database schema...")
    initialize_database()
    print("✅ Base schema created/verified.")

    # 2. Then, perform any specific upgrades
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        
        # Check if the sub_category column already exists
        cursor.execute("PRAGMA table_info(transactions)")
        columns = [row['name'] for row in cursor.fetchall()]
        
        if 'sub_category' not in columns:
            print("Applying upgrade: Adding 'sub_category' column...")
            cursor.execute("ALTER TABLE transactions ADD COLUMN sub_category TEXT")
            conn.commit()
            print("✅ Column 'sub_category' added successfully.")
        else:
            print("✅ 'sub_category' column already exists.")
            
    except Exception as e:
        print(f"❌ An error occurred during database upgrade: {e}")
    finally:
        conn.close()
        print("--- Setup & Upgrade Complete ---")

if __name__ == '__main__':
    setup_database()