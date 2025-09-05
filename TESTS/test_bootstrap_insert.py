# tests/test_bootstrap_insert.py
import sqlite3
from bootstrap_grail_once import insert_transactions, seed_rules_from_grail
from database import get_db_connection, initialize_database, apply_v1_compat_migrations

def test_bootstrap_insert_and_seed_rules(temp_db):
    conn = get_db_connection()
    try:
        rows = [
            {
                "transaction_id":"b1","date":"2025-08-01","amount":"-10.00","account":"Grail",
                "description":"ZELLE TO JANE REF 1","new_description":"Zelle To Jane Roe",
                "new_category":"Transfer","Sub_category":"P2P"
            },
            {
                "transaction_id":"b2","date":"2025-08-02","amount":"-20.00","account":"Grail",
                "description":"OPENAI","new_description":"OpenAI",
                "new_category":"Software","Sub_category":"AI"
            },
        ]
        added, skipped = insert_transactions(conn, rows)
        assert added == 2 and skipped == 0

        seeded = seed_rules_from_grail(conn, rows)
        assert seeded >= 2
    finally:
        conn.close()
