# backfill_zelle_merchants.py
import sqlite3, os, re
from parser import extract_zelle_to_from

BASE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(BASE, "finance.db")

con = sqlite3.connect(DB)
con.row_factory = sqlite3.Row
cur = con.cursor()

rows = cur.execute("""
  SELECT id, original_description, cleaned_description, merchant
  FROM transactions
  WHERE (merchant IS NULL OR TRIM(merchant) = '')
    AND original_description LIKE '%Zelle%' COLLATE NOCASE
""").fetchall()

updated = 0
for r in rows:
    newm = extract_zelle_to_from(r["original_description"] or "") or None
    if newm:
        cur.execute("UPDATE transactions SET merchant=? WHERE id=?", (newm, r["id"]))
        # If cleaned_description is blank, use merchant for readability
        if not (r["cleaned_description"] or "").strip():
            cur.execute("UPDATE transactions SET cleaned_description=? WHERE id=?", (newm, r["id"]))
        updated += 1

con.commit()
con.close()
print(f"Backfilled Zelle merchant for {updated} rows.")
