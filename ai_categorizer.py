# ai_categorizer.py — decisive, self-learning categorizer (finals + rules)
import os
import json
import time
from typing import Dict, List, Tuple, Any, Optional

from dotenv import load_dotenv
from openai import OpenAI
from database import get_db_connection

load_dotenv()

# --- Config ---
API_KEY = os.getenv("OPENAI_API_KEY")
MODEL = os.getenv("OPENAI_CATEGORIZATION_MODEL", "gpt-4o")
BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "100"))
REQUEST_SLEEP_SEC = float(os.getenv("AI_REQUEST_SLEEP_SEC", "1.0"))
MAX_RETRIES = int(os.getenv("AI_MAX_RETRIES", "2"))  # first try + 2 retries

# --- Client ---
try:
    openai_client = OpenAI(api_key=API_KEY) if API_KEY else None
except Exception as e:
    print(f"Error initializing OpenAI client: {e}")
    openai_client = None


# ------------------------------ Helpers ------------------------------

SEED_CATEGORIES = [
    # used only if DB has no categories at all yet
    "Groceries","Dining","Transport","Bills & Utilities","General Merchandise",
    "Health & Wellness","Travel & Lodging","Entertainment","Home","Education",
    "Electronics","Card Payment","Income","Transfer","Savings","Bank Fees",
    "Financial Transactions","Uncategorized"
]

def _normalize(s: Optional[str]) -> str:
    return (s or "").strip()

def _lower(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def _trim64(s: str) -> str:
    return s[:64] if s and len(s) > 64 else s

def _merchant_text(row: dict) -> str:
    # Prefer explicit merchant, then cleaned_description
    return _lower(row.get("merchant") or row.get("cleaned_description") or "")

def _get_allowed_vocab(conn) -> Tuple[List[str], List[str]]:
    """
    Returns (allowed_categories, allowed_subcategories) learned from DB content.
    We prefer learned vocab so AI stays within your taxonomy.
    """
    cats = set()
    subcats = set()

    # From finalized transactions
    for r in conn.execute("SELECT DISTINCT category FROM transactions WHERE category IS NOT NULL AND TRIM(category) != ''"):
        cats.add(_normalize(r["category"]))
    for r in conn.execute("SELECT DISTINCT subcategory FROM transactions WHERE subcategory IS NOT NULL AND TRIM(subcategory) != ''"):
        subcats.add(_normalize(r["subcategory"]))

    # From rules (in case transactions don't yet include everything)
    for r in conn.execute("SELECT DISTINCT category FROM category_rules WHERE category IS NOT NULL AND TRIM(category) != ''"):
        cats.add(_normalize(r["category"]))
    for r in conn.execute("SELECT DISTINCT subcategory FROM category_rules WHERE subcategory IS NOT NULL AND TRIM(subcategory) != ''"):
        subcats.add(_normalize(r["subcategory"]))

    # Ensure at least fallback exists
    if not cats:
        cats.update(SEED_CATEGORIES)

    # Always allow Uncategorized
    cats.add("Uncategorized")
    return sorted(c for c in cats if c), sorted(s for s in subcats if s)


def _apply_local_rules_final(conn, rows: List[dict]) -> Tuple[int, List[dict]]:
    """
    Apply learned rules to FINAL fields (category/subcategory) for rows still uncategorized.
    Rule selection: match by substring on merchant/cleaned_description; prefer the LONGEST pattern.
    """
    remaining = []
    updated = 0

    # Pull rules once
    rules = conn.execute("""
        SELECT merchant_pattern, category, COALESCE(subcategory,'') AS subcat, COALESCE(merchant_canonical,'') AS mc
        FROM category_rules
        WHERE merchant_pattern IS NOT NULL AND TRIM(merchant_pattern) != ''
    """).fetchall()

    # Pre-sort rules by pattern length desc so we break on first (strongest) match
    srules = sorted(
        [dict(r) for r in rules],
        key=lambda r: len(_lower(r["merchant_pattern"])),
        reverse=True
    )

    for t in rows:
        text = _merchant_text(t)
        if not text:
            remaining.append(t)
            continue

        matched = None
        for r in srules:
            pat = _lower(r["merchant_pattern"])
            if pat and pat in text:
                matched = r
                break

        if matched:
            # Update finals; keep existing finals if user already set them (we only got rows with final empty)
            cat = _normalize(matched.get("category"))
            sub = _normalize(matched.get("subcat"))
            mc  = _normalize(matched.get("mc"))

            conn.execute(
                """
                UPDATE transactions
                   SET category = ?,
                       subcategory = CASE
                                       WHEN (subcategory IS NULL OR TRIM(subcategory)='') AND ? != ''
                                         THEN ?
                                       ELSE subcategory
                                     END,
                       ai_category = COALESCE(ai_category, ?),
                       ai_subcategory = COALESCE(ai_subcategory, NULLIF(?, '')),
                       merchant = CASE
                                    WHEN (merchant IS NULL OR TRIM(merchant)='') AND ? != ''
                                      THEN ?
                                    ELSE merchant
                                  END
                 WHERE transaction_id = ?
                """,
                (cat, sub, sub, cat, sub, mc, mc, str(t["transaction_id"]))
            )
            updated += 1
        else:
            remaining.append(t)

    conn.commit()
    return updated, remaining


def _build_batch_prompt(batch: List[dict], allowed_categories: List[str], allowed_subcats: List[str]) -> str:
    """
    Ask for decisive final category (and optional subcategory) for each transaction.
    We key by transaction_id to avoid ambiguity.
    """
    # Instruction block is opinionated and mirrors your original style
    PREAMBLE = f"""
You are a world-class financial analyst. For each transaction, assign a SINGLE best Category
from the allowed list. If a precise Subcategory is obvious AND is present in the allowed
subcategory list, include it; otherwise omit subcategory.

Rules:
- Use the merchant/description and the AMOUNT SIGN to infer type (e.g., card payment vs expense).
- Payments to cards/banks -> "Card Payment".
- Bank interest/fees -> "Bank Fees".
- If you truly cannot infer, use "Uncategorized".
- Do NOT invent new categories or subcategories. Only use the allowed lists.

Allowed Categories:
{json.dumps(allowed_categories, indent=2)}

Allowed Subcategories (optional; only pick from these):
{json.dumps(allowed_subcats[:200], indent=2)}  # (list may be long; top N shown)
""".strip()

    items = [
        {
            "transaction_id": str(t["transaction_id"]),
            "description": t.get("cleaned_description") or t.get("merchant") or "",
            "amount": float(t.get("amount") or 0.0)
        }
        for t in batch
    ]

    EXPECTED = """
Return ONLY valid JSON (no prose). Format ONE of the following:

EITHER (preferred if you can provide subcategories):
{
  "results": [
    {"transaction_id": "123", "category": "Dining", "subcategory": "Coffee"},
    {"transaction_id": "456", "category": "Travel & Lodging"}
  ]
}

OR (compat form without subcategories):
{
  "123": "Dining",
  "456": "Travel & Lodging"
}
""".strip()

    return f"{PREAMBLE}\n\nTransactions:\n{json.dumps(items, indent=2)}\n\n{EXPECTED}"


def _call_openai(prompt: str, enforce_json: bool = True) -> Optional[Dict[str, Any]]:
    """
    Call OpenAI; prefer JSON-enforced response. Fallback: non-enforced once.
    """
    if not openai_client:
        return None

    delay = REQUEST_SLEEP_SEC
    for attempt in range(MAX_RETRIES + 1):
        try:
            kwargs = {"model": MODEL, "messages": [{"role": "user", "content": prompt}]}
            if enforce_json:
                kwargs["response_format"] = {"type": "json_object"}
            resp = openai_client.chat.completions.create(**kwargs)
            content = resp.choices[0].message.content
            return json.loads(content)
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(delay)
                delay *= 2
                continue
            if enforce_json:
                # final fallback without enforcement
                try:
                    resp = openai_client.chat.completions.create(
                        model=MODEL, messages=[{"role": "user", "content": prompt}]
                    )
                    return json.loads(resp.choices[0].message.content)
                except Exception:
                    return None
            return None


def _parse_ai_result(data: Dict[str, Any]) -> Dict[str, Dict[str, Optional[str]]]:
    """
    Normalize either format into: { txid: {"category": cat, "subcategory": sub or None } }
    """
    out: Dict[str, Dict[str, Optional[str]]] = {}
    if not isinstance(data, dict):
        return out

    if "results" in data and isinstance(data["results"], list):
        for item in data["results"]:
            try:
                txid = str(item.get("transaction_id"))
                cat = _normalize(item.get("category"))
                sub = _normalize(item.get("subcategory")) or None
                if txid and cat:
                    out[txid] = {"category": cat, "subcategory": sub}
            except Exception:
                continue
        return out

    # compat: simple mapping { "123": "Dining" }
    simple = {}
    try:
        simple = {str(k): _normalize(v) for k, v in data.items()}
    except Exception:
        simple = {}
    for txid, cat in simple.items():
        if txid and cat:
            out[txid] = {"category": cat, "subcategory": None}
    return out


def _update_transactions_and_rules(conn, ai_map: Dict[str, Dict[str, Optional[str]]], batch: List[dict]) -> int:
    """
    Write FINAL category/subcategory and mirror to ai_*.
    Upsert a rule per row using the merchant text as pattern.
    """
    count = 0
    batch_index = {str(t["transaction_id"]): t for t in batch}

    for txid, rec in ai_map.items():
        row = batch_index.get(str(txid))
        if not row:
            continue

        cat = _normalize(rec.get("category"))
        sub = _normalize(rec.get("subcategory")) or None
        if not cat:
            continue

        merch_text_lower = _merchant_text(row)
        if not merch_text_lower:
            # still write category even if we couldn't form a pattern
            conn.execute(
                """
                UPDATE transactions
                   SET category=?,
                       subcategory=COALESCE(NULLIF(?,''), subcategory),
                       ai_category=?,
                       ai_subcategory=COALESCE(NULLIF(?, ''), ai_subcategory)
                 WHERE transaction_id=?
                """,
                (cat, sub or "", cat, sub or "", str(txid))
            )
            count += 1
            continue

        # Update finals + ai_* + backfill merchant if missing
        conn.execute(
            """
            UPDATE transactions
               SET category=?,
                   subcategory=COALESCE(NULLIF(?,''), subcategory),
                   ai_category=?,
                   ai_subcategory=COALESCE(NULLIF(?, ''), ai_subcategory),
                   merchant = CASE
                                WHEN (merchant IS NULL OR TRIM(merchant)='') THEN ?
                                ELSE merchant
                              END
             WHERE transaction_id=?
            """,
            (cat, sub or "", cat, sub or "", row.get("merchant") or row.get("cleaned_description") or "", str(txid))
        )

        # Upsert rule (pattern keyed by lower(description/merchant))
        pattern = _trim64(merch_text_lower)
        conn.execute(
            """
            INSERT INTO category_rules (merchant_pattern, category, subcategory, merchant_canonical)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(merchant_pattern)
            DO UPDATE SET category=excluded.category,
                          subcategory=COALESCE(excluded.subcategory, category_rules.subcategory),
                          merchant_canonical=COALESCE(excluded.merchant_canonical, category_rules.merchant_canonical)
            """,
            (pattern, cat, sub, row.get("merchant") or row.get("cleaned_description") or None)
        )
        count += 1

    conn.commit()
    return count


def _process_batch(conn, batch: List[dict], allowed_categories: List[str], allowed_subcats: List[str]) -> Tuple[int, Optional[str]]:
    prompt = _build_batch_prompt(batch, allowed_categories, allowed_subcats)
    data = _call_openai(prompt, enforce_json=True)
    if data is None:
        return 0, "OpenAI call failed or invalid JSON."

    try:
        ai_map = _parse_ai_result(data)
        if not ai_map:
            return 0, "AI returned empty mapping."
        updated = _update_transactions_and_rules(conn, ai_map, batch)
        return updated, None
    except Exception as e:
        return 0, f"DB update failed: {e}"


# ------------------------------ Public entrypoint ------------------------------

def categorize_transactions_with_ai() -> Dict[str, Any]:
    """
    Decisive categorization:
    - Applies local rules to fill FINAL category/subcategory.
    - For still-uncategorized rows, asks AI to decide FINAL category (+ optional subcategory).
    - Learns by upserting rules for each decision.
    - Never overwrites user-decided finals (we only operate on 'Uncategorized'/empty).
    """
    if not openai_client:
        return {"status": "error", "message": "OpenAI client is not initialized. Check your API key."}

    conn = get_db_connection()
    try:
        # Load allowed vocab from DB state
        allowed_categories, allowed_subcats = _get_allowed_vocab(conn)

        # Candidates: only those with empty/Uncategorized finals
        rows = conn.execute(
            """
            SELECT transaction_id, cleaned_description, merchant, amount
            FROM transactions
            WHERE category IS NULL OR TRIM(category)='' OR category='Uncategorized'
            ORDER BY transaction_date DESC, id DESC
            """
        ).fetchall()
        to_process = [dict(r) for r in rows]

        if not to_process:
            return {"status": "success", "message": "No uncategorized transactions to process."}

        # 1) Local rules first (final)
        local_count, remaining = _apply_local_rules_final(conn, to_process)
        if not remaining:
            return {"status": "success", "message": f"Categorized {local_count} transactions from learned rules."}

        # 2) AI in batches (final)
        total_ai = 0
        for i in range(0, len(remaining), BATCH_SIZE):
            batch = remaining[i : i + BATCH_SIZE]
            count, error = _process_batch(conn, batch, allowed_categories, allowed_subcats)
            if error:
                return {
                    "status": "partial",
                    "message": f"Rules: {local_count}, AI: {total_ai}. Error: {error}",
                }
            total_ai += count
            if REQUEST_SLEEP_SEC > 0:
                time.sleep(REQUEST_SLEEP_SEC)

        return {
            "status": "success",
            "message": f"Categorized {local_count} from rules and {total_ai} with AI. Learned rules have been updated.",
        }
    finally:
        conn.close()
