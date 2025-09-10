from flask import Blueprint, jsonify, request
import requests
from plaid_integration import create_link_token, exchange_public_token

plaiddev_bp = Blueprint("plaiddev", __name__)

@plaiddev_bp.post("/plaiddev/create_link_token")
def plaiddev_create_link_token():
    try:
        tok = create_link_token("user-1")
        if isinstance(tok, dict):
            # If upstream already returned a dict (maybe with error), pass it through
            status = 200 if tok.get("link_token") else tok.get("status", 200)
            return jsonify(tok), status
        return jsonify({"link_token": tok}), 200
    except requests.HTTPError as e:
        r = getattr(e, "response", None)
        status = r.status_code if r is not None else 502
        try:
            data = r.json() if r is not None else {"error": str(e)}
        except Exception:
            data = {"error": "plaid_http_error", "status": status, "text": (r.text[:400] if r is not None else str(e))}
        return jsonify(data), status
    except Exception as e:
        return jsonify({"error": "server_error", "detail": str(e)}), 500

@plaiddev_bp.post("/plaiddev/exchange_public_token")
def plaiddev_exchange_public_token():
    body = request.get_json(silent=True) or {}
    pub = body.get("public_token")
    if not pub:
        return jsonify({"error": "missing public_token"}), 400
    try:
        data = exchange_public_token(pub)
        ok = isinstance(data, dict) and ("access_token" in data or "item_id" in data)
        return jsonify(data), 200 if ok else 400
    except requests.HTTPError as e:
        r = getattr(e, "response", None)
        status = r.status_code if r is not None else 502
        try:
            data = r.json() if r is not None else {"error": str(e)}
        except Exception:
            data = {"error":"plaid_http_error","status":status,"text":(r.text[:400] if r is not None else str(e))}
        return jsonify(data), status
    except Exception as e:
        return jsonify({"error":"server_error","detail":str(e)}), 500

@plaiddev_bp.route("/plaiddev/sync_now", methods=["POST"])
def plaiddev_sync_now():
    from flask import request, jsonify
    import sqlite3, os
    from plaid_integration import plaid_sync_transactions

    payload = request.get_json(silent=True) or {}
    item_id = payload.get("item_id")

    # Default to most recent item if none provided
    if not item_id:
        import sqlite3
        db = os.environ.get("DBPATH") or os.path.join(os.path.dirname(__file__), "finance.db")
        row = sqlite3.connect(db).execute("SELECT item_id FROM plaid_items ORDER BY id DESC LIMIT 1").fetchone()
        if row:
            item_id = row[0]

    if not item_id:
        return jsonify({"ok": False, "error": "no_item", "detail": "No Plaid item found/selected."}), 400

    try:
        stats = plaid_sync_transactions(item_id)
        return jsonify({"ok": True, "item_id": item_id, **stats})
    except Exception as e:
        return jsonify({"ok": False, "error": "sync_failed", "detail": str(e)}), 500
