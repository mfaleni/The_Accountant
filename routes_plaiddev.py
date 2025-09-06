from flask import Blueprint, render_template, jsonify, request
try:
    from plaid_integration import create_link_token, exchange_public_token
except Exception:
    create_link_token = exchange_public_token = None

plaiddev_bp = Blueprint("plaiddev", __name__)

@plaiddev_bp.get("/plaiddev")
def plaiddev_page():
    return render_template("plaiddev.html")

@plaiddev_bp.post("/plaiddev/create_link_token")
def plaiddev_create_link_token():
    if not create_link_token:
        return jsonify({"error":"Plaid not configured"}), 501
    return jsonify({"link_token": create_link_token("user-1")})

@plaiddev_bp.post("/plaiddev/exchange_public_token")
def plaiddev_exchange_public_token():
    if not exchange_public_token:
        return jsonify({"error":"Plaid not configured"}), 501
    public_token = (request.json or {}).get("public_token")
    if not public_token:
        return jsonify({"error":"missing public_token"}), 400
    return jsonify(exchange_public_token(public_token))
