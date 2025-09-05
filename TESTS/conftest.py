# TESTS/conftest.py
import io
import os
import sys
import types
import pandas as pd
import pytest

# --- Make project importable ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

@pytest.fixture
def temp_db(monkeypatch, tmp_path):
    # patch DB path before importing app
    import database as db
    test_db = str(tmp_path / "test_finance.db")
    monkeypatch.setattr(db, "DB_PATH", test_db, raising=False)
    db.initialize_database()
    db.apply_v1_compat_migrations()
    return test_db

@pytest.fixture
def app_client(temp_db, monkeypatch):
    import database as db

    # minimal parser: read CSV bytes into a DataFrame
    def fake_intelligent_parser(b: io.BytesIO) -> pd.DataFrame:
        b.seek(0)
        try:
            return pd.read_csv(b)
        except Exception:
            return pd.DataFrame()

    # mock merchant extractor(s) to avoid network and be deterministic
    import ai_merchant_extractor as ame

    def fake_extract_merchants_from_dataframe(frame: pd.DataFrame, use_columns=None, **kw):
        out = []
        cols = use_columns or []
        def pick(r):
            for c in cols + ["original_description", "description", "cleaned_description"]:
                if c in r and str(r[c]).strip():
                    return str(r[c]).strip()
            return ""
        for r in frame.to_dict(orient="records"):
            t = pick(r).lower()
            if "zelle to john doe" in t:
                out.append("Zelle To John Doe")
            elif "venmo from @maria" in t:
                out.append("Venmo @maria")
            elif "openai" in t:
                out.append("OpenAI")
            elif "zelle to" in t:
                who = t.split("zelle to",1)[1].split("ref")[0].strip().title()
                out.append(f"Zelle To {who}" if who else "Zelle")
            elif "zelle from" in t:
                who = t.split("zelle from",1)[1].split("ref")[0].strip().title()
                out.append(f"Zelle From {who}" if who else "Zelle")
            else:
                out.append("Unknown")
        return pd.Series(out)

    def fake_extract_merchant_names(texts, **kw):
        res = []
        for t in texts:
            s = (t or "").lower()
            if "openai" in s:
                res.append("OpenAI")
            elif "zelle to" in s and "jane" in s:
                res.append("Zelle To Jane Roe")
            else:
                res.append("Unknown")
        return res

    monkeypatch.setattr(ame, "extract_merchants_from_dataframe", fake_extract_merchants_from_dataframe, raising=False)
    monkeypatch.setattr(ame, "extract_merchant_names", fake_extract_merchant_names, raising=False)

    # mock AI categorizer used in /api/upload
    fake_ai_cat = types.SimpleNamespace(
        categorize_transactions_with_ai=lambda: {"status": "ok", "message": "categorized 0"}
    )

    # import app after patches
    import app as app_mod
    monkeypatch.setattr(app_mod, "intelligent_parser", fake_intelligent_parser, raising=False)
    monkeypatch.setattr(app_mod, "ai_categorizer", fake_ai_cat, raising=False)

    return app_mod.app.test_client()
