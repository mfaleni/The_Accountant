import os, base64, hashlib
from cryptography.fernet import Fernet

def _urlsafe_b64_pad(s: str) -> str:
    need = (-len(s)) % 4
    return s + ("=" * need)

def _get_fernet_key_str() -> str:
    k = os.getenv("FERNET_KEY") or ""
    if k:
        k = _urlsafe_b64_pad(k)
        try:
            raw = base64.urlsafe_b64decode(k.encode("utf-8"))
            if len(raw) == 32:
                return k
        except Exception:
            pass
    sec = (os.getenv("APP_SECRET") or "").encode("utf-8")
    if sec:
        raw32 = hashlib.sha256(sec).digest()[:32]
        return base64.urlsafe_b64encode(raw32).decode("utf-8")
    return Fernet.generate_key().decode("utf-8")

FERNET = Fernet(_get_fernet_key_str().encode("utf-8"))
