import os, base64, hashlib
from cryptography.fernet import Fernet

def _get_fernet_key_str() -> str:
    k = os.getenv("FERNET_KEY", "")
    if k:
        rem = len(k) % 4
        if rem:
            k += "=" * (4 - rem)
        try:
            base64.urlsafe_b64decode(k)
            return k
        except Exception:
            pass
    sec = (os.getenv("APP_SECRET") or "dev-secret").encode("utf-8")
    return base64.urlsafe_b64encode(hashlib.sha256(sec).digest()).decode()

_k = _get_fernet_key_str()
FERNET = Fernet(_k.encode("utf-8"))

def encrypt(s: str) -> str:
    return FERNET.encrypt(s.encode("utf-8")).decode("utf-8")
def decrypt(s: str) -> str:
    return FERNET.decrypt(s.encode("utf-8")).decode("utf-8")
