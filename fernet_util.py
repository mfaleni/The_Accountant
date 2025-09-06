import os
from cryptography.fernet import Fernet

def _build():
    k = os.getenv("FERNET_KEY")
    if not k:
        print("WARN: FERNET_KEY not set; storing tokens unencrypted.")
        return None
    try:
        return Fernet(k)
    except Exception:
        print("WARN: Invalid FERNET_KEY; storing tokens unencrypted.")
        return None

_F = _build()

def encrypt(s: str) -> str:
    if not s: return s
    if _F: return _F.encrypt(s.encode()).decode()
    return s

def decrypt(s: str) -> str:
    if not s: return s
    if _F:
        try:
            return _F.decrypt(s.encode()).decode()
        except Exception:
            pass
    return s
