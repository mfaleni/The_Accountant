import os, base64, hashlib
from cryptography.fernet import Fernet

def _make_fernet():
    fk = (os.getenv("FERNET_KEY") or "").strip()
    if fk:
        pad = (-len(fk)) % 4  # pad to multiple of 4
        fk2 = fk + ("=" * pad)
        try:
            raw = base64.urlsafe_b64decode(fk2.encode("utf-8"))
            if len(raw) == 32:
                return Fernet(fk2.encode("utf-8"))
        except Exception:
            pass
    sec = (os.getenv("APP_SECRET") or "").strip()
    if sec:
        raw32 = hashlib.sha256(sec.encode("utf-8")).digest()[:32]
        fk2 = base64.urlsafe_b64encode(raw32).decode("utf-8")
        return Fernet(fk2.encode("utf-8"))
    return Fernet(Fernet.generate_key())

FERNET = _make_fernet()

def encrypt(s: str) -> str:
    return FERNET.encrypt(s.encode("utf-8")).decode("utf-8")

def decrypt(s: str) -> str:
    return FERNET.decrypt(s.encode("utf-8")).decode("utf-8")
