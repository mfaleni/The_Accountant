import os
from cryptography.fernet import Fernet

FERNET_KEY = os.environ.get("FERNET_KEY")
if not FERNET_KEY:
    raise RuntimeError(
        "Missing FERNET_KEY. Generate with "
        "python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())' "
        "and set it in Render → Environment."
    )
FERNET = Fernet(FERNET_KEY.encode())
