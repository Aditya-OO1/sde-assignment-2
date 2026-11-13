"""PII protection.

* Phone numbers are stored as an HMAC-SHA256 under a secret key: events and leads can still be
  matched by equality, but the number cannot be recovered (or brute-forced, the 10-digit space
  is tiny) without the key. The last four digits are kept separately because the lead report
  only ever shows a masked number.
* Other PII fields (names...) are Fernet-encrypted as one JSON blob per lead.

The key comes from LEDGER_PII_KEY, else from a 0600 key file next to the database.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
from pathlib import Path

from cryptography.fernet import Fernet


class Vault:
    def __init__(self, key: bytes):
        self._fernet = Fernet(key)
        self._hmac_key = hashlib.sha256(b"phone-hash:" + key).digest()

    @classmethod
    def load(cls, key_path: Path) -> "Vault":
        env = os.environ.get("LEDGER_PII_KEY")
        if env:
            return cls(env.encode())
        if not key_path.exists():
            key_path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "wb") as f:
                f.write(Fernet.generate_key())
        return cls(key_path.read_bytes().strip())

    def hash_phone(self, canonical_phone: str) -> str:
        return hmac.new(self._hmac_key, canonical_phone.encode(), hashlib.sha256).hexdigest()

    def encrypt(self, data: dict) -> bytes:
        return self._fernet.encrypt(json.dumps(data, sort_keys=True).encode())

    def decrypt(self, blob: bytes) -> dict:
        return json.loads(self._fernet.decrypt(blob))
