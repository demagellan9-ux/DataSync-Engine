import hashlib
import json
from typing import Any


def hash_record(data: dict[str, Any]) -> str:
    """
    Stable SHA-256 fingerprint of a record's data fields.
    Keys sorted, values serialized with default=str for determinism
    across types (datetime, Decimal, etc.).
    """
    payload = json.dumps(data, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
