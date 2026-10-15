"""Request fingerprint used to detect idempotency-key reuse."""

import hashlib
import json
from decimal import Decimal
from typing import Any

from payments.schemas import PaymentCreate

# NUL is rejected in input strings, so the tag cannot collide with user data.
_NUM = "\x00num:"


def _canon(value: Any) -> Any:
    """Canonical form of a metadata value: numbers become tagged strings (1, 1.0 and 1.00 agree)."""
    if value is None or isinstance(value, bool | str):
        return value
    if isinstance(value, int | float):
        number = Decimal(repr(value)) if isinstance(value, float) else Decimal(value)
        return _NUM + format(Decimal(0) if number == 0 else number.normalize(), "f")
    if isinstance(value, list):
        return [_canon(item) for item in value]
    return {key: _canon(item) for key, item in value.items()}


def fingerprint(payload: PaymentCreate) -> str:
    """Return "v1:" followed by the sha256 hex digest of the canonical request (design §4.3)."""
    canonical = {
        "amount": format(payload.amount, "f"),
        "currency": payload.currency.value,
        "description": payload.description,
        "metadata": _canon(payload.metadata),
        "webhook_url": str(payload.webhook_url),
    }
    text = json.dumps(canonical, sort_keys=True, separators=(",", ":"))
    return "v1:" + hashlib.sha256(text.encode()).hexdigest()
