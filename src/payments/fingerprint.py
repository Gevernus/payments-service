"""Request fingerprint used to detect idempotency-key reuse."""

import hashlib
import json

from payments.schemas import PaymentCreate


def fingerprint(payload: PaymentCreate) -> str:
    """Return "v1:" followed by the sha256 hex digest of the validated request (design §4.3).

    The hash covers the validated model, so `100` and `"100.00"` agree; metadata is hashed as
    parsed, so `1` and `1.0` in it are different requests.
    """
    canonical = json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return "v1:" + hashlib.sha256(canonical.encode()).hexdigest()
