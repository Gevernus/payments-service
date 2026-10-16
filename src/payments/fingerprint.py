"""Request fingerprint used to detect idempotency-key reuse."""

import hashlib
import json

from payments.schemas import PaymentCreate

_FIELDS = {"amount", "currency", "description", "metadata", "webhook_url"}


def fingerprint(payload: PaymentCreate) -> str:
    """Return the sha256 hex digest of the validated request fields.

    The hash covers the validated model, so `100` and `"100.00"` agree; metadata is hashed as
    parsed, so `1` and `1.0` in it are different requests. The field list is explicit: a new
    optional schema field does not change the fingerprint of existing requests.
    """
    canonical = json.dumps(
        payload.model_dump(mode="json", include=_FIELDS), sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()
