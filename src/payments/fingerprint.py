"""Request fingerprint used to detect idempotency-key reuse."""

from payments.schemas import PaymentCreate


def fingerprint(payload: PaymentCreate) -> str:
    """Return "v1:" followed by the sha256 hex digest of the canonical request (design §4.3)."""
    raise NotImplementedError
