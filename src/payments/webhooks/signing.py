"""Standard Webhooks signing and verification (design §8.2)."""

from collections.abc import Mapping
from typing import Final
from uuid import UUID

from payments.db.models import Payment
from payments.schemas import PaymentStatus

# Namespace of the deterministic webhook ids. Never change: ids must stay stable across releases.
WEBHOOK_NAMESPACE: Final = UUID("71f44a23-9950-4c4b-9653-aff5df960073")


def decode_secret(value: str) -> bytes:
    """Decode a `whsec_`-prefixed base64 secret."""
    raise NotImplementedError


def webhook_id(payment_id: UUID, status: PaymentStatus) -> str:
    """Deterministic id of the webhook for a payment status."""
    raise NotImplementedError


def sign(key: bytes, msg_id: str, timestamp: int, body: bytes) -> str:
    """Return the `v1,<base64>` signature."""
    raise NotImplementedError


def verify_signature(
    key: bytes,
    headers: Mapping[str, str],
    body: bytes,
    *,
    tolerance_s: int = 300,
    now: int | None = None,
) -> bool:
    """Verify webhook headers against the body."""
    raise NotImplementedError


def build_body(payment: Payment) -> bytes:
    """Serialize {"type", "timestamp", "data"} for the payment."""
    raise NotImplementedError
