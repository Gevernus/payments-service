"""Standard Webhooks signing and verification (design §8.2)."""

import base64
import binascii
import hashlib
import hmac
import time
from collections.abc import Mapping
from datetime import datetime
from typing import Final, Literal
from uuid import UUID, uuid5

from pydantic import BaseModel

from payments.db.models import Payment
from payments.schemas import PaymentData, PaymentStatus

# Namespace of the deterministic webhook ids. Never change: ids must stay stable across releases.
WEBHOOK_NAMESPACE: Final = UUID("71f44a23-9950-4c4b-9653-aff5df960073")

_SECRET_PREFIX: Final = "whsec_"  # noqa: S105 - a format marker, not a credential
_MIN_KEY_BYTES: Final = 24
_MAX_KEY_BYTES: Final = 64


class _WebhookBody(BaseModel):
    type: Literal["payment.succeeded", "payment.failed"]
    timestamp: datetime | None
    data: PaymentData


def decode_secret(value: str) -> bytes:
    """Decode a `whsec_`-prefixed base64 secret."""
    if not value.startswith(_SECRET_PREFIX):
        raise ValueError("webhook secret must start with 'whsec_'")
    try:
        key = base64.b64decode(value.removeprefix(_SECRET_PREFIX), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("webhook secret is not valid base64") from exc
    if not _MIN_KEY_BYTES <= len(key) <= _MAX_KEY_BYTES:
        raise ValueError(f"webhook secret must decode to {_MIN_KEY_BYTES}..{_MAX_KEY_BYTES} bytes")
    return key


def webhook_id(payment_id: UUID, status: PaymentStatus) -> str:
    """Deterministic id of the webhook for a payment status."""
    return str(uuid5(WEBHOOK_NAMESPACE, f"{payment_id}:{status.value}"))


def _mac(key: bytes, msg_id: str, timestamp: int, body: bytes) -> bytes:
    content = f"{msg_id}.{timestamp}.".encode() + body
    return hmac.new(key, content, hashlib.sha256).digest()


def sign(key: bytes, msg_id: str, timestamp: int, body: bytes) -> str:
    """Return the `v1,<base64>` signature."""
    return "v1," + base64.b64encode(_mac(key, msg_id, timestamp, body)).decode()


def verify_signature(
    key: bytes,
    headers: Mapping[str, str],
    body: bytes,
    *,
    tolerance_s: int = 300,
    now: int | None = None,
) -> bool:
    """Verify webhook headers against the body."""
    lowered = {name.lower(): value for name, value in headers.items()}
    msg_id = lowered.get("webhook-id")
    raw_timestamp = lowered.get("webhook-timestamp")
    header = lowered.get("webhook-signature")
    if msg_id is None or raw_timestamp is None or header is None:
        return False
    try:
        timestamp = int(raw_timestamp)
    except ValueError:
        return False
    current = int(time.time()) if now is None else now
    if abs(current - timestamp) > tolerance_s:
        return False
    expected = _mac(key, msg_id, timestamp, body)
    for candidate in header.split():
        version, _, encoded = candidate.partition(",")
        if version != "v1":
            continue
        try:
            received = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError):
            continue
        if hmac.compare_digest(received, expected):
            return True
    return False


def build_body(payment: Payment) -> bytes:
    """Serialize {"type", "timestamp", "data"} for the payment."""
    data = PaymentData.model_validate(payment)
    body = _WebhookBody(
        type="payment.succeeded" if data.status is PaymentStatus.SUCCEEDED else "payment.failed",
        timestamp=data.processed_at,
        data=data,
    )
    return body.model_dump_json().encode()
