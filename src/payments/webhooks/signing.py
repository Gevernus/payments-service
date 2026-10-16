"""Standard Webhooks signing."""

import base64
import hashlib
import hmac
from datetime import datetime
from typing import Final, Literal
from uuid import UUID, uuid5

from pydantic import BaseModel

from payments.db.models import Payment
from payments.schemas import PaymentData, PaymentStatus

# Namespace of the deterministic webhook ids. Never change: ids must stay stable across releases.
WEBHOOK_NAMESPACE: Final = UUID("71f44a23-9950-4c4b-9653-aff5df960073")


class _WebhookBody(BaseModel):
    type: Literal["payment.succeeded", "payment.failed"]
    timestamp: datetime | None
    data: PaymentData


def webhook_id(payment_id: UUID, status: PaymentStatus) -> str:
    """Deterministic id of the webhook for a payment status."""
    return str(uuid5(WEBHOOK_NAMESPACE, f"{payment_id}:{status.value}"))


def _mac(key: bytes, msg_id: str, timestamp: int, body: bytes) -> bytes:
    content = f"{msg_id}.{timestamp}.".encode() + body
    return hmac.new(key, content, hashlib.sha256).digest()


def sign(key: bytes, msg_id: str, timestamp: int, body: bytes) -> str:
    """Return the `v1,<base64>` signature."""
    return "v1," + base64.b64encode(_mac(key, msg_id, timestamp, body)).decode()


def build_body(payment: Payment) -> bytes:
    """Serialize {"type", "timestamp", "data"} for the payment."""
    data = PaymentData.model_validate(payment)
    body = _WebhookBody(
        type="payment.succeeded" if data.status is PaymentStatus.SUCCEEDED else "payment.failed",
        timestamp=data.processed_at,
        data=data,
    )
    return body.model_dump_json().encode()
