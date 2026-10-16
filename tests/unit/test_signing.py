"""Standard Webhooks signing."""

import json
import time
from datetime import UTC, datetime
from uuid import UUID, uuid4, uuid5

import pytest
from standardwebhooks import Webhook
from standardwebhooks.webhooks import WebhookVerificationError

from payments.schemas import PaymentStatus
from payments.webhooks.signing import (
    WEBHOOK_NAMESPACE,
    build_body,
    sign,
    webhook_id,
)
from tests.support.fakes import TEST_WEBHOOK_KEY, TEST_WEBHOOK_SECRET, make_payment

KEY = TEST_WEBHOOK_KEY


def _headers(msg_id: str, timestamp: int, signature: str) -> dict[str, str]:
    return {
        "webhook-id": msg_id,
        "webhook-timestamp": str(timestamp),
        "webhook-signature": signature,
    }


def test_signing_matches_standard_webhooks() -> None:
    body = build_body(make_payment())
    msg_id = webhook_id(uuid4(), PaymentStatus.SUCCEEDED)
    timestamp = int(time.time())
    signature = sign(KEY, msg_id, timestamp, body)

    assert signature.startswith("v1,")
    headers = _headers(msg_id, timestamp, signature)
    # The reference library raises on any mismatch and returns the parsed payload otherwise.
    assert Webhook(TEST_WEBHOOK_SECRET).verify(body, headers) == json.loads(body)


def test_signature_of_reference_library_is_accepted() -> None:
    body = b'{"type":"payment.succeeded"}'
    msg_id = "msg_1"
    timestamp = int(time.time())
    signature = Webhook(TEST_WEBHOOK_SECRET).sign(
        msg_id, datetime.fromtimestamp(timestamp, tz=UTC), body.decode()
    )
    assert sign(KEY, msg_id, timestamp, body) == signature


def test_webhook_id_is_stable_uuid5() -> None:
    payment_id = UUID("11111111-2222-3333-4444-555555555555")
    expected = str(uuid5(WEBHOOK_NAMESPACE, f"{payment_id}:succeeded"))

    assert webhook_id(payment_id, PaymentStatus.SUCCEEDED) == expected
    assert webhook_id(payment_id, PaymentStatus.SUCCEEDED) == expected
    assert webhook_id(payment_id, PaymentStatus.FAILED) != expected
    assert webhook_id(uuid4(), PaymentStatus.SUCCEEDED) != expected
    assert UUID(expected).version == 5


def test_signature_is_bound_to_id_timestamp_and_body() -> None:
    body = b'{"a":1}'
    now = int(time.time())
    headers = _headers("msg_1", now, sign(KEY, "msg_1", now, body))
    reference = Webhook(TEST_WEBHOOK_SECRET)
    reference.verify(body, headers)

    for body_, headers_ in (
        (b'{"a":2}', headers),
        (body, {**headers, "webhook-id": "msg_2"}),
        (body, {**headers, "webhook-timestamp": str(now + 1)}),
    ):
        with pytest.raises(WebhookVerificationError):
            reference.verify(body_, headers_)


def test_body_uses_payment_data_schema() -> None:
    payment = make_payment(status=PaymentStatus.FAILED)
    document = json.loads(build_body(payment))

    assert set(document) == {"type", "timestamp", "data"}
    assert document["timestamp"] == document["data"]["processed_at"]
    assert document["data"].keys() == {
        "payment_id",
        "amount",
        "currency",
        "description",
        "metadata",
        "status",
        "created_at",
        "processed_at",
    }
    data = document["data"]
    assert data["payment_id"] == str(payment.id)
    assert data["amount"] == "100.00"
    assert data["currency"] == "RUB"
    assert data["status"] == "failed"
    assert data["metadata"] == {"order_id": 42}
    assert "delivery" not in data
    assert "webhook_url" not in data
    assert "idempotency_key" not in data


@pytest.mark.parametrize(
    ("status", "event"),
    [
        (PaymentStatus.SUCCEEDED, "payment.succeeded"),
        (PaymentStatus.FAILED, "payment.failed"),
    ],
)
def test_body_type_names_the_final_status(status: PaymentStatus, event: str) -> None:
    assert json.loads(build_body(make_payment(status=status)))["type"] == event
