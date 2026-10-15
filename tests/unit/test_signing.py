"""Standard Webhooks signing and verification (design §8.2)."""

import base64
import json
import time
from datetime import UTC, datetime
from uuid import UUID, uuid4, uuid5

import pytest
from standardwebhooks import Webhook

from payments.schemas import PaymentStatus
from payments.webhooks.signing import (
    WEBHOOK_NAMESPACE,
    build_body,
    decode_secret,
    sign,
    verify_signature,
    webhook_id,
)
from tests.support.fakes import TEST_WEBHOOK_KEY, TEST_WEBHOOK_SECRET, make_payment

KEY = TEST_WEBHOOK_KEY


def _secret(raw: bytes) -> str:
    return "whsec_" + base64.b64encode(raw).decode()


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
    assert verify_signature(KEY, headers, body)


def test_signature_of_reference_library_is_accepted() -> None:
    body = b'{"type":"payment.succeeded"}'
    msg_id = "msg_1"
    timestamp = int(time.time())
    signature = Webhook(TEST_WEBHOOK_SECRET).sign(
        msg_id, datetime.fromtimestamp(timestamp, tz=UTC), body.decode()
    )
    assert sign(KEY, msg_id, timestamp, body) == signature
    assert verify_signature(KEY, _headers(msg_id, timestamp, signature), body)


def test_decode_secret() -> None:
    assert decode_secret(TEST_WEBHOOK_SECRET) == KEY
    assert decode_secret(_secret(bytes(24))) == bytes(24)
    assert decode_secret(_secret(bytes(range(64)))) == bytes(range(64))

    too_short = _secret(bytes(23))
    too_long = _secret(bytes(65))
    no_prefix = base64.b64encode(bytes(32)).decode()
    not_base64 = "whsec_!!!not base64!!!"
    for bad in (too_short, too_long, no_prefix, not_base64, "whsec_", ""):
        with pytest.raises(ValueError):  # noqa: PT011 - the exact message is not part of the contract
            decode_secret(bad)


def test_webhook_id_is_stable_uuid5() -> None:
    payment_id = UUID("11111111-2222-3333-4444-555555555555")
    expected = str(uuid5(WEBHOOK_NAMESPACE, f"{payment_id}:succeeded"))

    assert webhook_id(payment_id, PaymentStatus.SUCCEEDED) == expected
    assert webhook_id(payment_id, PaymentStatus.SUCCEEDED) == expected
    assert webhook_id(payment_id, PaymentStatus.FAILED) != expected
    assert webhook_id(uuid4(), PaymentStatus.SUCCEEDED) != expected
    assert UUID(expected).version == 5


def test_verify_rejects_old_timestamp() -> None:
    body = b"{}"
    now = 1_800_000_000
    signature = sign(KEY, "msg_1", now - 301, body)
    stale = _headers("msg_1", now - 301, signature)
    assert not verify_signature(KEY, stale, body, now=now)
    assert verify_signature(KEY, stale, body, now=now, tolerance_s=600)

    fresh = _headers("msg_1", now - 299, sign(KEY, "msg_1", now - 299, body))
    assert verify_signature(KEY, fresh, body, now=now)

    future = _headers("msg_1", now + 301, sign(KEY, "msg_1", now + 301, body))
    assert not verify_signature(KEY, future, body, now=now)


def test_verify_rejects_tampering() -> None:
    body = b'{"a":1}'
    now = 1_800_000_000
    headers = _headers("msg_1", now, sign(KEY, "msg_1", now, body))
    assert verify_signature(KEY, headers, body, now=now)

    assert not verify_signature(KEY, headers, b'{"a":2}', now=now)
    assert not verify_signature(bytes(32), headers, body, now=now)
    assert not verify_signature(KEY, {**headers, "webhook-id": "msg_2"}, body, now=now)
    assert not verify_signature(KEY, {**headers, "webhook-timestamp": str(now + 1)}, body, now=now)
    assert not verify_signature(KEY, {**headers, "webhook-signature": "v1,AAAA"}, body, now=now)
    assert not verify_signature(KEY, {"webhook-id": "msg_1"}, body, now=now)


def test_verify_accepts_any_listed_signature() -> None:
    body = b"{}"
    now = 1_800_000_000
    good = sign(KEY, "msg_1", now, body)
    headers = _headers("msg_1", now, f"v1,AAAA {good}")
    assert verify_signature(KEY, headers, body, now=now)


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
