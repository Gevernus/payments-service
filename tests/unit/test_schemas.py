"""Request and response schemas (design §4.3, §4.4)."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from payments.db.models import Payment
from payments.schemas import (
    Currency,
    DeliveryState,
    PaymentCreate,
    PaymentOut,
    PaymentStatus,
)


def body(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "amount": "100.00",
        "currency": "RUB",
        "webhook_url": "https://example.com/hook",
    }
    data.update(overrides)
    return data


def nested(depth: int) -> dict[str, Any]:
    value: dict[str, Any] = {"leaf": 1}
    for _ in range(depth - 1):
        value = {"k": value}
    return value


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_metadata_rejects_non_finite(bad: float) -> None:
    assert PaymentCreate.model_validate(body(metadata={"ok": 1.5}))
    for metadata in ({"x": bad}, {"x": [1, bad]}, {"x": {"y": bad}}):
        with pytest.raises(ValidationError):
            PaymentCreate.model_validate(body(metadata=metadata))


def test_metadata_limits() -> None:
    assert PaymentCreate.model_validate(body(metadata={"k": "x" * 1000, "n": nested(3)}))

    with pytest.raises(ValidationError):  # larger than 16 KiB serialized
        PaymentCreate.model_validate(body(metadata={"k": "x" * (16 * 1024 + 1)}))
    with pytest.raises(ValidationError):  # deeper than 10 levels
        PaymentCreate.model_validate(body(metadata=nested(25)))
    with pytest.raises(ValidationError):  # NUL in a value
        PaymentCreate.model_validate(body(metadata={"k": "a\u0000b"}))
    with pytest.raises(ValidationError):  # NUL in a key
        PaymentCreate.model_validate(body(metadata={"a\u0000b": "v"}))
    with pytest.raises(ValidationError):  # NUL in a nested value
        PaymentCreate.model_validate(body(metadata={"k": [{"deep": "a\u0000"}]}))


def test_metadata_depth_boundary() -> None:
    assert PaymentCreate.model_validate(body(metadata=nested(10)))
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(metadata=nested(11)))


def test_metadata_size_boundary() -> None:
    """The serialized form `{"k":"..."}` has 8 bytes of overhead around the value."""
    limit = 16 * 1024
    assert PaymentCreate.model_validate(body(metadata={"k": "x" * (limit - 8 - 1)}))
    assert PaymentCreate.model_validate(body(metadata={"k": "x" * (limit - 8)}))
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(metadata={"k": "x" * (limit - 8 + 1)}))


def test_description_defaults_empty() -> None:
    payload = PaymentCreate.model_validate(body())
    assert payload.description == ""
    assert payload.metadata == {}


def test_description_limits() -> None:
    assert PaymentCreate.model_validate(body(description="x" * 1000))
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(description="x" * 1001))
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(description="a\u0000b"))


def test_extra_fields_forbidden() -> None:
    with pytest.raises(ValidationError):
        PaymentCreate.model_validate(body(unexpected="field"))


def test_payment_out_serializes_amount_as_string() -> None:
    payment_id = uuid4()
    created = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    payment = Payment(
        id=payment_id,
        amount=Decimal("100.50"),
        currency=Currency.RUB,
        description="d",
        meta={"order": 7},
        status=PaymentStatus.PENDING,
        idempotency_key="key-1",
        request_fingerprint="v1:abc",
        webhook_url="https://example.com/hook",
        created_at=created,
        processed_at=None,
        delivery_state=DeliveryState.PENDING,
        attempts=0,
        attempts_base=0,
        last_attempt_at=None,
        delivered_at=None,
        dead_lettered_at=None,
        dead_letter_reason=None,
        last_error=None,
    )
    out = PaymentOut.from_payment(payment)
    data = json.loads(out.model_dump_json())
    assert data["amount"] == "100.50"
    assert data["payment_id"] == str(payment_id)
    assert data["metadata"] == {"order": 7}
    assert data["idempotency_key"] == "key-1"
    assert data["webhook_url"] == "https://example.com/hook"
    assert data["processed_at"] is None
    assert data["delivery"] == {
        "state": "pending",
        "attempts": 0,
        "last_attempt_at": None,
        "delivered_at": None,
        "dead_lettered_at": None,
        "dead_letter_reason": None,
        "last_error": None,
    }
