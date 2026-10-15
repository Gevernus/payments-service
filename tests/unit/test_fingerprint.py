"""Request fingerprint used to detect idempotency-key reuse (design §4.3)."""

from typing import Any

import pytest
from pydantic import ValidationError

from payments.fingerprint import fingerprint
from payments.schemas import PaymentCreate


def make(**overrides: Any) -> PaymentCreate:
    body: dict[str, Any] = {
        "amount": "100.00",
        "currency": "RUB",
        "description": "order",
        "metadata": {},
        "webhook_url": "https://example.com/hook",
    }
    body.update(overrides)
    return PaymentCreate.model_validate(body)


def test_fingerprint_stable_under_key_order() -> None:
    first = make(metadata={"a": 1, "b": {"x": [1, 2], "y": "z"}})
    second = make(metadata={"b": {"y": "z", "x": [1, 2]}, "a": 1})
    assert fingerprint(first) == fingerprint(second)

    raw_a = (
        b'{"amount":"1.00","currency":"USD","webhook_url":"https://example.com/h",'
        b'"metadata":{"p":1,"q":2}}'
    )
    raw_b = (
        b'{"metadata":{"q":2,"p":1},"webhook_url":"https://example.com/h",'
        b'"currency":"USD","amount":"1.00"}'
    )
    assert fingerprint(PaymentCreate.model_validate_json(raw_a)) == fingerprint(
        PaymentCreate.model_validate_json(raw_b)
    )


def test_fingerprint_has_version_prefix_and_sha256_digest() -> None:
    value = fingerprint(make())
    assert value.startswith("v1:")
    assert len(value) == len("v1:") + 64
    int(value.removeprefix("v1:"), 16)


def test_fingerprint_number_forms_equal() -> None:
    by_int = fingerprint(make(amount=100))
    by_string = fingerprint(make(amount="100.00"))
    by_float = fingerprint(make(amount=1e2))
    assert by_int == by_string == by_float

    assert fingerprint(make(metadata={"n": 1})) == fingerprint(make(metadata={"n": 1.0}))
    parsed = PaymentCreate.model_validate_json(
        b'{"amount":"1","currency":"RUB","webhook_url":"https://example.com/h","metadata":{"n":1.00}}'
    )
    plain = PaymentCreate.model_validate_json(
        b'{"amount":"1","currency":"RUB","webhook_url":"https://example.com/h","metadata":{"n":1}}'
    )
    assert fingerprint(parsed) == fingerprint(plain)


def test_fingerprint_minus_zero_equals_zero() -> None:
    assert fingerprint(make(metadata={"n": -0.0})) == fingerprint(make(metadata={"n": 0}))
    assert fingerprint(make(metadata={"n": 0.0})) == fingerprint(make(metadata={"n": 0}))


def test_fingerprint_string_differs_from_number() -> None:
    assert fingerprint(make(metadata={"n": "1"})) != fingerprint(make(metadata={"n": 1}))


def test_fingerprint_tag_cannot_collide_with_data() -> None:
    """The number tag starts with NUL, which input strings may not contain."""
    assert fingerprint(make(metadata={"a": 1}))
    with pytest.raises(ValidationError):
        make(metadata={"a": "\u0000num:1"})


def test_fingerprint_differs_when_any_field_differs() -> None:
    base = fingerprint(make())
    variants = [
        make(amount="100.01"),
        make(currency="USD"),
        make(description="other"),
        make(metadata={"k": "v"}),
        make(webhook_url="https://example.com/other"),
    ]
    values = {fingerprint(v) for v in variants}
    assert base not in values
    assert len(values) == len(variants)


def test_fingerprint_golden() -> None:
    """Pins the canonical form: any change to it must introduce a new version prefix.

    Reference input: amount "100.50", RUB, description "Order 42", metadata
    {"b": [1, 2.5, null, true], "a": {"k": "v"}}, webhook_url https://example.com/hook.
    Canonical JSON (sorted keys, compact separators, numbers tagged with NUL + "num:"):
    {"amount":"100.50","currency":"RUB","description":"Order 42","metadata":{"a":{"k":"v"},
    "b":["\\u0000num:1","\\u0000num:2.5",null,true]},"webhook_url":"https://example.com/hook"}
    """
    payload = make(
        amount="100.50",
        description="Order 42",
        metadata={"b": [1, 2.5, None, True], "a": {"k": "v"}},
    )
    assert (
        fingerprint(payload)
        == "v1:1b3e0ec0d145332c4c02f3cf5f99a7c468b371774a67b768c645905af7e9a572"
    )
