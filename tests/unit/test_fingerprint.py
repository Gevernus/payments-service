"""Request fingerprint used to detect idempotency-key reuse (design §4.3)."""

from typing import Any

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


def test_fingerprint_amount_forms_equal() -> None:
    by_int = fingerprint(make(amount=100))
    by_string = fingerprint(make(amount="100.00"))
    by_float = fingerprint(make(amount=1e2))
    assert by_int == by_string == by_float


def test_fingerprint_metadata_is_hashed_as_parsed() -> None:
    assert fingerprint(make(metadata={"n": 1})) != fingerprint(make(metadata={"n": 1.0}))
    assert fingerprint(make(metadata={"n": "1"})) != fingerprint(make(metadata={"n": 1}))


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
    Canonical JSON (sorted keys, compact separators):
    {"amount":"100.50","currency":"RUB","description":"Order 42","metadata":{"a":{"k":"v"},
    "b":[1,2.5,null,true]},"webhook_url":"https://example.com/hook"}
    """
    payload = make(
        amount="100.50",
        description="Order 42",
        metadata={"b": [1, 2.5, None, True], "a": {"k": "v"}},
    )
    assert (
        fingerprint(payload)
        == "v1:fa3fb4f62eea7b283057cff9ff67fc1c5f68fc53f215526e577ce492a1e54180"
    )
