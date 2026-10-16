"""The payment gateway contract and its emulation."""

from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest

from payments.schemas import Currency, PaymentStatus
from payments.services.gateway import EmulatedGateway, Gateway
from tests.support.fakes import FakeGateway, idempotency_key_with_outcome

KEYS = [f"payment-{i}" for i in range(200)]


async def _no_sleep(_: float) -> None:
    return None


def emulated(**overrides: Any) -> EmulatedGateway:
    values: dict[str, Any] = {
        "success_rate": 0.5,
        "min_delay_s": 0,
        "max_delay_s": 0,
        "sleep": _no_sleep,
    }
    values.update(overrides)
    return EmulatedGateway(**values)


def fake() -> FakeGateway:
    return FakeGateway(
        {
            key: [PaymentStatus.SUCCEEDED if index % 2 else PaymentStatus.FAILED]
            for index, key in enumerate(KEYS)
        }
    )


GATEWAYS: dict[str, Callable[[], Gateway]] = {"emulated": emulated, "fake": fake}


async def charge(gateway: Gateway, key: str, **metadata: Any) -> PaymentStatus:
    return await gateway.charge(
        idempotency_key=key, amount=Decimal("10.00"), currency=Currency.RUB, metadata=metadata
    )


@pytest.mark.parametrize("kind", ["emulated", "fake"])
async def test_gateway_contract(kind: str) -> None:
    gateway = GATEWAYS[kind]()

    first = [await charge(gateway, key) for key in KEYS]
    again = [await charge(gateway, key) for key in KEYS]

    assert first == again  # one key, one outcome
    assert set(first) == {PaymentStatus.SUCCEEDED, PaymentStatus.FAILED}  # keys differ


async def test_gateway_distribution() -> None:
    gateway = emulated(success_rate=0.9)

    outcomes = [await charge(gateway, f"key-{i}") for i in range(100_000)]

    rate = outcomes.count(PaymentStatus.SUCCEEDED) / len(outcomes)
    assert abs(rate - 0.9) <= 0.005


@pytest.mark.parametrize("outcome", [PaymentStatus.SUCCEEDED, PaymentStatus.FAILED])
async def test_idempotency_key_with_outcome_is_deterministic(outcome: PaymentStatus) -> None:
    first = await idempotency_key_with_outcome(outcome)

    assert first == await idempotency_key_with_outcome(outcome)
    assert await charge(emulated(success_rate=0.9), first) is outcome
