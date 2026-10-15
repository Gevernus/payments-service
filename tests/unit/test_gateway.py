"""The payment gateway contract, its emulation and its outcome hints (design §8.1)."""

from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest

from payments.errors import GatewayUnavailable
from payments.schemas import Currency, PaymentStatus
from payments.services.gateway import EmulatedGateway, Gateway
from tests.support.fakes import FakeGateway

KEYS = [f"payment-{i}" for i in range(200)]


async def _no_sleep(_: float) -> None:
    return None


def emulated(**overrides: Any) -> EmulatedGateway:
    values: dict[str, Any] = {
        "success_rate": 0.5,
        "min_delay_s": 0,
        "max_delay_s": 0,
        "outcome_hints": False,
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


async def test_gateway_hints_respect_flag() -> None:
    always_ok = emulated(success_rate=1.0, outcome_hints=False)
    always_ok_hinted = emulated(success_rate=1.0, outcome_hints=True)
    never_ok_hinted = emulated(success_rate=0.0, outcome_hints=True)

    for key in KEYS[:20]:
        # Without the flag a hint is ordinary metadata.
        assert await charge(always_ok, key, emulator_outcome="failed") is PaymentStatus.SUCCEEDED
        assert await charge(always_ok, key, emulator_outcome="error") is PaymentStatus.SUCCEEDED
        # With the flag it decides the outcome.
        assert (
            await charge(always_ok_hinted, key, emulator_outcome="failed") is PaymentStatus.FAILED
        )
        assert (
            await charge(never_ok_hinted, key, emulator_outcome="succeeded")
            is PaymentStatus.SUCCEEDED
        )
        # An unknown hint is ignored.
        assert (
            await charge(never_ok_hinted, key, emulator_outcome="whatever") is PaymentStatus.FAILED
        )


async def test_gateway_error_hint_raises_unavailable() -> None:
    hinted = emulated(outcome_hints=True)
    plain = emulated(outcome_hints=False, success_rate=1.0)

    with pytest.raises(GatewayUnavailable):
        await charge(hinted, "payment-1", emulator_outcome="error")
    assert await charge(plain, "payment-1", emulator_outcome="error") is PaymentStatus.SUCCEEDED
