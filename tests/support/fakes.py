"""Test doubles for the gateway and the webhook sender, and a settings factory."""

import asyncio
import base64
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import NAMESPACE_OID, UUID, uuid4, uuid5

from pydantic import SecretStr

from payments.config import Settings
from payments.db.models import Payment
from payments.schemas import Currency, DeliveryState, PaymentStatus
from payments.services.gateway import EmulatedGateway
from payments.webhooks.sender import Delivered, SendResult

TEST_API_KEY = "test-api-key-0123456789abcdef0123456789"
TEST_WEBHOOK_KEY = b"0123456789abcdef0123456789abcdef"
TEST_WEBHOOK_SECRET = "whsec_" + base64.b64encode(TEST_WEBHOOK_KEY).decode()


# Bound at import time: tests may patch EmulatedGateway.charge afterwards.
_EMULATED_CHARGE = EmulatedGateway.charge


async def payment_id_with_outcome(
    outcome: PaymentStatus, *, nth: int = 0, success_rate: float = 0.9
) -> UUID:
    """The `nth` id of a fixed sequence that the emulated gateway answers with `outcome`."""

    async def no_sleep(_: float) -> None:
        return None

    gateway = EmulatedGateway(
        success_rate=success_rate, min_delay_s=0, max_delay_s=0, sleep=no_sleep
    )
    for i in range(10_000):
        candidate = uuid5(NAMESPACE_OID, f"{outcome.value}-{i}")
        status = await _EMULATED_CHARGE(
            gateway,
            idempotency_key=str(candidate),
            amount=Decimal(1),
            currency=Currency.RUB,
            metadata={},
        )
        if status is outcome:
            if nth == 0:
                return candidate
            nth -= 1
    raise RuntimeError(f"no payment id with outcome {outcome.value}")


@dataclass(frozen=True)
class GatewayCall:
    """One call received by FakeGateway."""

    idempotency_key: str
    amount: Decimal
    currency: Currency
    metadata: Mapping[str, Any]


class FakeGateway:
    """Gateway with scripted outcomes per idempotency key.

    Each key has a list of outcomes (a status, or an exception to raise) consumed in order;
    the last one repeats, like a real PSP answering the same key. Keys without a script get
    `default`. If `gate` is given, charge() waits for it before answering.
    """

    def __init__(
        self,
        outcomes: Mapping[str, Sequence[PaymentStatus | Exception]] | None = None,
        *,
        default: PaymentStatus = PaymentStatus.SUCCEEDED,
        gate: asyncio.Event | None = None,
    ) -> None:
        self._outcomes = {key: list(seq) for key, seq in (outcomes or {}).items()}
        self._default = default
        self.gate = gate
        self.calls: list[GatewayCall] = []

    async def charge(
        self,
        *,
        idempotency_key: str,
        amount: Decimal,
        currency: Currency,
        metadata: Mapping[str, Any],
    ) -> PaymentStatus:
        """Record the call and return the next scripted outcome."""
        self.calls.append(GatewayCall(idempotency_key, amount, currency, metadata))
        if self.gate is not None:
            await self.gate.wait()
        script = self._outcomes.get(idempotency_key)
        if not script:
            return self._default
        outcome = script.pop(0) if len(script) > 1 else script[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


class FakeWebhookSender:
    """Webhook sender that replays a scripted sequence of results and records the calls."""

    def __init__(
        self,
        results: Sequence[SendResult | Exception] | None = None,
        *,
        default: SendResult | None = None,
    ) -> None:
        self._results = list(results or [])
        self._default: SendResult = default if default is not None else Delivered()
        self.calls: list[Payment] = []

    async def send(self, payment: Payment) -> SendResult:
        """Record the call; return the next result, then the default."""
        self.calls.append(payment)
        if not self._results:
            return self._default
        result = self._results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def make_settings(**overrides: Any) -> Settings:
    """Settings for tests: no gateway delay, fast retries, short deadlines, fixed secrets."""
    values: dict[str, Any] = {
        "database_url": os.environ.get(
            "TEST_DATABASE_URL", "postgresql+asyncpg://postgres@127.0.0.1:55432/payments"
        ),
        "rabbitmq_url": os.environ.get("TEST_RABBITMQ_URL", "amqp://guest:guest@127.0.0.1:55672/"),
        "gateway_min_delay_s": 0,
        "gateway_max_delay_s": 0,
        "retry_base_delay_ms": 200,
        # A timeout chain valid under design §7.5 with short deadlines (stale = 0 + 0.5 + 2 * 1).
        "webhook_total_timeout_s": 0.5,
        "db_call_timeout_s": 1,
        "api_key": SecretStr(TEST_API_KEY),
        "webhook_secret": SecretStr(TEST_WEBHOOK_SECRET),
    }
    values.update(overrides)
    return Settings(**values)


def make_payment(
    *,
    webhook_url: str = "https://merchant.example/hooks/payments",
    status: PaymentStatus = PaymentStatus.SUCCEEDED,
    payment_id: UUID | None = None,
) -> Payment:
    """A processed payment (not persisted) for webhook tests."""
    created = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    return Payment(
        id=payment_id or uuid4(),
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="order 42",
        meta={"order_id": 42},
        status=status,
        idempotency_key="key-1",
        request_fingerprint="v1:abc",
        webhook_url=webhook_url,
        created_at=created,
        processed_at=created + timedelta(seconds=3),
        delivery_state=DeliveryState.PENDING,
        attempts=1,
        attempts_base=0,
    )
