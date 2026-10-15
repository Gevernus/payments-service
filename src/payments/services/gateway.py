"""Payment gateway contract and its emulation."""

import asyncio
import hashlib
import random
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from typing import Any, Protocol

import structlog

from payments.schemas import Currency, PaymentStatus

log = structlog.get_logger(__name__)


class Gateway(Protocol):
    """PSP contract: one key gives one outcome; a repeated call with the same key is safe.

    A technical failure, or a charge that is still in progress, raises GatewayUnavailable.
    """

    async def charge(
        self,
        *,
        idempotency_key: str,
        amount: Decimal,
        currency: Currency,
        metadata: Mapping[str, Any],
    ) -> PaymentStatus:
        """Charge the payment and return its final status."""
        ...


class EmulatedGateway:
    """Deterministic gateway emulation with a random delay."""

    def __init__(
        self,
        *,
        success_rate: float,
        min_delay_s: float,
        max_delay_s: float,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._success_rate = success_rate
        self._min_delay_s = min_delay_s
        self._max_delay_s = max_delay_s
        self._sleep = sleep

    async def charge(
        self,
        *,
        idempotency_key: str,
        amount: Decimal,
        currency: Currency,
        metadata: Mapping[str, Any],
    ) -> PaymentStatus:
        """Charge the payment; the outcome and the delay are functions of the key."""
        seed = int.from_bytes(hashlib.sha256(idempotency_key.encode()).digest(), "big")
        rng = random.Random(seed)  # noqa: S311 - a deterministic emulation, not cryptography
        succeeded = rng.random() < self._success_rate
        delay = rng.uniform(self._min_delay_s, self._max_delay_s)
        log.info(
            "gateway.charge_started",
            idempotency_key=idempotency_key,
            amount=str(amount),
            currency=str(currency),
            delay_s=round(delay, 3),
        )
        await self._sleep(delay)
        return PaymentStatus.SUCCEEDED if succeeded else PaymentStatus.FAILED
