"""Payment gateway contract and its emulation."""

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from typing import Any, Protocol

from payments.schemas import Currency, PaymentStatus


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
        outcome_hints: bool,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        raise NotImplementedError

    async def charge(
        self,
        *,
        idempotency_key: str,
        amount: Decimal,
        currency: Currency,
        metadata: Mapping[str, Any],
    ) -> PaymentStatus:
        """Charge the payment and return its final status."""
        raise NotImplementedError
