"""Payment state machine executed by the consumer."""

from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

import structlog

from payments.db import queries as q
from payments.db.models import Payment
from payments.db.queries import Attempt
from payments.db.session import Db
from payments.errors import (
    GATEWAY_UNAVAILABLE,
    UNEXPECTED,
    AttemptInFlight,
    DatabaseUnavailable,
    DeadReason,
    GatewayUnavailable,
)
from payments.schemas import DeliveryState, PaymentStatus
from payments.services.gateway import Gateway
from payments.webhooks.sender import Delivered, Permanent, Retryable, WebhookSender

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class KnownOutcome:
    """A delivery outcome that is known but could not be stored yet."""

    kind: Literal["delivered", "dead"]
    epoch: int
    reason: DeadReason | None = None
    code: str | None = None


@dataclass(frozen=True)
class Done:
    """Processing is complete."""


@dataclass(frozen=True)
class RetryLater:
    """Schedule another attempt."""

    attempt_no: int
    code: str


@dataclass(frozen=True)
class DeadLetter:
    """The payment is dead-lettered."""


Outcome = Done | RetryLater | DeadLetter


class Processor:
    """Runs one processing attempt for a payment."""

    def __init__(
        self,
        *,
        db: Db,
        gateway: Gateway,
        webhooks: WebhookSender,
        max_attempts: int,
        stale_after_s: float,
    ) -> None:
        self._db = db
        self._gateway = gateway
        self._webhooks = webhooks
        self._max_attempts = max_attempts
        self._stale_after_s = stale_after_s

    async def process(self, payment_id: UUID, *, known: KnownOutcome | None = None) -> Outcome:
        """Process one message for the payment."""
        if known is not None:
            return await self._persist(payment_id, known)

        async def begin(session: Any) -> Attempt | None:
            return await q.begin_attempt(session, payment_id, self._max_attempts)

        attempt = await self._db.run(begin)
        if attempt is None:
            return await self._no_attempt(payment_id)
        try:
            payment = attempt.payment
            if payment.status is PaymentStatus.PENDING:
                try:
                    status = await self._gateway.charge(
                        idempotency_key=str(payment.id),
                        amount=payment.amount,
                        currency=payment.currency,
                        metadata=payment.meta,
                    )
                except GatewayUnavailable:
                    return await self._fail(attempt, GATEWAY_UNAVAILABLE)
                payment = await self._finalized(payment.id, status)
            result = await self._webhooks.send(payment)
        except DatabaseUnavailable:
            raise
        except Exception:
            log.exception("payment.processing_failed")
            return await self._fail(attempt, UNEXPECTED)
        match result:
            case Delivered():
                return await self._persist(payment.id, KnownOutcome("delivered", attempt.epoch))
            case Retryable(code):
                return await self._fail(attempt, code)
            case Permanent(code):
                return await self._persist(
                    payment.id, KnownOutcome("dead", attempt.epoch, DeadReason.PERMANENT, code)
                )

    async def mark_unroutable(self, payment_id: UUID) -> None:
        """Dead-letter the payment because its retry message cannot be routed."""

        async def work(session: Any) -> None:
            payment = await q.get_payment(session, payment_id)
            if payment is not None:
                await q.mark_dead(
                    session,
                    payment_id,
                    epoch=payment.attempts_base,
                    reason=DeadReason.UNROUTABLE,
                    code=None,
                )

        await self._db.run(work)

    async def _finalized(self, payment_id: UUID, status: PaymentStatus) -> Payment:
        async def work(session: Any) -> Payment:
            payment = await q.finalize(session, payment_id, status)
            if payment is None:  # another chain won the CAS
                payment = await q.get_payment(session, payment_id)
            if payment is None:
                raise RuntimeError("payment disappeared during processing")
            return payment

        return await self._db.run(work)

    async def _no_attempt(self, payment_id: UUID) -> Outcome:
        async def exhaust(session: Any) -> bool:
            return await q.mark_exhausted(
                session, payment_id, self._max_attempts, self._stale_after_s
            )

        if await self._db.run(exhaust):
            return DeadLetter()

        async def state(session: Any) -> DeliveryState | None:
            return await q.get_delivery_state(session, payment_id)

        match await self._db.run(state):
            case None | DeliveryState.DEAD_LETTERED:
                return DeadLetter()
            case DeliveryState.DELIVERED:
                return Done()
            case _:
                raise AttemptInFlight

    async def _fail(self, attempt: Attempt, code: str) -> Outcome:
        if attempt.attempt_no >= self._max_attempts:
            return await self._persist(
                attempt.payment.id,
                KnownOutcome("dead", attempt.epoch, DeadReason.MAX_ATTEMPTS, code),
            )

        async def record(session: Any) -> None:
            await q.record_error(session, attempt.payment.id, epoch=attempt.epoch, code=code)

        await self._db.run(record)
        return RetryLater(attempt.attempt_no, code)

    async def _persist(self, payment_id: UUID, known: KnownOutcome) -> Outcome:
        try:
            if known.kind == "delivered":

                async def deliver(session: Any) -> None:
                    await q.mark_delivered(session, payment_id)

                await self._db.run(deliver)
                return Done()
            assert known.reason is not None  # noqa: S101 - "dead" outcomes always carry a reason
            reason = known.reason

            async def kill(session: Any) -> DeliveryState | None:
                return await q.mark_dead(
                    session, payment_id, epoch=known.epoch, reason=reason, code=known.code
                )

            state = await self._db.run(kill)
            # Delivered, or a newer epoch: the outcome is not ours any more.
            return DeadLetter() if state is DeliveryState.DEAD_LETTERED else Done()
        except DatabaseUnavailable as exc:
            raise DatabaseUnavailable(known=known) from exc
