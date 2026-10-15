"""All SQL that carries the delivery guarantees (design §3.1, §4.3, §5)."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import Outbox, Payment
from payments.errors import DeadReason
from payments.messaging.topology import MAIN_QUEUE_NAME
from payments.schemas import DeliveryState, PaymentCreate, PaymentStatus


@dataclass(frozen=True)
class Attempt:
    """A started processing attempt."""

    payment: Payment
    attempt_no: int
    epoch: int


@dataclass(frozen=True)
class ClaimedEvent:
    """An outbox row claimed by the relay."""

    id: UUID
    payment_id: UUID
    event_type: str
    routing_key: str
    payload: dict[str, Any]
    headers: dict[str, Any]
    attempts: int


async def insert_payment_if_absent(
    s: AsyncSession, *, data: PaymentCreate, idempotency_key: str, fingerprint: str
) -> Payment | None:
    """Insert the payment; None if the idempotency key already exists."""
    statement = (
        insert(Payment)
        .values(
            id=uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            meta=data.metadata,
            idempotency_key=idempotency_key,
            request_fingerprint=fingerprint,
            webhook_url=str(data.webhook_url),
        )
        .on_conflict_do_nothing(constraint="uq_payments_idempotency_key")
        .returning(Payment)
    )
    return (await s.execute(statement)).scalar_one_or_none()


async def get_by_idempotency_key(s: AsyncSession, key: str) -> Payment | None:
    """Load a payment by its idempotency key."""
    return (
        await s.execute(select(Payment).where(Payment.idempotency_key == key))
    ).scalar_one_or_none()


async def get_payment(s: AsyncSession, payment_id: UUID) -> Payment | None:
    """Load a payment by id."""
    return await s.get(Payment, payment_id)


async def add_outbox_event(
    s: AsyncSession, *, payment_id: UUID, event_type: str, request_id: str | None
) -> None:
    """Add an outbox row for the payment."""
    event_id = uuid4()
    s.add(
        Outbox(
            id=event_id,
            payment_id=payment_id,
            event_type=event_type,
            routing_key=MAIN_QUEUE_NAME,
            payload={
                "event_id": str(event_id),
                "payment_id": str(payment_id),
                "occurred_at": datetime.now(UTC).isoformat(),
            },
            headers={"x-request-id": request_id} if request_id else {},
        )
    )
    await s.flush()


async def claim_outbox(s: AsyncSession, *, batch: int, lease_s: float) -> list[ClaimedEvent]:
    """Lease up to `batch` due outbox rows."""
    raise NotImplementedError


async def delete_published(s: AsyncSession, published: Sequence[tuple[UUID, int]]) -> int:
    """Delete published rows, fenced by (id, attempts)."""
    raise NotImplementedError


async def defer_outbox(
    s: AsyncSession, *, event_id: UUID, attempts: int, backoff_s: float, error: str
) -> int:
    """Postpone a row after a failed publication, fenced by attempts."""
    raise NotImplementedError


async def outbox_backlog(s: AsyncSession) -> tuple[int, float | None]:
    """Return the number of due rows and the age in seconds of the oldest one."""
    row = (
        await s.execute(
            select(
                func.count(),
                func.extract("epoch", func.now() - func.min(Outbox.available_at)),
            ).where(Outbox.available_at <= func.now())
        )
    ).one()
    age = row[1]
    return int(row[0]), None if age is None else float(age)


async def begin_attempt(s: AsyncSession, payment_id: UUID, max_attempts: int) -> Attempt | None:
    """Start an attempt if the budget allows it."""
    raise NotImplementedError


async def mark_exhausted(
    s: AsyncSession, payment_id: UUID, max_attempts: int, stale_s: float
) -> bool:
    """Dead-letter a payment whose budget is spent and whose last attempt is stale."""
    raise NotImplementedError


async def get_delivery_state(s: AsyncSession, payment_id: UUID) -> DeliveryState | None:
    """Return the delivery state, or None if the payment does not exist."""
    raise NotImplementedError


async def finalize(s: AsyncSession, payment_id: UUID, status: PaymentStatus) -> Payment | None:
    """Set the final status of a pending payment."""
    raise NotImplementedError


async def mark_delivered(s: AsyncSession, payment_id: UUID) -> None:
    """Mark the webhook as delivered (delivered wins over dead-lettered)."""
    raise NotImplementedError


async def mark_dead(
    s: AsyncSession, payment_id: UUID, *, epoch: int, reason: DeadReason, code: str | None
) -> DeliveryState | None:
    """Dead-letter the payment, fenced by epoch; returns the resulting delivery state."""
    raise NotImplementedError


async def record_error(s: AsyncSession, payment_id: UUID, *, epoch: int, code: str) -> None:
    """Record the last error code, fenced by epoch."""
    raise NotImplementedError


async def reopen(s: AsyncSession, payment_id: UUID) -> bool:
    """Reopen a dead-lettered payment with a fresh attempt budget."""
    raise NotImplementedError


async def find_dead(
    s: AsyncSession, *, reason: DeadReason | None, payment_id: UUID | None, limit: int
) -> list[UUID]:
    """Find dead-lettered payments, optionally filtered."""
    raise NotImplementedError


async def find_stuck_pending(s: AsyncSession, *, older_than_s: float, limit: int) -> list[UUID]:
    """Find pending payments without recent activity and without an active outbox event."""
    raise NotImplementedError
