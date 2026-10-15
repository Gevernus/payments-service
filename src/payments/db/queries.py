"""All SQL that carries the delivery guarantees (design §3.1, §4.3, §5)."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    Float,
    Integer,
    Row,
    String,
    bindparam,
    delete,
    func,
    select,
    text,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
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


async def add_outbox_event(s: AsyncSession, *, payment_id: UUID, event_type: str) -> None:
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
        )
    )
    await s.flush()


_CLAIM = text(
    """
        WITH due AS MATERIALIZED (
          SELECT id FROM outbox WHERE available_at <= now()
          ORDER BY available_at LIMIT :batch FOR UPDATE SKIP LOCKED)
        UPDATE outbox o SET attempts = o.attempts + 1,
                            available_at = now() + make_interval(secs => :lease)
        FROM due WHERE o.id = due.id
        RETURNING o.id, o.payment_id, o.event_type, o.routing_key, o.payload,
                  o.attempts
        """
).bindparams(bindparam("batch", type_=Integer), bindparam("lease", type_=Float))


async def claim_outbox(s: AsyncSession, *, batch: int, lease_s: float) -> Sequence[Row[Any]]:
    """Lease up to `batch` due outbox rows (id, payment_id, event_type, routing_key, payload,
    attempts)."""
    return (await s.execute(_CLAIM, {"batch": batch, "lease": lease_s})).all()


async def delete_published(s: AsyncSession, published: Sequence[tuple[UUID, int]]) -> int:
    """Delete published rows, fenced by (id, attempts)."""
    if not published:
        return 0
    result = await s.execute(
        delete(Outbox).where(tuple_(Outbox.id, Outbox.attempts).in_(list(published)))
    )
    return int(result.rowcount)  # type: ignore[attr-defined]


async def defer_outbox(
    s: AsyncSession, *, event_id: UUID, attempts: int, backoff_s: float, error: str
) -> int:
    """Postpone a row after a failed publication, fenced by attempts."""
    result = await s.execute(
        update(Outbox)
        .where(Outbox.id == event_id, Outbox.attempts == attempts)
        .values(
            available_at=func.now() + func.make_interval(0, 0, 0, 0, 0, 0, backoff_s),
            last_error=error[:100],
        )
    )
    return int(result.rowcount)  # type: ignore[attr-defined]


async def begin_attempt(s: AsyncSession, payment_id: UUID, max_attempts: int) -> Attempt | None:
    """Start an attempt if the budget allows it."""
    attempt_no = (Payment.attempts - Payment.attempts_base).label("attempt_no")
    row = (
        await s.execute(
            update(Payment)
            .where(
                Payment.id == payment_id,
                Payment.delivery_state == DeliveryState.PENDING,
                Payment.attempts - Payment.attempts_base < max_attempts,
            )
            .values(attempts=Payment.attempts + 1, last_attempt_at=func.now())
            .returning(Payment, attempt_no, Payment.attempts_base.label("epoch"))
            .execution_options(synchronize_session=False)
        )
    ).one_or_none()
    if row is None:
        return None
    return Attempt(payment=row[0], attempt_no=row.attempt_no, epoch=row.epoch)


_EXHAUSTED = text(
    """
    UPDATE payments SET delivery_state = 'dead_lettered', dead_lettered_at = now(),
                        dead_letter_reason = 'max_attempts',
                        last_error = COALESCE(last_error, 'crash_loop')
    WHERE id = :id AND delivery_state = 'pending'
      AND attempts - attempts_base >= :max
      AND last_attempt_at < now() - make_interval(secs => :stale)
    RETURNING id
    """
).bindparams(
    bindparam("id", type_=PG_UUID(as_uuid=True)),
    bindparam("max", type_=Integer),
    bindparam("stale", type_=Float),
)


async def mark_exhausted(
    s: AsyncSession, payment_id: UUID, max_attempts: int, stale_s: float
) -> bool:
    """Dead-letter a payment whose budget is spent and whose last attempt is stale."""
    result = await s.execute(_EXHAUSTED, {"id": payment_id, "max": max_attempts, "stale": stale_s})
    return result.first() is not None


async def get_delivery_state(s: AsyncSession, payment_id: UUID) -> DeliveryState | None:
    """Return the delivery state, or None if the payment does not exist."""
    return (
        await s.execute(select(Payment.delivery_state).where(Payment.id == payment_id))
    ).scalar_one_or_none()


async def finalize(s: AsyncSession, payment_id: UUID, status: PaymentStatus) -> Payment | None:
    """Set the final status of a pending payment."""
    return (
        await s.execute(
            update(Payment)
            .where(Payment.id == payment_id, Payment.status == PaymentStatus.PENDING)
            .values(status=status, processed_at=func.now())
            .returning(Payment)
            .execution_options(synchronize_session=False)
        )
    ).scalar_one_or_none()


_DELIVERED = text(
    """
    UPDATE payments SET delivery_state = 'delivered', delivered_at = now(),
                        dead_lettered_at = NULL, dead_letter_reason = NULL, last_error = NULL
    WHERE id = :id AND delivery_state IN ('pending', 'dead_lettered')
    """
).bindparams(bindparam("id", type_=PG_UUID(as_uuid=True)))


async def mark_delivered(s: AsyncSession, payment_id: UUID) -> None:
    """Mark the webhook as delivered (delivered wins over dead-lettered)."""
    await s.execute(_DELIVERED, {"id": payment_id})


_DEAD = text(
    """
    UPDATE payments SET delivery_state = 'dead_lettered', dead_lettered_at = now(),
                        dead_letter_reason = :reason, last_error = COALESCE(:code, last_error)
    WHERE id = :id AND delivery_state = 'pending' AND attempts_base = :epoch
    RETURNING delivery_state
    """
).bindparams(
    bindparam("id", type_=PG_UUID(as_uuid=True)),
    bindparam("reason", type_=String),
    bindparam("code", type_=String),
    bindparam("epoch", type_=Integer),
)


async def mark_dead(
    s: AsyncSession, payment_id: UUID, *, epoch: int, reason: DeadReason, code: str | None
) -> DeliveryState | None:
    """Dead-letter the payment, fenced by epoch; returns the resulting delivery state."""
    updated = (
        await s.execute(
            _DEAD, {"id": payment_id, "reason": reason.value, "code": code, "epoch": epoch}
        )
    ).scalar_one_or_none()
    if updated is not None:
        return DeliveryState(updated)
    # Not ours: report the actual state (delivered, a newer epoch, already dead, or missing).
    return await get_delivery_state(s, payment_id)


_RECORD_ERROR = text(
    """
    UPDATE payments SET last_error = :code
    WHERE id = :id AND delivery_state = 'pending' AND attempts_base = :epoch
    """
).bindparams(
    bindparam("id", type_=PG_UUID(as_uuid=True)),
    bindparam("code", type_=String),
    bindparam("epoch", type_=Integer),
)


async def record_error(s: AsyncSession, payment_id: UUID, *, epoch: int, code: str) -> None:
    """Record the last error code, fenced by epoch and only while pending."""
    await s.execute(_RECORD_ERROR, {"id": payment_id, "code": code, "epoch": epoch})


_REOPEN = text(
    """
    UPDATE payments SET delivery_state = 'pending', dead_lettered_at = NULL,
                        dead_letter_reason = NULL, last_error = NULL, attempts_base = attempts
    WHERE id = :id AND delivery_state = 'dead_lettered'
    RETURNING id
    """
).bindparams(bindparam("id", type_=PG_UUID(as_uuid=True)))


async def reopen(s: AsyncSession, payment_id: UUID) -> bool:
    """Reopen a dead-lettered payment with a fresh attempt budget."""
    return (await s.execute(_REOPEN, {"id": payment_id})).first() is not None


async def find_dead(
    s: AsyncSession, *, reason: DeadReason | None, payment_id: UUID | None, limit: int
) -> list[UUID]:
    """Find dead-lettered payments, oldest first, optionally filtered."""
    stmt = select(Payment.id).where(Payment.delivery_state == DeliveryState.DEAD_LETTERED)
    if reason is not None:
        stmt = stmt.where(Payment.dead_letter_reason == reason)
    if payment_id is not None:
        stmt = stmt.where(Payment.id == payment_id)
    stmt = stmt.order_by(Payment.dead_lettered_at, Payment.id).limit(limit)
    return list((await s.execute(stmt)).scalars())


_STUCK = text(
    """
    SELECT p.id FROM payments p
    WHERE p.delivery_state = 'pending'
      AND COALESCE(p.last_attempt_at, p.created_at) <= now() - make_interval(secs => :older_than_s)
      AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.payment_id = p.id)
    ORDER BY COALESCE(p.last_attempt_at, p.created_at), p.id
    LIMIT :limit
    """
).bindparams(bindparam("older_than_s", type_=Float), bindparam("limit", type_=Integer))


async def find_stuck_pending(s: AsyncSession, *, older_than_s: float, limit: int) -> list[UUID]:
    """Find pending payments without recent activity and without an active outbox event.

    Nothing is locked: two concurrent replays may both enqueue a payment, which is harmless
    because every chain shares the epoch budget through the `begin_attempt` CAS.
    """
    rows = await s.execute(_STUCK, {"older_than_s": older_than_s, "limit": limit})
    return [row[0] for row in rows]
