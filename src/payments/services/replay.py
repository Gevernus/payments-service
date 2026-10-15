"""Replay of dead-lettered and stuck payments (CLI backend, design §7.4).

Replay never publishes: it writes an outbox row in the same transaction as the state change,
and the relay carries the event to the consumer.
"""

from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db import queries
from payments.db.session import Db
from payments.errors import DatabaseUnavailable, DeadReason

log = structlog.get_logger(__name__)

REPLAY_EVENT = "payment.replay_requested"


class ReplayStopped(Exception):  # noqa: N818 - carries partial progress, not a plain error
    """The database failed mid-run; `done` lists the payments already replayed."""

    def __init__(self, done: list[UUID]) -> None:
        super().__init__(f"stopped after {len(done)} payment(s)")
        self.done = done


async def replay_dead(
    db: Db,
    *,
    reason: DeadReason | None,
    payment_id: UUID | None,
    limit: int,
    dry_run: bool,
) -> list[UUID]:
    """Reopen dead-lettered payments and enqueue a replay event for each.

    One transaction per payment, so progress made before a failure is kept.
    """

    async def find(s: AsyncSession) -> list[UUID]:
        return await queries.find_dead(s, reason=reason, payment_id=payment_id, limit=limit)

    found = await db.run(find)
    if dry_run:
        return found
    done: list[UUID] = []
    for pid in found:

        async def replay_one(s: AsyncSession, pid: UUID = pid) -> bool:
            if not await queries.reopen(s, pid):  # lost a race with another replay
                return False
            await queries.add_outbox_event(
                s, payment_id=pid, event_type=REPLAY_EVENT, request_id=None
            )
            return True

        try:
            replayed = await db.run(replay_one)
        except DatabaseUnavailable as exc:
            raise ReplayStopped(done) from exc
        if replayed:
            done.append(pid)
            log.info("payment.replayed", payment_id=str(pid), kind="dead")
    return done


async def replay_stuck_pending(
    db: Db, *, older_than_s: float, limit: int, dry_run: bool
) -> list[UUID]:
    """Enqueue a replay event for pending payments without activity (one transaction each)."""

    async def find(s: AsyncSession) -> list[UUID]:
        return await queries.find_stuck_pending(s, older_than_s=older_than_s, limit=limit)

    found = await db.run(find)
    if dry_run:
        return found
    done: list[UUID] = []
    for pid in found:

        async def enqueue_one(s: AsyncSession, pid: UUID = pid) -> None:
            await queries.add_outbox_event(
                s, payment_id=pid, event_type=REPLAY_EVENT, request_id=None
            )

        try:
            await db.run(enqueue_one)
        except DatabaseUnavailable as exc:
            raise ReplayStopped(done) from exc
        done.append(pid)
        log.info("payment.replayed", payment_id=str(pid), kind="stuck_pending")
    return done
