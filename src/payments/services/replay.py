"""Replay of dead-lettered and stuck payments (CLI backend).

Replay never publishes: it writes an outbox row in the same transaction as the state change,
and the relay carries the event to the consumer. Running it again is safe: `reopen` is a
compare-and-set on `dead_lettered`, and stuck payments with an outbox row are not selected.
"""

from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db import queries
from payments.db.session import Db
from payments.errors import DeadReason

log = structlog.get_logger(__name__)

REPLAY_EVENT = "payment.replay_requested"


async def _enqueue(s: AsyncSession, payment_id: UUID, *, reopen: bool) -> bool:
    if reopen and not await queries.reopen(s, payment_id):  # lost a race with another replay
        return False
    await queries.add_outbox_event(s, payment_id=payment_id, event_type=REPLAY_EVENT)
    return True


async def _replay(db: Db, found: list[UUID], *, reopen: bool, kind: str) -> list[UUID]:
    """One transaction per payment, so progress made before a failure is kept."""
    done: list[UUID] = []
    for pid in found:
        if await db.run(_enqueue, pid, reopen=reopen):
            done.append(pid)
            log.info("payment.replayed", payment_id=str(pid), kind=kind)
    return done


async def replay_dead(
    db: Db,
    *,
    reason: DeadReason | None,
    payment_id: UUID | None,
    limit: int,
    dry_run: bool,
) -> list[UUID]:
    """Reopen dead-lettered payments and enqueue a replay event for each."""
    found = await db.run(queries.find_dead, reason=reason, payment_id=payment_id, limit=limit)
    return found if dry_run else await _replay(db, found, reopen=True, kind="dead")


async def replay_stuck_pending(
    db: Db, *, older_than_s: float, limit: int, dry_run: bool
) -> list[UUID]:
    """Enqueue a replay event for pending payments without activity."""
    found = await db.run(queries.find_stuck_pending, older_than_s=older_than_s, limit=limit)
    return found if dry_run else await _replay(db, found, reopen=False, kind="stuck_pending")
