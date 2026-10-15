"""Replay of dead-lettered and stuck payments (CLI backend)."""

from uuid import UUID

from payments.db.session import Db
from payments.errors import DeadReason


async def replay_dead(
    db: Db,
    *,
    reason: DeadReason | None,
    payment_id: UUID | None,
    limit: int,
    dry_run: bool,
) -> list[UUID]:
    """Reopen dead-lettered payments and enqueue a replay event for each."""
    raise NotImplementedError


async def replay_stuck_pending(
    db: Db, *, older_than_s: float, limit: int, dry_run: bool
) -> list[UUID]:
    """Enqueue a replay event for pending payments without activity."""
    raise NotImplementedError
