"""Idempotent payment creation under concurrency (design §4.3, §3.1)."""

import asyncio
from decimal import Decimal

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import Settings
from payments.db import queries
from payments.db.models import Payment
from payments.db.session import create_engine
from payments.schemas import PaymentCreate
from tests.support.api import count_rows, create_payment, payment_body
from tests.support.wait import wait_until

CONCURRENCY = 20


async def test_concurrent_same_key_creates_one_payment(
    client: httpx.AsyncClient, db_sessions: async_sessionmaker[AsyncSession]
) -> None:
    responses = await asyncio.gather(
        *(create_payment(client, key="same-key") for _ in range(CONCURRENCY))
    )

    assert [r.status_code for r in responses] == [202] * CONCURRENCY
    assert len({r.json()["payment_id"] for r in responses}) == 1
    replayed = sorted(r.headers["idempotent-replayed"] for r in responses)
    assert replayed.count("false") == 1
    assert replayed.count("true") == CONCURRENCY - 1
    assert await count_rows(db_sessions, "payments") == 1
    assert await count_rows(db_sessions, "outbox") == 1


async def test_concurrent_same_key_different_bodies(
    client: httpx.AsyncClient, db_sessions: async_sessionmaker[AsyncSession]
) -> None:
    responses = await asyncio.gather(
        *(
            create_payment(client, payment_body(amount=f"{index + 1}.00"), key="contested-key")
            for index in range(CONCURRENCY)
        )
    )

    accepted = [r for r in responses if r.status_code == 202]
    rejected = [r for r in responses if r.status_code == 422]
    assert len(accepted) == 1
    assert len(rejected) == CONCURRENCY - 1
    assert {r.json()["code"] for r in rejected} == {"idempotency_key_reused"}
    assert await count_rows(db_sessions, "payments") == 1
    assert await count_rows(db_sessions, "outbox") == 1


async def test_replay_returns_current_status(
    client: httpx.AsyncClient, db_sessions: async_sessionmaker[AsyncSession]
) -> None:
    first = await create_payment(client, payment_body(amount="100.00"), key="replay-1")
    payment_id = first.json()["payment_id"]
    assert first.json()["status"] == "pending"

    async with db_sessions() as session, session.begin():
        await session.execute(
            text("UPDATE payments SET status = 'succeeded', processed_at = now() WHERE id = :id"),
            {"id": payment_id},
        )

    # The same request, spelled differently (number instead of string), is still a replay.
    replay = await create_payment(client, payment_body(amount=100), key="replay-1")

    assert replay.status_code == 202
    assert replay.headers["idempotent-replayed"] == "true"
    assert replay.json()["payment_id"] == payment_id
    assert replay.json()["status"] == "succeeded"
    assert replay.json()["created_at"] == first.json()["created_at"]
    assert await count_rows(db_sessions, "payments") == 1
    assert await count_rows(db_sessions, "outbox") == 1

    different = await create_payment(client, payment_body(amount="100.01"), key="replay-1")
    assert different.status_code == 422
    assert different.json()["code"] == "idempotency_key_reused"


async def test_insert_waits_and_wins_after_holder_rollback(
    db_sessions: async_sessionmaker[AsyncSession],
) -> None:
    data = PaymentCreate.model_validate(payment_body())
    holder = db_sessions()
    contender = db_sessions()
    observer = db_sessions()
    try:
        first = await queries.insert_payment_if_absent(
            holder, data=data, idempotency_key="race-key", fingerprint="v1:holder"
        )
        assert first is not None  # the holder's transaction stays open

        async def contend() -> Payment | None:
            async with contender.begin():
                return await queries.insert_payment_if_absent(
                    contender, data=data, idempotency_key="race-key", fingerprint="v1:winner"
                )

        task = asyncio.create_task(contend())

        async def blocked_on_lock() -> bool:
            waiting = await observer.scalar(
                text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND datname = current_database()"
                )
            )
            await observer.rollback()
            return bool(waiting)

        await wait_until(blocked_on_lock, timeout=10)
        assert not task.done()
        await holder.rollback()

        winner = await asyncio.wait_for(task, timeout=10)
    finally:
        await holder.close()
        await contender.close()
        await observer.close()

    assert winner is not None
    assert winner.idempotency_key == "race-key"
    assert winner.request_fingerprint == "v1:winner"
    assert winner.amount == Decimal("100.00")
    async with db_sessions() as session:
        rows = (await session.execute(text("SELECT request_fingerprint FROM payments"))).all()
    assert [row.request_fingerprint for row in rows] == ["v1:winner"]


async def test_engine_uses_read_committed(settings: Settings) -> None:
    engine = create_engine(settings, role="test-role")
    try:
        async with engine.connect() as connection:
            isolation = await connection.scalar(text("SHOW transaction_isolation"))
            application = await connection.scalar(text("SHOW application_name"))
            statement_timeout = await connection.scalar(text("SHOW statement_timeout"))
    finally:
        await engine.dispose()

    assert isolation == "read committed"
    assert application == "test-role"
    assert statement_timeout == "4s"
