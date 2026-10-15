"""The migration, the models and the constraints of design §3.1 and §3.2 agree."""

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from payments.db.models import Base

PAYMENT_CONSTRAINTS = {
    "uq_payments_idempotency_key": "u",
    "ck_payments_amount_positive": "c",
    "ck_payments_currency": "c",
    "ck_payments_status": "c",
    "ck_payments_processed_at": "c",
    "ck_payments_delivery_state": "c",
    "ck_payments_delivered_at": "c",
    "ck_payments_delivered_final": "c",
    "ck_payments_dead_lettered_at": "c",
    "ck_payments_dead_reason": "c",
    "ck_payments_attempts": "c",
}

# The models deliberately omit CHECK constraints, the unique constraint and the index
# (they live only in the migration), so the comparison ignores exactly these objects.
MIGRATION_ONLY = {"uq_payments_idempotency_key", "ix_outbox_available_at"}

INSERT_PAYMENT = """
INSERT INTO payments (id, amount, currency, status, idempotency_key, request_fingerprint,
                      webhook_url, processed_at, delivery_state, delivered_at, attempts,
                      attempts_base)
VALUES (gen_random_uuid(), :amount, :currency, :status, :key, 'v1:x', 'https://example.com',
        :processed_at, :delivery_state, :delivered_at, :attempts, :attempts_base)
"""


def alembic_config() -> Config:
    return Config("alembic.ini")


async def run_alembic(
    monkeypatch: pytest.MonkeyPatch, url: str, action: str, revision: str
) -> None:
    """Run an Alembic command in a thread: its async env owns an event loop."""
    monkeypatch.setenv("DATABASE_URL", url)
    func = getattr(command, action)
    await asyncio.to_thread(func, alembic_config(), revision)


def unexpected_differences(connection: Connection) -> list[Any]:
    context = MigrationContext.configure(
        connection, opts={"compare_type": True, "compare_server_default": False}
    )
    differences: list[Any] = []
    for diff in compare_metadata(context, Base.metadata):
        flattened = diff if isinstance(diff, list) else [diff]
        for item in flattened:
            operation, subject = item[0], item[1]
            name = getattr(subject, "name", None)
            if operation in {"remove_index", "remove_constraint"} and name in MIGRATION_ONLY:
                continue
            differences.append(item)
    return differences


async def test_migrations_match_models(db_engine: AsyncEngine) -> None:
    async with db_engine.connect() as connection:
        tables = set(
            (
                await connection.scalars(
                    text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                )
            ).all()
        )
        differences = await connection.run_sync(unexpected_differences)

    assert tables == {"payments", "outbox", "alembic_version"}
    assert differences == []


async def test_constraints_present(db_engine: AsyncEngine) -> None:
    async with db_engine.connect() as connection:
        rows = await connection.execute(
            text(
                "SELECT c.conname, c.contype::text AS contype FROM pg_constraint c "
                "JOIN pg_class t ON t.oid = c.conrelid "
                "WHERE t.relname = 'payments' AND c.contype IN ('u', 'c')"
            )
        )
        found = {row.conname: row.contype for row in rows}
        index = await connection.scalar(
            text(
                "SELECT indexdef FROM pg_indexes "
                "WHERE tablename = 'outbox' AND indexname = 'ix_outbox_available_at'"
            )
        )
        outbox_fk = await connection.scalar(
            text(
                "SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid "
                "WHERE t.relname = 'outbox' AND c.contype = 'f'"
            )
        )

    assert found == PAYMENT_CONSTRAINTS
    assert index is not None
    assert "available_at" in index
    assert outbox_fk == 1


DONE = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.mark.parametrize(
    ("constraint", "overrides"),
    [
        ("ck_payments_amount_positive", {"amount": 0}),
        ("ck_payments_currency", {"currency": "GBP"}),
        ("ck_payments_status", {"status": "unknown", "processed_at": DONE}),
        ("ck_payments_processed_at", {"status": "succeeded"}),
        (
            "ck_payments_delivered_at",
            {"delivery_state": "delivered", "status": "succeeded", "processed_at": DONE},
        ),
        ("ck_payments_delivered_final", {"delivery_state": "delivered", "delivered_at": DONE}),
        ("ck_payments_attempts", {"attempts": 1, "attempts_base": 2}),
    ],
)
async def test_constraints_reject_invalid_rows(
    db_engine: AsyncEngine, constraint: str, overrides: dict[str, Any]
) -> None:
    params: dict[str, Any] = {
        "amount": 1,
        "currency": "RUB",
        "status": "pending",
        "key": "k",
        "processed_at": None,
        "delivery_state": "pending",
        "delivered_at": None,
        "attempts": 0,
        "attempts_base": 0,
    }
    params.update(overrides)
    statement = text(INSERT_PAYMENT)
    async with db_engine.connect() as connection:
        with pytest.raises(IntegrityError, match=constraint):
            await connection.execute(statement, params)


async def test_downgrade_upgrade(
    migrated_db: str, db_engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def tables() -> set[str]:
        async with db_engine.connect() as connection:
            return set(
                (
                    await connection.scalars(
                        text("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
                    )
                ).all()
            )

    try:
        await run_alembic(monkeypatch, migrated_db, "downgrade", "base")
        assert await tables() <= {"alembic_version"}
        await run_alembic(monkeypatch, migrated_db, "upgrade", "head")
        assert await tables() == {"payments", "outbox", "alembic_version"}
    finally:
        # The schema is shared by the whole session: always leave it migrated.
        await run_alembic(monkeypatch, migrated_db, "upgrade", "head")
