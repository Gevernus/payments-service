"""Integration infrastructure: PostgreSQL and RabbitMQ providers and per-test cleanup.

Environment variables take precedence (TEST_DATABASE_URL, TEST_RABBITMQ_URL,
TEST_RABBITMQ_MANAGEMENT_URL, TEST_RABBITMQCTL). When they are absent the providers start
testcontainers. If that is impossible the tests are skipped, or fail when REQUIRE_INFRA=1.
"""

import asyncio
import os
import shlex
from collections.abc import AsyncIterator, Iterator
from typing import NoReturn
from urllib.parse import quote, urlsplit

import httpx
import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

RABBITMQ_IMAGE = "rabbitmq:4.1.8-management-alpine"
POSTGRES_IMAGE = "postgres:17-alpine"
QUEUE_PREFIX = "payments"


def _unavailable(reason: str) -> NoReturn:
    if os.environ.get("REQUIRE_INFRA") == "1":
        pytest.fail(f"infrastructure required but unavailable: {reason}", pytrace=False)
    pytest.skip(f"infrastructure unavailable: {reason}")


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[object | None]:
    if os.environ.get("TEST_DATABASE_URL"):
        yield None
        return
    try:
        from testcontainers.community.postgres import PostgresContainer

        container = PostgresContainer(POSTGRES_IMAGE, driver="asyncpg")
        container.start()
    except Exception as exc:  # noqa: BLE001 - any failure to start means "no infrastructure"
        _unavailable(f"PostgreSQL container: {exc}")
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def rabbitmq_container() -> Iterator[object | None]:
    if os.environ.get("TEST_RABBITMQ_URL"):
        yield None
        return
    try:
        from testcontainers.community.rabbitmq import RabbitMqContainer

        container = RabbitMqContainer(RABBITMQ_IMAGE)
        container.with_exposed_ports(15672)
        container.start()
    except Exception as exc:  # noqa: BLE001
        _unavailable(f"RabbitMQ container: {exc}")
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def database_url(postgres_container: object | None) -> str:
    """SQLAlchemy URL (asyncpg driver) of the test database."""
    url = os.environ.get("TEST_DATABASE_URL")
    if url:
        return url
    assert postgres_container is not None
    return str(postgres_container.get_connection_url())  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def rabbitmq_url(rabbitmq_container: object | None) -> str:
    """AMQP URL of the test broker."""
    url = os.environ.get("TEST_RABBITMQ_URL")
    if url:
        return url
    assert rabbitmq_container is not None
    c = rabbitmq_container
    host = c.get_container_host_ip()  # type: ignore[attr-defined]
    port = c.get_exposed_port(c.port)  # type: ignore[attr-defined]
    return f"amqp://{c.username}:{c.password}@{host}:{port}/"  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def management_url(rabbitmq_container: object | None) -> str:
    """Base URL of the RabbitMQ management API, with credentials."""
    url = os.environ.get("TEST_RABBITMQ_MANAGEMENT_URL")
    if url:
        return url.rstrip("/")
    assert rabbitmq_container is not None
    c = rabbitmq_container
    host = c.get_container_host_ip()  # type: ignore[attr-defined]
    port = c.get_exposed_port(15672)  # type: ignore[attr-defined]
    return f"http://{c.username}:{c.password}@{host}:{port}"  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def rabbitmqctl(rabbitmq_container: object | None) -> list[str]:
    """Command prefix that runs rabbitmqctl against the test broker."""
    ctl = os.environ.get("TEST_RABBITMQCTL")
    if ctl:
        return shlex.split(ctl)
    assert rabbitmq_container is not None
    container_id = rabbitmq_container.get_wrapped_container().id  # type: ignore[attr-defined]
    return ["docker", "exec", container_id, "rabbitmqctl"]


def _alembic_upgrade(url: str) -> None:
    os.environ["DATABASE_URL"] = url
    config = Config("alembic.ini")
    command.upgrade(config, "head")


@pytest_asyncio.fixture(scope="session")
async def migrated_schema(database_url: str) -> None:
    """Recreate the schema once per session and migrate it to head."""
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
    except OSError as exc:
        _unavailable(f"PostgreSQL at {database_url}: {exc}")
    finally:
        await engine.dispose()
    # Alembic's async env runs its own event loop, so it must not run on ours.
    await asyncio.to_thread(_alembic_upgrade, database_url)


@pytest_asyncio.fixture
async def migrated_db(database_url: str, migrated_schema: None) -> AsyncIterator[str]:
    """The migrated test database, emptied after the test. Yields the database URL."""
    yield database_url
    engine = create_async_engine(database_url, poolclass=NullPool)
    try:
        async with engine.begin() as conn:
            rows = await conn.execute(
                text(
                    "SELECT tablename FROM pg_tables "
                    "WHERE schemaname = 'public' AND tablename <> 'alembic_version'"
                )
            )
            tables = [row[0] for row in rows]
            if tables:
                names = ", ".join(f'"{name}"' for name in tables)
                await conn.execute(text(f"TRUNCATE {names} RESTART IDENTITY CASCADE"))
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def db_engine(migrated_db: str) -> AsyncIterator[AsyncEngine]:
    """An engine on the migrated database."""
    engine = create_async_engine(migrated_db, poolclass=NullPool)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
def db_sessions(db_engine: AsyncEngine) -> async_sessionmaker:  # type: ignore[type-arg]
    """A session factory on the migrated database."""
    return async_sessionmaker(db_engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def broker_cleanup(management_url: str) -> AsyncIterator[None]:
    """Delete every `payments*` queue through the management API after the test."""
    yield
    parts = urlsplit(management_url)
    base = f"{parts.scheme}://{parts.hostname}:{parts.port}"
    auth = (parts.username or "guest", parts.password or "guest")
    async with httpx.AsyncClient(base_url=base, auth=auth, timeout=10) as client:
        response = await client.get("/api/queues")
        response.raise_for_status()
        for queue in response.json():
            name = queue["name"]
            if name.startswith(QUEUE_PREFIX):
                vhost = quote(queue["vhost"], safe="")
                await client.delete(f"/api/queues/{vhost}/{quote(name, safe='')}")
