"""The outbox relay publishes every row at least once, never loses one, and fences stale writes.

Runs against real PostgreSQL and RabbitMQ (design §5, §6; failure matrix in docs/guarantees.md). Faults are injected with TCP proxies
in front of either service, and with a RabbitMQ memory alarm for the blocked-connection case.
"""

import asyncio
import json
import subprocess
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Sequence
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import aio_pika
import httpx
import pytest
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import Settings
from payments.db import queries
from payments.db.session import Db, create_engine, create_session_factory
from payments.messaging.topology import MAIN_QUEUE_NAME, declare_core, declare_retry_ladder
from payments.workers.relay import OutboxRelay
from tests.support.fakes import make_settings
from tests.support.subprocess_worker import Worker, start_worker
from tests.support.tcp_proxy import TcpProxy
from tests.support.wait import wait_until

Sessions = async_sessionmaker[AsyncSession]


@dataclass(frozen=True)
class Event:
    """An outbox row written by the test, as the API would write it."""

    id: uuid.UUID
    payment_id: uuid.UUID
    payload: dict[str, str]
    headers: dict[str, str]


def relay_settings(
    database_url: str, rabbitmq_url: str | None = None, **overrides: Any
) -> Settings:
    """Fast-polling settings with a consistent timeout chain (design §7.5)."""
    values: dict[str, Any] = {
        "database_url": database_url,
        "outbox_poll_interval_s": 0.05,
        "outbox_lease_s": 30,
        "rabbit_publish_timeout_s": 2,
    }
    if rabbitmq_url is not None:
        values["rabbitmq_url"] = rabbitmq_url
    values.update(overrides)
    return make_settings(**values)


def through_proxy_amqp(url: str, port: int) -> str:
    """The AMQP URL with its port replaced."""
    parts = urlsplit(url)
    userinfo = f"{parts.username}:{parts.password}@" if parts.username else ""
    return f"{parts.scheme}://{userinfo}127.0.0.1:{port}{parts.path or '/'}"


def through_proxy_db(url: str, port: int) -> str:
    """The SQLAlchemy URL with its host and port replaced."""
    return make_url(url).set(host="127.0.0.1", port=port).render_as_string(hide_password=False)


async def add_event(
    sessions: Sessions,
    *,
    routing_key: str = MAIN_QUEUE_NAME,
    request_id: str | None = None,
) -> Event:
    """Insert a payment and its pending outbox row."""
    event_id, payment_id = uuid.uuid4(), uuid.uuid4()
    payload = {
        "event_id": str(event_id),
        "payment_id": str(payment_id),
        "occurred_at": datetime.now(UTC).isoformat(),
    }
    headers = {"x-request-id": request_id} if request_id else {}
    async with sessions.begin() as session:
        await session.execute(
            text(
                "INSERT INTO payments (id, amount, currency, idempotency_key, request_fingerprint,"
                " webhook_url) VALUES (:id, 10, 'RUB', :key, 'v1:x', 'https://example.com/hook')"
            ),
            {"id": payment_id, "key": f"relay-{payment_id}"},
        )
        await session.execute(
            text(
                "INSERT INTO outbox (id, payment_id, event_type, routing_key, payload, headers)"
                " VALUES (:id, :payment_id, 'payment.created', :routing_key,"
                " CAST(:payload AS jsonb), CAST(:headers AS jsonb))"
            ),
            {
                "id": event_id,
                "payment_id": payment_id,
                "routing_key": routing_key,
                "payload": json.dumps(payload),
                "headers": json.dumps(headers),
            },
        )
    return Event(event_id, payment_id, payload, headers)


async def outbox_rows(sessions: Sessions) -> dict[uuid.UUID, dict[str, Any]]:
    """Every outbox row by id: attempts, last_error and whether it is hidden from claims."""
    async with sessions() as session:
        result = await session.execute(
            text("SELECT id, attempts, last_error, available_at > now() AS hidden FROM outbox")
        )
        return {row.id: dict(row._mapping) for row in result}


async def outbox_count(sessions: Sessions) -> int:
    return len(await outbox_rows(sessions))


async def claim(sessions: Sessions, *, lease_s: float, batch: int = 100) -> list[Any]:
    async with sessions.begin() as session:
        return list(await queries.claim_outbox(session, batch=batch, lease_s=lease_s))


def make_broker(url: str) -> RabbitBroker:
    """A broker the way the relay needs it: confirms on, returned messages raise."""
    return RabbitBroker(
        url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        reconnect_interval=0.2,
    )


_BROKERS: dict[int, RabbitBroker] = {}


@asynccontextmanager
async def relay_stack(settings: Settings) -> AsyncIterator[OutboxRelay]:
    """A relay on its own engine and broker; the broker is connected and the topology declared."""
    engine = create_engine(settings, role="relay-test")
    broker = make_broker(settings.rabbitmq_url)
    try:
        await broker.start()
        await declare_core(broker)
        db = Db(create_session_factory(engine), settings.db_call_timeout_s)
        relay = OutboxRelay(db=db, broker=broker, settings=settings)
        _BROKERS[id(relay)] = broker
        yield relay
    finally:
        with suppress(Exception):
            await asyncio.wait_for(broker.stop(), 10)
        await engine.dispose()


@asynccontextmanager
async def running(relay: OutboxRelay) -> AsyncIterator[asyncio.Task[None]]:
    """Run the relay loop in a task and stop it on exit."""
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    try:
        yield task
    finally:
        stop.set()
        await asyncio.wait_for(task, 15)


class Observer:
    """Reads the messages that reach `payments.new` with basic.get, on its own connection."""

    def __init__(self, connection: aio_pika.abc.AbstractRobustConnection) -> None:
        self._connection = connection
        self.messages: list[AbstractIncomingMessage] = []
        self.queue: AbstractQueue | None = None

    async def open(self, *, timeout: float = 30) -> None:
        """Attach to `payments.new`; waits for the queue to appear (a process may declare it)."""

        async def attach() -> bool:
            channel = await self._connection.channel()
            try:
                self.queue = await channel.get_queue(MAIN_QUEUE_NAME, ensure=True)
            except aio_pika.exceptions.ChannelNotFoundEntity:
                return False
            return True

        await wait_until(attach, timeout=timeout, interval=0.2)

    async def channel(self) -> aio_pika.abc.AbstractChannel:
        return await self._connection.channel()

    async def drain(self) -> list[AbstractIncomingMessage]:
        """Take every message currently in the queue; returns the newly taken ones."""
        assert self.queue is not None
        taken: list[AbstractIncomingMessage] = []
        while (message := await self.queue.get(no_ack=True, fail=False)) is not None:
            taken.append(message)
        self.messages.extend(taken)
        return taken

    async def wait_for(self, count: int, *, timeout: float = 20) -> list[AbstractIncomingMessage]:
        async def enough() -> bool:
            await self.drain()
            return len(self.messages) >= count

        await wait_until(enough, timeout=timeout, interval=0.1)
        return self.messages

    def ids(self) -> list[str | None]:
        return [m.message_id for m in self.messages]

    async def publish_raw(self, message_id: str, body: bytes, routing_key: str) -> None:
        """Publish straight to the `payments` exchange, as a relay that then crashed would."""
        channel = await self._connection.channel(publisher_confirms=True)
        exchange = await channel.get_exchange("payments", ensure=True)
        await exchange.publish(
            aio_pika.Message(body, message_id=message_id, delivery_mode=2), routing_key=routing_key
        )


@pytest.fixture
def relay_config(migrated_db: str, rabbitmq_url: str) -> Settings:
    return relay_settings(migrated_db, rabbitmq_url)


@pytest.fixture(autouse=True)
def _clean_broker(broker_cleanup: None) -> None:
    """Every test removes its `payments*` queues, and the exchanges are recreated on demand."""


@pytest.fixture
async def observer_factory(
    rabbitmq_url: str,
) -> AsyncIterator[Callable[[], Awaitable[Observer]]]:
    """Open observers on the direct broker address; queues must exist before `open`."""
    connections: list[aio_pika.abc.AbstractRobustConnection] = []

    async def make() -> Observer:
        connection = await aio_pika.connect_robust(rabbitmq_url)
        connections.append(connection)
        observer = Observer(connection)
        await observer.open()
        return observer

    try:
        yield make
    finally:
        for connection in connections:
            with suppress(Exception):
                await connection.close()


async def test_relay_publishes_and_deletes(
    relay_config: Settings,
    db_sessions: Sessions,
    observer_factory: Callable[[], Awaitable[Observer]],
) -> None:
    async with relay_stack(relay_config) as relay:
        observer = await observer_factory()
        event = await add_event(db_sessions, request_id="req-relay-1")

        published = await relay.run_once()

        assert published == 1
        (message,) = await observer.wait_for(1)
        assert message.message_id == str(event.id)
        assert message.type == "payment.created"
        assert message.correlation_id == str(event.payment_id)
        assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
        assert json.loads(message.body) == event.payload
        assert message.headers["x-request-id"] == "req-relay-1"
        assert message.routing_key == MAIN_QUEUE_NAME
        assert await outbox_count(db_sessions) == 0
        assert await relay.run_once() == 0


async def test_two_relays_disjoint(
    migrated_db: str,
    rabbitmq_url: str,
    db_sessions: Sessions,
    observer_factory: Callable[[], Awaitable[Observer]],
) -> None:
    config = relay_settings(migrated_db, rabbitmq_url, outbox_batch_size=4)
    events = [await add_event(db_sessions) for _ in range(30)]

    async with (
        relay_stack(config) as first,
        relay_stack(config) as second,
        relay_stack(config) as third,
    ):
        observer = await observer_factory()
        async with running(first), running(second), running(third):
            await wait_until(lambda: _no_rows(db_sessions), timeout=30, interval=0.1)

    await observer.wait_for(30)
    await observer.drain()
    ids = observer.ids()
    assert sorted(ids, key=str) == sorted((str(e.id) for e in events), key=str)
    assert len(set(ids)) == 30


async def _no_rows(sessions: Sessions) -> bool:
    return await outbox_count(sessions) == 0


async def test_relay_row_reclaimed_after_lease_expiry(db_sessions: Sessions) -> None:
    event = await add_event(db_sessions)

    (first,) = await claim(db_sessions, lease_s=2)
    assert first.id == event.id
    assert first.attempts == 1
    assert first.payment_id == event.payment_id
    assert first.routing_key == MAIN_QUEUE_NAME
    assert first.payload == event.payload
    assert await claim(db_sessions, lease_s=0.5) == []  # still leased, hidden from others

    reclaimed: list[Any] = []

    async def lease_expired() -> bool:
        reclaimed.extend(await claim(db_sessions, lease_s=30))
        return bool(reclaimed)

    await wait_until(lease_expired, timeout=15, interval=0.1)

    (second,) = reclaimed
    assert second.id == event.id
    assert second.attempts == 2


async def test_relay_crash_after_confirm_republishes_same_message_id(
    migrated_db: str,
    rabbitmq_url: str,
    db_sessions: Sessions,
    observer_factory: Callable[[], Awaitable[Observer]],
) -> None:
    config = relay_settings(
        migrated_db, rabbitmq_url, outbox_lease_s=1, rabbit_publish_timeout_s=0.4
    )
    event = await add_event(db_sessions)
    async with relay_stack(config) as relay:
        observer = await observer_factory()
        # A relay claimed the row, got the broker confirm, and died before deleting the row.
        (claimed,) = await claim(db_sessions, lease_s=0.5)
        await observer.publish_raw(
            str(claimed.id), json.dumps(event.payload).encode(), claimed.routing_key
        )
        await observer.wait_for(1)

        async with running(relay):
            await observer.wait_for(2)
            await wait_until(lambda: _no_rows(db_sessions), timeout=15, interval=0.1)

    assert observer.ids() == [str(event.id), str(event.id)]


async def test_relay_lease_lost_fencing(db_sessions: Sessions) -> None:
    event = await add_event(db_sessions)
    (stale,) = await claim(db_sessions, lease_s=0.3)
    assert stale.attempts == 1

    current: list[Any] = []

    async def reclaimed() -> bool:
        current.extend(await claim(db_sessions, lease_s=60))
        return bool(current)

    await wait_until(reclaimed, timeout=15, interval=0.1)
    assert current[0].attempts == 2

    async with db_sessions.begin() as session:
        deleted = await queries.delete_published(session, [(event.id, stale.attempts)])
    async with db_sessions.begin() as session:
        deferred = await queries.defer_outbox(
            session, event_id=event.id, attempts=stale.attempts, backoff_s=60, error="stale"
        )
    assert deleted == 0
    assert deferred == 0
    rows = await outbox_rows(db_sessions)
    assert rows[event.id]["attempts"] == 2
    assert rows[event.id]["last_error"] is None

    async with db_sessions.begin() as session:
        deleted = await queries.delete_published(session, [(event.id, current[0].attempts)])
    assert deleted == 1
    assert await outbox_count(db_sessions) == 0


async def test_unroutable_not_marked_published(
    relay_config: Settings,
    db_sessions: Sessions,
    observer_factory: Callable[[], Awaitable[Observer]],
) -> None:
    async with relay_stack(relay_config) as relay:
        observer = await observer_factory()
        lost = await add_event(db_sessions, routing_key="payments.nobody-listens")
        good = await add_event(db_sessions)

        published = await relay.run_once()

        assert published == 1
        (message,) = await observer.wait_for(1)
        assert message.message_id == str(good.id)
        rows = await outbox_rows(db_sessions)
        assert set(rows) == {lost.id}
        assert rows[lost.id]["attempts"] == 1
        assert rows[lost.id]["last_error"]
        assert rows[lost.id]["hidden"] is True  # backed off, not hot-looped


async def test_relay_survives_db_outage(
    migrated_db: str,
    rabbitmq_url: str,
    db_sessions: Sessions,
    observer_factory: Callable[[], Awaitable[Observer]],
) -> None:
    target = make_url(migrated_db)
    proxy = TcpProxy()
    port = await proxy.start(target.host or "127.0.0.1", target.port or 5432)
    config = relay_settings(through_proxy_db(migrated_db, port), rabbitmq_url)
    try:
        async with relay_stack(config) as relay:
            observer = await observer_factory()
            async with running(relay) as task:
                await add_event(db_sessions)
                await observer.wait_for(1)
                await wait_until(lambda: _no_rows(db_sessions), timeout=15, interval=0.1)

                proxy.set_mode("drop")
                proxy.set_mode("refuse")
                await keeps_ticking(relay)
                during = [await add_event(db_sessions) for _ in range(2)]
                await keeps_ticking(relay)
                assert not task.done()
                assert await observer.drain() == []
                assert all(
                    row["attempts"] == 0 for row in (await outbox_rows(db_sessions)).values()
                )

                proxy.set_mode("pass")
                await observer.wait_for(3, timeout=30)
                await wait_until(lambda: _no_rows(db_sessions), timeout=15, interval=0.1)
                assert not task.done()
        assert set(observer.ids()[1:]) == {str(e.id) for e in during}
    finally:
        proxy.set_mode("pass")
        proxy.close()


async def test_relay_waits_while_broker_down(
    migrated_db: str,
    rabbitmq_url: str,
    db_sessions: Sessions,
    observer_factory: Callable[[], Awaitable[Observer]],
) -> None:
    target = urlsplit(rabbitmq_url)
    proxy = TcpProxy()
    port = await proxy.start(target.hostname or "127.0.0.1", target.port or 5672)
    config = relay_settings(migrated_db, through_proxy_amqp(rabbitmq_url, port))
    try:
        async with relay_stack(config) as relay:
            observer = await observer_factory()
            async with running(relay) as task:
                await add_event(db_sessions)
                await observer.wait_for(1)
                await wait_until(lambda: _no_rows(db_sessions), timeout=15, interval=0.1)

                proxy.set_mode("drop")
                proxy.set_mode("refuse")
                await wait_until(lambda: _broker_down(relay), timeout=20, interval=0.1)

                waiting = [await add_event(db_sessions) for _ in range(3)]
                await keeps_ticking(relay)
                rows = await outbox_rows(db_sessions)
                assert {e.id for e in waiting} == set(rows)
                assert all(row["attempts"] == 0 for row in rows.values())  # never claimed
                assert not task.done()

                proxy.set_mode("pass")
                await observer.wait_for(4, timeout=30)
                await wait_until(lambda: _no_rows(db_sessions), timeout=15, interval=0.1)
        assert set(observer.ids()[1:]) == {str(e.id) for e in waiting}
    finally:
        proxy.set_mode("pass")
        proxy.close()


async def keeps_ticking(relay: OutboxRelay, seconds: float = 0.3) -> None:
    """Wait until the relay loop has iterated for `seconds` more (it is alive, just idle)."""
    start = relay.last_tick or 0.0
    await wait_until(lambda: (relay.last_tick or 0.0) > start + seconds, timeout=20)


async def _broker_down(relay: OutboxRelay) -> bool:
    """True once the relay's broker connection has noticed the outage."""
    broker = _BROKERS[id(relay)]
    try:
        return not await broker.ping(timeout=0.5)
    except Exception:  # noqa: BLE001 - a failing ping means the connection is down
        return True


def worker_env(config: Settings, **extra: str) -> dict[str, str]:
    env = {
        "DATABASE_URL": config.database_url,
        "RABBITMQ_URL": config.rabbitmq_url,
        "OUTBOX_POLL_INTERVAL_S": "0.05",
        "LOG_FORMAT": "json",
    }
    env.update(extra)
    return env


async def test_relay_startup_fails_on_topology_conflict(
    relay_config: Settings,
    rabbitmq_url: str,
    db_sessions: Sessions,
    observer_factory: Callable[[], Awaitable[Observer]],
) -> None:
    # Control: on a clean broker the relay process starts and publishes.
    worker = await start_worker("relay", worker_env(relay_config))
    try:
        observer = await observer_factory()
        await add_event(db_sessions)
        await observer.wait_for(1, timeout=30)
        # The relay's own broker configuration must treat an unroutable message as a failure.
        lost = await add_event(db_sessions, routing_key="payments.nobody-listens")

        async def failure_recorded() -> bool:
            row = (await outbox_rows(db_sessions)).get(lost.id)
            return row is not None and bool(row["last_error"])

        await wait_until(failure_recorded, timeout=30, interval=0.1)
        assert (await outbox_rows(db_sessions))[lost.id]["attempts"] >= 1
        assert len(await observer.drain()) == 0
        assert worker.process.returncode is None
    finally:
        await worker.stop()

    async with db_sessions.begin() as session:
        await session.execute(text("DELETE FROM outbox"))

    # A queue with the same name but different arguments must stop the relay at startup.
    connection = await aio_pika.connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        await (await channel.get_queue(MAIN_QUEUE_NAME, ensure=True)).delete(
            if_unused=False, if_empty=False
        )
        await channel.declare_queue(MAIN_QUEUE_NAME, durable=True, arguments={"x-max-length": 5})

        conflicting = await start_worker("relay", worker_env(relay_config))
        try:
            await add_event(db_sessions)
            code = await conflicting.wait(timeout=30)
        finally:
            await conflicting.stop()
        assert code != 0
        assert await outbox_count(db_sessions) == 1  # nothing was published or settled
    finally:
        with suppress(Exception):
            await connection.close()


async def test_changing_retry_delay_does_not_break_startup(
    relay_config: Settings,
    migrated_db: str,
    rabbitmq_url: str,
    db_sessions: Sessions,
    observer_factory: Callable[[], Awaitable[Observer]],
) -> None:
    broker = make_broker(rabbitmq_url)
    await broker.start()
    try:
        await declare_core(broker)
        await declare_retry_ladder(broker, [5000, 10000])
        observer = await observer_factory()

        worker: Worker = await start_worker(
            "relay", worker_env(relay_config, RETRY_BASE_DELAY_MS="7000")
        )
        try:
            await add_event(db_sessions)
            await observer.wait_for(1, timeout=30)
            assert worker.process.returncode is None
        finally:
            await worker.stop()

        # A consumer started with the new delay declares new queues next to the old ones.
        await declare_retry_ladder(broker, [7000, 14000])
        channel = await observer.channel()
        for delay in (5000, 10000, 7000, 14000):
            await channel.get_queue(f"payments.new.retry.{delay}ms", ensure=True)
    finally:
        await broker.stop()


@pytest.fixture
def memory_alarm_guard(rabbitmqctl: Sequence[str]) -> Iterator[None]:
    """Always restore the default memory watermark, even when the test fails or is interrupted."""
    try:
        yield
    finally:
        subprocess.run(
            [*rabbitmqctl, "set_vm_memory_high_watermark", "0.4"],
            check=False,
            capture_output=True,
            timeout=60,
        )


async def _memory_alarm(management_url: str) -> bool:
    parts = urlsplit(management_url)
    base = f"{parts.scheme}://{parts.hostname}:{parts.port}"
    auth = (parts.username or "guest", parts.password or "guest")
    async with httpx.AsyncClient(base_url=base, auth=auth, timeout=10) as client:
        response = await client.get("/api/nodes")
        response.raise_for_status()
        return any(node.get("mem_alarm") for node in response.json())


async def _set_watermark(rabbitmqctl: Sequence[str], value: str) -> None:
    await asyncio.to_thread(
        subprocess.run,
        [*rabbitmqctl, "set_vm_memory_high_watermark", value],
        check=True,
        capture_output=True,
        timeout=60,
    )


@pytest.mark.usefixtures("memory_alarm_guard")
async def test_publish_timeout_enforced_when_connection_blocked(
    migrated_db: str,
    rabbitmq_url: str,
    management_url: str,
    rabbitmqctl: Sequence[str],
    db_sessions: Sessions,
) -> None:
    config = relay_settings(
        migrated_db, rabbitmq_url, rabbit_publish_timeout_s=1, outbox_lease_s=30
    )
    async with relay_stack(config) as relay:
        event = await add_event(db_sessions)
        try:
            await _set_watermark(rabbitmqctl, "0.0001")
            await wait_until(lambda: _memory_alarm(management_url), timeout=30, interval=0.2)

            loop = asyncio.get_running_loop()
            started = loop.time()
            attempt = asyncio.create_task(relay.run_once())

            # The claim must be committed before the publish starts: while the publish is
            # stuck, another session sees the row leased (attempts bumped, hidden).
            async def leased() -> bool:
                row = (await outbox_rows(db_sessions)).get(event.id)
                return row is not None and row["attempts"] == 1 and row["hidden"] is True

            await wait_until(leased, timeout=config.rabbit_publish_timeout_s, interval=0.05)
            assert not attempt.done(), "the claim became visible only after the publish ended"
            done, _ = await asyncio.wait({attempt}, timeout=config.rabbit_publish_timeout_s + 1)
            elapsed = loop.time() - started
            if not done:
                attempt.cancel()
                with suppress(asyncio.CancelledError):
                    await attempt
            assert done, "publish was not bounded by RABBIT_PUBLISH_TIMEOUT_S"
            assert elapsed < config.rabbit_publish_timeout_s + 1
            error = attempt.exception()
            assert error is None or isinstance(error, TimeoutError)
            if error is None:
                assert attempt.result() == 0

            rows = await outbox_rows(db_sessions)
            assert set(rows) == {event.id}  # not deleted: it was never confirmed
        finally:
            await _set_watermark(rabbitmqctl, "0.4")
            await wait_until(lambda: _alarm_cleared(management_url), timeout=60, interval=0.5)


async def _alarm_cleared(management_url: str) -> bool:
    return not await _memory_alarm(management_url)
