"""Shared harness for the consumer, replay and consumer-process tests.

It builds settings with a consistent timeout chain, inserts payments the way the API would,
publishes events the way the relay would, runs the real consumer application in-process or as a
subprocess, and gives tests a view of the queues (the dead-letter queue and a spy queue that
sees every message routed to `payments.new`, including copies that come back from retry queues).
"""

import asyncio
import json
import os
import sys
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlsplit

import aio_pika
import httpx
import pytest_asyncio
from aio_pika.abc import AbstractIncomingMessage, AbstractQueue, AbstractRobustConnection
from faststream import AckPolicy
from faststream.asgi import AsgiFastStream
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import Settings
from payments.db.session import Db, create_engine, create_session_factory
from payments.messaging.copies import CopyPublisher
from payments.messaging.topology import (
    DLQ_NAME,
    MAIN_QUEUE,
    MAIN_QUEUE_NAME,
    PAYMENTS_EXCHANGE,
    declare_core,
    declare_retry_ladder,
)
from payments.schemas import PaymentEvent
from payments.services.gateway import EmulatedGateway
from payments.services.processing import Processor
from payments.webhooks.guard import system_resolver
from payments.webhooks.sender import HttpWebhookSender, create_http_client
from payments.workers.consumer import ConsumerDeps, create_app, handle_payment_event
from payments.workers.relay import OutboxRelay
from tests.support.fakes import make_settings
from tests.support.receiver import Delivery, Receiver
from tests.support.tcp_proxy import TcpProxy
from tests.support.wait import wait_until

Sessions = async_sessionmaker[AsyncSession]
Row = dict[str, Any]

HOOK_HOST = "localhost"
BASE_DELAY_MS = 200
SPY_QUEUE = "payments.spy"


def consumer_settings(database_url: str, rabbitmq_url: str, **overrides: Any) -> Settings:
    """Settings with fast retries and a timeout chain that satisfies design §7.5.

    The webhook and database deadlines are short; `stale_after_s` and the graceful timeout are
    derived from them unless a test sets them.
    """
    values: dict[str, Any] = {
        "database_url": database_url,
        "rabbitmq_url": rabbitmq_url,
        "gateway_outcome_hints": True,
        "webhook_internal_hosts": frozenset({HOOK_HOST}),
        "webhook_total_timeout_s": 2,
        "db_call_timeout_s": 5,
        "rabbit_publish_timeout_s": 2,
        "retry_base_delay_ms": BASE_DELAY_MS,
    }
    values.update(overrides)
    gateway_max = float(values.get("gateway_max_delay_s", 0))
    webhook = float(values["webhook_total_timeout_s"])
    db_call = float(values["db_call_timeout_s"])
    publish = float(values["rabbit_publish_timeout_s"])
    values.setdefault("stale_after_s", gateway_max + webhook + 2 * db_call + 1)
    values.setdefault(
        "consumer_graceful_timeout_s", gateway_max + webhook + 4 * db_call + publish + 1
    )
    return make_settings(**values)


def worker_env(settings: Settings, **extra: str) -> dict[str, str]:
    """Environment variables that make a subprocess build the same settings."""
    secret = settings.webhook_secret
    assert secret is not None
    env = {
        "DATABASE_URL": settings.database_url,
        "RABBITMQ_URL": settings.rabbitmq_url,
        "WEBHOOK_SECRET": secret.get_secret_value(),
        "WEBHOOK_INTERNAL_HOSTS": ",".join(sorted(settings.webhook_internal_hosts)),
        "GATEWAY_MIN_DELAY_S": str(settings.gateway_min_delay_s),
        "GATEWAY_MAX_DELAY_S": str(settings.gateway_max_delay_s),
        "GATEWAY_OUTCOME_HINTS": "true" if settings.gateway_outcome_hints else "false",
        "RETRY_MAX_ATTEMPTS": str(settings.retry_max_attempts),
        "RETRY_BASE_DELAY_MS": str(settings.retry_base_delay_ms),
        "CONSUMER_PREFETCH": str(settings.consumer_prefetch),
        "CONSUMER_GRACEFUL_TIMEOUT_S": str(settings.consumer_graceful_timeout_s),
        "WEBHOOK_TOTAL_TIMEOUT_S": str(settings.webhook_total_timeout_s),
        "STALE_AFTER_S": str(settings.stale_after_s),
        "DB_CALL_TIMEOUT_S": str(settings.db_call_timeout_s),
        "RABBIT_PUBLISH_TIMEOUT_S": str(settings.rabbit_publish_timeout_s),
        "LOG_FORMAT": "json",
    }
    env.update(extra)
    return env


def through_proxy_amqp(url: str, port: int) -> str:
    """The AMQP URL with its host and port replaced by a local proxy."""
    parts = urlsplit(url)
    userinfo = f"{parts.username}:{parts.password}@" if parts.username else ""
    return f"{parts.scheme}://{userinfo}127.0.0.1:{port}{parts.path or '/'}"


def through_proxy_db(url: str, port: int) -> str:
    """The SQLAlchemy URL with its host and port replaced by a local proxy."""
    return make_url(url).set(host="127.0.0.1", port=port).render_as_string(hide_password=False)


async def start_amqp_proxy(rabbitmq_url: str) -> tuple[TcpProxy, str]:
    """A proxy in front of the broker and the URL that reaches the broker through it."""
    target = urlsplit(rabbitmq_url)
    proxy = TcpProxy()
    port = await proxy.start(target.hostname or "127.0.0.1", target.port or 5672)
    return proxy, through_proxy_amqp(rabbitmq_url, port)


async def start_db_proxy(database_url: str) -> tuple[TcpProxy, str]:
    """A proxy in front of PostgreSQL and the URL that reaches the database through it."""
    target = make_url(database_url)
    proxy = TcpProxy()
    port = await proxy.start(target.host or "127.0.0.1", target.port or 5432)
    return proxy, through_proxy_db(database_url, port)


def cut(proxy: TcpProxy) -> None:
    """Break existing connections and refuse new ones."""
    proxy.set_mode("drop")
    proxy.set_mode("refuse")


# --- payments ----------------------------------------------------------------------------------

_INSERT_PAYMENT = text(
    """
    INSERT INTO payments (
        id, amount, currency, description, metadata, status, idempotency_key,
        request_fingerprint, webhook_url, created_at, processed_at, delivery_state, attempts,
        attempts_base, last_attempt_at, delivered_at, dead_lettered_at, dead_letter_reason,
        last_error
    ) VALUES (
        :id, 100.00, 'RUB', 'test payment', CAST(:meta AS jsonb), CAST(:status AS varchar), :key,
        'v1:x', :url, now() - CAST(:age AS double precision) * interval '1 second',
        CASE WHEN CAST(:processed AS boolean) THEN now() END,
        CAST(:delivery_state AS varchar), :attempts, :attempts_base,
        CASE WHEN CAST(:last_attempt_ago AS double precision) IS NULL THEN NULL
             ELSE now() - CAST(:last_attempt_ago AS double precision) * interval '1 second' END,
        CASE WHEN CAST(:delivery_state AS varchar) = 'delivered' THEN now() END,
        CASE WHEN CAST(:delivery_state AS varchar) = 'dead_lettered' THEN now() END,
        CAST(:reason AS varchar), CAST(:last_error AS varchar)
    )
    """
)


async def insert_payment(
    sessions: Sessions,
    *,
    webhook_url: str,
    metadata: Mapping[str, Any] | None = None,
    status: str = "pending",
    delivery_state: str = "pending",
    attempts: int = 0,
    attempts_base: int = 0,
    last_attempt_ago_s: float | None = None,
    dead_letter_reason: str | None = None,
    last_error: str | None = None,
    age_s: float = 0,
    payment_id: uuid.UUID | None = None,
) -> uuid.UUID:
    """Insert a payment row in any consistent state, without an outbox row."""
    payment_id = payment_id or uuid.uuid4()
    async with sessions.begin() as session:
        await session.execute(
            _INSERT_PAYMENT,
            {
                "id": payment_id,
                "meta": json.dumps(dict(metadata or {})),
                "status": status,
                "key": f"consumer-{payment_id}",
                "url": webhook_url,
                "age": age_s,
                "processed": status != "pending",
                "delivery_state": delivery_state,
                "attempts": attempts,
                "attempts_base": attempts_base,
                "last_attempt_ago": last_attempt_ago_s,
                "reason": dead_letter_reason,
                "last_error": last_error,
            },
        )
    return payment_id


async def payment_row(sessions: Sessions, payment_id: uuid.UUID) -> Row:
    """The payment row as a dict (columns of design §3.1)."""
    async with sessions() as session:
        result = await session.execute(
            text("SELECT * FROM payments WHERE id = :id"), {"id": payment_id}
        )
        row = result.mappings().one()
        return dict(row)


async def make_attempt_stale(sessions: Sessions, payment_id: uuid.UUID) -> None:
    """Move the last attempt an hour into the past, as if no attempt were in flight."""
    async with sessions.begin() as session:
        await session.execute(
            text("UPDATE payments SET last_attempt_at = now() - interval '1 hour' WHERE id = :id"),
            {"id": payment_id},
        )


async def outbox_events(sessions: Sessions, payment_id: uuid.UUID) -> list[Row]:
    """Outbox rows of the payment."""
    async with sessions() as session:
        result = await session.execute(
            text("SELECT * FROM outbox WHERE payment_id = :id ORDER BY created_at"),
            {"id": payment_id},
        )
        return [dict(r) for r in result.mappings()]


async def insert_outbox_event(sessions: Sessions, payment_id: uuid.UUID) -> uuid.UUID:
    """Insert a due outbox row for the payment, as the API does when it accepts a payment."""
    event_id = uuid.uuid4()
    payload = {
        "event_id": str(event_id),
        "payment_id": str(payment_id),
        "occurred_at": datetime.now(UTC).isoformat(),
    }
    async with sessions.begin() as session:
        await session.execute(
            text(
                "INSERT INTO outbox (id, payment_id, event_type, routing_key, payload, headers)"
                " VALUES (:id, :payment_id, 'payment.created', :routing_key,"
                " CAST(:payload AS jsonb), '{}'::jsonb)"
            ),
            {
                "id": event_id,
                "payment_id": payment_id,
                "routing_key": MAIN_QUEUE_NAME,
                "payload": json.dumps(payload),
            },
        )
    return event_id


# --- RabbitMQ ----------------------------------------------------------------------------------


class QueueTap:
    """Reads the messages of one queue with basic.get; keeps everything it has seen."""

    def __init__(self, queue: AbstractQueue) -> None:
        self._queue = queue
        self.messages: list[AbstractIncomingMessage] = []

    async def drain(self) -> list[AbstractIncomingMessage]:
        """Take every message currently in the queue; returns the newly taken ones."""
        taken: list[AbstractIncomingMessage] = []
        while (message := await self._queue.get(no_ack=True, fail=False)) is not None:
            taken.append(message)
        self.messages.extend(taken)
        return taken

    async def wait_for(self, count: int, *, timeout: float = 20) -> list[AbstractIncomingMessage]:
        """Wait until at least `count` messages were seen in total."""

        async def enough() -> bool:
            await self.drain()
            return len(self.messages) >= count

        await wait_until(enough, timeout=timeout, interval=0.1)
        return self.messages

    async def wait_for_header(
        self, header: str, *, count: int = 1, timeout: float = 30
    ) -> list[AbstractIncomingMessage]:
        """Wait until `count` messages carry a truthy value of the header; returns them."""

        async def seen() -> bool:
            await self.drain()
            return len(self.with_header(header)) >= count

        await wait_until(seen, timeout=timeout, interval=0.1)
        return self.with_header(header)

    def with_header(self, header: str) -> list[AbstractIncomingMessage]:
        """Messages seen so far that carry a truthy value of the header."""
        return [m for m in self.messages if m.headers.get(header)]


class Rabbit:
    """A direct connection to the broker for publishing events and observing queues."""

    def __init__(self, url: str, management_url: str) -> None:
        self.url = url
        self._management_url = management_url
        self.connection: AbstractRobustConnection | None = None
        self._channel: aio_pika.abc.AbstractChannel | None = None

    async def open(self) -> None:
        self.connection = await aio_pika.connect_robust(self.url)
        self._channel = await self.connection.channel(publisher_confirms=True)

    async def close(self) -> None:
        if self.connection is not None:
            with suppress(Exception):
                await self.connection.close()

    @property
    def channel(self) -> aio_pika.abc.AbstractChannel:
        assert self._channel is not None
        return self._channel

    async def tap(self, name: str, *, timeout: float = 30) -> QueueTap:
        """Attach to an existing queue; waits for the queue to appear."""
        assert self.connection is not None
        holder: list[AbstractQueue] = []

        async def attach() -> bool:
            channel = await self.connection.channel()  # type: ignore[union-attr]
            try:
                holder.append(await channel.get_queue(name, ensure=True))
            except aio_pika.exceptions.ChannelNotFoundEntity:
                return False
            return True

        await wait_until(attach, timeout=timeout, interval=0.2)
        return QueueTap(holder[0])

    async def dlq(self) -> QueueTap:
        """The dead-letter queue."""
        return await self.tap(DLQ_NAME)

    async def spy(self) -> QueueTap:
        """A queue bound like `payments.new`: sees every publish and every returning copy."""
        exchange = await self.channel.declare_exchange(
            "payments", aio_pika.ExchangeType.DIRECT, durable=True
        )
        queue = await self.channel.declare_queue(SPY_QUEUE, durable=False)
        await queue.bind(exchange, routing_key=MAIN_QUEUE_NAME)
        return QueueTap(queue)

    async def depth(self, name: str) -> tuple[int, int]:
        """(ready messages, consumers) of an existing queue."""
        assert self.connection is not None
        channel = await self.connection.channel()
        try:
            queue = await channel.get_queue(name, ensure=True)
            result = queue.declaration_result
            return int(result.message_count or 0), int(result.consumer_count or 0)
        finally:
            with suppress(Exception):
                await channel.close()

    async def ready(self, name: str) -> int:
        """Number of ready messages in a queue; zero if it does not exist."""
        try:
            return (await self.depth(name))[0]
        except aio_pika.exceptions.ChannelNotFoundEntity:
            return 0

    async def wait_for_consumer(self, *, count: int = 1, timeout: float = 30) -> None:
        """Wait until `payments.new` exists and has at least `count` consumers."""

        async def consuming() -> bool:
            try:
                return (await self.depth(MAIN_QUEUE_NAME))[1] >= count
            except aio_pika.exceptions.ChannelNotFoundEntity:
                return False

        await wait_until(consuming, timeout=timeout, interval=0.2)

    async def queue_info(self, name: str) -> dict[str, Any]:
        """Management API view of a queue (statistics lag by a few seconds)."""
        parts = urlsplit(self._management_url)
        base = f"{parts.scheme}://{parts.hostname}:{parts.port}"
        auth = (parts.username or "guest", parts.password or "guest")
        async with httpx.AsyncClient(base_url=base, auth=auth, timeout=10) as client:
            response = await client.get(f"/api/queues/%2F/{quote(name, safe='')}")
            response.raise_for_status()
            return dict(response.json())

    async def publish(
        self,
        payment_id: uuid.UUID,
        *,
        message_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
        routing_key: str = MAIN_QUEUE_NAME,
    ) -> str:
        """Publish a payment event to the `payments` exchange as the relay would."""
        event_id = message_id or str(uuid.uuid4())
        body = json.dumps(
            {
                "event_id": event_id,
                "payment_id": str(payment_id),
                "occurred_at": datetime.now(UTC).isoformat(),
            }
        ).encode()
        await self.publish_raw(
            body,
            message_id=event_id,
            correlation_id=str(payment_id),
            headers=headers,
            routing_key=routing_key,
        )
        return event_id

    async def publish_raw(
        self,
        body: bytes,
        *,
        message_id: str | None = None,
        correlation_id: str | None = None,
        headers: Mapping[str, Any] | None = None,
        routing_key: str = MAIN_QUEUE_NAME,
        content_type: str = "application/json",
    ) -> None:
        """Publish arbitrary bytes to the `payments` exchange, persistent, with confirms."""
        exchange = await self.channel.declare_exchange(
            "payments", aio_pika.ExchangeType.DIRECT, durable=True
        )
        await exchange.publish(
            aio_pika.Message(
                body,
                message_id=message_id,
                correlation_id=correlation_id,
                content_type=content_type,
                type="payment.created",
                headers=dict(headers or {}),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            ),
            routing_key=routing_key,
        )

    async def unbind_retry_queue(self, delay_ms: int) -> None:
        """Remove the binding that routes copies to a retry queue (a broken topology)."""
        name = f"payments.new.retry.{delay_ms}ms"
        queue = await self.channel.get_queue(name, ensure=True)
        await queue.unbind("payments.retry", routing_key=name)


@pytest_asyncio.fixture
async def rabbit(
    rabbitmq_url: str, management_url: str, broker_cleanup: None
) -> AsyncIterator[Rabbit]:
    """A connection to the broker; queues are deleted after the test by `broker_cleanup`."""
    instance = Rabbit(rabbitmq_url, management_url)
    await instance.open()
    try:
        yield instance
    finally:
        await instance.close()


# --- environment --------------------------------------------------------------------------------


@dataclass
class Env:
    """Everything a consumer test needs, bound to the migrated database and the broker."""

    database_url: str
    rabbitmq_url: str
    sessions: Sessions
    receiver: Receiver
    rabbit: Rabbit
    extra: dict[str, Any] = field(default_factory=dict)

    def settings(self, **overrides: Any) -> Settings:
        """Consumer settings; `database_url` and `rabbitmq_url` may be overridden (proxies)."""
        urls = {"database_url": self.database_url, "rabbitmq_url": self.rabbitmq_url}
        urls.update({k: overrides.pop(k) for k in list(overrides) if k in urls})
        return consumer_settings(urls["database_url"], urls["rabbitmq_url"], **overrides)

    def hook(self, path: str) -> str:
        """Webhook URL of a receiver path (the host is on the internal-host list)."""
        return f"http://{HOOK_HOST}:{self.receiver.port}{path}"

    async def new_payment(
        self,
        path: str = "/hooks/ok",
        *,
        outcome: str | None = "succeeded",
        **columns: Any,
    ) -> uuid.UUID:
        """Insert a pending payment whose webhook goes to a receiver path."""
        metadata = dict(columns.pop("metadata", {}))
        if outcome is not None:
            metadata["emulator_outcome"] = outcome
        return await insert_payment(
            self.sessions, webhook_url=self.hook(path), metadata=metadata, **columns
        )

    async def row(self, payment_id: uuid.UUID) -> Row:
        return await payment_row(self.sessions, payment_id)

    @asynccontextmanager
    async def consumer(self, settings: Settings) -> AsyncIterator[AsgiFastStream]:
        """Run the consumer in-process and return once it is consuming and its queues exist.

        The application may report itself started before the broker has bound its queue, and a
        message published into that gap would be dropped by the exchange.
        """
        async with running_consumer(settings) as app:
            self.extra["consumers"] = self.extra.get("consumers", 0) + 1
            try:
                await self.rabbit.wait_for_consumer(count=self.extra["consumers"])
                for delay in settings.retry_delays_ms:
                    await self.rabbit.tap(f"payments.new.retry.{delay}ms")
                yield app
            finally:
                self.extra["consumers"] -= 1

    def posts(self, payment_id: uuid.UUID) -> list[Delivery]:
        """Webhook requests received for the payment."""
        return self.receiver.deliveries(payment_id)

    async def wait_for_state(
        self, payment_id: uuid.UUID, state: str, *, timeout: float = 30
    ) -> Row:
        """Wait until the payment reaches a delivery state; returns the row."""

        async def reached() -> bool:
            return bool((await self.row(payment_id))["delivery_state"] == state)

        await wait_until(reached, timeout=timeout, interval=0.1)
        return await self.row(payment_id)


@pytest_asyncio.fixture
async def env(
    migrated_db: str,
    rabbitmq_url: str,
    db_sessions: Sessions,
    receiver: Receiver,
    rabbit: Rabbit,
) -> Env:
    """The test environment: migrated database, broker, receiver."""
    return Env(
        database_url=migrated_db,
        rabbitmq_url=rabbitmq_url,
        sessions=db_sessions,
        receiver=receiver,
        rabbit=rabbit,
    )


# --- running the consumer -----------------------------------------------------------------------


@asynccontextmanager
async def running_consumer(settings: Settings) -> AsyncIterator[AsgiFastStream]:
    """The real consumer application, started in-process and stopped on exit."""
    app = create_app(settings)
    async with app.start_lifespan_context():
        yield app


@asynccontextmanager
async def running_handler(settings: Settings, *, copies_url: str) -> AsyncIterator[ConsumerDeps]:
    """The consumer's handler on real collaborators, with the copy publisher on its own URL.

    Wires the same pieces as the consumer application, but lets a test put a proxy in front of
    the copy publisher's connection only. The subscriber just forwards to `handle_payment_event`.
    """
    engine = create_engine(settings, role="consumer-test")
    http = create_http_client()
    broker = RabbitBroker(
        settings.rabbitmq_url,
        graceful_timeout=settings.consumer_graceful_timeout_s,
        default_channel=Channel(prefetch_count=settings.consumer_prefetch),
    )
    copies = CopyPublisher(copies_url, publish_timeout_s=settings.rabbit_publish_timeout_s)
    processor = Processor(
        db=Db(create_session_factory(engine), settings.db_call_timeout_s),
        gateway=EmulatedGateway(
            success_rate=settings.gateway_success_rate,
            min_delay_s=settings.gateway_min_delay_s,
            max_delay_s=settings.gateway_max_delay_s,
            outcome_hints=settings.gateway_outcome_hints,
        ),
        webhooks=HttpWebhookSender(
            client=http,
            key=settings.require_webhook_key(),
            internal_hosts=settings.webhook_internal_hosts,
            resolver=system_resolver,
            total_timeout_s=settings.webhook_total_timeout_s,
        ),
        max_attempts=settings.retry_max_attempts,
        stale_after_s=settings.stale_after_s,
    )
    deps = ConsumerDeps(processor=processor, copies=copies, settings=settings)

    @broker.subscriber(MAIN_QUEUE, PAYMENTS_EXCHANGE, ack_policy=AckPolicy.REJECT_ON_ERROR)
    async def on_payment_event(event: PaymentEvent, message: RabbitMessage) -> None:
        await handle_payment_event(
            event,
            message.headers,
            message.body,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
            content_type=message.content_type,
            deps=deps,
        )

    _ = on_payment_event
    try:
        await broker.start()
        await declare_core(broker)
        await declare_retry_ladder(broker, settings.retry_delays_ms)
        await copies.connect()
        yield deps
    finally:
        with suppress(Exception):
            await asyncio.wait_for(broker.stop(), settings.consumer_graceful_timeout_s + 5)
        with suppress(Exception):
            await copies.close()
        await http.aclose()
        await engine.dispose()


@asynccontextmanager
async def relay_runner(settings: Settings) -> AsyncIterator[OutboxRelay]:
    """A relay on its own engine and broker, for tests that need outbox rows published."""
    engine = create_engine(settings, role="consumer-test-relay")
    broker = RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    try:
        await broker.start()
        await declare_core(broker)
        db = Db(create_session_factory(engine), settings.db_call_timeout_s)
        yield OutboxRelay(db=db, broker=broker, settings=settings)
    finally:
        with suppress(Exception):
            await asyncio.wait_for(broker.stop(), 10)
        await engine.dispose()


async def publish_outbox(settings: Settings, *, timeout: float = 20) -> None:
    """Run the relay until it publishes at least one row."""
    async with relay_runner(settings) as relay:

        async def published() -> bool:
            return await relay.run_once() > 0

        await wait_until(published, timeout=timeout, interval=0.1)


async def run_cli(settings: Settings, *args: str, timeout: float = 60) -> tuple[int, str]:
    """Run `python -m payments.cli <args>` against the test services; returns (code, output)."""
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "payments.cli",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env={**os.environ, **worker_env(settings)},
    )
    output, _ = await asyncio.wait_for(process.communicate(), timeout)
    return process.returncode if process.returncode is not None else -1, output.decode()
