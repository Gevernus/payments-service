"""Outbox relay process (design §5)."""

import asyncio
import contextlib
import os
import random
import signal
import time
from typing import Any

import structlog
from aio_pika.exceptions import DeliveryError
from aiormq.exceptions import PublishError
from faststream.asgi import AsgiFastStream, AsgiResponse, get
from faststream.rabbit import Channel, RabbitBroker
from pamqp.commands import Basic
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from payments.config import Settings, get_settings
from payments.db import queries
from payments.db.queries import ClaimedEvent
from payments.db.session import Db, create_engine, create_session_factory
from payments.errors import NotAcked
from payments.logging import configure_logging
from payments.messaging.topology import PAYMENTS_EXCHANGE, declare_core

log = structlog.get_logger(__name__)

_MAX_ATTEMPTS_BEFORE_ERROR = 10
_MAX_ROW_BACKOFF_S = 60.0
_MAX_LOOP_BACKOFF_S = 30.0
_HEALTH_MAX_TICK_AGE_S = 60.0
_PING_TIMEOUT_S = 0.2


def row_backoff(attempts: int) -> float:
    """Per-row backoff after a failed publication: min(60, 2**(attempts-1)) with +-20% jitter."""
    base = min(_MAX_ROW_BACKOFF_S, 2.0 ** (attempts - 1))
    return float(base * random.uniform(0.8, 1.2))  # noqa: S311


def loop_backoff(poll_interval_s: float, streak: int) -> float:
    """Global backoff after failed iterations: min(30, poll_interval * 2**streak)."""
    return float(min(_MAX_LOOP_BACKOFF_S, poll_interval_s * 2**streak))


def publish_error_code(exc: BaseException) -> str:
    """Stable code stored in outbox.last_error; exception text only goes to the log."""
    if isinstance(exc, PublishError):
        return "unroutable"
    if isinstance(exc, DeliveryError | NotAcked):
        return "nack"
    if isinstance(exc, TimeoutError):
        return "publish_timeout"
    return "publish_error"


class OutboxRelay:
    """Publishes outbox rows to RabbitMQ with leased claims and publisher confirms."""

    def __init__(self, *, db: Db, broker: RabbitBroker, settings: Settings) -> None:
        self._db = db
        self._broker = broker
        self._settings = settings
        self._last_tick: float | None = None

    @property
    def last_tick(self) -> float | None:
        """Monotonic time of the last loop iteration."""
        return self._last_tick

    async def connected(self) -> bool:
        """Whether the broker connection is currently alive."""
        try:
            return await self._broker.ping(_PING_TIMEOUT_S)
        except Exception:  # noqa: BLE001 - a failing ping means no connection
            return False

    async def _publish(self, event: ClaimedEvent) -> bool:
        """Publish one event; True only on Basic.Ack. Failures are recorded, not raised."""
        try:
            async with asyncio.timeout(self._settings.rabbit_publish_timeout_s):
                result = await self._broker.publish(
                    event.payload,
                    exchange=PAYMENTS_EXCHANGE,
                    routing_key=event.routing_key,
                    persist=True,
                    mandatory=True,
                    message_id=str(event.id),
                    message_type=event.event_type,
                    correlation_id=str(event.payment_id),
                    headers=event.headers,
                    timeout=self._settings.rabbit_publish_timeout_s,
                )
            if not isinstance(result, Basic.Ack):
                raise NotAcked(type(result).__name__)
        except Exception as exc:  # noqa: BLE001 - every failure defers just this row
            error = publish_error_code(exc)
            log.warning(
                "relay.publish_failed",
                event_id=str(event.id),
                code=error,
                error_class=type(exc).__name__,
                error=str(exc),
            )
            await self._defer(event, error)
            return False
        return True

    async def _defer(self, event: ClaimedEvent, error: str) -> None:
        backoff = row_backoff(event.attempts)
        try:
            await self._db.run(
                queries.defer_outbox,
                event_id=event.id,
                attempts=event.attempts,
                backoff_s=backoff,
                error=error,
            )
        except Exception as exc:  # noqa: BLE001 - the lease expires and the row is retried
            log.warning("relay.defer_failed", event_id=str(event.id), error=repr(exc))

    async def run_once(self) -> int:
        """Claim, publish and settle one batch; returns the number of published rows."""
        if not await self.connected():
            return 0
        settings = self._settings
        events = await self._db.run(
            queries.claim_outbox, batch=settings.outbox_batch_size, lease_s=settings.outbox_lease_s
        )
        if not events:
            return 0
        for event in events:
            if event.attempts >= _MAX_ATTEMPTS_BEFORE_ERROR:
                log.error(
                    "relay.row_stuck",
                    event_id=str(event.id),
                    payment_id=str(event.payment_id),
                    attempts=event.attempts,
                )
        outcomes = await asyncio.gather(*(self._publish(e) for e in events))
        published = [(e.id, e.attempts) for e, ok in zip(events, outcomes, strict=True) if ok]
        if published:
            await self._db.run(queries.delete_published, published)
        return len(published)

    async def run(self, stop: asyncio.Event) -> None:
        """Run until stop is set."""
        failures = 0
        while not stop.is_set():
            self._last_tick = time.monotonic()
            delay = self._settings.outbox_poll_interval_s
            try:
                published = await self.run_once()
                failures = 0
                if published:
                    delay = 0
            except Exception as exc:  # noqa: BLE001 - any iteration error triggers a global backoff
                failures += 1
                delay = loop_backoff(self._settings.outbox_poll_interval_s, failures)
                log.warning("relay.iteration_failed", error=repr(exc), backoff_s=round(delay, 2))
            if delay:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), delay)
        self._last_tick = time.monotonic()


def create_app(settings: Settings | None = None) -> AsgiFastStream:
    """Build the relay application."""
    settings = settings or get_settings()
    configure_logging(settings)

    broker = RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
        reconnect_interval=1.0,
    )
    engine: AsyncEngine = create_engine(settings, role="relay")
    db = Db(create_session_factory(engine), settings.db_call_timeout_s)
    relay = OutboxRelay(db=db, broker=broker, settings=settings)
    stop = asyncio.Event()
    tasks: list[asyncio.Task[None]] = []

    async def on_startup() -> None:
        log.info("settings.loaded", **settings.loggable())

        async def probe(session: Any) -> None:
            await session.execute(text("SELECT 1"))

        await db.run(probe)
        await broker.connect()
        await declare_core(broker)

    def on_loop_done(task: asyncio.Task[None]) -> None:
        if task.cancelled() or stop.is_set():
            return
        log.critical("relay.loop_stopped", exc_info=task.exception())
        os.kill(os.getpid(), signal.SIGTERM)

    async def after_startup() -> None:
        task = asyncio.create_task(relay.run(stop), name="outbox-relay")
        task.add_done_callback(on_loop_done)
        tasks.append(task)
        log.info("relay.started")

    async def on_shutdown() -> None:
        stop.set()
        for task in tasks:
            try:
                await asyncio.wait_for(task, settings.rabbit_publish_timeout_s * 2 + 5)
            except Exception:  # noqa: BLE001 - shutting down anyway
                task.cancel()

    async def after_shutdown() -> None:
        await engine.dispose()

    @get
    async def health(_scope: Any) -> AsgiResponse:
        tick = relay.last_tick
        fresh = tick is not None and time.monotonic() - tick < _HEALTH_MAX_TICK_AGE_S
        alive = bool(tasks) and not tasks[0].done()
        connected = await relay.connected()
        ok = connected and alive and fresh
        body = (
            b'{"status":"ok"}'
            if ok
            else b'{"status":"unhealthy","connected":%s,"loop_alive":%s,"tick_fresh":%s}'
            % tuple(str(v).lower().encode() for v in (connected, alive, fresh))
        )
        return AsgiResponse(body, 200 if ok else 503, headers={"content-type": "application/json"})

    return AsgiFastStream(
        broker,
        asgi_routes=[("/health", health)],
        on_startup=[on_startup],
        after_startup=[after_startup],
        on_shutdown=[on_shutdown],
        after_shutdown=[after_shutdown],
    )
