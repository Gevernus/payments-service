"""Payment consumer process."""

from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import structlog
from faststream import AckPolicy
from faststream.asgi import AsgiFastStream, AsgiResponse, get
from faststream.exceptions import RejectMessage
from faststream.rabbit import Channel, RabbitBroker, RabbitMessage
from sqlalchemy import text
from structlog.contextvars import bind_contextvars, clear_contextvars

from payments.config import Settings, get_settings
from payments.db.session import Db, create_engine, create_session_factory
from payments.errors import AttemptInFlight, CopyUnroutable, DatabaseUnavailable
from payments.logging import configure_logging
from payments.messaging.copies import CopyPublisher
from payments.messaging.envelope import DEFERRALS, KNOWN_OUTCOME, Envelope
from payments.messaging.topology import (
    MAIN_QUEUE,
    PAYMENTS_EXCHANGE,
    declare_core,
    declare_retry_ladder,
    retry_queue,
)
from payments.schemas import PaymentEvent
from payments.services.gateway import EmulatedGateway
from payments.services.processing import DeadLetter, Done, Processor, RetryLater
from payments.webhooks.guard import system_resolver
from payments.webhooks.sender import HttpWebhookSender, create_http_client

log = structlog.get_logger(__name__)

_PING_TIMEOUT_S = 0.2


@dataclass
class ConsumerDeps:
    """Collaborators of the message handler."""

    processor: Processor
    copies: CopyPublisher
    settings: Settings


async def _copy(
    deps: ConsumerDeps,
    event: PaymentEvent,
    headers: dict[str, Any],
    body: bytes,
    *,
    delay_ms: int,
    message_id: str | None,
    correlation_id: str | None,
    content_type: str | None,
) -> None:
    """Put a copy of the message into a retry queue; the original is acked by returning."""
    try:
        await deps.copies.publish_until_confirmed(
            body,
            routing_key=retry_queue(delay_ms).routing(),
            headers=headers,
            message_id=message_id,
            correlation_id=correlation_id,
            content_type=content_type,
        )
    except CopyUnroutable:  # the topology is broken
        log.error("consumer.copy_unroutable", delay_ms=delay_ms)
        if KNOWN_OUTCOME not in headers:
            with suppress(DatabaseUnavailable):
                await deps.processor.mark_unroutable(event.payment_id)
        raise RejectMessage from None


async def handle_payment_event(
    event: PaymentEvent,
    headers: Mapping[str, Any],
    body: bytes,
    *,
    message_id: str | None,
    correlation_id: str | None,
    content_type: str | None,
    deps: ConsumerDeps,
) -> None:
    """Handle one message; raises faststream.exceptions.RejectMessage to dead-letter it."""
    delays = deps.settings.retry_delays_ms
    meta = {
        "message_id": message_id,
        "correlation_id": correlation_id,
        "content_type": content_type,
    }
    try:
        bind_contextvars(
            payment_id=str(event.payment_id),
            event_id=message_id or str(event.event_id),
        )
        envelope = Envelope.from_headers(headers)
        try:
            outcome = await deps.processor.process(event.payment_id, known=envelope.known_outcome)
        except (DatabaseUnavailable, AttemptInFlight) as exc:
            known = exc.known if isinstance(exc, DatabaseUnavailable) else None
            deferred = envelope.deferred(known)
            root: BaseException = exc
            while root.__cause__ is not None:
                root = root.__cause__
            log.warning(
                "consumer.deferred",
                deferrals=deferred[DEFERRALS],
                reason=type(root).__name__,
            )
            await _copy(deps, event, deferred, body, delay_ms=delays[0], **meta)
            return
        match outcome:
            case Done():
                return
            case RetryLater(attempt_no=attempt_no):
                index = min(max(attempt_no, 1), len(delays)) - 1
                await _copy(deps, event, envelope.retried(), body, delay_ms=delays[index], **meta)
                return
            case DeadLetter():
                raise RejectMessage
    finally:
        clear_contextvars()


def create_app(settings: Settings | None = None) -> AsgiFastStream:
    """Build the consumer application."""
    settings = settings or get_settings()
    configure_logging(settings)

    broker = RabbitBroker(
        settings.rabbitmq_url,
        graceful_timeout=settings.consumer_graceful_timeout_s,
        default_channel=Channel(prefetch_count=settings.consumer_prefetch),
        reconnect_interval=1.0,
    )
    copies = CopyPublisher(
        settings.rabbitmq_url, publish_timeout_s=settings.rabbit_publish_timeout_s
    )
    engine = create_engine(settings, role="consumer")
    db = Db(create_session_factory(engine), settings.db_call_timeout_s)
    http = create_http_client()
    processor = Processor(
        db=db,
        gateway=EmulatedGateway(
            success_rate=settings.gateway_success_rate,
            min_delay_s=settings.gateway_min_delay_s,
            max_delay_s=settings.gateway_max_delay_s,
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

    async def on_startup() -> None:
        log.info("settings.loaded", **settings.loggable())

        async def probe(session: Any) -> None:
            await session.execute(text("SELECT 1"))

        await db.run(probe)
        await copies.connect()
        await broker.connect()
        await declare_core(broker)
        await declare_retry_ladder(broker, settings.retry_delays_ms)

    async def after_startup() -> None:
        log.info("consumer.started")

    async def after_shutdown() -> None:
        # Runs after the consume broker has finished its in-flight handlers.
        await copies.close()
        await http.aclose()
        await engine.dispose()

    @get
    async def health(_scope: Any) -> AsgiResponse:
        consuming = bool(broker.subscribers) and all(s.running for s in broker.subscribers)
        try:
            broker_ok = await broker.ping(_PING_TIMEOUT_S)
        except Exception:  # noqa: BLE001 - a failing ping means no connection
            broker_ok = False
        copies_ok = await copies.is_connected()
        ok = consuming and broker_ok and copies_ok
        body = b'{"status":"ok"}' if ok else b'{"status":"unhealthy"}'
        return AsgiResponse(body, 200 if ok else 503, headers={"content-type": "application/json"})

    return AsgiFastStream(
        broker,
        asgi_routes=[("/health", health)],
        on_startup=[on_startup],
        after_startup=[after_startup],
        after_shutdown=[after_shutdown],
    )
