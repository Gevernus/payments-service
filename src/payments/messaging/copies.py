"""Publishing of message copies on a dedicated connection."""

import asyncio
from collections.abc import Mapping
from typing import Any

import structlog
from aiormq.exceptions import PublishError
from faststream.rabbit import Channel, RabbitBroker
from pamqp.commands import Basic

from payments.errors import CopyUnroutable, NotAcked
from payments.messaging.topology import RETRY_EXCHANGE

log = structlog.get_logger(__name__)

_PING_TIMEOUT_S = 0.2
_FIRST_BACKOFF_S = 0.2
_MAX_BACKOFF_S = 5.0


def _is_unroutable(exc: BaseException) -> bool:
    return isinstance(exc, PublishError) or isinstance(exc.__cause__, PublishError)


class CopyPublisher:
    """Publishes retry and deferral copies and waits for the broker's confirmation.

    It owns a broker and a connection of its own and is not an application broker, so that it
    outlives the consume broker during shutdown and in-flight handlers can still copy.
    """

    def __init__(self, url: str, *, publish_timeout_s: float, close_timeout_s: float = 5) -> None:
        self._broker = RabbitBroker(
            url,
            default_channel=Channel(publisher_confirms=True, on_return_raises=True),
            reconnect_interval=1.0,
        )
        self._publish_timeout_s = publish_timeout_s
        self._close_timeout_s = close_timeout_s

    async def connect(self) -> None:
        """Open the connection."""
        await self._broker.connect()

    async def close(self) -> None:
        """Close the connection; a blocked connection must not hold the shutdown."""
        try:
            async with asyncio.timeout(self._close_timeout_s):
                await self._broker.stop()
        except Exception as exc:  # noqa: BLE001 - shutting down anyway
            log.warning(
                "copy.close_failed",
                timed_out=isinstance(exc, TimeoutError),
                error_class=type(exc).__name__,
                error=str(exc)[:200],
            )

    async def publish_until_confirmed(
        self,
        body: bytes,
        *,
        routing_key: str,
        headers: Mapping[str, Any],
        message_id: str | None,
        correlation_id: str | None,
        content_type: str | None,
    ) -> None:
        """Publish until the broker confirms; raises CopyUnroutable if the message is returned."""
        backoff = _FIRST_BACKOFF_S
        while True:
            try:
                # publish(timeout=) is ignored while the connection is blocked: bound it here.
                async with asyncio.timeout(self._publish_timeout_s):
                    result = await self._broker.publish(
                        body,
                        exchange=RETRY_EXCHANGE,
                        routing_key=routing_key,
                        persist=True,
                        mandatory=True,
                        headers=dict(headers),
                        message_id=message_id,
                        correlation_id=correlation_id,
                        content_type=content_type,
                        timeout=self._publish_timeout_s,
                    )
                if not isinstance(result, Basic.Ack):
                    raise NotAcked(type(result).__name__)
                return
            except Exception as exc:
                if _is_unroutable(exc):
                    raise CopyUnroutable(routing_key) from exc
                log.warning(
                    "copy.publish_failed",
                    routing_key=routing_key,
                    error_class=type(exc).__name__,
                    error=str(exc),
                    backoff_s=backoff,
                )
            await asyncio.sleep(backoff)
            backoff = min(_MAX_BACKOFF_S, backoff * 2)

    async def is_connected(self) -> bool:
        """Tell whether the connection is open."""
        try:
            return await self._broker.ping(_PING_TIMEOUT_S)
        except Exception:  # noqa: BLE001 - a failing ping means no connection
            return False
