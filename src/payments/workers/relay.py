"""Outbox relay process."""

import asyncio

from faststream.asgi import AsgiFastStream
from faststream.rabbit import RabbitBroker

from payments.config import Settings
from payments.db.session import Db


class OutboxRelay:
    """Publishes outbox rows to RabbitMQ with leased claims and publisher confirms."""

    def __init__(self, *, db: Db, broker: RabbitBroker, settings: Settings) -> None:
        raise NotImplementedError

    async def run_once(self) -> int:
        """Claim, publish and settle one batch; returns the number of published rows."""
        raise NotImplementedError

    async def run(self, stop: asyncio.Event) -> None:
        """Run until stop is set."""
        raise NotImplementedError

    @property
    def last_tick(self) -> float | None:
        """Monotonic time of the last loop iteration."""
        raise NotImplementedError


def create_app(settings: Settings | None = None) -> AsgiFastStream:
    """Build the relay application."""
    raise NotImplementedError
