"""Payment consumer process."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from faststream.asgi import AsgiFastStream

from payments.config import Settings
from payments.messaging.copies import CopyPublisher
from payments.schemas import PaymentEvent
from payments.services.processing import Processor


@dataclass
class ConsumerDeps:
    """Collaborators of the message handler."""

    processor: Processor
    copies: CopyPublisher
    settings: Settings


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
    raise NotImplementedError


def create_app(settings: Settings | None = None) -> AsgiFastStream:
    """Build the consumer application."""
    raise NotImplementedError
