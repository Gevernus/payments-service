"""Payment state machine executed by the consumer."""

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from payments.db.session import Db
from payments.errors import DeadReason
from payments.services.gateway import Gateway
from payments.webhooks.sender import WebhookSender


@dataclass(frozen=True)
class KnownOutcome:
    """A delivery outcome that is known but could not be stored yet."""

    kind: Literal["delivered", "dead"]
    epoch: int
    reason: DeadReason | None = None
    code: str | None = None


@dataclass(frozen=True)
class Done:
    """Processing is complete."""


@dataclass(frozen=True)
class RetryLater:
    """Schedule another attempt."""

    attempt_no: int
    code: str


@dataclass(frozen=True)
class DeadLetter:
    """The payment is dead-lettered."""


Outcome = Done | RetryLater | DeadLetter


class Processor:
    """Runs one processing attempt for a payment."""

    def __init__(
        self,
        *,
        db: Db,
        gateway: Gateway,
        webhooks: WebhookSender,
        max_attempts: int,
        stale_after_s: float,
    ) -> None:
        raise NotImplementedError

    async def process(self, payment_id: UUID, *, known: KnownOutcome | None = None) -> Outcome:
        """Process one message for the payment."""
        raise NotImplementedError

    async def mark_unroutable(self, payment_id: UUID) -> None:
        """Dead-letter the payment because its retry message cannot be routed."""
        raise NotImplementedError
