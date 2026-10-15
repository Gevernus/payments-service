"""Payment creation and lookup."""

from uuid import UUID

from payments.config import Settings
from payments.db.models import Payment
from payments.db.session import Db
from payments.schemas import PaymentCreate


class PaymentService:
    """Use cases behind the HTTP API."""

    def __init__(self, db: Db, settings: Settings) -> None:
        raise NotImplementedError

    async def create(
        self, data: PaymentCreate, *, idempotency_key: str, request_id: str | None
    ) -> tuple[Payment, bool]:
        """Create a payment idempotently; returns (payment, replayed)."""
        raise NotImplementedError

    async def get(self, payment_id: UUID) -> Payment:
        """Return the payment or raise PaymentNotFound."""
        raise NotImplementedError
