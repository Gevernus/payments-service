"""Payment creation and lookup."""

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from payments.config import Settings
from payments.db import queries
from payments.db.models import Payment
from payments.db.session import Db
from payments.errors import (
    BlockedTarget,
    IdempotencyKeyReused,
    PaymentNotFound,
    WebhookUrlNotAllowed,
)
from payments.fingerprint import fingerprint
from payments.schemas import PaymentCreate
from payments.webhooks.guard import check_url

EVENT_PAYMENT_CREATED = "payment.created"


class PaymentService:
    """Use cases behind the HTTP API."""

    def __init__(self, db: Db, settings: Settings) -> None:
        self._db = db
        self._internal_hosts = settings.webhook_internal_hosts

    async def create(
        self, data: PaymentCreate, *, idempotency_key: str, request_id: str | None
    ) -> tuple[Payment, bool]:
        """Create a payment idempotently; returns (payment, replayed)."""
        try:
            check_url(str(data.webhook_url), self._internal_hosts)
        except BlockedTarget as exc:
            raise WebhookUrlNotAllowed(exc.reason) from exc
        digest = fingerprint(data)

        async def transaction(session: AsyncSession) -> tuple[Payment, bool]:
            created = await queries.insert_payment_if_absent(
                session, data=data, idempotency_key=idempotency_key, fingerprint=digest
            )
            if created is not None:
                await queries.add_outbox_event(
                    session,
                    payment_id=created.id,
                    event_type=EVENT_PAYMENT_CREATED,
                    request_id=request_id,
                )
                return created, False
            existing = await queries.get_by_idempotency_key(session, idempotency_key)
            if existing is None:
                # The conflicting row is committed before the conflict is reported, so under
                # READ COMMITTED it is visible here; reaching this point is a broken invariant.
                raise RuntimeError("idempotency key conflict without a visible row")
            if existing.request_fingerprint != digest:
                raise IdempotencyKeyReused
            return existing, True

        # Db.run commits when the callback returns, before the caller builds a response.
        return await self._db.run(transaction)

    async def get(self, payment_id: UUID) -> Payment:
        """Return the payment or raise PaymentNotFound."""
        payment = await self._db.run(queries.get_payment, payment_id)
        if payment is None:
            raise PaymentNotFound
        return payment
