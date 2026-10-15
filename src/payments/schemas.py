"""Request, response and event schemas."""

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, HttpUrl, field_serializer

from payments.money import Amount

if TYPE_CHECKING:
    from payments.db.models import Payment


class Currency(StrEnum):
    """Supported currencies."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


class PaymentStatus(StrEnum):
    """Payment processing status."""

    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class DeliveryState(StrEnum):
    """Webhook delivery state."""

    PENDING = "pending"
    DELIVERED = "delivered"
    DEAD_LETTERED = "dead_lettered"


class PaymentCreate(BaseModel):
    """Body of POST /api/v1/payments."""

    model_config = ConfigDict(extra="forbid")

    amount: Amount
    currency: Currency
    description: str = Field(default="", max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    """Response of POST /api/v1/payments."""

    payment_id: UUID
    status: PaymentStatus
    created_at: datetime


class PaymentData(BaseModel):
    """Payment fields shared by the API response and the webhook body."""

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    payment_id: UUID = Field(validation_alias=AliasChoices("payment_id", "id"))
    amount: Decimal
    currency: Currency
    description: str
    # The ORM attribute is `meta`; `metadata` is taken by the declarative base.
    metadata: dict[str, Any] = Field(validation_alias=AliasChoices("meta", "metadata"))
    status: PaymentStatus
    created_at: datetime
    processed_at: datetime | None = None

    @field_serializer("amount", when_used="json")
    def _serialize_amount(self, value: Decimal) -> str:
        return format(value, "f")


class DeliveryInfo(BaseModel):
    """Webhook delivery progress of a payment."""

    model_config = ConfigDict(from_attributes=True)

    state: DeliveryState
    attempts: int
    last_attempt_at: datetime | None = None
    delivered_at: datetime | None = None
    dead_lettered_at: datetime | None = None
    dead_letter_reason: str | None = None
    last_error: str | None = None


class PaymentOut(PaymentData):
    """Response of GET /api/v1/payments/{id}."""

    idempotency_key: str
    webhook_url: str
    delivery: DeliveryInfo

    @classmethod
    def from_payment(cls, p: "Payment") -> "PaymentOut":
        """Build the response from an ORM row."""
        raise NotImplementedError


class PaymentEvent(BaseModel):
    """Message body published by the outbox relay."""

    model_config = ConfigDict(extra="ignore")

    event_id: UUID
    payment_id: UUID
    occurred_at: datetime
