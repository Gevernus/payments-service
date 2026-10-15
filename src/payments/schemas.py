"""Request, response and event schemas."""

import json
import math
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import UUID

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_serializer,
    field_validator,
)

from payments.money import Amount

if TYPE_CHECKING:
    from payments.db.models import Payment


class Currency(StrEnum):
    """Supported currencies."""

    RUB = "RUB"
    USD = "USD"
    EUR = "EUR"


METADATA_MAX_BYTES = 16 * 1024
METADATA_MAX_DEPTH = 10


def _check_metadata_value(value: Any, depth: int) -> None:
    """Walk a JSON value; `depth` is the nesting level of the containers around it."""
    if isinstance(value, str):
        if "\x00" in value:
            raise ValueError("metadata must not contain NUL characters")
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("metadata numbers must be finite")
    elif isinstance(value, list | dict):
        if depth > METADATA_MAX_DEPTH:
            raise ValueError(f"metadata is nested deeper than {METADATA_MAX_DEPTH} levels")
        if isinstance(value, list):
            for item in value:
                _check_metadata_value(item, depth + 1)
        else:
            for key, item in value.items():
                _check_metadata_value(key, depth + 1)
                _check_metadata_value(item, depth + 1)


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

    @field_validator("description")
    @classmethod
    def _description_without_nul(cls, value: str) -> str:
        if "\x00" in value:
            raise ValueError("description must not contain NUL characters")
        return value

    @field_validator("metadata")
    @classmethod
    def _metadata_within_limits(cls, value: dict[str, Any]) -> dict[str, Any]:
        _check_metadata_value(value, 1)
        size = len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode())
        if size > METADATA_MAX_BYTES:
            raise ValueError(f"metadata is larger than {METADATA_MAX_BYTES} bytes")
        return value


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
        reason = p.dead_letter_reason
        return cls.model_validate(
            {
                "payment_id": p.id,
                "amount": p.amount,
                "currency": p.currency,
                "description": p.description,
                "metadata": p.meta,
                "status": p.status,
                "created_at": p.created_at,
                "processed_at": p.processed_at,
                "idempotency_key": p.idempotency_key,
                "webhook_url": p.webhook_url,
                "delivery": {
                    "state": p.delivery_state,
                    "attempts": p.attempts,
                    "last_attempt_at": p.last_attempt_at,
                    "delivered_at": p.delivered_at,
                    "dead_lettered_at": p.dead_lettered_at,
                    "dead_letter_reason": reason.value if reason is not None else None,
                    "last_error": p.last_error,
                },
            }
        )


class PaymentEvent(BaseModel):
    """Message body published by the outbox relay."""

    model_config = ConfigDict(extra="ignore")

    event_id: UUID
    payment_id: UUID
    occurred_at: datetime
