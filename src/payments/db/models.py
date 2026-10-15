"""Declarative mapping of the `payments` and `outbox` tables (design §3).

CHECK constraints and the index live only in the migration.
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Integer, Numeric, String, text
from sqlalchemy import Enum as SaEnum
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from payments.errors import DeadReason
from payments.schemas import Currency, DeliveryState, PaymentStatus


def _str_enum(enum_cls: type[Enum], length: int) -> SaEnum:
    """A VARCHAR column holding the enum's values; the CHECK lives in the migration."""
    return SaEnum(
        enum_cls,
        length=length,
        native_enum=False,
        create_constraint=False,
        values_callable=lambda cls: [str(member.value) for member in cls],
    )


class Base(DeclarativeBase):
    """Declarative base."""


class Payment(Base):
    """A payment and its delivery automaton."""

    __tablename__ = "payments"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(_str_enum(Currency, 3))
    description: Mapped[str] = mapped_column(String(1000), server_default="")
    meta: Mapped[dict[str, Any]] = mapped_column("metadata", JSONB, server_default=text("'{}'"))
    status: Mapped[PaymentStatus] = mapped_column(
        _str_enum(PaymentStatus, 16),
        server_default=PaymentStatus.PENDING.value,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_fingerprint: Mapped[str] = mapped_column(String(72))
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    delivery_state: Mapped[DeliveryState] = mapped_column(
        _str_enum(DeliveryState, 16),
        server_default=DeliveryState.PENDING.value,
    )
    attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    attempts_base: Mapped[int] = mapped_column(Integer, server_default="0")
    last_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_lettered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dead_letter_reason: Mapped[DeadReason | None] = mapped_column(_str_enum(DeadReason, 32))
    last_error: Mapped[str | None] = mapped_column(String(64))


class Outbox(Base):
    """A pending broker publication, written in the same transaction as its payment."""

    __tablename__ = "outbox"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    payment_id: Mapped[UUID] = mapped_column(ForeignKey("payments.id"))
    event_type: Mapped[str] = mapped_column(String(64))
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    headers: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()")
    )
    attempts: Mapped[int] = mapped_column(Integer, server_default="0")
    last_error: Mapped[str | None] = mapped_column(String(100))
