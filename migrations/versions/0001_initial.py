"""Create the payments and outbox tables.

Revision ID: 0001
Revises:
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE payments (
          id                   uuid          PRIMARY KEY,
          amount               numeric(18,2) NOT NULL,
          currency             varchar(3)    NOT NULL,
          description          varchar(1000) NOT NULL DEFAULT '',
          metadata             jsonb         NOT NULL DEFAULT '{}',
          status               varchar(16)   NOT NULL DEFAULT 'pending',
          idempotency_key      varchar(255)  NOT NULL,
          request_fingerprint  varchar(64)   NOT NULL,
          webhook_url          varchar(2048) NOT NULL,
          created_at           timestamptz   NOT NULL DEFAULT now(),
          processed_at         timestamptz,
          delivery_state       varchar(16)   NOT NULL DEFAULT 'pending',
          attempts             integer       NOT NULL DEFAULT 0,
          attempts_base        integer       NOT NULL DEFAULT 0,
          last_attempt_at      timestamptz,
          delivered_at         timestamptz,
          dead_lettered_at     timestamptz,
          dead_letter_reason   varchar(32),
          last_error           varchar(64),
          CONSTRAINT uq_payments_idempotency_key  UNIQUE (idempotency_key),
          CONSTRAINT ck_payments_amount_positive  CHECK (amount > 0),
          CONSTRAINT ck_payments_currency         CHECK (currency IN ('RUB','USD','EUR')),
          CONSTRAINT ck_payments_status           CHECK (status IN ('pending','succeeded','failed')),
          CONSTRAINT ck_payments_processed_at     CHECK ((status = 'pending') = (processed_at IS NULL)),
          CONSTRAINT ck_payments_delivery_state   CHECK (delivery_state IN ('pending','delivered','dead_lettered')),
          CONSTRAINT ck_payments_delivered_at     CHECK ((delivery_state = 'delivered') = (delivered_at IS NOT NULL)),
          CONSTRAINT ck_payments_delivered_final  CHECK (delivery_state <> 'delivered' OR status <> 'pending'),
          CONSTRAINT ck_payments_dead_lettered_at CHECK ((delivery_state = 'dead_lettered') = (dead_lettered_at IS NOT NULL)),
          CONSTRAINT ck_payments_dead_reason      CHECK ((delivery_state = 'dead_lettered') = (dead_letter_reason IS NOT NULL)
                                                         AND (dead_letter_reason IS NULL OR dead_letter_reason IN ('max_attempts','permanent','unroutable'))),
          CONSTRAINT ck_payments_attempts         CHECK (attempts >= attempts_base AND attempts_base >= 0)
        )
        """  # noqa: E501
    )
    op.execute(
        """
        CREATE TABLE outbox (
          id           uuid         PRIMARY KEY,
          payment_id   uuid         NOT NULL REFERENCES payments(id),
          event_type   varchar(64)  NOT NULL,
          routing_key  varchar(255) NOT NULL,
          payload      jsonb        NOT NULL,
          created_at   timestamptz  NOT NULL DEFAULT now(),
          available_at timestamptz  NOT NULL DEFAULT now(),
          attempts     integer      NOT NULL DEFAULT 0 CHECK (attempts >= 0),
          last_error   varchar(100)
        )
        """
    )
    op.execute("CREATE INDEX ix_outbox_available_at ON outbox (available_at)")


def downgrade() -> None:
    op.execute("DROP TABLE outbox")
    op.execute("DROP TABLE payments")
