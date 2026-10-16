"""The processor state machine on fakes, with a real database.

Every test drives `Processor.process` and checks the returned outcome, the payment row and what
the gateway and the webhook sender were asked to do.
"""

from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from payments.db import queries
from payments.db.models import Payment
from payments.db.session import Db
from payments.errors import AttemptInFlight, DatabaseUnavailable, DeadReason, GatewayUnavailable
from payments.schemas import PaymentStatus
from payments.services.processing import (
    DeadLetter,
    Done,
    KnownOutcome,
    Processor,
    RetryLater,
)
from payments.webhooks.sender import Permanent, Retryable, SendResult
from tests.support.consumer_harness import (
    Row,
    Sessions,
    cut,
    insert_payment,
    make_attempt_stale,
    payment_row,
    start_db_proxy,
)
from tests.support.fakes import FakeGateway, FakeWebhookSender
from tests.support.tcp_proxy import TcpProxy

URL = "https://merchant.example/hook"
MAX_ATTEMPTS = 3


@pytest.fixture
def db(db_sessions: Sessions) -> Db:
    return Db(db_sessions, call_timeout_s=5)


def make_processor(
    db: Db,
    gateway: FakeGateway | None = None,
    webhooks: FakeWebhookSender | None = None,
    *,
    stale_after_s: float = 30,
) -> tuple[Processor, FakeGateway, FakeWebhookSender]:
    gateway = gateway or FakeGateway()
    webhooks = webhooks or FakeWebhookSender()
    processor = Processor(
        db=db,
        gateway=gateway,
        webhooks=webhooks,
        max_attempts=MAX_ATTEMPTS,
        stale_after_s=stale_after_s,
    )
    return processor, gateway, webhooks


async def pending(sessions: Sessions, **columns: Any) -> UUID:
    return await insert_payment(sessions, webhook_url=URL, **columns)


async def dead(sessions: Sessions, *, attempts: int = 3, reason: str = "max_attempts") -> UUID:
    return await insert_payment(
        sessions,
        webhook_url=URL,
        status="succeeded",
        delivery_state="dead_lettered",
        dead_letter_reason=reason,
        attempts=attempts,
        last_error="http_503",
    )


async def test_final_payment_does_not_call_gateway(db: Db, db_sessions: Sessions) -> None:
    processor, gateway, webhooks = make_processor(db)
    payment_id = await pending(db_sessions, status="succeeded", attempts=1, last_attempt_ago_s=60)

    outcome = await processor.process(payment_id)

    assert outcome == Done()
    assert gateway.calls == []
    assert len(webhooks.calls) == 1
    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == "delivered"
    assert row["attempts"] == 2


async def test_decline_is_final_and_notified(db: Db, db_sessions: Sessions) -> None:
    gateway = FakeGateway(default=PaymentStatus.FAILED)
    processor, _, webhooks = make_processor(db, gateway)
    payment_id = await pending(db_sessions)

    outcome = await processor.process(payment_id)

    assert outcome == Done()
    row = await payment_row(db_sessions, payment_id)
    assert row["status"] == "failed"
    assert row["processed_at"] is not None
    assert row["delivery_state"] == "delivered"
    (sent,) = webhooks.calls
    assert sent.status is PaymentStatus.FAILED  # the webhook reports the final status
    assert len(gateway.calls) == 1
    assert gateway.calls[0].idempotency_key == str(payment_id)


async def test_gateway_unavailable_is_retry(db: Db, db_sessions: Sessions) -> None:
    payment_id = uuid4()
    gateway = FakeGateway({str(payment_id): [GatewayUnavailable()]})
    processor, _, webhooks = make_processor(db, gateway)
    await pending(db_sessions, payment_id=payment_id)

    outcome = await processor.process(payment_id)

    assert outcome == RetryLater(attempt_no=1, code="gateway_unavailable")
    assert webhooks.calls == []
    row = await payment_row(db_sessions, payment_id)
    assert row["status"] == "pending"
    assert row["delivery_state"] == "pending"
    assert row["attempts"] == 1
    assert row["last_error"] == "gateway_unavailable"


async def test_webhook_retryable_is_retry_without_gateway(db: Db, db_sessions: Sessions) -> None:
    webhooks = FakeWebhookSender([Retryable("http_503")])
    processor, gateway, _ = make_processor(db, webhooks=webhooks)
    payment_id = await pending(db_sessions)

    first = await processor.process(payment_id)
    second = await processor.process(payment_id)

    assert first == RetryLater(attempt_no=1, code="http_503")
    assert second == Done()
    assert (
        len(gateway.calls) == 1
    )  # the second pass saw a final status and went straight to the webhook
    assert len(webhooks.calls) == 2
    row = await payment_row(db_sessions, payment_id)
    assert row["status"] == "succeeded"
    assert row["delivery_state"] == "delivered"
    assert row["attempts"] == 2
    assert row["last_error"] is None


async def test_permanent_is_dead_letter(db: Db, db_sessions: Sessions) -> None:
    webhooks = FakeWebhookSender([Permanent("http_404")])
    processor, _, _ = make_processor(db, webhooks=webhooks)
    payment_id = await pending(db_sessions)

    outcome = await processor.process(payment_id)

    assert outcome == DeadLetter()
    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "permanent"
    assert row["last_error"] == "http_404"
    assert row["dead_lettered_at"] is not None
    assert row["attempts"] == 1


async def test_budget_exhausted_marks_max_attempts(db: Db, db_sessions: Sessions) -> None:
    webhooks = FakeWebhookSender(default=Retryable("http_503"))
    processor, gateway, _ = make_processor(db, webhooks=webhooks)
    payment_id = await pending(db_sessions)

    outcomes = [await processor.process(payment_id) for _ in range(MAX_ATTEMPTS)]
    after = await processor.process(payment_id)

    assert outcomes == [
        RetryLater(attempt_no=1, code="http_503"),
        RetryLater(attempt_no=2, code="http_503"),
        DeadLetter(),
    ]
    assert after == DeadLetter()  # a further message for a dead payment is rejected again
    assert len(webhooks.calls) == MAX_ATTEMPTS  # and sends nothing
    assert len(gateway.calls) == 1
    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "max_attempts"
    assert row["last_error"] == "http_503"
    assert row["attempts"] == MAX_ATTEMPTS


@pytest.mark.parametrize(
    ("known", "state", "reason", "error"),
    [
        pytest.param(KnownOutcome("delivered", 0), "delivered", None, None, id="delivered"),
        pytest.param(
            KnownOutcome("dead", 0, DeadReason.PERMANENT, "http_404"),
            "dead_lettered",
            "permanent",
            "http_404",
            id="dead",
        ),
    ],
)
async def test_known_outcome_persists_without_network(
    db: Db,
    db_sessions: Sessions,
    known: KnownOutcome,
    state: str,
    reason: str | None,
    error: str | None,
) -> None:
    processor, gateway, webhooks = make_processor(db)
    payment_id = await pending(db_sessions, status="succeeded", attempts=1, last_attempt_ago_s=1)

    outcome = await processor.process(payment_id, known=known)

    assert outcome == (Done() if state == "delivered" else DeadLetter())
    assert gateway.calls == []
    assert webhooks.calls == []
    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == state
    assert row["dead_letter_reason"] == reason
    assert row["last_error"] == error
    assert row["attempts"] == 1  # persisting an outcome spends no attempt


class CuttingSender(FakeWebhookSender):
    """Reports a delivery and breaks the database connection right after sending."""

    def __init__(self, proxy: TcpProxy) -> None:
        super().__init__()
        self._proxy = proxy

    async def send(self, payment: Payment) -> SendResult:
        result = await super().send(payment)
        cut(self._proxy)
        return result


async def test_db_down_after_delivery_raises_with_known(
    migrated_db: str, db_sessions: Sessions
) -> None:
    proxy, proxied_url = await start_db_proxy(migrated_db)
    engine = create_async_engine(proxied_url, poolclass=NullPool)
    try:
        sender = CuttingSender(proxy)
        processor, _, _ = make_processor(
            Db(async_sessionmaker(engine, expire_on_commit=False), 5), webhooks=sender
        )
        payment_id = await pending(db_sessions)

        with pytest.raises(DatabaseUnavailable) as raised:
            await processor.process(payment_id)

        assert raised.value.known is not None
        assert raised.value.known.kind == "delivered"
        assert raised.value.known.epoch == 0
        assert len(sender.calls) == 1
        assert (await payment_row(db_sessions, payment_id))["delivery_state"] == "pending"

        # The outcome carried by the exception is written later, without another send.
        proxy.set_mode("pass")
        assert await processor.process(payment_id, known=raised.value.known) == Done()
        assert len(sender.calls) == 1
        assert (await payment_row(db_sessions, payment_id))["delivery_state"] == "delivered"
    finally:
        proxy.close()
        await engine.dispose()


async def test_duplicate_when_delivered_is_done(db: Db, db_sessions: Sessions) -> None:
    processor, gateway, webhooks = make_processor(db)
    payment_id = await pending(
        db_sessions, status="succeeded", delivery_state="delivered", attempts=1
    )

    assert await processor.process(payment_id) == Done()

    assert gateway.calls == []
    assert webhooks.calls == []
    row = await payment_row(db_sessions, payment_id)
    assert row["attempts"] == 1
    assert row["delivery_state"] == "delivered"


async def test_duplicate_when_dead_is_dead_letter(db: Db, db_sessions: Sessions) -> None:
    processor, gateway, webhooks = make_processor(db)
    payment_id = await dead(db_sessions, attempts=1, reason="permanent")

    assert await processor.process(payment_id) == DeadLetter()

    assert gateway.calls == []
    assert webhooks.calls == []
    row = await payment_row(db_sessions, payment_id)
    assert row["attempts"] == 1
    assert row["dead_letter_reason"] == "permanent"


async def test_missing_payment_is_dead_letter(db: Db) -> None:
    processor, gateway, webhooks = make_processor(db)

    assert await processor.process(UUID(int=0)) == DeadLetter()

    assert gateway.calls == []
    assert webhooks.calls == []


async def test_stale_outcome_does_not_kill_replay(db: Db, db_sessions: Sessions) -> None:
    # A deferred "dead" outcome of the old epoch arrives after the payment was reopened.
    processor, _, webhooks = make_processor(db)
    payment_id = await dead(db_sessions)
    async with db_sessions.begin() as session:
        assert await queries.reopen(session, payment_id)

    stale = KnownOutcome("dead", 0, DeadReason.MAX_ATTEMPTS, "http_503")
    assert await processor.process(payment_id, known=stale) == Done()  # not ours: ignored

    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == "pending"
    assert row["attempts_base"] == 3
    assert row["dead_letter_reason"] is None
    assert webhooks.calls == []

    # An attempt of the old epoch that is still running fails after the replay.
    class ReopeningSender(FakeWebhookSender):
        def __init__(self, sessions: Sessions) -> None:
            super().__init__(default=Retryable("http_503"))
            self._sessions = sessions
            self.payment_id: UUID | None = None

        async def send(self, payment: Payment) -> SendResult:
            result = await super().send(payment)
            assert self.payment_id is not None
            async with self._sessions.begin() as session:
                state = await queries.mark_dead(
                    session, self.payment_id, epoch=0, reason=DeadReason.UNROUTABLE, code=None
                )
                assert state == "dead_lettered"
                assert await queries.reopen(session, self.payment_id)
            return result

    sender = ReopeningSender(db_sessions)
    other, _, _ = make_processor(db, webhooks=sender)
    running = await pending(db_sessions)
    sender.payment_id = running

    outcome = await other.process(running)

    assert isinstance(outcome, RetryLater)
    row = await payment_row(db_sessions, running)
    assert row["attempts_base"] == 1  # the replay opened a new epoch
    assert row["delivery_state"] == "pending"
    assert row["last_error"] is None  # the stale error did not touch the new epoch


async def test_exhausted_budget_with_attempt_in_flight_defers(
    db: Db, db_sessions: Sessions
) -> None:
    processor, gateway, webhooks = make_processor(db, stale_after_s=5)
    payment_id = await pending(db_sessions, attempts=3, last_attempt_ago_s=0, last_error="http_503")

    with pytest.raises(AttemptInFlight):
        await processor.process(payment_id)

    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == "pending"
    assert row["attempts"] == 3

    await make_attempt_stale(db_sessions, payment_id)
    assert await processor.process(payment_id) == DeadLetter()

    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "max_attempts"
    assert row["last_error"] == "http_503"  # the real error is not overwritten by crash_loop
    assert gateway.calls == []
    assert webhooks.calls == []


async def test_stale_exhausted_budget_without_error_is_crash_loop(
    db: Db, db_sessions: Sessions
) -> None:
    processor, _, _ = make_processor(db, stale_after_s=5)
    payment_id = await pending(db_sessions, attempts=3, last_attempt_ago_s=3600)

    assert await processor.process(payment_id) == DeadLetter()

    row = await payment_row(db_sessions, payment_id)
    assert row["dead_letter_reason"] == "max_attempts"
    assert row["last_error"] == "crash_loop"


async def test_delivered_wins_over_dead(db: Db, db_sessions: Sessions) -> None:
    processor, _, _ = make_processor(db)

    # A payment already marked dead is delivered by the other copy.
    marked_dead = await dead(db_sessions)
    assert await processor.process(marked_dead, known=KnownOutcome("delivered", 0)) == Done()
    row: Row = await payment_row(db_sessions, marked_dead)
    assert row["delivery_state"] == "delivered"
    assert row["delivered_at"] is not None
    assert row["dead_lettered_at"] is None
    assert row["dead_letter_reason"] is None
    assert row["last_error"] is None

    # A delivered payment cannot be marked dead by a late copy.
    delivered = await pending(db_sessions, status="succeeded", delivery_state="delivered")
    late = KnownOutcome("dead", 0, DeadReason.MAX_ATTEMPTS, "http_503")
    assert await processor.process(delivered, known=late) == Done()
    row = await payment_row(db_sessions, delivered)
    assert row["delivery_state"] == "delivered"
    assert row["dead_letter_reason"] is None


async def test_unexpected_exception_spends_attempt(db: Db, db_sessions: Sessions) -> None:
    webhooks = FakeWebhookSender([RuntimeError("boom")] * MAX_ATTEMPTS)
    processor, _, _ = make_processor(db, webhooks=webhooks)
    payment_id = await pending(db_sessions)

    first = await processor.process(payment_id)

    assert first == RetryLater(attempt_no=1, code="unexpected")
    row = await payment_row(db_sessions, payment_id)
    assert row["attempts"] == 1
    assert row["last_error"] == "unexpected"

    assert await processor.process(payment_id) == RetryLater(attempt_no=2, code="unexpected")
    assert await processor.process(payment_id) == DeadLetter()
    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "max_attempts"
    assert row["last_error"] == "unexpected"


async def test_mark_unroutable_dead_letters_pending_payment(db: Db, db_sessions: Sessions) -> None:
    processor, _, _ = make_processor(db)
    payment_id = await pending(db_sessions, attempts=1)

    await processor.mark_unroutable(payment_id)

    row = await payment_row(db_sessions, payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "unroutable"
