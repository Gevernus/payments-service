"""The consumer runs the payment state machine under retries, duplicates and failures.

Runs the real consumer application against PostgreSQL, RabbitMQ and an in-process webhook
receiver (failure matrix in docs/guarantees.md). Webhook outcomes come from
receiver paths, gateway outcomes from the idempotency key, which seeds the emulator. Faults are
injected with TCP proxies in front of PostgreSQL or RabbitMQ, and by replacing a single `queries`
function or collaborator method in the consumer's process.
"""

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack
from typing import Any
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from sqlalchemy import text
from structlog.testing import capture_logs

from payments.db import queries
from payments.errors import DeadReason, GatewayUnavailable
from payments.messaging.envelope import decode_known
from payments.messaging.topology import DLQ_NAME, MAIN_QUEUE_NAME
from payments.schemas import PaymentStatus
from payments.services.gateway import EmulatedGateway
from payments.services.processing import KnownOutcome
from payments.webhooks.sender import HttpWebhookSender
from tests.support.api import AUTH, api_client
from tests.support.consumer_harness import (
    BASE_DELAY_MS,
    Env,
    Rabbit,
    cut,
    make_attempt_stale,
    running_handler,
    start_amqp_proxy,
    start_db_proxy,
)
from tests.support.wait import wait_until

BASE_S = BASE_DELAY_MS / 1000


@pytest_asyncio.fixture
async def api(env: Env) -> AsyncIterator[httpx.AsyncClient]:
    """The API application on the same database, for reading payments back."""
    async with api_client(env.settings()) as client:
        yield client


async def get_delivery(client: httpx.AsyncClient, payment_id: UUID) -> dict[str, Any]:
    response = await client.get(f"/api/v1/payments/{payment_id}", headers=AUTH)
    assert response.status_code == 200
    body: dict[str, Any] = response.json()
    return body


async def in_flight_count(rabbit: Rabbit) -> int:
    """Unacknowledged messages of the main queue (management statistics lag by seconds)."""
    return int((await rabbit.queue_info(MAIN_QUEUE_NAME)).get("messages_unacknowledged", 0))


def replace_query(monkeypatch: pytest.MonkeyPatch, name: str, replacement: Any) -> None:
    """Replace one function of `queries` for the consumer running in this process."""
    monkeypatch.setattr(queries, name, replacement)


# --- retries and budget -------------------------------------------------------------------------


async def test_retry_ladder_exactly_three_posts_then_dlq(
    env: Env, rabbit: Rabbit, api: httpx.AsyncClient
) -> None:
    async with env.consumer(env.settings()):
        dlq = await rabbit.dlq()
        payment_id = await env.new_payment("/hooks/status/503")

        event_id = await rabbit.publish(payment_id)
        await dlq.wait_for(1)

    posts = env.posts(payment_id)
    assert len(posts) == 3
    first, second, third = (post.received_at for post in posts)
    assert (second - first).total_seconds() >= 0.9 * BASE_S
    assert (third - second).total_seconds() >= 0.9 * 2 * BASE_S
    assert {post.headers["webhook-id"] for post in posts} == {posts[0].headers["webhook-id"]}
    (dead_message,) = dlq.messages
    assert dead_message.message_id == event_id
    # Dead-lettered natively by a reject, not published to the DLQ by the consumer.
    deaths: Any = dead_message.headers.get("x-death")
    assert isinstance(deaths, list)
    assert any(d.get("reason") == "rejected" and d.get("queue") == MAIN_QUEUE_NAME for d in deaths)
    info = await get_delivery(api, payment_id)
    assert info["delivery"]["state"] == "dead_lettered"
    assert info["delivery"]["dead_letter_reason"] == "max_attempts"
    assert info["delivery"]["last_error"] == "http_503"
    assert info["delivery"]["attempts"] == 3
    assert info["status"] == "succeeded"


async def test_webhook_timeout_follows_the_retry_ladder(
    env: Env, rabbit: Rabbit, api: httpx.AsyncClient
) -> None:
    # The endpoint answers after 2 s, the total webhook timeout is 0.5 s: every attempt times out.
    async with env.consumer(env.settings(webhook_total_timeout_s=0.5)):
        dlq = await rabbit.dlq()
        payment_id = await env.new_payment("/hooks/slow/2")

        await rabbit.publish(payment_id)
        await dlq.wait_for(1)

    assert len(env.posts(payment_id)) == 3
    info = await get_delivery(api, payment_id)
    assert info["delivery"]["state"] == "dead_lettered"
    assert info["delivery"]["dead_letter_reason"] == "max_attempts"
    assert info["delivery"]["last_error"] == "timeout"
    assert info["delivery"]["attempts"] == 3


async def test_duplicate_chains_share_attempt_budget(env: Env, rabbit: Rabbit) -> None:
    async with env.consumer(env.settings()):
        dlq = await rabbit.dlq()
        payment_id = await env.new_payment("/hooks/status/503")

        await rabbit.publish(payment_id)
        await rabbit.publish(payment_id)
        await dlq.wait_for(2)  # both chains end in the DLQ: the second one finds the payment dead

    assert len(env.posts(payment_id)) == 3
    row = await env.row(payment_id)
    assert row["attempts"] == 3
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "max_attempts"


async def test_permanent_goes_to_dlq_after_one_post(env: Env, rabbit: Rabbit) -> None:
    async with env.consumer(env.settings()):
        dlq = await rabbit.dlq()
        payment_id = await env.new_payment("/hooks/status/404")

        with capture_logs() as logs:
            await rabbit.publish(payment_id)
            await dlq.wait_for(1)

    processed = [e for e in logs if e["event"] == "payment.processed"]
    assert [e["outcome"] for e in processed] == ["dead_letter"]
    assert len(env.posts(payment_id)) == 1
    row = await env.row(payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "permanent"
    assert row["last_error"] == "http_404"
    assert row["attempts"] == 1


async def test_gateway_unavailable_three_times_dead_letters_pending(
    env: Env, rabbit: Rabbit, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def unavailable(*_args: object, **_kwargs: object) -> PaymentStatus:
        raise GatewayUnavailable("gateway down")

    async with env.consumer(env.settings()):
        dlq = await rabbit.dlq()
        payment_id = await env.new_payment()
        monkeypatch.setattr(EmulatedGateway, "charge", unavailable)

        await rabbit.publish(payment_id)
        await dlq.wait_for(1)

    assert env.posts(payment_id) == []
    row = await env.row(payment_id)
    assert row["status"] == "pending"
    assert row["processed_at"] is None
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "max_attempts"
    assert row["last_error"] == "gateway_unavailable"
    assert row["attempts"] == 3


# --- database failures --------------------------------------------------------------------------


async def test_db_outage_defers_without_spending_attempt(env: Env, rabbit: Rabbit) -> None:
    proxy, proxied_url = await start_db_proxy(env.database_url)
    try:
        async with env.consumer(env.settings(database_url=proxied_url)):
            spy = await rabbit.spy()
            payment_id = await env.new_payment()

            proxy.set_mode("blackhole")  # the connection stays open and says nothing
            await rabbit.publish(payment_id)
            (deferred, *_) = await spy.wait_for_header("x-deferrals", timeout=60)

            assert not deferred.headers.get("x-known-outcome")
            row = await env.row(payment_id)
            assert row["attempts"] == 0
            assert row["delivery_state"] == "pending"
            assert env.posts(payment_id) == []
            assert await rabbit.ready(DLQ_NAME) == 0

            proxy.set_mode("pass")
            row = await env.wait_for_state(payment_id, "delivered")
    finally:
        proxy.close()

    assert row["attempts"] == 1  # the outage spent nothing
    assert len(env.posts(payment_id)) == 1
    assert await rabbit.ready(DLQ_NAME) == 0


async def test_db_failure_after_attempt_started_counts_the_attempt(
    env: Env, rabbit: Rabbit, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy, proxied_url = await start_db_proxy(env.database_url)
    original = queries.finalize
    cut_once: list[bool] = []

    async def cutting_finalize(*args: Any, **kwargs: Any) -> Any:
        if not cut_once:
            cut_once.append(True)
            cut(proxy)  # begin_attempt has committed; the gateway was called; finalize fails
        return await original(*args, **kwargs)

    replace_query(monkeypatch, "finalize", cutting_finalize)
    try:
        async with env.consumer(env.settings(database_url=proxied_url)):
            spy = await rabbit.spy()
            payment_id = await env.new_payment()

            await rabbit.publish(payment_id)
            (deferred, *_) = await spy.wait_for_header("x-deferrals", timeout=60)

            assert not deferred.headers.get("x-known-outcome")
            assert await rabbit.ready(DLQ_NAME) == 0  # deferred, never dead-lettered
            assert env.posts(payment_id) == []

            proxy.set_mode("pass")
            row = await env.wait_for_state(payment_id, "delivered")
    finally:
        proxy.close()

    assert cut_once  # finalize was reached and failed
    assert row["attempts"] == 2  # one spent by the interrupted pass, one by the successful pass
    assert len(env.posts(payment_id)) == 1
    assert await rabbit.ready(DLQ_NAME) == 0


async def test_db_down_after_delivery_persists_without_resend(env: Env, rabbit: Rabbit) -> None:
    proxy, proxied_url = await start_db_proxy(env.database_url)
    try:
        async with env.consumer(env.settings(database_url=proxied_url)):
            spy = await rabbit.spy()
            payment_id = await env.new_payment("/hooks/hold")

            await rabbit.publish(payment_id)
            await wait_until(lambda: len(env.posts(payment_id)) == 1, timeout=30)
            cut(proxy)  # the webhook is in flight; the database goes away before it answers
            env.receiver.release_hold()  # now the receiver accepts it
            (carried, *_) = await spy.wait_for_header("x-known-outcome")

            assert decode_known(carried.headers["x-known-outcome"]) == KnownOutcome("delivered", 0)
            assert (await env.row(payment_id))["delivery_state"] == "pending"

            proxy.set_mode("pass")
            row = await env.wait_for_state(payment_id, "delivered")
    finally:
        proxy.close()

    assert len(env.posts(payment_id)) == 1  # the outcome was written, the webhook not repeated
    assert row["attempts"] == 1
    assert await rabbit.ready(DLQ_NAME) == 0


async def test_db_down_at_dead_letter_marks_later(
    env: Env, rabbit: Rabbit, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy, proxied_url = await start_db_proxy(env.database_url)
    original = queries.mark_dead
    cut_once: list[bool] = []

    async def cutting_mark_dead(*args: Any, **kwargs: Any) -> Any:
        if not cut_once:
            cut_once.append(True)
            cut(proxy)
        return await original(*args, **kwargs)

    replace_query(monkeypatch, "mark_dead", cutting_mark_dead)
    try:
        async with env.consumer(env.settings(database_url=proxied_url)):
            spy = await rabbit.spy()
            dlq = await rabbit.dlq()
            payment_id = await env.new_payment("/hooks/status/404")

            await rabbit.publish(payment_id)
            (carried, *_) = await spy.wait_for_header("x-known-outcome")

            assert decode_known(carried.headers["x-known-outcome"]) == KnownOutcome(
                "dead", 0, DeadReason.PERMANENT, "http_404"
            )
            assert await rabbit.ready(DLQ_NAME) == 0  # nothing is dead-lettered unmarked
            assert (await env.row(payment_id))["delivery_state"] == "pending"

            proxy.set_mode("pass")
            await dlq.wait_for(1)
    finally:
        proxy.close()

    row = await env.row(payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "permanent"
    assert row["last_error"] == "http_404"
    assert len(env.posts(payment_id)) == 1


async def test_db_error_in_consumer_never_dead_letters(env: Env, rabbit: Rabbit) -> None:
    async with env.consumer(env.settings()):
        spy = await rabbit.spy()
        dlq = await rabbit.dlq()
        payment_id = await env.new_payment()

        # A transaction holds the row, so the consumer's UPDATE hits the lock_timeout (2 s) and
        # gets a genuine LockNotAvailable: a database error that is not a network failure.
        async with env.sessions() as holder:
            await holder.execute(
                text("SELECT id FROM payments WHERE id = :id FOR UPDATE"), {"id": payment_id}
            )
            await rabbit.publish(payment_id)
            await spy.wait_for_header("x-deferrals", count=2, timeout=60)  # again and again

            assert await rabbit.ready(DLQ_NAME) == 0
            assert len(dlq.messages) == 0
            assert env.posts(payment_id) == []
            await holder.rollback()  # release the lock

        row = await env.wait_for_state(payment_id, "delivered", timeout=60)

    assert row["attempts"] == 1  # the lock errors spent no attempt
    assert len(env.posts(payment_id)) == 1
    assert await rabbit.ready(DLQ_NAME) == 0


# --- dead payments ------------------------------------------------------------------------------


async def test_dead_payment_redelivery_rejects_again(env: Env, rabbit: Rabbit) -> None:
    async with env.consumer(env.settings()):
        dlq = await rabbit.dlq()
        payment_id = await env.new_payment("/hooks/status/404")
        event_id = await rabbit.publish(payment_id)
        await dlq.wait_for(1)
        await env.wait_for_state(payment_id, "dead_lettered")

        # The broker redelivers a message whose reject was lost (a crash after the dead mark).
        await rabbit.publish(payment_id, message_id=event_id)
        await dlq.wait_for(2)

    assert len(env.posts(payment_id)) == 1
    assert (await env.row(payment_id))["attempts"] == 1


async def test_ui_requeue_from_dlq_is_noop_for_dead(env: Env, rabbit: Rabbit) -> None:
    async with env.consumer(env.settings()):
        dlq = await rabbit.dlq()
        payment_id = await env.new_payment("/hooks/status/404")
        await rabbit.publish(payment_id)
        (dead_message,) = await dlq.wait_for(1)

        # An operator moves the message back to the main queue, keeping its headers.
        await rabbit.publish_raw(
            dead_message.body,
            message_id=dead_message.message_id,
            correlation_id=dead_message.correlation_id,
            headers=dict(dead_message.headers),
        )
        await dlq.wait_for(2)

    assert len(env.posts(payment_id)) == 1
    row = await env.row(payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["attempts"] == 1


async def test_malformed_message_dead_lettered(env: Env, rabbit: Rabbit) -> None:
    async with env.consumer(env.settings()):
        dlq = await rabbit.dlq()
        bystander = await env.new_payment()

        await rabbit.publish_raw(b"{this is not json", message_id="malformed-1")
        await rabbit.publish_raw(json.dumps({"hello": "world"}).encode(), message_id="malformed-2")
        await dlq.wait_for(2)

        # The consumer is still alive and processes a good message.
        payment_id = await env.new_payment()
        await rabbit.publish(payment_id)
        await env.wait_for_state(payment_id, "delivered")

    assert {m.message_id for m in dlq.messages} == {"malformed-1", "malformed-2"}
    row = await env.row(bystander)
    assert row["delivery_state"] == "pending"
    assert row["attempts"] == 0
    assert env.posts(bystander) == []


# --- the retry copy -----------------------------------------------------------------------------


async def test_unroutable_copy_marks_and_dead_letters(env: Env, rabbit: Rabbit) -> None:
    async with env.consumer(env.settings()):
        dlq = await rabbit.dlq()
        await rabbit.unbind_retry_queue(BASE_DELAY_MS)
        payment_id = await env.new_payment("/hooks/status/503")

        await rabbit.publish(payment_id)
        await dlq.wait_for(1)

    assert len(env.posts(payment_id)) == 1
    row = await env.row(payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "unroutable"


async def test_copy_waits_while_publish_connection_down(env: Env, rabbit: Rabbit) -> None:
    proxy, copies_url = await start_amqp_proxy(env.rabbitmq_url)
    retry_queue = "payments.new.retry.30000ms"
    settings = env.settings(retry_base_delay_ms=30000)
    try:
        async with running_handler(settings, copies_url=copies_url):
            payment_id = await env.new_payment("/hooks/status/503")

            proxy.set_mode("drop")  # only the copy publisher's connection goes through the proxy
            await rabbit.publish(payment_id)
            await wait_until(lambda: len(env.posts(payment_id)) == 1, timeout=30)

            async def unacked() -> bool:
                return (
                    int(
                        (await rabbit.queue_info(MAIN_QUEUE_NAME)).get("messages_unacknowledged", 0)
                    )
                    == 1
                )

            await wait_until(unacked, timeout=30, interval=0.5)  # the handler is waiting
            assert await rabbit.ready(retry_queue) == 0

            proxy.set_mode("pass")

            async def copy_published() -> bool:
                return await rabbit.ready(retry_queue) == 1

            await wait_until(copy_published, timeout=30, interval=0.2)

            async def acked() -> bool:
                return (
                    int(
                        (await rabbit.queue_info(MAIN_QUEUE_NAME)).get("messages_unacknowledged", 0)
                    )
                    == 0
                )

            await wait_until(
                acked, timeout=30, interval=0.5
            )  # the original is acked after the copy
    finally:
        proxy.close()

    assert len(env.posts(payment_id)) == 1
    row = await env.row(payment_id)
    assert row["attempts"] == 1
    assert row["delivery_state"] == "pending"
    assert await rabbit.ready(DLQ_NAME) == 0


# --- crashes and connection loss ----------------------------------------------------------------


async def test_crash_loop_spends_budget_and_marks(
    env: Env, rabbit: Rabbit, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy, proxied_url = await start_amqp_proxy(env.rabbitmq_url)
    crashes: list[bool] = []

    async def crashing_charge(self: EmulatedGateway, **kwargs: Any) -> PaymentStatus:
        # The attempt has started (and is committed); the process dies before it finishes.
        crashes.append(True)
        proxy.set_mode("drop")
        raise asyncio.CancelledError

    def crashed_times(count: int) -> Callable[[], bool]:
        return lambda: len(crashes) >= count

    monkeypatch.setattr(EmulatedGateway, "charge", crashing_charge)
    try:
        async with env.consumer(env.settings(rabbitmq_url=proxied_url)):
            dlq = await rabbit.dlq()
            spy = await rabbit.spy()
            payment_id = await env.new_payment()
            await rabbit.publish(payment_id)

            for crash in (1, 2, 3):
                await wait_until(crashed_times(crash), timeout=60)
                proxy.set_mode("pass")

            # The fourth delivery finds the budget spent and the last attempt fresh: it defers.
            await spy.wait_for_header("x-deferrals", timeout=60)
            assert await rabbit.ready(DLQ_NAME) == 0
            assert (await env.row(payment_id))["delivery_state"] == "pending"
            await make_attempt_stale(env.sessions, payment_id)
            await dlq.wait_for(1, timeout=60)
    finally:
        proxy.close()

    assert len(crashes) == 3
    row = await env.row(payment_id)
    assert row["delivery_state"] == "dead_lettered"
    assert row["dead_letter_reason"] == "max_attempts"
    assert row["last_error"] == "crash_loop"
    assert row["attempts"] == 3
    assert env.posts(payment_id) == []


async def test_broker_connection_drop_redelivers(env: Env, rabbit: Rabbit) -> None:
    proxy, proxied_url = await start_amqp_proxy(env.rabbitmq_url)
    try:
        async with env.consumer(env.settings(rabbitmq_url=proxied_url)):
            payment_id = await env.new_payment("/hooks/hold")
            await rabbit.publish(payment_id)
            await wait_until(lambda: len(env.posts(payment_id)) == 1, timeout=30)

            proxy.set_mode("drop")  # the connection is lost while the webhook is in flight
            proxy.set_mode("pass")
            env.receiver.release_hold()
            row = await env.wait_for_state(payment_id, "delivered", timeout=60)
    finally:
        proxy.close()

    posts = env.posts(payment_id)
    assert len(posts) in (1, 2)  # a second POST is possible, with the same webhook-id
    assert row["attempts"] == len(posts)
    assert len({post.headers["webhook-id"] for post in posts}) == 1
    assert await rabbit.ready(DLQ_NAME) == 0


async def test_redelivery_after_finalize_does_not_recall_gateway(
    env: Env, rabbit: Rabbit, monkeypatch: pytest.MonkeyPatch
) -> None:
    proxy, proxied_url = await start_amqp_proxy(env.rabbitmq_url)
    charges: list[str] = []
    sends: list[bool] = []
    original_charge = EmulatedGateway.charge
    original_send = HttpWebhookSender.send

    async def counting_charge(self: EmulatedGateway, **kwargs: Any) -> PaymentStatus:
        charges.append(kwargs["idempotency_key"])
        return await original_charge(self, **kwargs)

    async def crashing_send(self: HttpWebhookSender, payment: Any) -> Any:
        sends.append(True)
        if len(sends) == 1:  # the status is final; the process dies before the webhook
            proxy.set_mode("drop")
            raise asyncio.CancelledError
        return await original_send(self, payment)

    monkeypatch.setattr(EmulatedGateway, "charge", counting_charge)
    monkeypatch.setattr(HttpWebhookSender, "send", crashing_send)
    try:
        async with env.consumer(env.settings(rabbitmq_url=proxied_url)):
            payment_id = await env.new_payment()
            await rabbit.publish(payment_id)
            await wait_until(lambda: len(sends) >= 1, timeout=30)
            proxy.set_mode("pass")
            row = await env.wait_for_state(payment_id, "delivered", timeout=60)
    finally:
        proxy.close()

    assert charges == [row["idempotency_key"]]  # the gateway was called once
    assert len(env.posts(payment_id)) == 1
    assert row["status"] == "succeeded"
    assert row["attempts"] == 2


async def test_parallel_duplicates_single_transition(
    env: Env, rabbit: Rabbit, monkeypatch: pytest.MonkeyPatch
) -> None:
    settings = env.settings(gateway_min_delay_s=0.3, gateway_max_delay_s=0.3)
    original = queries.finalize
    wins: list[bool] = []

    async def counting_finalize(*args: Any, **kwargs: Any) -> Any:
        result = await original(*args, **kwargs)
        if result is not None:
            wins.append(True)
        return result

    replace_query(monkeypatch, "finalize", counting_finalize)
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(env.consumer(settings))
        await stack.enter_async_context(env.consumer(settings))
        payment_id = await env.new_payment()

        for _ in range(6):
            await rabbit.publish(payment_id)
        row = await env.wait_for_state(payment_id, "delivered")

    posts = env.posts(payment_id)
    assert len(wins) == 1  # one status transition, however many chains ran
    assert row["status"] == "succeeded"
    assert 1 <= len(posts) <= 3
    assert len(posts) <= row["attempts"] <= 3
    assert len({post.headers["webhook-id"] for post in posts}) == 1
    assert {post.json()["data"]["status"] for post in posts} == {"succeeded"}
    assert await rabbit.ready(DLQ_NAME) == 0


# --- the webhook body and shutdown --------------------------------------------------------------


async def test_webhook_body_matches_get(env: Env, rabbit: Rabbit, api: httpx.AsyncClient) -> None:
    async with env.consumer(env.settings()):
        payment_id = await env.new_payment(metadata={"order_id": 42})
        await rabbit.publish(payment_id)
        await env.wait_for_state(payment_id, "delivered")

    (post,) = env.posts(payment_id)
    sent = post.json()
    shown = await get_delivery(api, payment_id)
    assert sent["type"]
    assert sent["data"].keys() == {
        "payment_id",
        "amount",
        "currency",
        "description",
        "metadata",
        "status",
        "created_at",
        "processed_at",
    }
    for field, value in sent["data"].items():
        assert shown[field] == value, field
    assert sent["data"]["amount"] == "100.00"
    assert sent["data"]["metadata"] == {"order_id": 42}
    assert sent["data"]["status"] == "succeeded"
    assert sent["data"]["processed_at"] is not None


async def test_copies_closed_after_in_flight_handlers(env: Env, rabbit: Rabbit) -> None:
    retry_queue = "payments.new.retry.30000ms"
    settings = env.settings(gateway_min_delay_s=1, gateway_max_delay_s=1, retry_base_delay_ms=30000)
    async with AsyncExitStack() as stack:
        await stack.enter_async_context(env.consumer(settings))
        payment_id = await env.new_payment("/hooks/status/503")
        await rabbit.publish(payment_id)

        async def attempt_started() -> bool:
            return int((await env.row(payment_id))["attempts"]) == 1

        await wait_until(attempt_started, timeout=30, interval=0.05)
        # Leaving the context stops the application while the handler is in the gateway.

    # The handler finished: webhook sent, copy published and confirmed, original acked.
    assert len(env.posts(payment_id)) == 1
    assert await rabbit.ready(retry_queue) == 1
    assert await rabbit.ready(MAIN_QUEUE_NAME) == 0
    assert await rabbit.ready(DLQ_NAME) == 0
    row = await env.row(payment_id)
    assert row["delivery_state"] == "pending"
    assert row["last_error"] == "http_503"
