"""End-to-end scenarios against `compose.yaml` + `compose.e2e.yaml`.

Every test drives the stack through its published ports: the API, the webhook receiver and the
RabbitMQ management API. Waiting is polling with a deadline.
"""

import json
from typing import Any
from uuid import UUID, uuid4

import pytest

from payments.schemas import PaymentStatus
from tests.e2e.stack import RECEIVER_INTERNAL_URL, Stack, compose, dump, outbox_count, unique_key
from tests.support.wait import wait_until

pytestmark = pytest.mark.e2e

DEADLINE_S = 40.0


async def _wait_delivery(stack: Stack, payment_id: UUID, state: str) -> dict[str, Any]:
    async def reached() -> bool:
        return await stack.delivery_state(payment_id) == state

    await wait_until(reached, timeout=DEADLINE_S, interval=0.1)
    return await stack.get_payment(payment_id)


async def test_e2e_happy_path(stack: Stack) -> None:
    key = await unique_key(PaymentStatus.SUCCEEDED)
    response = await stack.api.post(
        "/api/v1/payments",
        json={
            "amount": "100.00",
            "currency": "RUB",
            "description": "happy path",
            "metadata": {"order_id": 42},
            "webhook_url": f"{RECEIVER_INTERNAL_URL}/hooks/ok",
        },
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 202, response.text
    payment_id = UUID(response.json()["payment_id"])
    assert response.headers["Location"] == f"/api/v1/payments/{payment_id}"

    # A GET right after the 202 finds the payment (it was committed before the response).
    immediate = await stack.api.get(f"/api/v1/payments/{payment_id}")
    assert immediate.status_code == 200
    assert immediate.json()["idempotency_key"] == key

    payment = await _wait_delivery(stack, payment_id, "delivered")
    assert payment["status"] == "succeeded"
    assert payment["processed_at"] is not None
    delivery = payment["delivery"]
    assert delivery["attempts"] == 1
    assert delivery["delivered_at"] is not None

    posts = await stack.received(payment_id)
    assert len(posts) == 1, dump(posts)
    post = posts[0]
    assert post["path"] == "/hooks/ok"
    assert post["signature_valid"] is True
    body = json.loads(post["body"])
    assert body["type"] == "payment.succeeded"
    for field in ("payment_id", "amount", "currency", "description", "metadata", "status"):
        assert body["data"][field] == payment[field], field
    assert body["data"]["metadata"] == {"order_id": 42}


async def test_e2e_gateway_decline_is_final(stack: Stack) -> None:
    payment_id = await stack.create_payment(hook="/hooks/ok", outcome=PaymentStatus.FAILED)

    payment = await _wait_delivery(stack, payment_id, "delivered")

    # The decline is the final status and was notified, not retried or turned into a success.
    assert payment["status"] == "failed"
    delivery = payment["delivery"]
    assert delivery["attempts"] == 1
    posts = await stack.received(payment_id)
    assert len(posts) == 1, dump(posts)
    assert posts[0]["signature_valid"] is True
    body = json.loads(posts[0]["body"])
    assert body["type"] == "payment.failed"
    assert body["data"]["status"] == "failed"
    # Reading the payment again shows the same final status.
    assert (await stack.get_payment(payment_id))["status"] == "failed"


async def test_e2e_webhook_flaky_then_delivered(stack: Stack) -> None:
    # The receiver answers 503 to the first two POSTs of a webhook-id, then 200.
    payment_id = await stack.create_payment(hook="/hooks/fail/2")

    payment = await _wait_delivery(stack, payment_id, "delivered")

    assert payment["status"] == "succeeded"
    delivery = payment["delivery"]
    assert delivery["attempts"] == 3
    assert delivery["dead_lettered_at"] is None
    posts = await stack.received(payment_id)
    assert len(posts) == 3, dump(posts)
    assert {post["path"] for post in posts} == {"/hooks/fail/2"}
    assert len({post["webhook_id"] for post in posts}) == 1  # the merchant can deduplicate
    assert all(post["signature_valid"] for post in posts)  # each attempt is signed again
    assert not await stack.in_dlq(payment_id)


async def test_e2e_webhook_retry_exhausted_to_dlq(stack: Stack) -> None:
    payment_id = await stack.create_payment(hook="/hooks/status/503")

    payment = await _wait_delivery(stack, payment_id, "dead_lettered")

    delivery = payment["delivery"]
    assert delivery["dead_letter_reason"] == "max_attempts"
    assert delivery["attempts"] == 3
    assert delivery["last_error"] == "http_503"
    assert delivery["dead_lettered_at"] is not None
    assert delivery["delivered_at"] is None
    assert payment["status"] == "succeeded"  # the business result is unaffected

    async def message_in_dlq() -> bool:
        return await stack.in_dlq(payment_id)

    await wait_until(message_in_dlq, timeout=DEADLINE_S, interval=0.2)

    # Exactly three POSTs. The budget CAS refuses a fourth attempt, so a fourth POST would
    # mean the budget was bypassed; the DLQ message above shows the consumer finished with it.
    posts = await stack.received(payment_id)
    assert len(posts) == 3, dump(posts)
    assert len({post["webhook_id"] for post in posts}) == 1


async def test_e2e_ssrf_internal_host_rejected(stack: Stack) -> None:
    # The e2e override leaves only `webhook-receiver` in WEBHOOK_INTERNAL_HOSTS, so
    # `host.docker.internal` is an internal name that nobody allowed. The address policy is
    # applied when the payment is created (design §4.3).
    key = await unique_key(PaymentStatus.SUCCEEDED)
    marker = uuid4().hex
    response = await stack.api.post(
        "/api/v1/payments",
        json={
            "amount": "10.00",
            "currency": "USD",
            "description": marker,
            "webhook_url": "http://host.docker.internal:9000/hooks/ok",
        },
        headers={"Idempotency-Key": key},
    )

    assert response.status_code == 422, response.text
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["code"] == "webhook_url_not_allowed"

    # Nothing was stored: the same key with an allowed URL creates a fresh payment instead of
    # being a replay of (or a conflict with) the rejected request.
    accepted = await stack.api.post(
        "/api/v1/payments",
        json={
            "amount": "10.00",
            "currency": "USD",
            "description": marker,
            "webhook_url": f"{RECEIVER_INTERNAL_URL}/hooks/ok",
        },
        headers={"Idempotency-Key": key},
    )
    assert accepted.status_code == 202, accepted.text
    assert accepted.headers["Idempotent-Replayed"] == "false"
    await _wait_delivery(stack, UUID(accepted.json()["payment_id"]), "delivered")


async def test_e2e_broker_outage_outbox_catches_up(stack: Stack) -> None:
    await compose("stop", "rabbitmq")
    try:
        payment_ids = [await stack.create_payment(hook="/hooks/ok") for _ in range(5)]
        assert len(set(payment_ids)) == 5

        # While the broker is down the events wait in the outbox and nothing is processed.
        assert await outbox_count(payment_ids) == 5
        for payment_id in payment_ids:
            payment = await stack.get_payment(payment_id)
            assert payment["status"] == "pending"
            assert payment["delivery"]["state"] == "pending"
            assert payment["delivery"]["attempts"] == 0
            assert await stack.received(payment_id) == []
    finally:
        # Blocks until the broker is healthy again.
        await compose("up", "-d", "--no-deps", "--wait", "rabbitmq")

    for payment_id in payment_ids:
        payment = await _wait_delivery(stack, payment_id, "delivered")
        assert payment["status"] == "succeeded"

    async def outbox_drained() -> bool:
        return await outbox_count(payment_ids) == 0

    await wait_until(outbox_drained, timeout=DEADLINE_S, interval=0.5)
    for payment_id in payment_ids:
        posts = await stack.received(payment_id)
        assert len(posts) == 1, dump(posts)  # no duplicate effect
        assert posts[0]["signature_valid"] is True


async def test_e2e_replay_cli(stack: Stack) -> None:
    # The first five POSTs of a webhook-id get 503. The first budget of 3 is spent and the
    # payment is dead; after a replay a fresh budget of 3 covers POSTs 4 to 6, and only the
    # sixth is accepted. Delivery on the sixth proves a full new budget, not one extra attempt.
    payment_id = await stack.create_payment(hook="/hooks/fail/5")
    dead = await _wait_delivery(stack, payment_id, "dead_lettered")
    assert dead["delivery"]["dead_letter_reason"] == "max_attempts"
    assert len(await stack.received(payment_id)) == 3

    base = ["exec", "-T", "consumer", "python", "-m", "payments.cli", "replay", "dead"]
    target = ["--payment-id", str(payment_id)]

    dry = await compose(*base, *target, "--dry-run")
    assert str(payment_id) in dry.stdout
    assert "would replay 1 payment(s)" in dry.stdout
    assert await stack.delivery_state(payment_id) == "dead_lettered"
    assert len(await stack.received(payment_id)) == 3

    replayed = await compose(*base, *target)
    assert str(payment_id) in replayed.stdout
    assert "replayed 1 payment(s)" in replayed.stdout

    payment = await _wait_delivery(stack, payment_id, "delivered")
    assert payment["status"] == "succeeded"
    assert payment["delivery"]["dead_letter_reason"] is None
    posts = await stack.received(payment_id)
    assert len(posts) == 6, dump(posts)
    assert payment["delivery"]["attempts"] == 6
    assert len({post["webhook_id"] for post in posts}) == 1
    assert all(post["signature_valid"] for post in posts)
