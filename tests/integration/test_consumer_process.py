"""The consumer as a real process: SIGKILL during a webhook and SIGTERM with work in flight."""

import signal

from payments.messaging.topology import DLQ_NAME, MAIN_QUEUE_NAME
from tests.support.consumer_harness import Env, Rabbit, worker_env
from tests.support.subprocess_worker import start_worker
from tests.support.wait import wait_until


async def test_consumer_sigkill_during_webhook(env: Env, rabbit: Rabbit) -> None:
    settings = env.settings()
    payment_id = await env.new_payment("/hooks/hold")

    first = await start_worker("consumer", worker_env(settings))
    try:
        await rabbit.wait_for_consumer()
        await rabbit.publish(payment_id)
        # The webhook has reached the receiver and is held there: the status is already final.
        await wait_until(lambda: len(env.posts(payment_id)) == 1, timeout=30)
        before = await env.row(payment_id)
        assert before["status"] == "succeeded"
        assert before["attempts"] == 1
        assert before["delivery_state"] == "pending"

        first.kill(signal.SIGKILL)
        await first.wait()
    finally:
        await first.stop()

    env.receiver.release_hold()
    second = await start_worker("consumer", worker_env(settings))
    try:
        row = await env.wait_for_state(payment_id, "delivered", timeout=60)
    finally:
        await second.stop()

    posts = env.posts(payment_id)
    assert len(posts) >= 2  # the killed attempt's POST and the redelivered one
    assert len({post.headers["webhook-id"] for post in posts}) == 1
    assert row["processed_at"] == before["processed_at"]  # the gateway was not called again
    assert any("gateway.charge_started" in line for line in first.lines)
    assert not any("gateway.charge_started" in line for line in second.lines)
    assert row["status"] == "succeeded"
    assert row["attempts"] == len(posts)
    assert await rabbit.ready(DLQ_NAME) == 0


async def test_consumer_sigterm_finishes_in_flight(env: Env, rabbit: Rabbit) -> None:
    settings = env.settings()
    payment_id = await env.new_payment("/hooks/hold")

    worker = await start_worker("consumer", worker_env(settings))
    try:
        await rabbit.wait_for_consumer()
        await rabbit.publish(payment_id)
        await wait_until(lambda: len(env.posts(payment_id)) == 1, timeout=30)

        worker.kill(signal.SIGTERM)
        await worker.wait_for_log("Shutting down")
        assert worker.process.returncode is None  # waiting for the handler that is in flight

        env.receiver.release_hold()
        code = await worker.wait(timeout=60)
    finally:
        await worker.stop()

    assert code in (0, -signal.SIGTERM)  # uvicorn re-raises the signal after a clean shutdown
    row = await env.row(payment_id)
    assert row["delivery_state"] == "delivered"
    assert row["attempts"] == 1
    assert len(env.posts(payment_id)) == 1
    # The message was acked before the process exited: nothing came back to the queue.
    assert await rabbit.ready(MAIN_QUEUE_NAME) == 0
    assert await rabbit.ready(DLQ_NAME) == 0
