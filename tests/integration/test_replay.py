"""Replay: the CLI reopens dead payments and recovers stuck ones through the outbox (design §7.4)."""

from payments.db.session import Db
from payments.services.replay import replay_dead, replay_stuck_pending
from tests.support.consumer_harness import (
    Env,
    Rabbit,
    insert_outbox_event,
    outbox_events,
    publish_outbox,
    run_cli,
)


async def test_replay_reopens_and_delivers(env: Env, rabbit: Rabbit) -> None:
    settings = env.settings()
    async with env.consumer(settings):
        dlq = await rabbit.dlq()
        # The receiver rejects the first three deliveries of a webhook id, then accepts.
        payment_id = await env.new_payment("/hooks/fail/3")
        await rabbit.publish(payment_id)
        await dlq.wait_for(1)
        dead = await env.wait_for_state(payment_id, "dead_lettered")
        assert dead["dead_letter_reason"] == "max_attempts"
        assert len(env.posts(payment_id)) == 3

        code, output = await run_cli(settings, "replay", "dead", "--payment-id", str(payment_id))

        assert code == 0, output
        reopened = await env.row(payment_id)
        assert reopened["delivery_state"] == "pending"
        assert reopened["attempts_base"] == 3  # a new epoch with a fresh budget
        assert reopened["attempts"] == 3
        assert reopened["dead_lettered_at"] is None
        assert reopened["dead_letter_reason"] is None
        assert reopened["last_error"] is None
        (event,) = await outbox_events(env.sessions, payment_id)
        assert event["event_type"] == "payment.replay_requested"
        assert event["routing_key"] == "payments.new"

        await publish_outbox(settings)  # the relay carries the event to the consumer
        row = await env.wait_for_state(payment_id, "delivered")

    assert len(env.posts(payment_id)) == 4
    assert row["attempts"] == 4
    assert row["attempts_base"] == 3
    assert row["dead_letter_reason"] is None
    assert await outbox_events(env.sessions, payment_id) == []


async def test_replay_stuck_pending(env: Env, rabbit: Rabbit) -> None:
    settings = env.settings()
    # Pending for an hour, its message lost, nothing waiting in the outbox.
    stuck = await env.new_payment(age_s=3600, attempts=1, last_attempt_ago_s=3600)
    # Also pending, but its original event is still waiting in the outbox.
    waiting = await env.new_payment()
    await insert_outbox_event(env.sessions, waiting)

    code, output = await run_cli(settings, "replay", "stuck", "--older-than", "0")

    assert code == 0, output
    (event,) = await outbox_events(env.sessions, stuck)
    assert event["event_type"] == "payment.replay_requested"
    assert len(await outbox_events(env.sessions, waiting)) == 1  # not duplicated
    row = await env.row(stuck)
    assert row["attempts_base"] == 0  # no new budget
    assert row["attempts"] == 1
    assert row["delivery_state"] == "pending"

    async with env.consumer(settings):
        await publish_outbox(settings)
        row = await env.wait_for_state(stuck, "delivered")

    assert len(env.posts(stuck)) == 1
    assert row["attempts"] == 2


async def test_replay_dry_run_changes_nothing(env: Env) -> None:
    settings = env.settings()
    dead = await env.new_payment(
        status="succeeded",
        delivery_state="dead_lettered",
        dead_letter_reason="max_attempts",
        attempts=3,
        last_error="http_503",
        age_s=3600,
    )
    stuck = await env.new_payment(age_s=3600, attempts=1, last_attempt_ago_s=3600)
    before = {pid: await env.row(pid) for pid in (dead, stuck)}

    code, output = await run_cli(settings, "replay", "dead", "--dry-run")
    assert code == 0, output
    code, output = await run_cli(settings, "replay", "dead", "--limit", "0")
    assert code == 2, output  # a usage error, before any database access
    code, output = await run_cli(settings, "replay", "stuck", "--older-than", "0", "--dry-run")
    assert code == 0, output

    assert {pid: await env.row(pid) for pid in (dead, stuck)} == before
    assert await outbox_events(env.sessions, dead) == []
    assert await outbox_events(env.sessions, stuck) == []

    # The functions behind the CLI report what they would do.
    db = Db(env.sessions, call_timeout_s=5)
    assert await replay_dead(db, reason=None, payment_id=None, limit=100, dry_run=True) == [dead]
    assert await replay_stuck_pending(db, older_than_s=0, limit=100, dry_run=True) == [stuck]
    assert await outbox_events(env.sessions, dead) == []
    assert await outbox_events(env.sessions, stuck) == []
