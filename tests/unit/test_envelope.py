"""The message headers carried between deliveries (design §7.3)."""

from typing import Any

import pytest

from payments.errors import DeadReason
from payments.messaging.envelope import Envelope, decode_known, encode_known
from payments.services.processing import KnownOutcome

WHITELIST = {"x-known-outcome", "x-deferrals"}
DELIVERED = KnownOutcome("delivered", 2)
DEAD = KnownOutcome("dead", 0, DeadReason.PERMANENT, "http_404")


def test_envelope_table() -> None:
    envelope = Envelope.from_headers(
        {"x-known-outcome": '{"kind":"delivered","epoch":2}', "x-deferrals": 3}
    )
    assert envelope == Envelope(known_outcome=DELIVERED, deferrals=3)

    # Retry: the known outcome is removed, the counter is reset.
    retried = envelope.retried()
    assert "x-known-outcome" not in retried
    assert retried.get("x-deferrals", 0) == 0
    assert set(retried) <= WHITELIST

    # Defer: the outcome is set from the exception or kept, the counter is incremented.
    deferred_with_new = envelope.deferred(DEAD)
    assert deferred_with_new["x-known-outcome"] == encode_known(DEAD)
    assert deferred_with_new["x-deferrals"] == 4

    deferred_keeping = envelope.deferred(None)
    assert deferred_keeping["x-known-outcome"] == encode_known(DELIVERED)
    assert deferred_keeping["x-deferrals"] == 4

    # Without any input headers nothing is invented.
    empty = Envelope.from_headers({})
    assert empty == Envelope(known_outcome=None, deferrals=0)
    assert empty.deferred(None) == {"x-deferrals": 1}
    assert empty.retried().get("x-deferrals", 0) == 0
    assert empty.deferred(DELIVERED)["x-known-outcome"] == encode_known(DELIVERED)


def test_envelope_drops_broker_counters() -> None:
    headers: dict[str, Any] = {
        "x-delivery-count": 7,
        "x-death": [{"count": 1, "reason": "rejected", "queue": "payments.new"}],
        "x-first-death-queue": "payments.new",
        "x-first-death-reason": "rejected",
        "x-last-death-queue": "payments.new",
        "x-opt-something-else": "value",
        "x-known-outcome": encode_known(KnownOutcome("delivered", 0)),
    }
    envelope = Envelope.from_headers(headers)

    for produced in (
        envelope.retried(),
        envelope.deferred(None),
        envelope.deferred(DEAD),
    ):
        assert set(produced) <= WHITELIST


def test_envelope_ignores_invalid_values() -> None:
    envelope = Envelope.from_headers({"x-known-outcome": "dead:bogus", "x-deferrals": "many"})

    assert envelope.known_outcome is None
    assert envelope.deferrals == 0
    assert envelope.deferred(None).get("x-known-outcome") is None


@pytest.mark.parametrize(
    "known", [DELIVERED, DEAD, KnownOutcome("dead", 3, DeadReason.MAX_ATTEMPTS, "timeout")]
)
def test_known_outcome_round_trip(known: KnownOutcome) -> None:
    assert decode_known(encode_known(known)) == known


def test_encode_known_format() -> None:
    assert encode_known(DELIVERED) == ('{"kind":"delivered","epoch":2,"reason":null,"code":null}')
    assert encode_known(DEAD) == (
        '{"kind":"dead","epoch":0,"reason":"permanent","code":"http_404"}'
    )


def test_decode_known_handles_colons() -> None:
    known = KnownOutcome("dead", 0, DeadReason.PERMANENT, "blocked:private")
    assert decode_known(encode_known(known)) == known


def test_invalid_known_outcome_is_logged_and_other_headers_are_not() -> None:
    from structlog.testing import capture_logs

    with capture_logs() as logs:
        Envelope.from_headers({"x-known-outcome": "garbage", "x-deferrals": "many"})
        Envelope.from_headers({"x-known-outcome": encode_known(DELIVERED)})
        Envelope.from_headers({})

    assert [entry["event"] for entry in logs] == ["envelope.invalid_known_outcome"]


@pytest.mark.parametrize(
    "value",
    [
        None,
        42,
        "",
        "garbage",
        "delivered:2",
        '{"kind":"delivered","epoch":-1}',
        '{"kind":"delivered","epoch":"x"}',
        '{"kind":"dead","epoch":0}',
        '{"kind":"dead","epoch":0,"reason":"nonsense"}',
        '{"kind":"unknown","epoch":1}',
    ],
)
def test_decode_known_invalid_ignored(value: object) -> None:
    assert decode_known(value) is None
