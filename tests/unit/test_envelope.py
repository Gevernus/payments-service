"""The message headers carried between deliveries (design §7.3)."""

from typing import Any

import pytest

from payments.errors import DeadReason
from payments.messaging.envelope import Envelope, decode_known, encode_known
from payments.services.processing import KnownOutcome

WHITELIST = {"x-request-id", "x-known-outcome", "x-deferrals"}
DELIVERED = KnownOutcome("delivered", 2)
DEAD = KnownOutcome("dead", 0, DeadReason.PERMANENT, "http_404")


def test_envelope_table() -> None:
    envelope = Envelope.from_headers(
        {"x-request-id": "req-1", "x-known-outcome": "delivered:2", "x-deferrals": 3}
    )
    assert envelope == Envelope(request_id="req-1", known_outcome=DELIVERED, deferrals=3)

    # Retry: the request id is kept, the known outcome is removed, the counter is reset.
    retried = envelope.retried("http_503")
    assert retried["x-request-id"] == "req-1"
    assert "x-known-outcome" not in retried
    assert retried.get("x-deferrals", 0) == 0
    assert set(retried) <= WHITELIST

    # Defer: the request id is kept, the outcome is set from the exception or kept, the counter
    # is incremented.
    deferred_with_new = envelope.deferred(DEAD)
    assert deferred_with_new["x-request-id"] == "req-1"
    assert deferred_with_new["x-known-outcome"] == "dead:0:permanent:http_404"
    assert deferred_with_new["x-deferrals"] == 4

    deferred_keeping = envelope.deferred(None)
    assert deferred_keeping["x-request-id"] == "req-1"
    assert deferred_keeping["x-known-outcome"] == "delivered:2"
    assert deferred_keeping["x-deferrals"] == 4

    # Without any input headers nothing is invented.
    empty = Envelope.from_headers({})
    assert empty == Envelope(request_id=None, known_outcome=None, deferrals=0)
    assert empty.deferred(None) == {"x-deferrals": 1}
    assert empty.retried("timeout").get("x-deferrals", 0) == 0
    assert "x-request-id" not in empty.retried("timeout")
    assert empty.deferred(DELIVERED)["x-known-outcome"] == "delivered:2"


def test_envelope_drops_broker_counters() -> None:
    headers: dict[str, Any] = {
        "x-request-id": "req-2",
        "x-delivery-count": 7,
        "x-death": [{"count": 1, "reason": "rejected", "queue": "payments.new"}],
        "x-first-death-queue": "payments.new",
        "x-first-death-reason": "rejected",
        "x-last-death-queue": "payments.new",
        "x-opt-something-else": "value",
        "x-known-outcome": "delivered:0",
    }
    envelope = Envelope.from_headers(headers)

    for produced in (
        envelope.retried("http_503"),
        envelope.deferred(None),
        envelope.deferred(DEAD),
    ):
        assert set(produced) <= WHITELIST
        assert produced["x-request-id"] == "req-2"


def test_envelope_ignores_invalid_values() -> None:
    envelope = Envelope.from_headers(
        {"x-known-outcome": "dead:bogus", "x-deferrals": "many", "x-request-id": 42}
    )

    assert envelope.known_outcome is None
    assert envelope.deferrals == 0
    assert envelope.deferred(None).get("x-known-outcome") is None


@pytest.mark.parametrize(
    "known", [DELIVERED, DEAD, KnownOutcome("dead", 3, DeadReason.MAX_ATTEMPTS, "timeout")]
)
def test_known_outcome_round_trip(known: KnownOutcome) -> None:
    assert decode_known(encode_known(known)) == known


def test_encode_known_format() -> None:
    assert encode_known(DELIVERED) == "delivered:2"
    assert encode_known(DEAD) == "dead:0:permanent:http_404"


def test_decode_known_handles_colons() -> None:
    assert decode_known("dead:0:permanent:blocked:private") == KnownOutcome(
        "dead", 0, DeadReason.PERMANENT, "blocked:private"
    )


@pytest.mark.parametrize(
    "value",
    [
        None,
        42,
        "",
        "garbage",
        "delivered",
        "delivered:abc",
        "dead:0",
        "dead:x:permanent:http_404",
        "dead:0:nonsense:http_404",
        "unknown:1",
    ],
)
def test_decode_known_invalid_ignored(value: object) -> None:
    assert decode_known(value) is None
