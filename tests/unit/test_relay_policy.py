"""Backoff formulas and stored error codes of the relay."""

from types import SimpleNamespace

import pytest
from aiormq.exceptions import DeliveryError, PublishError
from pamqp.commands import Basic

from payments.workers.relay import loop_backoff, publish_error_code, row_backoff


@pytest.mark.parametrize(("attempts", "base"), [(1, 1), (2, 2), (3, 4), (6, 32), (7, 60), (50, 60)])
def test_row_backoff_is_exponential_with_bounded_jitter(attempts: int, base: float) -> None:
    for _ in range(200):
        assert base * 0.8 <= row_backoff(attempts) <= base * 1.2


@pytest.mark.parametrize(
    ("streak", "expected"), [(1, 1.0), (2, 2.0), (3, 4.0), (5, 16.0), (6, 30.0), (20, 30.0)]
)
def test_loop_backoff(streak: int, expected: float) -> None:
    assert loop_backoff(0.5, streak) == expected


def test_row_backoff_survives_huge_attempt_counts() -> None:
    for _ in range(200):
        assert 60.0 * 0.8 <= row_backoff(10_000) <= 60.0 * 1.2


def test_loop_backoff_survives_long_outages() -> None:
    assert loop_backoff(0.5, 10_000) == 30.0


def test_error_codes() -> None:
    nack = Basic.Nack()
    returned = Basic.Return(reply_code=312, reply_text="NO_ROUTE", routing_key="x")
    unroutable = PublishError(SimpleNamespace(delivery=returned), nack)  # type: ignore[arg-type]
    assert publish_error_code(unroutable) == "unroutable"
    assert publish_error_code(DeliveryError(None, nack)) == "nack"
    assert publish_error_code(TimeoutError()) == "publish_timeout"
    assert publish_error_code(ValueError("secret text")) == "publish_error"
