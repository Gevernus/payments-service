"""Settings validators and key accessors (design §7.5, §9)."""

import base64
from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from tests.support.fakes import TEST_WEBHOOK_SECRET, make_settings

# A consistent timeout chain: stale = 5 + 15 + 2*5 and graceful = 5 + 15 + 4*5 + 5.
VALID: dict[str, Any] = {
    "gateway_min_delay_s": 2,
    "gateway_max_delay_s": 5,
    "webhook_total_timeout_s": 15,
    "db_call_timeout_s": 5,
    "rabbit_publish_timeout_s": 5,
    "outbox_lease_s": 30,
    "retry_max_attempts": 3,
}


@pytest.mark.parametrize(
    "override",
    [
        pytest.param({"gateway_min_delay_s": 6, "gateway_max_delay_s": 5}, id="min-above-max"),
        pytest.param({"retry_max_attempts": 1}, id="single-attempt"),
        pytest.param({"outbox_lease_s": 10}, id="lease-equals-twice-publish-timeout"),
        pytest.param({"outbox_lease_s": 9}, id="lease-below-twice-publish-timeout"),
    ],
)
def test_settings_validators(override: dict[str, Any]) -> None:
    make_settings(**VALID)
    with pytest.raises(ValidationError):
        make_settings(**{**VALID, **override})


def test_require_api_key_short_fails() -> None:
    assert make_settings(**VALID, api_key=SecretStr("k" * 32)).require_api_key() == b"k" * 32
    with pytest.raises(RuntimeError):
        make_settings(**VALID, api_key=SecretStr("k" * 31)).require_api_key()
    with pytest.raises(RuntimeError):
        make_settings(**VALID, api_key=None).require_api_key()


def test_require_webhook_key_decodes_whsec() -> None:
    raw = b"0123456789abcdef0123456789abcdef"
    configured = make_settings(**VALID, webhook_secret=SecretStr(TEST_WEBHOOK_SECRET))
    assert configured.require_webhook_key() == raw
    other = b"\x00\x01\x02 binary key \xff"
    secret = SecretStr("whsec_" + base64.b64encode(other).decode())
    assert make_settings(**VALID, webhook_secret=secret).require_webhook_key() == other
    with pytest.raises(RuntimeError):
        make_settings(**VALID, webhook_secret=None).require_webhook_key()


def test_timeout_chain_is_derived() -> None:
    settings = make_settings(**VALID)
    assert settings.stale_after_s == 30
    assert settings.consumer_graceful_timeout_s == 45
    longer = make_settings(**{**VALID, "webhook_total_timeout_s": 16, "db_call_timeout_s": 6})
    assert longer.stale_after_s == 5 + 16 + 2 * 6
    assert longer.consumer_graceful_timeout_s == 5 + 16 + 4 * 6 + 5


@pytest.mark.parametrize(
    ("base_ms", "max_attempts", "expected"),
    [
        (5000, 2, [5000]),
        (5000, 3, [5000, 10000]),
        (200, 4, [200, 400, 800]),
        (1000, 5, [1000, 2000, 4000, 8000]),
    ],
)
def test_retry_delays_double_from_base(
    base_ms: int, max_attempts: int, expected: list[int]
) -> None:
    settings = make_settings(
        **{**VALID, "retry_base_delay_ms": base_ms, "retry_max_attempts": max_attempts}
    )
    assert settings.retry_delays_ms == expected


def test_internal_hosts_parse_csv() -> None:
    settings = make_settings(**VALID, webhook_internal_hosts=" Receiver , hooks.internal,")
    assert settings.webhook_internal_hosts == {"receiver", "hooks.internal"}


def test_loggable_masks_credentials_and_omits_secrets() -> None:
    settings = make_settings(
        **VALID,
        database_url="postgresql+asyncpg://user:s3cr3t@db.example:5432/payments",
        rabbitmq_url="amqp://guest:hunter2@mq.example:5672/?heartbeat=10",
        api_key=SecretStr("k" * 32),
        webhook_secret=SecretStr(TEST_WEBHOOK_SECRET),
    )

    logged = settings.loggable()

    assert logged["database_url"] == "postgresql+asyncpg://user:***@db.example:5432/payments"
    assert logged["rabbitmq_url"] == "amqp://guest:***@mq.example:5672/?heartbeat=10"
    assert logged["api_key_set"] is True
    assert "s3cr3t" not in repr(logged)
    assert "hunter2" not in repr(logged)
    assert TEST_WEBHOOK_SECRET not in repr(logged)
    assert "k" * 32 not in repr(logged)


def test_loggable_keeps_urls_without_password() -> None:
    settings = make_settings(**VALID, database_url="postgresql+asyncpg://user@db.example/payments")
    assert settings.loggable()["database_url"] == "postgresql+asyncpg://user@db.example/payments"


def test_loggable_includes_the_derived_timeouts() -> None:
    logged = make_settings(**VALID).loggable()
    assert logged["stale_after_s"] == 30
    assert logged["consumer_graceful_timeout_s"] == 45
