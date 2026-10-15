"""Settings validators and key accessors (design §7.5, §9)."""

import base64
from typing import Any

import pytest
from pydantic import SecretStr, ValidationError

from tests.support.fakes import TEST_WEBHOOK_SECRET, make_settings

# A consistent timeout chain: stale >= 5 + 15 + 2*5 and graceful >= 5 + 15 + 4*5 + 5.
VALID: dict[str, Any] = {
    "gateway_min_delay_s": 2,
    "gateway_max_delay_s": 5,
    "webhook_total_timeout_s": 15,
    "db_call_timeout_s": 5,
    "rabbit_publish_timeout_s": 5,
    "outbox_lease_s": 30,
    "stale_after_s": 30,
    "consumer_graceful_timeout_s": 45,
    "retry_max_attempts": 3,
}


@pytest.mark.parametrize(
    "override",
    [
        pytest.param({"gateway_min_delay_s": 6, "gateway_max_delay_s": 5}, id="min-above-max"),
        pytest.param({"retry_max_attempts": 1}, id="single-attempt"),
        pytest.param({"outbox_lease_s": 10}, id="lease-equals-twice-publish-timeout"),
        pytest.param({"outbox_lease_s": 9}, id="lease-below-twice-publish-timeout"),
        pytest.param({"stale_after_s": 29}, id="stale-below-chain"),
        pytest.param({"consumer_graceful_timeout_s": 44}, id="graceful-below-chain"),
        pytest.param({"webhook_total_timeout_s": 16}, id="longer-webhook-breaks-stale-chain"),
    ],
)
def test_settings_validators(override: dict[str, Any]) -> None:
    assert make_settings(**VALID).stale_after_s == 30
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


def test_retry_delays_double_from_base() -> None:
    settings = make_settings(**VALID, retry_base_delay_ms=5000)
    assert settings.retry_delays_ms == [5000, 10000]
    assert make_settings(**{**VALID, "retry_max_attempts": 5}).retry_delays_ms == [
        200,
        400,
        800,
        1600,
    ]


def test_internal_hosts_parse_csv() -> None:
    settings = make_settings(**VALID, webhook_internal_hosts=" Receiver , hooks.internal,")
    assert settings.webhook_internal_hosts == {"receiver", "hooks.internal"}
