"""Application settings."""

import base64
import binascii
import re
from functools import lru_cache
from typing import Annotated, Any, Literal

from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

_MIN_API_KEY_BYTES = 32
_URL_PASSWORD = re.compile(r"(://[^:/?#]*:)[^/?#]*@")


def mask_url_password(url: str) -> str:
    """Replace the password in a URL with `***`."""
    return _URL_PASSWORD.sub(r"\1***@", url)


class Settings(BaseSettings):
    """Environment-driven configuration (design §9)."""

    model_config = SettingsConfigDict(extra="ignore")

    database_url: str
    rabbitmq_url: str
    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "console"
    api_key: SecretStr | None = None
    webhook_secret: SecretStr | None = None
    webhook_internal_hosts: Annotated[frozenset[str], NoDecode] = frozenset()

    db_call_timeout_s: float = 5
    rabbit_publish_timeout_s: float = 5

    outbox_poll_interval_s: float = 0.5
    outbox_batch_size: int = 100
    outbox_lease_s: float = 30

    gateway_min_delay_s: float = 2
    gateway_max_delay_s: float = 5
    gateway_success_rate: float = 0.9
    gateway_outcome_hints: bool = False

    retry_max_attempts: int = 3
    retry_base_delay_ms: int = 5000

    consumer_prefetch: int = 10
    consumer_graceful_timeout_s: float = 45
    webhook_total_timeout_s: float = 15
    stale_after_s: float = 30

    @field_validator("webhook_internal_hosts", mode="before")
    @classmethod
    def _split_hosts(cls, value: Any) -> Any:
        """Parse a comma-separated host list into a set of lower-case host names."""
        if isinstance(value, str):
            return frozenset(part.strip().lower() for part in value.split(",") if part.strip())
        return value

    @model_validator(mode="after")
    def _check_consistency(self) -> "Settings":
        """Check min <= max, RETRY_MAX_ATTEMPTS >= 2 and the timeout chain (design §7.5)."""
        if self.gateway_min_delay_s > self.gateway_max_delay_s:
            raise ValueError("GATEWAY_MIN_DELAY_S must not exceed GATEWAY_MAX_DELAY_S")
        if self.retry_max_attempts < 2:
            raise ValueError("RETRY_MAX_ATTEMPTS must be at least 2")
        if self.outbox_lease_s <= 2 * self.rabbit_publish_timeout_s:
            raise ValueError("OUTBOX_LEASE_S must exceed 2 * RABBIT_PUBLISH_TIMEOUT_S")
        in_flight = self.gateway_max_delay_s + self.webhook_total_timeout_s
        stale_min = in_flight + 2 * self.db_call_timeout_s
        if self.stale_after_s < stale_min:
            raise ValueError(
                "STALE_AFTER_S must be at least GATEWAY_MAX_DELAY_S + WEBHOOK_TOTAL_TIMEOUT_S "
                f"+ 2 * DB_CALL_TIMEOUT_S ({stale_min})"
            )
        graceful_min = in_flight + 4 * self.db_call_timeout_s + self.rabbit_publish_timeout_s
        if self.consumer_graceful_timeout_s < graceful_min:
            raise ValueError(
                "CONSUMER_GRACEFUL_TIMEOUT_S must be at least GATEWAY_MAX_DELAY_S + "
                "WEBHOOK_TOTAL_TIMEOUT_S + 4 * DB_CALL_TIMEOUT_S + RABBIT_PUBLISH_TIMEOUT_S "
                f"({graceful_min})"
            )
        return self

    def require_api_key(self) -> bytes:
        """Return the API key, or raise RuntimeError if it is missing or shorter than 32 bytes."""
        if self.api_key is None:
            raise RuntimeError("API_KEY is not set")
        key = self.api_key.get_secret_value().encode()
        if len(key) < _MIN_API_KEY_BYTES:
            raise RuntimeError(f"API_KEY must be at least {_MIN_API_KEY_BYTES} bytes long")
        return key

    def require_webhook_key(self) -> bytes:
        """Return the decoded webhook signing key (the part after the `whsec_` prefix)."""
        if self.webhook_secret is None:
            raise RuntimeError("WEBHOOK_SECRET is not set")
        encoded = self.webhook_secret.get_secret_value().removeprefix("whsec_")
        try:
            key = base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise RuntimeError("WEBHOOK_SECRET is not valid base64") from exc
        if not key:
            raise RuntimeError("WEBHOOK_SECRET is empty")
        return key

    def loggable(self) -> dict[str, Any]:
        """Settings safe to log: no secrets, URL passwords masked."""
        values = self.model_dump(exclude={"api_key", "webhook_secret"})
        values["database_url"] = mask_url_password(self.database_url)
        values["rabbitmq_url"] = mask_url_password(self.rabbitmq_url)
        values["webhook_internal_hosts"] = sorted(self.webhook_internal_hosts)
        values["api_key_set"] = self.api_key is not None
        values["webhook_secret_set"] = self.webhook_secret is not None
        return values

    @property
    def retry_delays_ms(self) -> list[int]:
        """Delays of the retry ladder in milliseconds: base * 2**(i-1), i in 1..attempts-1."""
        return [self.retry_base_delay_ms * 2 ** (i - 1) for i in range(1, self.retry_max_attempts)]


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings."""
    return Settings()
