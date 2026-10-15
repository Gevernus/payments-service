"""Application settings."""

from functools import lru_cache
from typing import Annotated, Any, Literal

from pydantic import SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


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
        """Parse a comma-separated host list (the string form is not implemented yet)."""
        if isinstance(value, str):
            raise NotImplementedError
        return value

    @model_validator(mode="after")
    def _check_consistency(self) -> "Settings":
        """Check min <= max, RETRY_MAX_ATTEMPTS >= 2 and the timeout chain (design §7.5).

        Pass-through until the validator is implemented, so that settings can be constructed.
        """
        return self

    def require_api_key(self) -> bytes:
        """Return the API key, or raise RuntimeError if it is missing or shorter than 32 bytes."""
        raise NotImplementedError

    def require_webhook_key(self) -> bytes:
        """Return the decoded webhook signing key (the part after the `whsec_` prefix)."""
        raise NotImplementedError

    @property
    def retry_delays_ms(self) -> list[int]:
        """Delays of the retry ladder in milliseconds."""
        raise NotImplementedError


@lru_cache
def get_settings() -> Settings:
    """Return the process-wide settings."""
    return Settings()
