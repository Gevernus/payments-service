"""Domain errors, dead-letter reasons and delivery error codes."""

from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from payments.services.processing import KnownOutcome


class DeadReason(StrEnum):
    """Why a payment was dead-lettered."""

    MAX_ATTEMPTS = "max_attempts"
    PERMANENT = "permanent"
    UNROUTABLE = "unroutable"


# Error codes stored in payments.last_error (design §3.1).
CRASH_LOOP = "crash_loop"
UNEXPECTED = "unexpected"
GATEWAY_UNAVAILABLE = "gateway_unavailable"
TLS_ERROR = "tls_error"
CONNECT_ERROR = "connect_error"
TIMEOUT = "timeout"
DNS_ERROR = "dns_error"
REDIRECT = "redirect"


def http_code(status: int) -> str:
    """Error code for an unsuccessful webhook HTTP status."""
    return f"http_{status}"


def blocked(reason: str) -> str:
    """Error code for a webhook target rejected by the address policy."""
    return f"blocked:{reason}"


class DomainError(Exception):
    """Base class for errors rendered as problem+json responses."""

    code: str = "domain_error"
    status_code: int = 400
    title: str = "Domain error"


class IdempotencyKeyMissing(DomainError):
    """The Idempotency-Key header is absent."""

    code = "idempotency_key_missing"
    status_code = 400
    title = "Idempotency-Key header is required"


class IdempotencyKeyInvalid(DomainError):
    """The Idempotency-Key header is malformed."""

    code = "idempotency_key_invalid"
    status_code = 400
    title = "Idempotency-Key header is invalid"


class IdempotencyKeyReused(DomainError):
    """The key was already used with a different request body."""

    code = "idempotency_key_reused"
    status_code = 422
    title = "Idempotency-Key was already used with a different request"


class PaymentNotFound(DomainError):
    """No payment with the requested id."""

    code = "payment_not_found"
    status_code = 404
    title = "Payment not found"


class WebhookUrlNotAllowed(DomainError):
    """The webhook URL is rejected by the address policy."""

    code = "webhook_url_not_allowed"
    status_code = 422
    title = "Webhook URL is not allowed"

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class DatabaseUnavailable(Exception):
    """The database did not answer in time or the connection failed."""

    def __init__(self, known: "KnownOutcome | None" = None) -> None:
        super().__init__("database unavailable")
        self.known = known


class GatewayUnavailable(Exception):
    """The payment gateway failed or the charge is still in progress."""


class AttemptInFlight(Exception):
    """Another worker is still processing the current attempt."""


class CopyUnroutable(Exception):
    """A message copy was returned by the broker as unroutable."""


class BlockedTarget(Exception):
    """The webhook target is rejected by the address policy."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason
