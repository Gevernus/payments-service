"""Webhook delivery over HTTP."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Protocol

import httpx

from payments.db.models import Payment
from payments.webhooks.guard import Resolver


@dataclass(frozen=True)
class Delivered:
    """The receiver accepted the webhook."""


@dataclass(frozen=True)
class Retryable:
    """A failure worth retrying."""

    code: str


@dataclass(frozen=True)
class Permanent:
    """A failure that retrying cannot fix."""

    code: str


SendResult = Delivered | Retryable | Permanent


class WebhookSender(Protocol):
    """Sends the webhook for a payment."""

    async def send(self, payment: Payment) -> SendResult:
        """Deliver one webhook attempt."""
        ...


def _unix_now() -> int:
    return int(time.time())


class HttpWebhookSender:
    """Signs and posts webhooks to the payment's webhook_url."""

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        key: bytes,
        internal_hosts: frozenset[str],
        resolver: Resolver,
        total_timeout_s: float,
        clock: Callable[[], int] = _unix_now,
    ) -> None:
        raise NotImplementedError

    async def send(self, payment: Payment) -> SendResult:
        """Deliver one webhook attempt."""
        raise NotImplementedError


def create_http_client(
    *, connect_timeout_s: float = 3, read_timeout_s: float = 10
) -> httpx.AsyncClient:
    """Create the shared HTTP client (no redirects, no proxy from the environment)."""
    raise NotImplementedError


def classify_status(status: int) -> SendResult:
    """Classify an HTTP status of the receiver."""
    raise NotImplementedError
