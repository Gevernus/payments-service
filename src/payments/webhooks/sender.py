"""Webhook delivery over HTTP."""

import asyncio
import ssl
import time
from collections.abc import Callable
from dataclasses import dataclass
from ipaddress import ip_address
from typing import Protocol

import httpx
import structlog

from payments.db.models import Payment
from payments.errors import BlockedTarget
from payments.webhooks.guard import Resolver, check_url, resolve_checked
from payments.webhooks.signing import build_body, sign, webhook_id

logger = structlog.get_logger(__name__)

_USER_AGENT = "payments-webhooks/1.0"
_RETRYABLE_STATUSES = frozenset({408, 425, 429})


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
        self._client = client
        self._key = key
        self._internal_hosts = internal_hosts
        self._resolver = resolver
        self._total_timeout_s = total_timeout_s
        self._clock = clock

    async def send(self, payment: Payment) -> SendResult:
        """Deliver one webhook attempt."""
        target = _safe_target(payment.webhook_url)
        try:
            async with asyncio.timeout(self._total_timeout_s):
                return await self._post(payment, target)
        except TimeoutError:
            return Retryable("timeout")
        except BlockedTarget as exc:
            logger.warning("webhook.blocked", target=target, reason=exc.reason)
            return Permanent(f"blocked:{exc.reason}")
        except httpx.TransportError as exc:
            result = _transport_result(exc)
            logger.warning("webhook.transport_error", target=target, error=type(exc).__name__)
            return result

    async def _post(self, payment: Payment, target: str) -> SendResult:
        url = check_url(payment.webhook_url, self._internal_hosts)
        name = url.raw_host.decode("ascii").lower().rstrip(".")
        try:
            addresses = await resolve_checked(url, self._internal_hosts, self._resolver)
        except OSError:
            logger.warning("webhook.dns_error", target=target)
            return Retryable("dns_error")
        is_literal = _is_ip_literal(name)
        host_name = f"[{name}]" if ":" in name else name
        host_header = host_name if url.port is None else f"{host_name}:{url.port}"
        body = build_body(payment)
        timestamp = self._clock()
        msg_id = webhook_id(payment.id, payment.status)
        headers = {
            "Host": host_header,
            "Content-Type": "application/json",
            "User-Agent": _USER_AGENT,
            "webhook-id": msg_id,
            "webhook-timestamp": str(timestamp),
            "webhook-signature": sign(self._key, msg_id, timestamp, body),
        }
        extensions = {} if is_literal else {"sni_hostname": name}
        # Only the status line and headers are needed; leaving the block closes the connection
        # without reading the body.
        async with self._client.stream(
            "POST",
            url.copy_with(host=str(addresses[0])),
            content=body,
            headers=headers,
            extensions=extensions,
        ) as response:
            result = classify_status(response.status_code)
            logger.info(
                "webhook.response",
                target=target,
                status=response.status_code,
                retry_after=response.headers.get("retry-after"),
            )
        return result


def _is_ip_literal(name: str) -> bool:
    try:
        ip_address(name)
    except ValueError:
        return False
    return True


def _safe_target(url: str) -> str:
    """Log form of a webhook URL: scheme and host only."""
    try:
        parsed = httpx.URL(url)
    except httpx.InvalidURL:
        return "invalid"
    return f"{parsed.scheme}://{parsed.host}"


def _has_cert_failure(exc: BaseException | None) -> bool:
    while exc is not None:
        if isinstance(exc, ssl.SSLCertVerificationError):
            return True
        exc = exc.__cause__ or exc.__context__
    return False


def _transport_result(exc: httpx.TransportError) -> SendResult:
    if _has_cert_failure(exc):
        return Permanent("tls_error")
    if isinstance(exc, httpx.TimeoutException):
        return Retryable("timeout")
    if isinstance(exc, httpx.ConnectError):
        return Retryable("connect_error")
    return Retryable("transport_error")


def create_http_client(
    *, connect_timeout_s: float = 3, read_timeout_s: float = 10
) -> httpx.AsyncClient:
    """Create the shared HTTP client (no redirects, no proxy from the environment)."""
    return httpx.AsyncClient(
        timeout=httpx.Timeout(read_timeout_s, connect=connect_timeout_s, pool=3),
        follow_redirects=False,
        trust_env=False,
        limits=httpx.Limits(max_connections=50, max_keepalive_connections=0),
    )


def classify_status(status: int) -> SendResult:
    """Classify an HTTP status of the receiver."""
    if 200 <= status < 300:
        return Delivered()
    if 300 <= status < 400:
        return Permanent("redirect")
    if status in _RETRYABLE_STATUSES or status >= 500:
        return Retryable(f"http_{status}")
    return Permanent(f"http_{status}")
