"""Pinned TLS delivery against a local TLS server.

The sender connects to the resolved address, but the Host header, the SNI name and the
certificate check all use the merchant's host name.
"""

import asyncio
import contextlib
import json
import ssl
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from ipaddress import ip_address

import httpx
import trustme
from standardwebhooks import Webhook

from payments.webhooks.guard import IPAddress
from payments.webhooks.sender import Delivered, HttpWebhookSender, Permanent
from tests.support.fakes import TEST_WEBHOOK_KEY, TEST_WEBHOOK_SECRET, make_payment

KEY = TEST_WEBHOOK_KEY
HOST = "merchant.test"


@dataclass
class Seen:
    """What the TLS server observed."""

    sni_names: list[str | None] = field(default_factory=list)
    requests: list[tuple[dict[str, str], bytes]] = field(default_factory=list)


@dataclass(frozen=True)
class TlsServer:
    """A running TLS server and its observations."""

    port: int
    seen: Seen


async def _handle(seen: Seen, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        head = await reader.readuntil(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        headers = {
            name.lower(): value.strip()
            for name, _, value in (line.partition(":") for line in lines[1:] if line)
        }
        body = await reader.readexactly(int(headers.get("content-length", "0")))
        seen.requests.append((headers, body))
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, ssl.SSLError):
        pass
    finally:
        writer.close()
        with contextlib.suppress(ConnectionError, ssl.SSLError):
            await writer.wait_closed()


@contextlib.asynccontextmanager
async def tls_server(ca: trustme.CA, certificate_name: str) -> AsyncIterator[TlsServer]:
    seen = Seen()
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ca.issue_cert(certificate_name).configure_cert(context)

    def observe(_: object, server_name: str | None, __: ssl.SSLContext) -> None:
        seen.sni_names.append(server_name)

    context.sni_callback = observe
    server = await asyncio.start_server(
        lambda r, w: _handle(seen, r, w), host="127.0.0.1", port=0, ssl=context
    )
    port = server.sockets[0].getsockname()[1]
    try:
        yield TlsServer(port, seen)
    finally:
        server.close()
        await server.wait_closed()


async def _resolve_to_loopback(host: str, port: int) -> list[IPAddress]:
    assert host == HOST
    return [ip_address("127.0.0.1")]


def _sender(ca: trustme.CA) -> HttpWebhookSender:
    context = ssl.create_default_context()
    ca.configure_trust(context)
    client = httpx.AsyncClient(verify=context, follow_redirects=False, trust_env=False)
    return HttpWebhookSender(
        client=client,
        key=KEY,
        internal_hosts=frozenset({HOST}),
        resolver=_resolve_to_loopback,
        total_timeout_s=10,
    )


async def test_webhook_pinned_tls_delivery() -> None:
    ca = trustme.CA()
    payment = make_payment()
    async with tls_server(ca, HOST) as server:
        payment.webhook_url = f"https://{HOST}:{server.port}/hooks/payments"

        result = await _sender(ca).send(payment)

        assert result == Delivered()
        assert server.seen.sni_names == [HOST]
        ((headers, body),) = server.seen.requests
    assert headers["host"] == f"{HOST}:{server.port}"
    Webhook(TEST_WEBHOOK_SECRET).verify(body, headers)
    assert json.loads(body)["data"]["payment_id"] == str(payment.id)


async def test_webhook_wrong_certificate_is_permanent_tls_error() -> None:
    ca = trustme.CA()
    payment = make_payment()
    async with tls_server(ca, "other.test") as server:
        payment.webhook_url = f"https://{HOST}:{server.port}/hooks/payments"

        result = await _sender(ca).send(payment)

        assert result == Permanent("tls_error")
        # One handshake, no retry inside the sender, and no request reached the handler.
        assert server.seen.sni_names == [HOST]
        assert server.seen.requests == []


async def test_webhook_untrusted_certificate_is_permanent_tls_error() -> None:
    trusted, rogue = trustme.CA(), trustme.CA()
    payment = make_payment()
    async with tls_server(rogue, HOST) as server:
        payment.webhook_url = f"https://{HOST}:{server.port}/hooks/payments"

        result = await _sender(trusted).send(payment)

        assert result == Permanent("tls_error")
        assert server.seen.requests == []
