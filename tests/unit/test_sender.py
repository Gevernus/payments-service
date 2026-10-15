"""Webhook delivery: classification, transport errors, address pinning and signing (design §8.3)."""

import asyncio
import socket
import ssl
import time
from collections.abc import Awaitable, Callable, Iterator
from ipaddress import IPv4Address, ip_address

import httpx
import pytest
from standardwebhooks import Webhook

from payments.schemas import PaymentStatus
from payments.webhooks.guard import IPAddress
from payments.webhooks.sender import (
    Delivered,
    HttpWebhookSender,
    Permanent,
    Retryable,
    SendResult,
    classify_status,
    create_http_client,
)
from payments.webhooks.signing import build_body, verify_signature, webhook_id
from tests.support.fakes import TEST_WEBHOOK_KEY, TEST_WEBHOOK_SECRET, make_payment

KEY = TEST_WEBHOOK_KEY
PUBLIC_URL = "https://merchant.example/hooks/payments"

Handler = Callable[[httpx.Request], httpx.Response | Awaitable[httpx.Response]]


class Resolver:
    """A resolver returning canned answers, or failing, and counting its calls."""

    def __init__(self, answers: list[str] | Exception | None = None) -> None:
        self.answers: list[str] | Exception = ["8.8.8.8"] if answers is None else answers
        self.calls: list[tuple[str, int]] = []

    async def __call__(self, host: str, port: int) -> list[IPAddress]:
        self.calls.append((host, port))
        if isinstance(self.answers, Exception):
            raise self.answers
        return [ip_address(answer) for answer in self.answers]


class Recorder:
    """Records the requests reaching the transport and answers with a fixed response."""

    def __init__(self, handler: Handler | None = None) -> None:
        self.requests: list[httpx.Request] = []
        self._handler = handler

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self._handler is None:
            return httpx.Response(200)
        result = self._handler(request)
        if isinstance(result, httpx.Response):
            return result
        return await result


def make_sender(
    recorder: Recorder,
    *,
    resolver: Resolver | None = None,
    internal_hosts: frozenset[str] = frozenset(),
    total_timeout_s: float = 5,
    clock: Callable[[], int] | None = None,
) -> HttpWebhookSender:
    client = httpx.AsyncClient(transport=httpx.MockTransport(recorder))
    extra = {} if clock is None else {"clock": clock}
    return HttpWebhookSender(
        client=client,
        key=KEY,
        internal_hosts=internal_hosts,
        resolver=resolver or Resolver(),
        total_timeout_s=total_timeout_s,
        **extra,
    )


def counting_clock(start: int) -> Callable[[], int]:
    ticks: Iterator[int] = iter(range(start, start + 1000))
    return lambda: next(ticks)


def lowered(request: httpx.Request) -> dict[str, str]:
    return {name.lower(): value for name, value in request.headers.items()}


@pytest.mark.parametrize("status", [200, 201, 202, 204, 299])
def test_classify_status_delivered(status: int) -> None:
    assert classify_status(status) == Delivered()


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_classify_status_redirect_is_permanent(status: int) -> None:
    assert classify_status(status) == Permanent("redirect")


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410, 413, 422])
def test_classify_status_client_errors_are_permanent(status: int) -> None:
    assert classify_status(status) == Permanent(f"http_{status}")


@pytest.mark.parametrize("status", [408, 425, 429, 500, 502, 503, 504])
def test_classify_status_transient_errors_are_retryable(status: int) -> None:
    assert classify_status(status) == Retryable(f"http_{status}")


def test_classify_status_table() -> None:
    table: dict[int, SendResult] = {
        200: Delivered(),
        204: Delivered(),
        301: Permanent("redirect"),
        302: Permanent("redirect"),
        400: Permanent("http_400"),
        404: Permanent("http_404"),
        410: Permanent("http_410"),
        422: Permanent("http_422"),
        408: Retryable("http_408"),
        425: Retryable("http_425"),
        429: Retryable("http_429"),
        500: Retryable("http_500"),
        503: Retryable("http_503"),
    }
    assert {status: classify_status(status) for status in table} == table


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (200, Delivered()),
        (302, Permanent("redirect")),
        (404, Permanent("http_404")),
        (429, Retryable("http_429")),
        (503, Retryable("http_503")),
    ],
)
async def test_sender_applies_status_classification(status: int, expected: SendResult) -> None:
    recorder = Recorder(lambda _: httpx.Response(status, headers={"Location": "https://other/"}))
    result = await make_sender(recorder).send(make_payment())
    assert result == expected
    assert len(recorder.requests) == 1  # redirects are never followed


async def test_sender_maps_transport_errors() -> None:
    def failing(error: Exception) -> Handler:
        def handler(request: httpx.Request) -> httpx.Response:
            raise error

        return handler

    cert_failure = httpx.ConnectError("certificate verify failed")
    cert_failure.__cause__ = ssl.SSLCertVerificationError(1, "hostname mismatch")
    wrapped_cert_failure = httpx.ConnectError("tls")
    wrapped_cert_failure.__cause__ = OSError("handshake")
    wrapped_cert_failure.__cause__.__cause__ = ssl.SSLCertVerificationError(1, "unknown CA")

    cases: list[tuple[Exception, SendResult]] = [
        (httpx.ConnectError("refused"), Retryable("connect_error")),
        (httpx.ReadTimeout("slow"), Retryable("timeout")),
        (httpx.ConnectTimeout("slow"), Retryable("timeout")),
        (httpx.PoolTimeout("busy"), Retryable("timeout")),
        (cert_failure, Permanent("tls_error")),
        (wrapped_cert_failure, Permanent("tls_error")),
    ]
    for error, expected in cases:
        recorder = Recorder(failing(error))
        result = await make_sender(recorder).send(make_payment())
        assert result == expected, repr(error)
        assert len(recorder.requests) == 1


async def test_sender_maps_dns_failure() -> None:
    recorder = Recorder()
    resolver = Resolver(socket.gaierror(socket.EAI_NONAME, "Name or service not known"))

    result = await make_sender(recorder, resolver=resolver).send(make_payment())

    assert result == Retryable("dns_error")
    assert recorder.requests == []


async def test_sender_blocks_non_public_targets_without_sending() -> None:
    for answers in (["10.0.0.1"], ["8.8.8.8", "169.254.169.254"], ["127.0.0.1"]):
        recorder = Recorder()
        result = await make_sender(recorder, resolver=Resolver(answers)).send(make_payment())
        assert isinstance(result, Permanent)
        assert result.code.startswith("blocked:")
        assert recorder.requests == []

    recorder = Recorder()
    result = await make_sender(recorder).send(make_payment(webhook_url="http://localhost/hook"))
    assert isinstance(result, Permanent)
    assert result.code.startswith("blocked:")
    assert recorder.requests == []


async def test_sender_total_timeout_is_retryable() -> None:
    release = asyncio.Event()

    async def hang(request: httpx.Request) -> httpx.Response:
        await release.wait()
        return httpx.Response(200)

    recorder = Recorder(hang)
    result = await make_sender(recorder, total_timeout_s=0.05).send(make_payment())
    release.set()

    assert result == Retryable("timeout")


async def test_sender_pins_public_host() -> None:
    recorder = Recorder()
    resolver = Resolver(["8.8.8.8"])
    sender = make_sender(recorder, resolver=resolver)

    result = await sender.send(make_payment(webhook_url="https://merchant.example/hooks/p?x=1"))

    assert result == Delivered()
    assert resolver.calls == [("merchant.example", 443)]
    (request,) = recorder.requests
    assert request.method == "POST"
    assert request.url.host == "8.8.8.8"
    assert request.url.path == "/hooks/p"
    assert request.url.query == b"x=1"
    assert lowered(request)["host"] == "merchant.example"
    sni = request.extensions["sni_hostname"]
    assert (sni.decode() if isinstance(sni, bytes) else sni) == "merchant.example"


async def test_sender_pins_host_with_non_default_port() -> None:
    recorder = Recorder()
    resolver = Resolver(["8.8.8.8"])

    await make_sender(recorder, resolver=resolver).send(
        make_payment(webhook_url="https://merchant.example:8443/hook")
    )

    assert resolver.calls == [("merchant.example", 8443)]
    (request,) = recorder.requests
    assert request.url.host == "8.8.8.8"
    assert request.url.port == 8443
    assert lowered(request)["host"] == "merchant.example:8443"


async def test_sender_prefers_ipv4_answer() -> None:
    recorder = Recorder()
    resolver = Resolver(["2001:4860:4860::8888", "8.8.4.4"])

    await make_sender(recorder, resolver=resolver).send(make_payment())

    (request,) = recorder.requests
    assert IPv4Address(request.url.host) == IPv4Address("8.8.4.4")
    assert len(resolver.calls) == 1


async def test_sender_allows_internal_host_through_the_same_path() -> None:
    recorder = Recorder()
    resolver = Resolver(["10.0.0.7"])
    sender = make_sender(recorder, resolver=resolver, internal_hosts=frozenset({"receiver"}))

    result = await sender.send(make_payment(webhook_url="http://receiver:9000/hooks/ok"))

    assert result == Delivered()
    (request,) = recorder.requests
    assert request.url.host == "10.0.0.7"
    assert request.url.port == 9000
    assert lowered(request)["host"] == "receiver:9000"


async def test_sender_signs_the_sent_bytes() -> None:
    recorder = Recorder()
    now = int(time.time())  # the reference library checks the real clock
    payment = make_payment(status=PaymentStatus.FAILED)

    await make_sender(recorder, clock=lambda: now).send(payment)

    (request,) = recorder.requests
    headers = lowered(request)
    assert headers["webhook-id"] == webhook_id(payment.id, PaymentStatus.FAILED)
    assert headers["webhook-timestamp"] == str(now)
    assert headers["content-type"].startswith("application/json")
    assert headers["user-agent"]
    assert verify_signature(KEY, headers, request.content, now=now)
    assert request.content == build_body(payment)
    # Non-circular check with the reference library.
    reference = Webhook(TEST_WEBHOOK_SECRET)
    reference.verify(request.content, headers)


async def test_sender_signs_each_attempt_fresh_timestamp() -> None:
    recorder = Recorder()
    sender = make_sender(recorder, clock=counting_clock(1_800_000_000))
    payment = make_payment()

    await sender.send(payment)
    await sender.send(payment)

    first, second = recorder.requests
    first_headers, second_headers = lowered(first), lowered(second)
    assert first.content == second.content
    assert first_headers["webhook-id"] == second_headers["webhook-id"]
    assert first_headers["webhook-timestamp"] != second_headers["webhook-timestamp"]
    assert first_headers["webhook-signature"] != second_headers["webhook-signature"]
    assert verify_signature(KEY, first_headers, first.content, now=1_800_000_000)
    assert verify_signature(KEY, second_headers, second.content, now=1_800_000_001)
    # Each signature is bound to its own timestamp.
    swapped = {**first_headers, "webhook-signature": second_headers["webhook-signature"]}
    assert not verify_signature(KEY, swapped, first.content, now=1_800_000_000)


async def test_sender_sends_no_internal_headers() -> None:
    recorder = Recorder()

    await make_sender(recorder).send(make_payment())

    (request,) = recorder.requests
    names = set(lowered(request))
    contract = {
        "webhook-id",
        "webhook-timestamp",
        "webhook-signature",
        "content-type",
        "user-agent",
        "host",
    }
    # Headers the HTTP client adds on its own are not part of the payment contract.
    transport_defaults = {"accept", "accept-encoding", "connection", "content-length"}
    assert contract <= names
    assert names <= contract | transport_defaults
    assert not any(name.startswith("x-") for name in names)
    assert "authorization" not in names
    assert "cookie" not in names


async def test_create_http_client_policy() -> None:
    client = create_http_client()
    try:
        assert client.follow_redirects is False
        assert client.trust_env is False
        assert client.timeout.connect == 3
        assert client.timeout.read == 10
        assert client.timeout.pool == 3
    finally:
        await client.aclose()

    custom = create_http_client(connect_timeout_s=1, read_timeout_s=2)
    try:
        assert custom.timeout.connect == 1
        assert custom.timeout.read == 2
    finally:
        await custom.aclose()


async def test_create_http_client_does_not_keep_connections_alive() -> None:
    accepted = 0

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        nonlocal accepted
        accepted += 1
        try:
            while True:
                await reader.readuntil(b"\r\n\r\n")
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(serve, host="127.0.0.1", port=0)
    port = server.sockets[0].getsockname()[1]
    client = create_http_client()
    try:
        for _ in range(2):
            response = await client.post(f"http://127.0.0.1:{port}/", content=b"{}")
            assert response.status_code == 200
        assert accepted == 2
    finally:
        await client.aclose()
        server.close()
        await server.wait_closed()


async def test_sender_brackets_ipv6_literal_host_and_skips_sni() -> None:
    for url, host in (
        ("https://[2001:4860:4860::8888]/hook", "[2001:4860:4860::8888]"),
        ("https://[2001:4860:4860::8888]:8443/hook", "[2001:4860:4860::8888]:8443"),
    ):
        recorder = Recorder()
        result = await make_sender(recorder).send(make_payment(webhook_url=url))
        assert result == Delivered()
        (request,) = recorder.requests
        assert lowered(request)["host"] == host
        assert "sni_hostname" not in request.extensions


async def test_sender_maps_any_resolution_oserror_to_dns_error() -> None:
    for error in (
        OSError("network unreachable"),
        TimeoutError("resolver"),
        socket.gaierror(-3, "x"),
    ):
        recorder = Recorder()
        result = await make_sender(recorder, resolver=Resolver(error)).send(make_payment())
        assert result == Retryable("dns_error")
        assert recorder.requests == []


async def test_sender_does_not_read_the_response_body() -> None:
    release = asyncio.Event()

    async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 100000000\r\n\r\npartial")
            await writer.drain()
            await release.wait()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(serve, host="127.0.0.1", port=0)
    port = server.sockets[0].getsockname()[1]
    client = create_http_client()
    sender = HttpWebhookSender(
        client=client,
        key=KEY,
        internal_hosts=frozenset({"receiver"}),
        resolver=Resolver(["127.0.0.1"]),
        total_timeout_s=30,
    )
    try:
        started = time.monotonic()
        result = await sender.send(make_payment(webhook_url=f"http://receiver:{port}/hook"))
        assert result == Delivered()
        assert time.monotonic() - started < 3
    finally:
        release.set()
        await client.aclose()
        server.close()
        await server.wait_closed()
