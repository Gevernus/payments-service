"""Address policy for webhook targets (design §8.4).

The `is_public` table assumes Python 3.13 `ipaddress` semantics (`is_global`, special-use
registries as of 3.13); the guard adds its own blocks on top for ranges the stdlib treats as global.
"""

import socket
from ipaddress import IPv4Address, IPv6Address, ip_address

import httpx
import pytest

from payments.errors import BlockedTarget
from payments.webhooks.guard import IPAddress, check_url, is_public, resolve_checked

NO_HOSTS: frozenset[str] = frozenset()

NOT_PUBLIC = [
    "100.64.0.1",  # shared address space
    "224.0.0.1",  # IPv4 multicast
    "ff02::1",  # IPv6 multicast
    "fec0::1",  # site-local
    "::ffff:127.0.0.1",  # IPv4-mapped loopback
    "64:ff9b::7f00:1",  # NAT64 of 127.0.0.1
    "::7f00:1",  # IPv4-compatible loopback
    "3fff::1",  # documentation 3fff::/20
    "5f00::1",  # SRv6 SIDs 5f00::/16
    "2001:20::1",  # ORCHIDv2 2001:20::/28
    "2002::1",  # 6to4
    "2001::1",  # Teredo
    "2001:db8::1",  # documentation
    "fd00::1",  # unique local
    "fe80::1",  # link-local
    "169.254.169.254",  # cloud metadata
    "10.0.0.1",
    "172.16.0.1",
    "192.168.1.1",
    "192.0.2.1",  # documentation
    "0.0.0.0",
    "127.0.0.1",
    "::1",
    "::",
    "::ffff:10.0.0.1",
    "64:ff9b::a00:1",  # NAT64 of 10.0.0.1
    "255.255.255.255",
]

PUBLIC = [
    "8.8.8.8",
    "2001:4860:4860::8888",
    "::ffff:8.8.8.8",
    "64:ff9b::808:808",
    "1.1.1.1",
    # Boundaries just outside the blocked ranges (global in Python 3.13).
    "100.63.255.255",
    "100.128.0.1",
    "2001:30::1",
    "5f01::1",
    "3ffe::1",
]


@pytest.mark.parametrize("address", NOT_PUBLIC)
def test_is_public_rejects(address: str) -> None:
    assert not is_public(ip_address(address))


@pytest.mark.parametrize("address", PUBLIC)
def test_is_public_accepts(address: str) -> None:
    assert is_public(ip_address(address))


def test_is_public_table() -> None:
    assert len(NOT_PUBLIC) + len(PUBLIC) >= 20
    verdicts = {address: is_public(ip_address(address)) for address in [*NOT_PUBLIC, *PUBLIC]}
    assert {a for a, public in verdicts.items() if public} == set(PUBLIC)


@pytest.mark.parametrize(
    "url",
    [
        "https://user:pass@example.com/hook",
        "https://user@example.com/hook",
        "https://example.com/hook#frag",
        "ftp://example.com/hook",
        "file:///etc/passwd",
        "example.com/hook",
        "https:///hook",
        "http://localhost/hook",
        "http://LOCALHOST:8080/hook",
        "http://localhost./hook",
        "http://rabbitmq/hook",
        "http://rabbitmq:5672/hook",
        "https://x.internal/hook",
        "https://x.local/hook",
        "https://x.localhost/hook",
        "http://10.0.0.1/",
        "http://127.0.0.1:8000/hook",
        "http://169.254.169.254/latest/meta-data",
        "http://[::1]/hook",
        "http://[::ffff:127.0.0.1]/hook",
    ],
)
def test_check_url_rejects(url: str) -> None:
    with pytest.raises(BlockedTarget):
        check_url(url, NO_HOSTS)


def test_check_url_accepts_public_urls() -> None:
    url = check_url("https://merchant.example/hooks/payments?x=1", NO_HOSTS)
    assert isinstance(url, httpx.URL)
    assert url.host == "merchant.example"
    assert url.path == "/hooks/payments"
    assert url.query == b"x=1"

    assert check_url("http://merchant.example:8080/hook", NO_HOSTS).port == 8080


def test_check_url_allows_internal_hosts() -> None:
    allowed = frozenset({"webhook-receiver", "host.docker.internal"})

    assert check_url("http://webhook-receiver:9000/hooks/ok", allowed).host == "webhook-receiver"
    assert check_url("http://host.docker.internal:9000/x", allowed).port == 9000
    # The allow-list is exact: it does not open sibling names.
    with pytest.raises(BlockedTarget):
        check_url("http://rabbitmq/hook", allowed)
    with pytest.raises(BlockedTarget):
        check_url("http://other.docker.internal/hook", allowed)
    # Syntax rules still apply to allowed hosts.
    with pytest.raises(BlockedTarget):
        check_url("http://user:pw@webhook-receiver/hook", allowed)
    with pytest.raises(BlockedTarget):
        check_url("ftp://webhook-receiver/hook", allowed)


def test_check_url_idna() -> None:
    url = check_url("https://bücher.example/hook", NO_HOSTS)
    assert url.raw_host == b"xn--bcher-kva.example"

    # The allow-list is matched against the IDNA form of the host.
    unicode_internal = check_url("https://bücher.test/hook", frozenset({"xn--bcher-kva.test"}))
    assert unicode_internal.raw_host == b"xn--bcher-kva.test"


class RecordingResolver:
    """A resolver that returns canned answers and records its calls."""

    def __init__(self, answers: list[str] | Exception) -> None:
        self._answers = answers
        self.calls: list[tuple[str, int]] = []

    async def __call__(self, host: str, port: int) -> list[IPAddress]:
        self.calls.append((host, port))
        if isinstance(self._answers, Exception):
            raise self._answers
        return [ip_address(answer) for answer in self._answers]


URL = httpx.URL("https://merchant.example/hooks")


async def test_resolve_checked_returns_public_answers() -> None:
    resolver = RecordingResolver(["8.8.8.8", "1.1.1.1"])

    result = await resolve_checked(URL, NO_HOSTS, resolver)

    assert set(result) == {IPv4Address("8.8.8.8"), IPv4Address("1.1.1.1")}
    assert resolver.calls == [("merchant.example", 443)]


async def test_resolve_checked_uses_explicit_port() -> None:
    resolver = RecordingResolver(["8.8.8.8"])
    await resolve_checked(httpx.URL("http://merchant.example:8080/h"), NO_HOSTS, resolver)
    await resolve_checked(httpx.URL("http://merchant.example/h"), NO_HOSTS, resolver)
    assert resolver.calls == [("merchant.example", 8080), ("merchant.example", 80)]


@pytest.mark.parametrize(
    "answers",
    [
        ["8.8.8.8", "10.0.0.1"],
        ["10.0.0.1", "8.8.8.8"],
        ["2001:4860:4860::8888", "fe80::1"],
        ["8.8.8.8", "::ffff:127.0.0.1"],
        ["127.0.0.1"],
        [],
    ],
)
async def test_resolve_checked_rejects_mixed_answers(answers: list[str]) -> None:
    with pytest.raises(BlockedTarget):
        await resolve_checked(URL, NO_HOSTS, RecordingResolver(answers))


async def test_resolve_checked_prefers_ipv4() -> None:
    resolver = RecordingResolver(["2001:4860:4860::8888", "8.8.8.8", "2606:4700::1111"])

    result = await resolve_checked(URL, NO_HOSTS, resolver)

    assert result[0] == IPv4Address("8.8.8.8")
    assert IPv6Address("2001:4860:4860::8888") in result


async def test_resolve_checked_internal_host_relaxes_only_the_address_predicate() -> None:
    internal = frozenset({"webhook-receiver"})
    url = httpx.URL("http://webhook-receiver:9000/hooks/ok")

    result = await resolve_checked(url, internal, RecordingResolver(["10.0.0.7"]))
    assert result == [IPv4Address("10.0.0.7")]

    # A host outside the allow-list stays blocked when it resolves to a private address.
    with pytest.raises(BlockedTarget):
        await resolve_checked(URL, internal, RecordingResolver(["10.0.0.7"]))


async def test_resolve_checked_propagates_resolver_failure() -> None:
    resolver = RecordingResolver(socket.gaierror(socket.EAI_NONAME, "not known"))
    with pytest.raises(socket.gaierror):
        await resolve_checked(URL, NO_HOSTS, resolver)
