"""Address policy for webhook targets (SSRF guard, design §8.4)."""

from collections.abc import Awaitable, Callable
from ipaddress import IPv4Address, IPv6Address

import httpx

IPAddress = IPv4Address | IPv6Address
Resolver = Callable[[str, int], Awaitable[list[IPAddress]]]


def is_public(ip: IPAddress) -> bool:
    """Tell whether the address is publicly routable."""
    raise NotImplementedError


def check_url(url: str, internal_hosts: frozenset[str]) -> httpx.URL:
    """Check the URL without any I/O; raises BlockedTarget."""
    raise NotImplementedError


async def system_resolver(host: str, port: int) -> list[IPAddress]:
    """Resolve a host through the system resolver."""
    raise NotImplementedError


async def resolve_checked(
    url: httpx.URL, internal_hosts: frozenset[str], resolver: Resolver
) -> list[IPAddress]:
    """Resolve the host and require every address to be public (fail closed)."""
    raise NotImplementedError
