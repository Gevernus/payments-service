"""Address policy for webhook targets (SSRF guard)."""

import asyncio
import socket
from collections.abc import Awaitable, Callable
from ipaddress import IPv4Address, IPv6Address, IPv6Network, ip_address

import httpx

from payments.errors import BlockedTarget

IPAddress = IPv4Address | IPv6Address
Resolver = Callable[[str, int], Awaitable[list[IPAddress]]]


_NAT64 = IPv6Network("64:ff9b::/96")
_BLOCKED_V6 = tuple(
    IPv6Network(net) for net in ("fec0::/10", "::/96", "3fff::/20", "5f00::/16", "2001:20::/28")
)
_SPECIAL_SUFFIXES = (
    ".localhost",
    ".local",
    ".localdomain",
    ".internal",
    ".intranet",
    ".private",
    ".home.arpa",
    ".test",
    ".invalid",
    ".onion",
)


def _unwrap(ip: IPAddress) -> IPAddress:
    """Return the IPv4 address embedded in an IPv4-mapped or NAT64 address, else the address."""
    if isinstance(ip, IPv6Address):
        if ip.ipv4_mapped is not None:
            return ip.ipv4_mapped
        if ip in _NAT64:
            return IPv4Address(int(ip) & 0xFFFFFFFF)
    return ip


def is_public(ip: IPAddress) -> bool:
    """Tell whether the address is publicly routable."""
    ip = _unwrap(ip)
    if isinstance(ip, IPv6Address) and any(ip in net for net in _BLOCKED_V6):
        return False
    return ip.is_global and not ip.is_multicast


def _literal(host: str) -> IPAddress | None:
    try:
        return ip_address(host)
    except ValueError:
        return None


def check_url(url: str, internal_hosts: frozenset[str]) -> httpx.URL:
    """Check the URL without any I/O; raises BlockedTarget."""
    try:
        parsed = httpx.URL(url)
    except httpx.InvalidURL as exc:
        raise BlockedTarget("invalid_url") from exc
    if parsed.scheme not in ("http", "https"):
        raise BlockedTarget("scheme")
    if parsed.userinfo:
        raise BlockedTarget("userinfo")
    if parsed.fragment:
        raise BlockedTarget("fragment")
    host = parsed.raw_host.decode("ascii").lower().rstrip(".")
    if not host:
        raise BlockedTarget("host")
    if host in internal_hosts:
        return parsed
    literal = _literal(host)
    if literal is not None:
        if not is_public(literal):
            raise BlockedTarget("address")
        return parsed
    labels = host.split(".")
    if len(labels) < 2:
        raise BlockedTarget("single_label")
    if host == "localhost" or host.endswith(_SPECIAL_SUFFIXES):
        raise BlockedTarget("special_name")
    if labels[-1].isdigit():
        raise BlockedTarget("numeric_host")
    return parsed


async def system_resolver(host: str, port: int) -> list[IPAddress]:
    """Resolve a host through the system resolver."""
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addresses: list[IPAddress] = []
    for _, _, _, _, sockaddr in infos:
        address = ip_address(str(sockaddr[0]).split("%", 1)[0])
        if address not in addresses:
            addresses.append(address)
    return addresses


async def resolve_checked(
    url: httpx.URL, internal_hosts: frozenset[str], resolver: Resolver
) -> list[IPAddress]:
    """Resolve the host and require every address to be public (fail closed)."""
    host = url.raw_host.decode("ascii").lower().rstrip(".")
    port = url.port or (443 if url.scheme == "https" else 80)
    literal = _literal(host)
    addresses = [literal] if literal is not None else await resolver(host, port)
    if not addresses:
        raise BlockedTarget("no_address")
    if host not in internal_hosts and not all(is_public(address) for address in addresses):
        raise BlockedTarget("address")
    # IPv4 first; the sort is stable, so the resolver's order is kept within a family.
    return sorted(addresses, key=lambda address: address.version != 4)
