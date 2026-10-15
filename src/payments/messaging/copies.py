"""Publishing of message copies on a dedicated connection."""

from collections.abc import Mapping
from typing import Any


class CopyPublisher:
    """Publishes retry and deferral copies and waits for the broker's confirmation."""

    def __init__(self, url: str, *, publish_timeout_s: float, close_timeout_s: float = 5) -> None:
        raise NotImplementedError

    async def connect(self) -> None:
        """Open the connection."""
        raise NotImplementedError

    async def close(self) -> None:
        """Close the connection."""
        raise NotImplementedError

    async def publish_until_confirmed(
        self,
        body: bytes,
        *,
        routing_key: str,
        headers: Mapping[str, Any],
        message_id: str | None,
        correlation_id: str | None,
        content_type: str | None,
    ) -> None:
        """Publish until the broker confirms; raises CopyUnroutable if the message is returned."""
        raise NotImplementedError

    async def is_connected(self) -> bool:
        """Tell whether the connection is open."""
        raise NotImplementedError
