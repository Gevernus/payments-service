"""Message headers that are carried between deliveries (design §7.3)."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from payments.services.processing import KnownOutcome


@dataclass(frozen=True)
class Envelope:
    """Whitelisted headers of a payment message."""

    request_id: str | None
    known_outcome: KnownOutcome | None
    deferrals: int

    @classmethod
    def from_headers(cls, headers: Mapping[str, Any]) -> "Envelope":
        """Read the whitelisted headers; invalid values are ignored."""
        raise NotImplementedError

    def deferred(self, known: KnownOutcome | None) -> dict[str, Any]:
        """Headers for a message that is put back because the database was unavailable."""
        raise NotImplementedError

    def retried(self, last_error: str) -> dict[str, Any]:
        """Headers for a message sent to a retry queue."""
        raise NotImplementedError


def encode_known(known: KnownOutcome) -> str:
    """Encode a known outcome as a header value."""
    raise NotImplementedError


def decode_known(value: object) -> KnownOutcome | None:
    """Decode a header value; None if invalid."""
    raise NotImplementedError
