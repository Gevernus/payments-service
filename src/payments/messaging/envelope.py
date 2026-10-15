"""Message headers that are carried between deliveries (design §7.3)."""

from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import structlog
from pydantic import TypeAdapter, ValidationError

from payments.services.processing import KnownOutcome

KNOWN_OUTCOME = "x-known-outcome"
DEFERRALS = "x-deferrals"

log = structlog.get_logger(__name__)

_KNOWN = TypeAdapter(KnownOutcome)


def _count(value: object) -> int:
    return value if isinstance(value, int) and value >= 0 else 0


@dataclass(frozen=True)
class Envelope:
    """Whitelisted headers of a payment message."""

    known_outcome: KnownOutcome | None
    deferrals: int

    @classmethod
    def from_headers(cls, headers: Mapping[str, Any]) -> "Envelope":
        """Read the whitelisted headers; invalid values are ignored."""
        return cls(
            known_outcome=decode_known(headers.get(KNOWN_OUTCOME)),
            deferrals=_count(headers.get(DEFERRALS)),
        )

    def deferred(self, known: KnownOutcome | None) -> dict[str, Any]:
        """Headers for a message that is put back because the database was unavailable."""
        headers: dict[str, Any] = {}
        carried = known or self.known_outcome
        if carried is not None:
            headers[KNOWN_OUTCOME] = encode_known(carried)
        headers[DEFERRALS] = self.deferrals + 1
        return headers

    def retried(self) -> dict[str, Any]:
        """Headers for a message sent to a retry queue (the error stays in the database)."""
        return {}


def encode_known(known: KnownOutcome) -> str:
    """Encode a known outcome as a header value (JSON)."""
    return _KNOWN.dump_json(known).decode()


def decode_known(value: object) -> KnownOutcome | None:
    """Decode a header value; None if absent or invalid.

    A present but invalid value is logged: it carries "the 2xx was received, do not resend".
    """
    if value is None:
        return None
    known = None
    if isinstance(value, str | bytes):
        with suppress(ValidationError):
            known = _KNOWN.validate_json(value)
    if known is None or (known.kind == "dead" and known.reason is None):
        log.warning("envelope.invalid_known_outcome", value=repr(value)[:100])
        return None
    return known
