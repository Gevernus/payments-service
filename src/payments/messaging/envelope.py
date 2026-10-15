"""Message headers that are carried between deliveries (design §7.3)."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import structlog

from payments.errors import DeadReason
from payments.services.processing import KnownOutcome

log = structlog.get_logger(__name__)

REQUEST_ID = "x-request-id"
KNOWN_OUTCOME = "x-known-outcome"
DEFERRALS = "x-deferrals"


def _text(value: object) -> str | None:
    if isinstance(value, bytes):
        try:
            return value.decode()
        except UnicodeDecodeError:
            return None
    return value if isinstance(value, str) else None


def _count(value: object) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return 0


def _ignored(header: str, value: object) -> None:
    log.warning("envelope.invalid_header", header=header, value=repr(value)[:100])


@dataclass(frozen=True)
class Envelope:
    """Whitelisted headers of a payment message."""

    request_id: str | None
    known_outcome: KnownOutcome | None
    deferrals: int

    @classmethod
    def from_headers(cls, headers: Mapping[str, Any]) -> "Envelope":
        """Read the whitelisted headers; invalid values are ignored."""
        request_id = _text(headers.get(REQUEST_ID))
        known_raw = _text(headers.get(KNOWN_OUTCOME))
        known = decode_known(known_raw)
        deferrals = _count(headers.get(DEFERRALS))
        if REQUEST_ID in headers and request_id is None:
            _ignored(REQUEST_ID, headers[REQUEST_ID])
        if KNOWN_OUTCOME in headers and known is None:
            _ignored(KNOWN_OUTCOME, headers[KNOWN_OUTCOME])
        if DEFERRALS in headers and (
            (deferrals == 0 and headers[DEFERRALS] != 0) or isinstance(headers[DEFERRALS], bool)
        ):
            _ignored(DEFERRALS, headers[DEFERRALS])
        return cls(request_id=request_id, known_outcome=known, deferrals=deferrals)

    def _base(self) -> dict[str, Any]:
        return {REQUEST_ID: self.request_id} if self.request_id is not None else {}

    def deferred(self, known: KnownOutcome | None) -> dict[str, Any]:
        """Headers for a message that is put back because the database was unavailable."""
        headers = self._base()
        carried = known or self.known_outcome
        if carried is not None:
            headers[KNOWN_OUTCOME] = encode_known(carried)
        headers[DEFERRALS] = self.deferrals + 1
        return headers

    def retried(self, last_error: str) -> dict[str, Any]:
        """Headers for a message sent to a retry queue (the error stays in the database)."""
        del last_error
        return self._base()


def encode_known(known: KnownOutcome) -> str:
    """Encode a known outcome as a header value."""
    if known.kind == "delivered":
        return f"delivered:{known.epoch}"
    reason = known.reason.value if known.reason is not None else ""
    return f"dead:{known.epoch}:{reason}:{known.code or ''}"


def decode_known(value: object) -> KnownOutcome | None:
    """Decode a header value; None if invalid."""
    if not isinstance(value, str):
        return None
    parts = value.split(":", 3)
    try:
        epoch = int(parts[1]) if len(parts) > 1 else -1
    except ValueError:
        return None
    if epoch < 0:
        return None
    if parts[0] == "delivered" and len(parts) == 2:
        return KnownOutcome("delivered", epoch)
    if parts[0] == "dead" and len(parts) == 4:
        try:
            reason = DeadReason(parts[2])
        except ValueError:
            return None
        return KnownOutcome("dead", epoch, reason, parts[3] or None)
    return None
