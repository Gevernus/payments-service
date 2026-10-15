"""Demo and end-to-end webhook receiver (design §11).

Records every POST under /hooks/..., checks its Standard Webhooks signature with the standard
library and lists the records at GET /received
(`?compact=1` leaves out headers and body, for load tests). Paths:

    /hooks/ok                 200
    /hooks/fail/{n}           503 with Retry-After: 1 for the first n deliveries per webhook-id
    /hooks/status/{code}      the given status code
    /hooks/slow/{seconds}     200 after the given delay

Run: uvicorn --app-dir tools webhook_receiver:app
"""

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import os
import time
from collections import Counter, deque
from datetime import UTC, datetime
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

_TOLERANCE_S = 300
_MAX_RECORDS = 10_000
_COMPACT_FIELDS = ("path", "received_at", "webhook_id", "payment_id", "signature_valid")

_records: deque[dict[str, Any]] = deque(maxlen=_MAX_RECORDS)
_failures: Counter[str] = Counter()


def _is_number(text: str) -> bool:
    """True for a non-empty string of ASCII digits (str.isdigit also accepts e.g. superscripts)."""
    return text.isascii() and text.isdigit()


def _key() -> bytes:
    secret = os.environ.get("WEBHOOK_SECRET", "")
    try:
        return base64.b64decode(secret.removeprefix("whsec_"), validate=True)
    except (binascii.Error, ValueError):
        return b""


def verify(key: bytes, headers: dict[str, str], body: bytes) -> bool:
    """Check webhook-signature against HMAC-SHA256(key, "<id>.<timestamp>." + body)."""
    msg_id = headers.get("webhook-id", "")
    stamp = headers.get("webhook-timestamp", "")
    if not key or not msg_id or not _is_number(stamp) or len(stamp) > 20:
        return False
    if abs(time.time() - int(stamp)) > _TOLERANCE_S:
        return False
    mac = hmac.new(key, f"{msg_id}.{stamp}.".encode() + body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode()
    # The header holds space-separated "v1,<signature>" entries.
    for entry in headers.get("webhook-signature", "").split():
        version, _, signature = entry.partition(",")
        if version == "v1" and hmac.compare_digest(signature.encode(), expected.encode()):
            return True
    return False


async def hook(request: Request) -> Response:
    """Record a delivery and answer according to the path."""
    body = await request.body()
    headers = {name.lower(): value for name, value in request.headers.items()}
    payment_id = None
    try:
        data = json.loads(body).get("data")
        if isinstance(data, dict):
            payment_id = data.get("payment_id")
    except (ValueError, AttributeError):
        pass
    _records.append(
        {
            "path": request.url.path,
            "received_at": datetime.now(UTC).isoformat(),
            "webhook_id": headers.get("webhook-id"),
            "payment_id": payment_id,
            "signature_valid": verify(_key(), headers, body),
            "headers": headers,
            "body": body.decode(errors="replace"),
        }
    )
    kind, _, arg = request.path_params["rest"].partition("/")
    if kind == "ok":
        return PlainTextResponse("ok")
    if kind == "fail" and _is_number(arg):
        webhook_id = headers.get("webhook-id", request.url.path)
        _failures[webhook_id] += 1
        if _failures[webhook_id] <= int(arg):
            return PlainTextResponse("unavailable", 503, headers={"Retry-After": "1"})
        return PlainTextResponse("ok")
    if kind == "status" and _is_number(arg):
        return PlainTextResponse("status", int(arg))
    if kind == "slow":
        try:
            delay = float(arg)
        except ValueError:
            return PlainTextResponse("not found", 404)
        await asyncio.sleep(delay)
        return PlainTextResponse("ok")
    return PlainTextResponse("not found", 404)


async def received(request: Request) -> Response:
    """List recorded deliveries, optionally for one payment; `compact=1` omits headers and body."""
    payment_id = request.query_params.get("payment_id")
    items = [r for r in _records if payment_id is None or r["payment_id"] == payment_id]
    if request.query_params.get("compact") == "1":
        items = [{k: r[k] for k in _COMPACT_FIELDS} for r in items]
    return JSONResponse(items)


async def health(_request: Request) -> Response:
    """Liveness probe."""
    return JSONResponse({"status": "ok"})


app = Starlette(
    routes=[
        Route("/hooks/{rest:path}", hook, methods=["POST"]),
        Route("/received", received, methods=["GET"]),
        Route("/health", health, methods=["GET"]),
    ]
)
