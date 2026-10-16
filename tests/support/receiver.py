"""An in-process HTTP receiver that stands in for the merchant's webhook endpoint."""

import asyncio
import contextlib
import json
import socket
from collections import Counter
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest_asyncio
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response
from starlette.routing import Route


@dataclass(frozen=True)
class Delivery:
    """One request received by the receiver."""

    path: str
    headers: dict[str, str]
    body: bytes
    received_at: datetime

    def json(self) -> Any:
        """The body decoded as JSON."""
        return json.loads(self.body)


class _QuietServer(uvicorn.Server):
    """A uvicorn server that does not touch process signal handlers."""

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        yield


class Receiver:
    """Records every POST and answers according to the path.

    /hooks/ok                 200
    /hooks/fail/{n}           503 with Retry-After: 1 for the first n deliveries per webhook-id
    /hooks/status/{code}      the given status code
    /hooks/slow/{seconds}     200 after the given delay
    /hooks/hold               blocks until release_hold() is called, then 200
    """

    def __init__(self) -> None:
        self.records: list[Delivery] = []
        self._failures: Counter[str] = Counter()
        self._hold = asyncio.Event()
        self._server: _QuietServer | None = None
        self._task: asyncio.Task[None] | None = None
        self.port = 0
        self.app = Starlette(
            routes=[
                Route("/hooks/{rest:path}", self._handle, methods=["POST"]),
            ]
        )

    @property
    def host(self) -> str:
        """The address the receiver listens on."""
        return "127.0.0.1"

    def release_hold(self) -> None:
        """Release requests blocked on /hooks/hold, now and in the future."""
        self._hold.set()

    def deliveries(self, payment_id: UUID | str) -> list[Delivery]:
        """Deliveries whose body carries the given payment id."""
        wanted = str(payment_id)
        found = []
        for record in self.records:
            try:
                data = record.json().get("data", {})
            except (ValueError, AttributeError):
                continue
            if isinstance(data, dict) and data.get("payment_id") == wanted:
                found.append(record)
        return found

    async def _handle(self, request: Request) -> Response:
        body = await request.body()
        headers = {k.lower(): v for k, v in request.headers.items()}
        path = request.url.path
        self.records.append(Delivery(path, headers, body, datetime.now(UTC)))
        parts = path.split("/")[2:]
        kind = parts[0] if parts else ""
        arg = parts[1] if len(parts) > 1 else ""
        if kind == "fail" and arg.isdigit():
            key = headers.get("webhook-id", path)
            self._failures[key] += 1
            if self._failures[key] <= int(arg):
                return PlainTextResponse(
                    "unavailable", status_code=503, headers={"Retry-After": "1"}
                )
            return PlainTextResponse("ok")
        if kind == "status" and arg.isdigit():
            return PlainTextResponse("status", status_code=int(arg))
        if kind == "slow":
            await asyncio.sleep(float(arg or 0))
            return PlainTextResponse("ok")
        if kind == "hold":
            await self._hold.wait()
            return PlainTextResponse("ok")
        if kind == "ok":
            return PlainTextResponse("ok")
        return PlainTextResponse("not found", status_code=404)

    async def start(self) -> None:
        """Start serving on a free port."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind((self.host, 0))
        self.port = sock.getsockname()[1]
        config = uvicorn.Config(self.app, log_level="warning", lifespan="off")
        self._server = _QuietServer(config)
        self._task = asyncio.create_task(self._server.serve(sockets=[sock]))
        while not self._server.started:
            if self._task.done():
                self._task.result()
            await asyncio.sleep(0.01)

    async def stop(self) -> None:
        """Stop serving; blocked requests are released first."""
        self._hold.set()
        if self._server is not None and self._task is not None:
            self._server.should_exit = True
            self._server.force_exit = True
            await self._task


@pytest_asyncio.fixture
async def receiver() -> AsyncIterator[Receiver]:
    """A running receiver, stopped after the test."""
    instance = Receiver()
    await instance.start()
    try:
        yield instance
    finally:
        await instance.stop()
