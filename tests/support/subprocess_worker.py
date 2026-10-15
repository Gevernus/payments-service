"""Runs a worker process (relay or consumer) so tests can kill it for real."""

import asyncio
import contextlib
import os
import signal
import socket
import sys
from dataclasses import dataclass, field

from tests.support.wait import wait_until


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class Worker:
    """A running worker process in its own session, with its output captured."""

    process: asyncio.subprocess.Process
    port: int
    lines: list[str] = field(default_factory=list)
    _reader: asyncio.Task[None] | None = None

    async def wait_for_log(self, event_name: str, timeout: float = 15.0) -> str:
        """Wait for an output line that contains event_name and return it."""

        def seen() -> bool:
            return any(event_name in line for line in self.lines)

        try:
            await wait_until(seen, timeout=timeout)
        except TimeoutError:
            tail = "\n".join(self.lines[-20:])
            raise TimeoutError(
                f"log event {event_name!r} not seen within {timeout}s; "
                f"exit code {self.process.returncode}; last output:\n{tail}"
            ) from None
        return next(line for line in self.lines if event_name in line)

    def kill(self, sig: int = signal.SIGKILL) -> None:
        """Send a signal to the whole process group."""
        with contextlib.suppress(ProcessLookupError):
            os.killpg(self.process.pid, sig)

    async def wait(self, timeout: float = 30.0) -> int:
        """Wait for the process to exit and return its exit code."""
        code = await asyncio.wait_for(self.process.wait(), timeout)
        if self._reader is not None:
            await self._reader
        return code

    async def stop(self) -> None:
        """Terminate gracefully, then kill; used for cleanup."""
        if self.process.returncode is None:
            self.kill(signal.SIGTERM)
            try:
                await self.wait(timeout=10)
            except TimeoutError:
                self.kill(signal.SIGKILL)
                await self.wait(timeout=10)


async def start_worker(module: str, env: dict[str, str]) -> Worker:
    """Start `python -m uvicorn payments.workers.<module>:create_app --factory`.

    `module` is a worker name such as "relay" or "consumer"; a full "package.module:factory"
    target is also accepted. The environment is the current one plus `env`.
    """
    target = module if ":" in module else f"payments.workers.{module}:create_app"
    port = _free_port()
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "uvicorn",
        target,
        "--factory",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env={**os.environ, "PYTHONUNBUFFERED": "1", **env},
        start_new_session=True,
    )
    worker = Worker(process=process, port=port)

    async def read_output() -> None:
        assert process.stdout is not None
        async for raw in process.stdout:
            worker.lines.append(raw.decode(errors="replace").rstrip("\n"))

    worker._reader = asyncio.create_task(read_output())
    return worker
