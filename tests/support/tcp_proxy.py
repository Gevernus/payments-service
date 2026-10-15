"""A TCP proxy with switchable failure modes, for fault-injection tests."""

import asyncio
import contextlib
import socket
from typing import Literal

Mode = Literal["pass", "refuse", "drop", "blackhole"]


class TcpProxy:
    """Forwards connections to a target and can simulate network failures.

    pass:      forward bytes in both directions.
    refuse:    the listener is closed, so new connections are refused.
    drop:      all active connections are closed; new connections are accepted and closed at once.
    blackhole: connections are accepted, but bytes are read and discarded in both directions,
               on existing and new connections alike.
    """

    def __init__(self) -> None:
        self._target: tuple[str, int] | None = None
        self._port = 0
        self._mode: Mode = "pass"
        self._server: asyncio.Server | None = None
        self._sock: socket.socket | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def port(self) -> int:
        """The port the proxy listens on."""
        return self._port

    async def start(self, target_host: str, target_port: int) -> int:
        """Start listening on a free local port and return it."""
        self._target = (target_host, target_port)
        self._listen(0)
        while self._server is None:
            task = next(iter(self._tasks), None)
            if task is not None and task.done():
                task.result()
            await asyncio.sleep(0)
        return self._port

    def _listen(self, port: int) -> None:
        # The socket is bound and listening before this method returns, so connections made
        # right after set_mode("pass") queue in the backlog until the server starts accepting.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("127.0.0.1", port))
        sock.listen(128)
        sock.setblocking(False)
        self._sock = sock
        self._port = sock.getsockname()[1]
        task = asyncio.get_running_loop().create_task(self._serve(sock))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _serve(self, sock: socket.socket) -> None:
        server = await asyncio.start_server(self._handle, sock=sock)
        if self._sock is not sock:  # closed or replaced while the server was starting
            server.close()
            return
        self._server = server

    def set_mode(self, mode: Mode) -> None:
        """Switch the failure mode; takes effect for existing and new connections."""
        previous, self._mode = self._mode, mode
        if mode == "refuse":
            self._close_listener()
        elif mode == "drop":
            self._drop_connections()
        if previous == "refuse" and mode != "refuse":
            self._listen(self._port)

    def _close_listener(self) -> None:
        if self._server is not None:
            self._server.close()
            self._server = None
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def _drop_connections(self) -> None:
        for writer in list(self._writers):
            writer.close()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        assert self._target is not None
        self._writers.add(writer)
        upstream_writer: asyncio.StreamWriter | None = None
        try:
            if self._mode == "drop":
                return
            try:
                upstream_reader, upstream_writer = await asyncio.open_connection(*self._target)
            except OSError:
                return
            self._writers.add(upstream_writer)
            await asyncio.gather(
                self._pump(reader, upstream_writer),
                self._pump(upstream_reader, writer),
            )
        finally:
            for w in (writer, upstream_writer):
                if w is not None:
                    self._writers.discard(w)
                    w.close()

    async def _pump(self, source: asyncio.StreamReader, sink: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await source.read(65536)
                if not data:
                    break
                if self._mode == "blackhole":
                    continue
                sink.write(data)
                await sink.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            with contextlib.suppress(Exception):
                sink.close()

    def close(self) -> None:
        """Stop listening and close every connection."""
        self._close_listener()
        self._drop_connections()
        for task in list(self._tasks):
            task.cancel()
