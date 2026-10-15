"""Engine, session factory and the single database-call wrapper."""

import asyncio
import errno
import socket
from collections.abc import Awaitable, Callable
from typing import TypeVar

import asyncpg  # type: ignore[import-untyped]
import sqlalchemy.exc
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payments.config import Settings
from payments.errors import DatabaseUnavailable

T = TypeVar("T")

_NET_ERRNO = {errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ENETDOWN, errno.EHOSTDOWN}


def create_engine(settings: Settings, *, role: str) -> AsyncEngine:
    """Create an engine for the given process role (used as application_name)."""
    return create_async_engine(
        settings.database_url,
        pool_pre_ping=True,
        connect_args={
            "timeout": 3,
            "command_timeout": 5,
            "server_settings": {
                "statement_timeout": "4000",
                "lock_timeout": "2000",
                "idle_in_transaction_session_timeout": "10000",
                "application_name": role,
            },
        },
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create a session factory with expire_on_commit disabled."""
    return async_sessionmaker(engine, expire_on_commit=False)


def _is_network(exc: BaseException | None) -> bool:
    return isinstance(exc, ConnectionError | socket.gaierror | TimeoutError) or (
        isinstance(exc, OSError) and exc.errno in _NET_ERRNO
    )


def is_db_unavailable(exc: BaseException) -> bool:
    """Tell whether an exception means the database is unreachable (design §7.1)."""
    if _is_network(exc) or isinstance(exc, sqlalchemy.exc.TimeoutError):
        return True
    if isinstance(exc, sqlalchemy.exc.DBAPIError) and exc.connection_invalidated:
        return True
    orig = getattr(exc, "orig", None)
    root = getattr(orig, "__cause__", None) or orig
    return _is_network(root) or isinstance(
        root,
        asyncpg.PostgresConnectionError
        | asyncpg.CannotConnectNowError
        | asyncpg.TooManyConnectionsError,
    )


class Db:
    """Runs one short transaction per call under a timeout."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], call_timeout_s: float) -> None:
        self._sessions = sessions
        self._call_timeout_s = call_timeout_s

    async def run(self, fn: Callable[[AsyncSession], Awaitable[T]]) -> T:
        """Run fn in a transaction under asyncio.timeout.

        DBAPIError, OSError and timeouts become DatabaseUnavailable; other errors propagate.
        """
        try:
            async with asyncio.timeout(self._call_timeout_s):
                async with self._sessions() as session, session.begin():
                    return await fn(session)
        except (
            sqlalchemy.exc.DBAPIError,
            sqlalchemy.exc.TimeoutError,
            OSError,
            TimeoutError,
        ) as exc:
            raise DatabaseUnavailable from exc
