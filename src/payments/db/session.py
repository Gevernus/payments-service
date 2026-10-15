"""Engine, session factory and the single database-call wrapper."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Concatenate, ParamSpec, TypeVar

import sqlalchemy.exc
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from payments.config import Settings
from payments.errors import DatabaseUnavailable

P = ParamSpec("P")
T = TypeVar("T")


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


class Db:
    """Runs one short transaction per call under a timeout."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], call_timeout_s: float) -> None:
        self._sessions = sessions
        self._call_timeout_s = call_timeout_s

    async def run(
        self,
        fn: Callable[Concatenate[AsyncSession, P], Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        """Run fn(session, *args, **kwargs) in a transaction under asyncio.timeout.

        DBAPIError, OSError and timeouts become DatabaseUnavailable; other errors propagate.
        """
        try:
            async with asyncio.timeout(self._call_timeout_s):
                async with self._sessions() as session, session.begin():
                    return await fn(session, *args, **kwargs)
        except (
            sqlalchemy.exc.DBAPIError,
            sqlalchemy.exc.TimeoutError,
            OSError,
            TimeoutError,
        ) as exc:
            raise DatabaseUnavailable from exc
