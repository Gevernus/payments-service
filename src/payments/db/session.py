"""Engine, session factory and the single database-call wrapper."""

from collections.abc import Awaitable, Callable
from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from payments.config import Settings

T = TypeVar("T")


def create_engine(settings: Settings, *, role: str) -> AsyncEngine:
    """Create an engine for the given process role (used as application_name)."""
    raise NotImplementedError


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create a session factory with expire_on_commit disabled."""
    raise NotImplementedError


def is_db_unavailable(exc: BaseException) -> bool:
    """Tell whether an exception means the database is unreachable (design §7.1)."""
    raise NotImplementedError


class Db:
    """Runs one short transaction per call under a timeout."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession], call_timeout_s: float) -> None:
        self._sessions = sessions
        self._call_timeout_s = call_timeout_s

    async def run(self, fn: Callable[[AsyncSession], Awaitable[T]]) -> T:
        """Run fn in a transaction under asyncio.timeout.

        DBAPIError, OSError and timeouts become DatabaseUnavailable; other errors propagate.
        """
        raise NotImplementedError
