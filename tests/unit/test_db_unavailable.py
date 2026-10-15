"""Classification of database failures into 503 versus 500 (design §7.1)."""

import errno
import socket
from collections.abc import Callable

import asyncpg  # type: ignore[import-untyped]
import pytest
import sqlalchemy.exc

from payments.db.session import is_db_unavailable


def dbapi_error(cause: BaseException | None = None, *, invalidated: bool = False) -> BaseException:
    """A DBAPIError wrapping a driver exception whose __cause__ is `cause`."""
    orig = RuntimeError("driver error")
    orig.__cause__ = cause
    return sqlalchemy.exc.DBAPIError("SELECT 1", None, orig, connection_invalidated=invalidated)


CASES: list[tuple[str, Callable[[], BaseException], bool]] = [
    ("connection-refused", lambda: ConnectionRefusedError(), True),
    ("dns-failure", lambda: socket.gaierror(socket.EAI_NONAME, "unknown host"), True),
    ("builtin-timeout", lambda: TimeoutError(), True),
    ("host-unreachable", lambda: OSError(errno.EHOSTUNREACH, "no route to host"), True),
    ("network-unreachable", lambda: OSError(errno.ENETUNREACH, "network unreachable"), True),
    ("pool-timeout", lambda: sqlalchemy.exc.TimeoutError("pool exhausted"), True),
    ("connection-invalidated", lambda: dbapi_error(invalidated=True), True),
    ("wrapped-refused", lambda: dbapi_error(ConnectionRefusedError()), True),
    ("wrapped-dns", lambda: dbapi_error(socket.gaierror(socket.EAI_NONAME, "x")), True),
    ("too-many-connections", lambda: dbapi_error(asyncpg.TooManyConnectionsError("full")), True),
    ("cannot-connect-now", lambda: dbapi_error(asyncpg.CannotConnectNowError("starting")), True),
    (
        "connection-does-not-exist",
        lambda: dbapi_error(asyncpg.ConnectionDoesNotExistError("gone")),
        True,
    ),
    ("file-not-found", lambda: FileNotFoundError("no such file"), False),
    (
        "client-configuration",
        lambda: asyncpg.exceptions.ClientConfigurationError("bad option"),
        False,
    ),
    ("data-error", lambda: asyncpg.exceptions.DataError("bad value"), False),
    ("wrapped-data-error", lambda: dbapi_error(asyncpg.exceptions.DataError("bad")), False),
    ("query-canceled", lambda: dbapi_error(asyncpg.QueryCanceledError("timeout")), False),
    (
        "integrity-error",
        lambda: sqlalchemy.exc.IntegrityError("INSERT", None, RuntimeError("duplicate")),
        False,
    ),
    ("plain-value-error", lambda: ValueError("nope"), False),
]


@pytest.mark.parametrize(
    ("make_exc", "expected"),
    [pytest.param(make, expected, id=name) for name, make, expected in CASES],
)
def test_is_db_unavailable_chains(make_exc: Callable[[], BaseException], expected: bool) -> None:
    assert is_db_unavailable(make_exc()) is expected
