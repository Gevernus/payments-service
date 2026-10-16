"""Db.run turns every database failure into DatabaseUnavailable."""

from contextlib import asynccontextmanager
from typing import Any

import asyncpg
import pytest

from payments.db.session import Db
from payments.errors import DatabaseUnavailable


class _Session:
    @asynccontextmanager
    async def begin(self) -> Any:
        yield self


class _Sessions:
    @asynccontextmanager
    async def __call__(self) -> Any:
        yield _Session()


@pytest.mark.parametrize(
    "error",
    [
        asyncpg.exceptions.CannotConnectNowError("starting up"),
        asyncpg.exceptions.AdminShutdownError("administrator command"),
        asyncpg.exceptions.ConnectionDoesNotExistError("connection was closed"),
        asyncpg.exceptions.InterfaceError("another operation is in progress"),
        ConnectionRefusedError(),
    ],
)
async def test_driver_errors_raised_unwrapped_become_database_unavailable(error: Exception) -> None:
    async def fail(_session: Any) -> None:
        raise error

    with pytest.raises(DatabaseUnavailable) as caught:
        await Db(_Sessions(), call_timeout_s=1).run(fail)  # type: ignore[arg-type]
    assert caught.value.__cause__ is error


async def test_programming_errors_propagate() -> None:
    async def fail(_session: Any) -> None:
        raise ValueError("a bug")

    with pytest.raises(ValueError, match="a bug"):
        await Db(_Sessions(), call_timeout_s=1).run(fail)  # type: ignore[arg-type]
