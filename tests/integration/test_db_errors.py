"""Database errors must not carry statement parameters (request bodies)."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from payments.config import Settings
from payments.db.session import create_engine


async def test_statement_errors_hide_parameters(settings: Settings) -> None:
    engine = create_engine(settings, role="test")
    try:
        async with engine.connect() as connection:
            with pytest.raises(DBAPIError) as caught:
                await connection.execute(
                    text("SELECT * FROM no_such_table WHERE x = :value"), {"value": "secret-body"}
                )
    finally:
        await engine.dispose()

    assert "secret-body" not in str(caught.value)
    assert "[parameters:" not in str(caught.value)
