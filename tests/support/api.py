"""Helpers for tests that drive the API in-process through ASGI."""

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any

import httpx
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.api.app import create_app
from payments.config import Settings
from tests.support.fakes import TEST_API_KEY

AUTH = {"X-API-Key": TEST_API_KEY}

_COUNT_SQL = {
    "payments": text("SELECT count(*) FROM payments"),
    "outbox": text("SELECT count(*) FROM outbox"),
}


def payment_body(**overrides: Any) -> dict[str, Any]:
    """A valid POST /api/v1/payments body."""
    body: dict[str, Any] = {
        "amount": "100.00",
        "currency": "RUB",
        "description": "test payment",
        "metadata": {"order_id": 42},
        "webhook_url": "https://example.com/hook",
    }
    body.update(overrides)
    return body


@asynccontextmanager
async def api_client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """An HTTP client bound to a freshly created app with its lifespan running.

    Server errors are returned as 500 responses instead of being raised into the test.
    """
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


async def create_payment(
    client: httpx.AsyncClient,
    body: Mapping[str, Any] | None = None,
    *,
    key: str | None = "idem-key-1",
    headers: Sequence[tuple[str, str]] | Mapping[str, str] | None = None,
) -> httpx.Response:
    """POST a payment with the API key and, unless `key` is None, an Idempotency-Key."""
    merged = dict(AUTH if headers is None else headers)
    if key is not None:
        merged["Idempotency-Key"] = key
    return await client.post(
        "/api/v1/payments", json=payment_body() if body is None else dict(body), headers=merged
    )


async def count_rows(sessions: async_sessionmaker[AsyncSession], table: str) -> int:
    """Number of rows in `payments` or `outbox`."""
    async with sessions() as session:
        return int((await session.execute(_COUNT_SQL[table])).scalar_one())
