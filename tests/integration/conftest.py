"""Fixtures for tests that run the API against the migrated database."""

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from payments.config import Settings
from tests.support.api import api_client
from tests.support.consumer_harness import env, rabbit  # noqa: F401
from tests.support.fakes import make_settings


@pytest.fixture
def settings(migrated_db: str) -> Settings:
    """API settings for the migrated test database (a timeout chain valid under design §7.5)."""
    return make_settings(database_url=migrated_db, stale_after_s=30)


@pytest_asyncio.fixture
async def client(settings: Settings) -> AsyncIterator[httpx.AsyncClient]:
    """An HTTP client on the running API app."""
    async with api_client(settings) as running:
        yield running
