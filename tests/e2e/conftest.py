"""Fixtures for the end-to-end suite: skip cleanly when the compose stack is not reachable."""

import os
from collections.abc import AsyncIterator
from typing import NoReturn

import httpx
import pytest
import pytest_asyncio

from tests.e2e.stack import (
    API_KEY,
    API_URL,
    RABBITMQ_API_URL,
    RABBITMQ_PASSWORD,
    RABBITMQ_USER,
    RECEIVER_URL,
    Stack,
)


def _unavailable(reason: str) -> NoReturn:
    if os.environ.get("REQUIRE_INFRA") == "1":
        pytest.fail(f"compose stack required but unavailable: {reason}", pytrace=False)
    pytest.skip(f"compose stack unavailable: {reason}")


@pytest.fixture(scope="session")
def stack_ready() -> None:
    """Check that the API, the receiver and the broker's management API answer."""
    probes = (
        (f"{API_URL}/health/ready", None),
        (f"{RECEIVER_URL}/health", None),
        (f"{RABBITMQ_API_URL}/api/overview", (RABBITMQ_USER, RABBITMQ_PASSWORD)),
    )
    for url, auth in probes:
        try:
            httpx.get(url, auth=auth, timeout=3, trust_env=False).raise_for_status()
        except httpx.HTTPError as exc:
            _unavailable(f"{url}: {exc!r}")


@pytest_asyncio.fixture
async def stack(stack_ready: None) -> AsyncIterator[Stack]:
    """HTTP clients for the running stack."""
    async with (
        httpx.AsyncClient(
            base_url=API_URL, headers={"X-API-Key": API_KEY}, timeout=10, trust_env=False
        ) as api,
        httpx.AsyncClient(base_url=RECEIVER_URL, timeout=10, trust_env=False) as receiver,
        httpx.AsyncClient(
            base_url=RABBITMQ_API_URL,
            auth=(RABBITMQ_USER, RABBITMQ_PASSWORD),
            timeout=10,
            trust_env=False,
        ) as rabbitmq,
    ):
        yield Stack(api=api, receiver=receiver, rabbitmq=rabbitmq)
