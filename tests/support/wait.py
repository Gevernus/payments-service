"""Polling helper used instead of fixed sleeps."""

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable


async def wait_until(
    predicate: Callable[[], bool | Awaitable[bool]],
    *,
    timeout: float = 10.0,
    interval: float = 0.05,
) -> None:
    """Poll predicate until it is truthy; raise TimeoutError after timeout seconds."""
    deadline = time.monotonic() + timeout
    while True:
        result = predicate()
        if inspect.isawaitable(result):
            result = await result
        if result:
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"condition not met within {timeout}s")
        await asyncio.sleep(interval)
