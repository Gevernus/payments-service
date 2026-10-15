"""FastAPI application factory."""

from fastapi import FastAPI
from starlette.types import ASGIApp, Receive, Scope, Send

from payments.config import Settings


class ApiKeyMiddleware:
    """Pure ASGI middleware that checks the API key before the body is read."""

    def __init__(self, app: ASGIApp, *, key: bytes, prefix: str = "/api/") -> None:
        raise NotImplementedError

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Reject requests under the prefix that lack a valid key."""
        raise NotImplementedError


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the API application."""
    raise NotImplementedError
