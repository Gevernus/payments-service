"""FastAPI application factory."""

import re
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast
from uuid import uuid4

import structlog
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from payments.api.problem import problem_response
from payments.api.routes import health_router, payments_router
from payments.config import Settings, get_settings
from payments.db.session import Db, create_engine, create_session_factory, is_db_unavailable
from payments.errors import DatabaseUnavailable, DomainError, WebhookUrlNotAllowed
from payments.logging import configure_logging
from payments.services.payments import PaymentService

log = structlog.get_logger(__name__)

REQUEST_ID_HEADER = "x-request-id"
_REQUEST_ID = re.compile(r"[\x21-\x7E]{1,128}")


class ApiKeyMiddleware:
    """Pure ASGI middleware that checks the API key before the body is read."""

    def __init__(self, app: ASGIApp, *, key: bytes, prefix: str = "/api/") -> None:
        self._app = app
        self._key = key
        self._prefix = prefix

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Reject requests under the prefix that lack a valid key."""
        if scope["type"] == "http" and scope["path"].startswith(self._prefix):
            supplied = next((v for k, v in scope["headers"] if k == b"x-api-key"), b"")
            if not secrets.compare_digest(supplied, self._key):
                response = problem_response(
                    401,
                    code="unauthorized",
                    title="Missing or invalid API key",
                    headers={"WWW-Authenticate": "APIKey"},
                )
                await response(scope, receive, send)
                return
        await self._app(scope, receive, send)


class RequestIdMiddleware:
    """Propagates a valid X-Request-Id or generates one; binds it to the log context."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Attach the request id to the scope, the log context and the response."""
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        supplied = next(
            (v.decode("latin-1") for k, v in scope["headers"] if k == REQUEST_ID_HEADER.encode()),
            "",
        )
        request_id = supplied if _REQUEST_ID.fullmatch(supplied) else uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id

        async def send_with_id(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = {
                    **message,
                    "headers": [*message["headers"], (b"x-request-id", request_id.encode())],
                }
            await send(message)

        with structlog.contextvars.bound_contextvars(request_id=request_id):
            await self._app(scope, receive, send_with_id)


async def _domain_error(_request: Request, error: Exception) -> JSONResponse:
    exc = cast(DomainError, error)
    detail = exc.reason if isinstance(exc, WebhookUrlNotAllowed) else None
    return problem_response(exc.status_code, code=exc.code, title=exc.title, detail=detail)


def _validation_errors(exc: RequestValidationError) -> list[dict[str, Any]]:
    return [
        {"type": item["type"], "loc": list(item["loc"]), "msg": item["msg"]}
        for item in exc.errors()
    ]


async def _validation_error(_request: Request, error: Exception) -> JSONResponse:
    exc = cast(RequestValidationError, error)
    return problem_response(
        422,
        code="validation_error",
        title="Request validation failed",
        errors=_validation_errors(exc),
    )


async def _http_error(_request: Request, error: Exception) -> JSONResponse:
    exc = cast(StarletteHTTPException, error)
    # FastAPI turns an exception raised while parsing the body into a 400; a ValueError or
    # RecursionError there means the client sent a body that cannot be decoded.
    if exc.status_code == 400 and isinstance(exc.__cause__, ValueError | RecursionError):
        return problem_response(
            422,
            code="validation_error",
            title="Request validation failed",
            errors=[{"type": "json_invalid", "loc": ["body"], "msg": "The body is not valid JSON"}],
        )
    return problem_response(
        exc.status_code,
        code=f"http_{exc.status_code}",
        title=str(exc.detail),
        headers=exc.headers,
    )


async def _database_unavailable(_request: Request, error: Exception) -> JSONResponse:
    exc = cast(DatabaseUnavailable, error)
    cause = exc.__cause__
    if cause is not None and is_db_unavailable(cause):
        return problem_response(
            503,
            code="database_unavailable",
            title="Database is unavailable",
            headers={"Retry-After": "1"},
        )
    log.error("db.error", exc_info=exc)
    return problem_response(500, code="internal_error", title="Internal server error")


async def _unexpected(_request: Request, exc: Exception) -> JSONResponse:
    log.error("request.failed", exc_info=exc)
    return problem_response(500, code="internal_error", title="Internal server error")


def create_app(settings: Settings | None = None) -> FastAPI:
    """Build the API application."""
    settings = settings or get_settings()
    configure_logging(settings)
    api_key = settings.require_api_key()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings, role="api")
        db = Db(create_session_factory(engine), settings.db_call_timeout_s)
        app.state.db = db
        app.state.payments = PaymentService(db, settings)
        log.info("settings.loaded", **settings.loggable())
        try:
            yield
        finally:
            await engine.dispose()

    app = FastAPI(title="Payments service", version="1.0.0", lifespan=lifespan)
    app.include_router(payments_router)
    app.include_router(health_router)
    app.add_exception_handler(DomainError, _domain_error)
    app.add_exception_handler(RequestValidationError, _validation_error)
    app.add_exception_handler(StarletteHTTPException, _http_error)
    app.add_exception_handler(DatabaseUnavailable, _database_unavailable)
    app.add_exception_handler(Exception, _unexpected)
    # The last middleware added is the outermost: the request id wraps even the 401 response.
    app.add_middleware(ApiKeyMiddleware, key=api_key)
    app.add_middleware(RequestIdMiddleware)
    return app
