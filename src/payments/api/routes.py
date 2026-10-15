"""HTTP routes."""

import asyncio
import re
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request, Response, Security
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from sqlalchemy import text

from payments.api.problem import problem_response
from payments.db.session import Db
from payments.errors import DatabaseUnavailable, IdempotencyKeyInvalid, IdempotencyKeyMissing
from payments.schemas import PaymentAccepted, PaymentCreate, PaymentOut
from payments.services.payments import PaymentService

# Documents the header in OpenAPI only; it is enforced by ApiKeyMiddleware.
_api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

payments_router = APIRouter(
    prefix="/api/v1/payments", tags=["payments"], dependencies=[Security(_api_key_scheme)]
)
health_router = APIRouter(tags=["health"])

_IDEMPOTENCY_KEY = re.compile(r"[\x21-\x7E]{1,255}")
_READY_TIMEOUT_S = 2


def _service(request: Request) -> PaymentService:
    service: PaymentService = request.app.state.payments
    return service


Service = Annotated[PaymentService, Depends(_service)]


def _check_idempotency_key(value: str | None) -> str:
    if value is None:
        raise IdempotencyKeyMissing
    if _IDEMPOTENCY_KEY.fullmatch(value) is None:
        raise IdempotencyKeyInvalid
    return value


@payments_router.post("", status_code=202, response_model=PaymentAccepted)
async def create_payment(
    payload: PaymentCreate,
    response: Response,
    service: Service,
    idempotency_key: Annotated[str | None, Header()] = None,
) -> PaymentAccepted:
    """Accept a payment (POST /api/v1/payments)."""
    key = _check_idempotency_key(idempotency_key)
    payment, replayed = await service.create(payload, idempotency_key=key)
    response.headers["Location"] = f"/api/v1/payments/{payment.id}"
    response.headers["Idempotent-Replayed"] = "true" if replayed else "false"
    return PaymentAccepted(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@payments_router.get("/{payment_id}", response_model=PaymentOut)
async def get_payment(payment_id: UUID, service: Service) -> PaymentOut:
    """Return a payment (GET /api/v1/payments/{payment_id})."""
    return PaymentOut.from_payment(await service.get(payment_id))


@health_router.get("/health/live")
async def live() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok"}


@health_router.get("/health/ready", response_model=None)
async def ready(request: Request) -> Response:
    """Readiness probe: the database answers."""
    db: Db = request.app.state.db

    async def probe(session: Any) -> None:
        await session.execute(text("SELECT 1"))

    try:
        async with asyncio.timeout(_READY_TIMEOUT_S):
            await db.run(probe)
    except (DatabaseUnavailable, TimeoutError):
        return problem_response(503, code="database_unavailable", title="Database is unavailable")
    return JSONResponse({"status": "ok"})
