"""HTTP routes."""

from fastapi import APIRouter

payments_router = APIRouter(prefix="/api/v1/payments", tags=["payments"])
health_router = APIRouter(tags=["health"])


@payments_router.post("", status_code=202)
async def create_payment() -> None:
    """Accept a payment (POST /api/v1/payments)."""
    raise NotImplementedError


@payments_router.get("/{payment_id}")
async def get_payment() -> None:
    """Return a payment (GET /api/v1/payments/{payment_id})."""
    raise NotImplementedError


@health_router.get("/health/live")
async def live() -> None:
    """Liveness probe."""
    raise NotImplementedError


@health_router.get("/health/ready")
async def ready() -> None:
    """Readiness probe."""
    raise NotImplementedError
