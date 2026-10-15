"""RFC 9457 problem+json responses."""

from collections.abc import Mapping, Sequence
from typing import Any

from fastapi.responses import JSONResponse

PROBLEM_JSON = "application/problem+json"


def problem_response(
    status: int,
    *,
    code: str,
    title: str,
    detail: str | None = None,
    errors: Sequence[Mapping[str, Any]] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """A problem+json response: type, title, status, detail, code and optional errors."""
    body: dict[str, Any] = {
        "type": f"urn:payments:problem:{code}",
        "title": title,
        "status": status,
        "detail": detail or title,
        "code": code,
    }
    if errors is not None:
        body["errors"] = list(errors)
    return JSONResponse(
        body,
        status_code=status,
        media_type=PROBLEM_JSON,
        headers=dict(headers) if headers else None,
    )
