"""HTTP API behaviour against a migrated database (design §4)."""

import uuid
from typing import Any

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.config import Settings
from payments.db import queries
from tests.support.api import AUTH, api_client, count_rows, create_payment, payment_body
from tests.support.fakes import TEST_API_KEY
from tests.support.tcp_proxy import TcpProxy

JSON = {"Content-Type": "application/json"}


def error_kinds(problem: dict[str, Any]) -> set[Any]:
    """The `code` of a problem document plus the type of each listed validation error."""
    return {problem.get("code"), *(item.get("type") for item in problem.get("errors", []))}


async def test_create_payment_returns_202(
    client: httpx.AsyncClient, db_sessions: async_sessionmaker[AsyncSession]
) -> None:
    response = await create_payment(client, key="create-1")

    assert response.status_code == 202
    body = response.json()
    assert set(body) == {"payment_id", "status", "created_at"}
    payment_id = uuid.UUID(body["payment_id"])
    assert body["status"] == "pending"
    assert response.headers["location"].endswith(f"/api/v1/payments/{payment_id}")
    assert response.headers["idempotent-replayed"] == "false"

    async with db_sessions() as session:
        payment = (
            await session.execute(
                text("SELECT status, amount, idempotency_key FROM payments WHERE id = :id"),
                {"id": payment_id},
            )
        ).one()
        outbox = (
            await session.execute(
                text("SELECT payment_id, event_type, routing_key, payload FROM outbox")
            )
        ).all()
    assert (payment.status, str(payment.amount), payment.idempotency_key) == (
        "pending",
        "100.00",
        "create-1",
    )
    assert len(outbox) == 1
    assert outbox[0].payment_id == payment_id
    assert outbox[0].event_type == "payment.created"
    assert outbox[0].routing_key == "payments.new"
    assert outbox[0].payload["payment_id"] == str(payment_id)


async def test_get_payment_returns_details(client: httpx.AsyncClient) -> None:
    created = await create_payment(client, payment_body(amount=250.5), key="get-1")
    payment_id = created.json()["payment_id"]

    response = await client.get(f"/api/v1/payments/{payment_id}", headers=AUTH)

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == "250.50"
    assert body["currency"] == "RUB"
    assert body["description"] == "test payment"
    assert body["metadata"] == {"order_id": 42}
    assert body["status"] == "pending"
    assert body["idempotency_key"] == "get-1"
    assert body["webhook_url"] == "https://example.com/hook"
    assert body["created_at"] == created.json()["created_at"]
    assert body["processed_at"] is None
    assert body["delivery"] == {
        "state": "pending",
        "attempts": 0,
        "last_attempt_at": None,
        "delivered_at": None,
        "dead_lettered_at": None,
        "dead_letter_reason": None,
        "last_error": None,
    }


async def test_get_unknown_payment_404(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=AUTH)

    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["code"] == "payment_not_found"


async def test_get_invalid_uuid_422(client: httpx.AsyncClient) -> None:
    response = await client.get("/api/v1/payments/not-a-uuid", headers=AUTH)

    assert response.status_code == 422
    assert response.headers["content-type"].startswith("application/problem+json")
    assert response.json()["errors"]


async def test_error_precedence(client: httpx.AsyncClient) -> None:
    malformed = b"{not json"
    deep = b"[" * 100_000

    # 401 comes before anything that needs the body.
    no_key = await client.post("/api/v1/payments", content=malformed, headers=JSON)
    assert no_key.status_code == 401

    # With a key, a malformed body is a 422 json_invalid, even without an Idempotency-Key.
    bad_json = await client.post("/api/v1/payments", content=malformed, headers={**AUTH, **JSON})
    assert bad_json.status_code == 422
    assert "json_invalid" in error_kinds(bad_json.json())

    too_deep = await client.post("/api/v1/payments", content=deep, headers={**AUTH, **JSON})
    assert too_deep.status_code == 422

    # A valid body without the Idempotency-Key is a 400 ...
    no_idempotency = await create_payment(client, key=None)
    assert no_idempotency.status_code == 400
    assert no_idempotency.json()["code"] == "idempotency_key_missing"

    # ... but an invalid body wins over the missing header.
    invalid = await create_payment(client, payment_body(amount=-1), key=None)
    assert invalid.status_code == 422
    assert invalid.json()["errors"]


async def test_api_auth_missing_key_401(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments", json=payment_body(), headers={"Idempotency-Key": "k"}
    )

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "APIKey"
    assert response.headers["content-type"].startswith("application/problem+json")


async def test_api_auth_wrong_key_401(client: httpx.AsyncClient) -> None:
    wrong = TEST_API_KEY[:-1] + ("0" if TEST_API_KEY[-1] != "0" else "1")
    for candidate in ("wrong", wrong, TEST_API_KEY + "x"):
        response = await client.post(
            "/api/v1/payments",
            json=payment_body(),
            headers={"X-API-Key": candidate, "Idempotency-Key": "k"},
        )
        assert response.status_code == 401, candidate
        assert response.headers["www-authenticate"] == "APIKey"


async def test_api_auth_non_ascii_key_401(client: httpx.AsyncClient) -> None:
    headers = [
        (b"x-api-key", "ключ-доступа".encode()),
        (b"idempotency-key", b"k"),
    ]
    response = await client.post("/api/v1/payments", json=payment_body(), headers=headers)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "APIKey"


async def test_get_requires_key(client: httpx.AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "APIKey"


async def test_health_and_docs_without_key(client: httpx.AsyncClient) -> None:
    assert (await client.get("/health/live")).status_code == 200
    assert (await client.get("/health/ready")).status_code == 200
    assert (await client.get("/docs")).status_code == 200
    schema = await client.get("/openapi.json")
    assert schema.status_code == 200
    assert "/api/v1/payments" in schema.json()["paths"]


@pytest.mark.parametrize(
    "key",
    [
        pytest.param("", id="empty"),
        pytest.param("a" * 256, id="too-long"),
        pytest.param("has space", id="space"),
        pytest.param("tab\there", id="tab"),
        pytest.param("ctrl\x01char", id="control-character"),
        pytest.param("del\x7fchar", id="del"),
    ],
)
async def test_idempotency_key_header_validation(
    client: httpx.AsyncClient, db_sessions: async_sessionmaker[AsyncSession], key: str
) -> None:
    response = await create_payment(client, key=key)

    assert response.status_code == 400
    assert response.json()["code"] in {"idempotency_key_invalid", "idempotency_key_missing"}
    assert await count_rows(db_sessions, "payments") == 0


async def test_idempotency_key_header_length_boundaries(client: httpx.AsyncClient) -> None:
    assert (await create_payment(client, key="a" * 255)).status_code == 202
    assert (await create_payment(client, key="b")).status_code == 202
    assert (await create_payment(client, key="!~Z0:/.-_")).status_code == 202


async def test_webhook_url_policy_api(
    client: httpx.AsyncClient, db_sessions: async_sessionmaker[AsyncSession]
) -> None:
    internal = await create_payment(
        client, payment_body(webhook_url="http://rabbitmq:15672/"), key="policy-1"
    )
    assert internal.status_code == 422
    assert internal.json()["code"] == "webhook_url_not_allowed"

    # The documentation range is not globally routable.
    documentation = await create_payment(
        client, payment_body(webhook_url="http://203.0.113.10/"), key="policy-2"
    )
    assert documentation.status_code == 422
    assert documentation.json()["code"] == "webhook_url_not_allowed"
    assert await count_rows(db_sessions, "payments") == 0

    allowed = await create_payment(
        client, payment_body(webhook_url="https://example.com/hook"), key="policy-3"
    )
    assert allowed.status_code == 202


async def test_create_returns_503_when_db_down(settings: Settings) -> None:
    target = make_url(settings.database_url)
    assert target.host is not None
    assert target.port is not None
    proxy = TcpProxy()
    port = await proxy.start(target.host, target.port)
    proxied = settings.model_copy(
        update={"database_url": target.set(host="127.0.0.1", port=port).render_as_string(False)}
    )
    try:
        async with api_client(proxied) as client:
            assert (await create_payment(client, key="up-1")).status_code == 202

            proxy.set_mode("drop")  # sever the pooled connections ...
            proxy.set_mode("refuse")  # ... and refuse new ones

            response = await create_payment(client, key="down-1")
            assert response.status_code == 503
            assert response.headers["content-type"].startswith("application/problem+json")
            assert response.json()["code"] == "database_unavailable"

            lookup = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers=AUTH)
            assert lookup.status_code == 503
    finally:
        proxy.close()


async def test_create_is_atomic(
    client: httpx.AsyncClient,
    db_sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failing_add_outbox_event(*args: object, **kwargs: object) -> None:
        raise RuntimeError("outbox write failed")

    monkeypatch.setattr(queries, "add_outbox_event", failing_add_outbox_event)

    response = await create_payment(client, key="atomic-1")

    assert response.status_code == 500
    assert await count_rows(db_sessions, "payments") == 0
    assert await count_rows(db_sessions, "outbox") == 0

    # The failed attempt left nothing behind: the same key works once the fault is gone.
    monkeypatch.undo()
    retry = await create_payment(client, key="atomic-1")
    assert retry.status_code == 202
    assert retry.headers["idempotent-replayed"] == "false"
    assert await count_rows(db_sessions, "payments") == 1
    assert await count_rows(db_sessions, "outbox") == 1
