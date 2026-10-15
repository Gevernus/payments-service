"""Access to the compose stack: base URLs, HTTP clients and docker compose commands."""

import asyncio
import itertools
import json
import os
import secrets
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx

from payments.schemas import PaymentStatus
from tests.support.fakes import idempotency_key_with_outcome

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILES = ("compose.yaml", "compose.e2e.yaml")

API_URL = os.environ.get("E2E_API_URL", "http://127.0.0.1:8000")
RECEIVER_URL = os.environ.get("E2E_RECEIVER_URL", "http://127.0.0.1:9000")
RABBITMQ_API_URL = os.environ.get("E2E_RABBITMQ_API_URL", "http://127.0.0.1:15672")
API_KEY = os.environ.get("E2E_API_KEY", "dev-only-api-key-not-for-production-use-0000")
RABBITMQ_USER = os.environ.get("E2E_RABBITMQ_USER", "payments")
RABBITMQ_PASSWORD = os.environ.get("E2E_RABBITMQ_PASSWORD", "payments")
# How the consumer container reaches the receiver.
RECEIVER_INTERNAL_URL = os.environ.get("E2E_RECEIVER_INTERNAL_URL", "http://webhook-receiver:9000")

DLQ = "payments.new.dlq"


@dataclass(frozen=True)
class Stack:
    """HTTP clients for the services of the running stack."""

    api: httpx.AsyncClient
    receiver: httpx.AsyncClient
    rabbitmq: httpx.AsyncClient

    async def create_payment(
        self, *, hook: str, outcome: PaymentStatus = PaymentStatus.SUCCEEDED
    ) -> UUID:
        """POST a payment whose webhook goes to `hook` on the receiver; return its id.

        The emulated gateway derives the outcome from the Idempotency-Key, so the key is picked
        to give `outcome`.
        """
        key = await unique_key(outcome)
        body: dict[str, Any] = {
            "amount": "100.00",
            "currency": "RUB",
            "description": "e2e",
            "metadata": {"suite": "e2e"},
            "webhook_url": f"{RECEIVER_INTERNAL_URL}{hook}",
        }
        response = await self.api.post(
            "/api/v1/payments", json=body, headers={"Idempotency-Key": key}
        )
        assert response.status_code == 202, response.text
        return UUID(response.json()["payment_id"])

    async def get_payment(self, payment_id: UUID) -> dict[str, Any]:
        """GET the payment and return its JSON."""
        response = await self.api.get(f"/api/v1/payments/{payment_id}")
        assert response.status_code == 200, response.text
        payload: dict[str, Any] = response.json()
        return payload

    async def delivery_state(self, payment_id: UUID) -> str:
        """The payment's delivery state."""
        return str((await self.get_payment(payment_id))["delivery"]["state"])

    async def received(self, payment_id: UUID) -> list[dict[str, Any]]:
        """Webhook POSTs that reached the receiver for the payment."""
        response = await self.receiver.get("/received", params={"payment_id": str(payment_id)})
        response.raise_for_status()
        records: list[dict[str, Any]] = response.json()
        return records

    async def dlq_messages(self) -> list[str]:
        """Payloads currently in the DLQ (read and put back)."""
        response = await self.rabbitmq.post(
            f"/api/queues/%2F/{DLQ}/get",
            json={"count": 1000, "ackmode": "ack_requeue_true", "encoding": "auto"},
        )
        response.raise_for_status()
        return [str(message["payload"]) for message in response.json()]

    async def in_dlq(self, payment_id: UUID) -> bool:
        """Whether a message that mentions the payment is in the DLQ."""
        return any(str(payment_id) in payload for payload in await self.dlq_messages())


# A per-process salt keeps the keys of a rerun on the same database apart from earlier runs.
RUN = secrets.token_hex(4)
_counter = itertools.count()


async def unique_key(outcome: PaymentStatus) -> str:
    """A new Idempotency-Key that the emulated gateway answers with `outcome`.

    The salt separates processes and the counter separates calls within one process.
    """
    return await idempotency_key_with_outcome(outcome, nth=next(_counter), prefix=f"e2e-{RUN}-")


async def compose(*args: str, timeout: float = 120.0) -> subprocess.CompletedProcess[str]:
    """Run a compose command from the repository root; fail with its output on a non-zero exit."""
    command = ["docker", "compose"]
    for name in COMPOSE_FILES:
        command += ["-f", name]
    command += args

    def run() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            command,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )

    result = await asyncio.to_thread(run)
    assert result.returncode == 0, (
        f"{' '.join(command)} exited with {result.returncode}\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )
    return result


async def outbox_count(payment_ids: Sequence[UUID]) -> int:
    """Number of outbox rows of the given payments, read with psql inside the postgres service."""
    ids = ",".join(f"'{payment_id}'" for payment_id in payment_ids)
    result = await compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "payments",
        "-d",
        "payments",
        "-tAc",
        f"SELECT count(*) FROM outbox WHERE payment_id IN ({ids})",  # noqa: S608 - UUIDs
    )
    return int(result.stdout.strip())


def dump(value: Any) -> str:
    """Pretty JSON for assertion messages."""
    return json.dumps(value, indent=2, default=str)
