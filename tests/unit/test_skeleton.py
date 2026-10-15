"""Smoke test: every module of the package imports."""

import importlib

import pytest

MODULES = [
    "payments",
    "payments.errors",
    "payments.schemas",
    "payments.money",
    "payments.fingerprint",
    "payments.config",
    "payments.logging",
    "payments.db.models",
    "payments.db.session",
    "payments.db.queries",
    "payments.services.gateway",
    "payments.services.payments",
    "payments.services.processing",
    "payments.services.replay",
    "payments.webhooks.signing",
    "payments.webhooks.guard",
    "payments.webhooks.sender",
    "payments.messaging.topology",
    "payments.messaging.envelope",
    "payments.messaging.copies",
    "payments.api.app",
    "payments.api.routes",
    "payments.workers.relay",
    "payments.workers.consumer",
    "payments.cli",
]


@pytest.mark.parametrize("name", MODULES)
def test_module_imports(name: str) -> None:
    assert importlib.import_module(name)
