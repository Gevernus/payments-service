"""Logging setup: JSON output, context variables and secret redaction (design §10)."""

import json
import logging

import pytest
import structlog

from payments.logging import configure_logging
from tests.support.fakes import make_settings


def test_json_logs_merge_context_and_redact_secrets(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(make_settings(log_format="json"))
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id="r-1")
    try:
        structlog.get_logger("test").info("payment.created", payment_id="p-1", api_key="hunter2")
        logging.getLogger("stdlib").warning("from stdlib")
    finally:
        structlog.contextvars.clear_contextvars()

    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    created, stdlib = lines
    assert created["event"] == "payment.created"
    assert created["request_id"] == "r-1"
    assert created["payment_id"] == "p-1"
    assert created["api_key"] == "***"
    assert created["timestamp"].endswith("Z")
    assert stdlib["event"] == "from stdlib"
    assert stdlib["level"] == "warning"
