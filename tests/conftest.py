"""Shared pytest configuration: markers by path and the support fixtures."""

from pathlib import Path

import pytest

from tests.support.infra import (  # noqa: F401
    broker_cleanup,
    database_url,
    db_engine,
    db_sessions,
    management_url,
    migrated_db,
    migrated_schema,
    postgres_container,
    rabbitmq_container,
    rabbitmq_url,
    rabbitmqctl,
)
from tests.support.receiver import receiver  # noqa: F401

_TESTS_DIR = Path(__file__).resolve().parent
_MARKED_DIRS = ("unit", "integration", "e2e")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Mark tests by their directory."""
    for item in items:
        relative = Path(str(item.path)).relative_to(_TESTS_DIR)
        area = relative.parts[0] if len(relative.parts) > 1 else None
        if area in _MARKED_DIRS:
            item.add_marker(getattr(pytest.mark, area))
