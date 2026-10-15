"""The delivery guarantees document only names tests that exist."""

import re
from pathlib import Path

import pytest

from tests.conftest import COLLECTED_NAMES

GUARANTEES = Path(__file__).resolve().parents[2] / "docs" / "guarantees.md"


def test_guarantees_doc_references_existing_tests(request: pytest.FixtureRequest) -> None:
    named = set(re.findall(r"test_[a-z0-9_]+", GUARANTEES.read_text()))
    collected = request.config.stash[COLLECTED_NAMES]  # all collected, even if deselected

    missing = sorted(named - collected)

    assert named, "the document names no tests"
    assert not missing, f"docs/guarantees.md names tests that do not exist: {missing}"
