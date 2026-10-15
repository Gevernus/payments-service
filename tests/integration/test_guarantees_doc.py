"""The delivery guarantees document only names tests that exist."""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GUARANTEES = REPO / "docs" / "guarantees.md"
_DEFINITION = re.compile(r"^\s*(?:async )?def (test_[a-z0-9_]+)", re.MULTILINE)


def test_guarantees_doc_references_existing_tests() -> None:
    named = set(re.findall(r"test_[a-z0-9_]+", GUARANTEES.read_text()))
    defined = {
        match.group(1)
        for path in (REPO / "tests").rglob("test_*.py")
        for match in _DEFINITION.finditer(path.read_text())
    }

    missing = sorted(named - defined)

    assert named, "the document names no tests"
    assert not missing, f"docs/guarantees.md names tests that do not exist: {missing}"
