"""The delivery guarantees document and the README only reference things that exist."""

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
GUARANTEES = REPO / "docs" / "guarantees.md"
README = REPO / "README.md"
_DEFINITION = re.compile(r"^\s*(?:async )?def (test_[a-z0-9_]+)", re.MULTILINE)
# `path` or `path::Symbol.name` in backticks, for paths under the listed top-level directories.
_REFERENCE = re.compile(
    r"`((?:src|tests|tools|migrations|examples|docs|\.github)/[^`\s:]+)(?:::([A-Za-z_][\w.]*))?`"
)


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


def test_readme_references_resolve() -> None:
    references = _REFERENCE.findall(README.read_text())
    broken = []
    for path, symbol in references:
        target = REPO / path
        if not target.exists():
            broken.append(path)
        elif symbol:
            text = target.read_text()
            if any(re.search(rf"\b{part}\b", text) is None for part in symbol.split(".")):
                broken.append(f"{path}::{symbol}")

    assert any(symbol for _, symbol in references), "the README names no symbols"
    assert not broken, f"README.md references that do not resolve: {broken}"
