"""Command line entry point: `python -m payments.cli replay [...]`."""

from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return the process exit code."""
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())
