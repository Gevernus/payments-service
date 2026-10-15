"""Structured logging setup."""

from payments.config import Settings


def configure_logging(settings: Settings) -> None:
    """Configure structlog and the standard library logger."""
    raise NotImplementedError
