"""Command line entry point: `python -m payments.cli replay [...]`."""

import argparse
import asyncio
import re
import sys
from collections.abc import Sequence
from uuid import UUID

from pydantic import ValidationError

from payments.config import get_settings
from payments.db.session import Db, create_engine, create_session_factory
from payments.errors import DatabaseUnavailable, DeadReason
from payments.logging import configure_logging
from payments.services.replay import ReplayStopped, replay_dead, replay_stuck_pending

_EXIT_OK = 0
_EXIT_FAILURE = 1
_EXIT_USAGE = 2
_DEFAULT_LIMIT = 100
_DURATION = re.compile(r"(\d+(?:\.\d+)?)([smh]?)")
_UNITS = {"": 1, "s": 1, "m": 60, "h": 3600}


def _out(line: str) -> None:
    sys.stdout.write(line + "\n")


def _err(line: str) -> None:
    sys.stderr.write(line + "\n")


def _root_cause(exc: BaseException) -> str:
    """First line of the innermost cause, or a generic text when there is none."""
    cause: BaseException = exc
    while cause.__cause__ is not None:
        cause = cause.__cause__
    lines = str(cause).strip().splitlines()
    return lines[0] if lines else type(cause).__name__


def parse_duration(value: str) -> float:
    """Parse `30s`, `15m`, `2h` or a bare number of seconds (`0`) into seconds."""
    match = _DURATION.fullmatch(value.strip())
    if match is None:
        raise argparse.ArgumentTypeError(f"invalid duration {value!r}; use e.g. 30s, 15m, 2h or 0")
    return float(match.group(1)) * _UNITS[match.group(2)]


def _positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid integer {value!r}") from None
    if number < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return number


def _uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid payment id {value!r}") from None


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(prog="python -m payments.cli")
    commands = parser.add_subparsers(dest="command", required=True)
    replay = commands.add_parser("replay", help="reopen dead-lettered or stuck payments")
    replay.add_argument(
        "--reason",
        type=DeadReason,
        choices=list(DeadReason),
        help="only payments dead-lettered for this reason",
    )
    replay.add_argument("--payment-id", type=_uuid, help="only this payment")
    replay.add_argument(
        "--limit",
        type=_positive_int,
        default=_DEFAULT_LIMIT,
        help=f"maximum number of payments to replay (default {_DEFAULT_LIMIT})",
    )
    replay.add_argument("--dry-run", action="store_true", help="only list the affected payments")
    replay.add_argument(
        "--stuck-pending",
        action="store_true",
        help="enqueue pending payments without activity instead of reopening dead ones",
    )
    replay.add_argument(
        "--older-than",
        type=parse_duration,
        metavar="DURATION",
        help=(
            "with --stuck-pending: minimum idle time (30s, 15m, 2h, 0). Keep it well above the "
            "retry ladder plus STALE_AFTER_S (minutes, not seconds); otherwise payments still "
            "waiting in a retry queue get a duplicate message (harmless: the budget is fenced)"
        ),
    )
    return parser


async def _replay(args: argparse.Namespace) -> list[UUID]:
    settings = get_settings()
    configure_logging(settings)
    engine = create_engine(settings, role="cli")
    try:
        db = Db(create_session_factory(engine), settings.db_call_timeout_s)
        if args.stuck_pending:
            return await replay_stuck_pending(
                db, older_than_s=args.older_than, limit=args.limit, dry_run=args.dry_run
            )
        return await replay_dead(
            db,
            reason=args.reason,
            payment_id=args.payment_id,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    finally:
        await engine.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return the process exit code."""
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else _EXIT_USAGE
    if args.stuck_pending:
        if args.older_than is None:
            parser.print_usage(sys.stderr)
            _err("error: --stuck-pending requires --older-than")
            return _EXIT_USAGE
        if args.reason is not None or args.payment_id is not None:
            parser.print_usage(sys.stderr)
            _err("error: --reason and --payment-id apply to dead payments only")
            return _EXIT_USAGE
    elif args.older_than is not None:
        parser.print_usage(sys.stderr)
        _err("error: --older-than requires --stuck-pending")
        return _EXIT_USAGE

    try:
        ids = asyncio.run(_replay(args))
    except ReplayStopped as exc:
        for payment_id in exc.done:
            _out(str(payment_id))
        _err(f"error: stopped after {len(exc.done)} payment(s): {_root_cause(exc)}")
        return _EXIT_FAILURE
    except DatabaseUnavailable as exc:
        _err(f"error: database is unavailable: {_root_cause(exc)}")
        return _EXIT_FAILURE
    except ValidationError as exc:
        _err(f"error: invalid configuration: {exc.error_count()} problem(s)")
        return _EXIT_FAILURE
    for payment_id in ids:
        _out(str(payment_id))
    verb = "would replay" if args.dry_run else "replayed"
    _out(f"{verb} {len(ids)} payment(s)")
    return _EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
