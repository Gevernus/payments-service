"""Command line entry point: `python -m payments.cli replay dead|stuck [...]`."""

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
from payments.services.replay import replay_dead, replay_stuck_pending

_DEFAULT_LIMIT = 100
_DURATION = re.compile(r"(\d+(?:\.\d+)?)([smh]?)")
_UNITS = {"": 1, "s": 1, "m": 60, "h": 3600}


def _out(line: str) -> None:
    sys.stdout.write(line + "\n")


def _err(line: str) -> None:
    sys.stderr.write(line + "\n")


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
        number = 0
    if number < 1:
        raise argparse.ArgumentTypeError(f"--limit must be a positive integer, got {value!r}")
    return number


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(prog="python -m payments.cli")
    commands = parser.add_subparsers(dest="command", required=True)
    replay = commands.add_parser("replay", help="reopen dead-lettered or stuck payments")
    kinds = replay.add_subparsers(dest="kind", required=True)
    dead = kinds.add_parser("dead", help="reopen dead-lettered payments with a new budget")
    dead.add_argument("--reason", type=DeadReason, choices=list(DeadReason))
    dead.add_argument("--payment-id", type=UUID)
    stuck = kinds.add_parser("stuck", help="enqueue pending payments without activity")
    stuck.add_argument(
        "--older-than",
        type=parse_duration,
        required=True,
        metavar="DURATION",
        help="minimum idle time: 30s, 15m, 2h or 0",
    )
    for sub in (dead, stuck):
        sub.add_argument("--limit", type=_positive_int, default=_DEFAULT_LIMIT)
        sub.add_argument("--dry-run", action="store_true", help="only list the affected payments")
    return parser


async def _replay(args: argparse.Namespace) -> list[UUID]:
    settings = get_settings()
    configure_logging(settings)
    engine = create_engine(settings, role="cli")
    try:
        db = Db(create_session_factory(engine), settings.db_call_timeout_s)
        if args.kind == "stuck":
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
    args = build_parser().parse_args(argv)
    try:
        ids = asyncio.run(_replay(args))
    except DatabaseUnavailable:
        _err("error: database is unavailable; re-running is safe")
        return 1
    except ValidationError as exc:
        _err(f"error: invalid configuration: {exc.error_count()} problem(s)")
        return 1
    for payment_id in ids:
        _out(str(payment_id))
    verb = "would replay" if args.dry_run else "replayed"
    _out(f"{verb} {len(ids)} payment(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
