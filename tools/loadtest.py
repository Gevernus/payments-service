"""Load generator and reconciliation for the compose stack (design §12.5).

    python tools/loadtest.py sample --seconds 60 --label k6-100   # beside an external generator
    python tools/loadtest.py idempotency --rate 100 --duration 30
    python tools/loadtest.py e2e --rate 20 --duration 30 --label p10r1
    python tools/loadtest.py outage --rate 5 --duration 90
    python tools/loadtest.py verify --label after-e2e

The load is an open model: request i is due at start + i / rate and is sent at that moment
whatever happened to the earlier requests, so a slow server cannot slow the generator down.
Latency is reported twice: from the actual send, and from the due time (which includes the
generator's own lateness, i.e. it corrects for coordinated omission). Run it where the compose
stack runs, from the repository root; the database is reached with `docker compose exec`.
"""

import argparse
import asyncio
import contextlib
import json
import math
import os
import random
import resource
import sys
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

API_URL = os.environ.get("API_URL", "http://127.0.0.1:8000")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://127.0.0.1:9000")
RABBITMQ_URL = os.environ.get("RABBITMQ_URL", "http://127.0.0.1:15672")
API_KEY = os.environ.get("API_KEY", "dev-only-api-key-not-for-production-use-0000")
HOOK_URL = os.environ.get("HOOK_URL", "http://webhook-receiver:9000/hooks/ok")
OUT_DIR = Path(os.environ.get("LOADTEST_OUT", "loadtest-results"))
MAX_POSTS = 3
EXCHANGES = ("payments", "payments.retry")

Json = dict[str, Any]


async def sh(*cmd: str) -> str:
    """Run a command and return its stdout."""
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    out, err = await proc.communicate()
    if proc.returncode:
        raise RuntimeError(f"{' '.join(cmd)}: {err.decode().strip()}")
    return out.decode()


async def psql(sql: str) -> list[list[str]]:
    """Run a query in the postgres container; rows come back as lists of strings."""
    cmd = ("docker", "compose", "exec", "-T", "postgres", "psql", "-U", "payments", "-d")
    out = await sh(*cmd, "payments", "-AtF", "|", "-c", sql)
    return [line.split("|") for line in out.splitlines() if line]


def pct(values: list[float], q: float) -> float:
    """Nearest-rank percentile; 0 for no data."""
    if not values:
        return 0.0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(q / 100 * len(ordered)) - 1)]


def dist(values: list[float]) -> Json:
    """p50/p95/p99/max of a list, rounded to 0.1."""
    return {k: round(pct(values, q), 1) for k, q in (("p50", 50), ("p95", 95), ("p99", 99))} | {
        "max": round(max(values, default=0.0), 1)
    }


def mib(text: str) -> float:
    """Convert docker's '45.3MiB' (also B, KiB, GiB) to mebibytes."""
    text = text.strip()
    for unit, scale in (("GiB", 1024), ("MiB", 1), ("KiB", 1 / 1024), ("B", 1 / 2**20)):
        if text.endswith(unit):
            return float(text.removesuffix(unit)) * scale
    raise ValueError(f"unknown size {text!r}")


def cpu_seconds() -> float:
    """CPU time of this process (user + system)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


def host_cpu() -> tuple[int, int]:
    """(total, idle) jiffies of the whole host from /proc/stat, or (0, 0) without procfs."""
    try:
        fields = [int(x) for x in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    except (OSError, ValueError):
        return 0, 0
    return sum(fields), fields[3] + fields[4]


async def sampler(stop: asyncio.Event, pause_s: float = 5, pg: bool = False) -> list[Json]:
    """docker stats, host CPU, this process's CPU and (`pg`) database sessions by state."""
    rows: list[Json] = []
    prev_host, prev_cpu, prev_t = host_cpu(), cpu_seconds(), time.monotonic()
    while not stop.is_set():
        row: Json = {"t": time.time()}
        stats = await sh("docker", "stats", "--no-stream", "--format", "{{json .}}")
        row["containers"] = {}
        for line in stats.splitlines():
            item = json.loads(line)
            mem = mib(item["MemUsage"].split("/")[0])
            row["containers"][item["Name"].removeprefix("payments-service-")] = {
                "cpu": float(item["CPUPerc"].rstrip("%")),
                "mem_mib": round(mem, 1),
            }
        host, now_cpu, now_t = host_cpu(), cpu_seconds(), time.monotonic()
        if host[0] > prev_host[0]:
            row["host_cpu_pct"] = round(
                100 * (1 - (host[1] - prev_host[1]) / (host[0] - prev_host[0])), 1
            )
        row["generator_cpu_pct"] = round(100 * (now_cpu - prev_cpu) / (now_t - prev_t), 1)
        prev_host, prev_cpu, prev_t = host, now_cpu, now_t
        if pg:
            q = "select application_name || '/' || coalesce(state, 'none'), count(*)"
            rows_pg = await psql(q + " from pg_stat_activity where datname = 'payments' group by 1")
            row["pg_sessions"] = {key: int(n) for key, n in rows_pg}  # "<role>/<state>"
        rows.append(row)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), pause_s)
    return rows


async def backlog_poller(stop: asyncio.Event) -> list[Json]:
    """Every 2 s: outbox size and age, payments delivered and created so far."""
    q = (
        "select (select count(*) from outbox),"
        " (select coalesce(extract(epoch from now() - min(created_at)), 0) from outbox),"
        " (select count(*) from payments where delivery_state = 'delivered'),"
        " (select count(*) from payments)"
    )
    rows: list[Json] = []
    while not stop.is_set():
        outbox, age, delivered, total = (await psql(q))[0]
        rows.append(
            {
                "t": time.time(),
                "outbox": int(outbox),
                "outbox_age_s": round(float(age), 1),
                "delivered": int(delivered),
                "payments": int(total),
            }
        )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), 2)
    return rows


def body_for(key: str) -> Json:
    """A request derived from its key, so a repeat of the key is an identical request."""
    rng = random.Random(key)
    return {
        "amount": f"{rng.randint(1, 99999)}.{rng.randint(0, 99):02d}",
        "currency": rng.choice(["RUB", "USD", "EUR"]),
        "description": "loadtest",
        "metadata": {"run": key.split("-")[1]},
        "webhook_url": HOOK_URL,
    }


class Load:
    """One open-model run against POST /api/v1/payments."""

    def __init__(self, rate: float, duration: float, repeat: float = 0.0) -> None:
        self.rate, self.duration, self.repeat = rate, duration, repeat
        self.run_id = uuid.uuid4().hex[:8]
        self.keys: list[str] = []
        self.samples: list[Json] = []
        self.key_ids: dict[str, set[str]] = {}

    def pick_key(self, i: int) -> tuple[str, bool]:
        """A new key, or (with probability `repeat`) one of the 20 most recent: still in flight."""
        if self.keys and random.random() < self.repeat:
            return random.choice(self.keys[-20:]), True
        key = f"lt-{self.run_id}-{i}"
        self.keys.append(key)
        return key, False

    async def send(self, client: httpx.AsyncClient, key: str, repeat: bool, due: float) -> None:
        loop = asyncio.get_running_loop()
        started, sent_at = loop.time(), time.time()
        sample: Json = {"key": key, "repeat": repeat, "sent_at": sent_at, "lateness": started - due}
        try:
            resp = await client.post(
                "/api/v1/payments", json=body_for(key), headers={"Idempotency-Key": key}
            )
            sample["status"] = resp.status_code
            if resp.status_code == 202:
                sample["payment_id"] = resp.json()["payment_id"]
                sample["replayed"] = resp.headers.get("idempotent-replayed") == "true"
                self.key_ids.setdefault(key, set()).add(sample["payment_id"])
        except httpx.HTTPError as exc:
            sample["status"] = type(exc).__name__
        end = loop.time()
        sample["latency"], sample["corrected"] = end - started, end - due
        self.samples.append(sample)

    async def run(self) -> float:
        """Send rate * duration requests on schedule; return the wall time until all answered."""
        loop = asyncio.get_running_loop()
        headers = {"X-API-Key": API_KEY}
        # Idle connections expire before uvicorn's 5 s keep-alive, so none is reused after the
        # server closed it.
        limits = httpx.Limits(
            max_connections=None, max_keepalive_connections=1000, keepalive_expiry=2
        )
        async with httpx.AsyncClient(
            base_url=API_URL, headers=headers, timeout=30, limits=limits, trust_env=False
        ) as client:
            start, tasks = loop.time(), []
            for i in range(int(self.rate * self.duration)):
                due = start + i / self.rate
                if due > loop.time():
                    await asyncio.sleep(due - loop.time())
                else:
                    await asyncio.sleep(0)  # behind schedule: send now, but let tasks run
                key, repeat = self.pick_key(i)
                tasks.append(asyncio.create_task(self.send(client, key, repeat, due)))
            await asyncio.gather(*tasks)
            return loop.time() - start

    def summary(self, elapsed: float) -> Json:
        ok = [s for s in self.samples if s["status"] == 202]

        def ms(name: str, rows: list[Json]) -> list[float]:
            return [s[name] * 1000 for s in rows]

        return {
            "rate": self.rate,
            "duration_s": self.duration,
            "requests": len(self.samples),
            "accepted": len(ok),
            "status": dict(Counter(str(s["status"]) for s in self.samples)),
            "error_rate": round(1 - len(ok) / max(1, len(self.samples)), 4),
            "achieved_rps": round(len(ok) / elapsed, 1),
            "latency_ms": dist(ms("latency", ok)),
            "latency_from_due_ms": dist(ms("corrected", ok)),
            "lateness_ms": dist(ms("lateness", self.samples)),
        }


def stats_summary(rows: list[Json]) -> Json:
    """Mean and max CPU % and max RSS per container, and the generator and host CPU."""
    names = {n for r in rows for n in r["containers"]}
    out: Json = {}
    for name in sorted(names):
        cpu = [r["containers"][name]["cpu"] for r in rows if name in r["containers"]]
        mem = [r["containers"][name]["mem_mib"] for r in rows if name in r["containers"]]
        out[name] = {
            "cpu_mean": round(sum(cpu) / len(cpu), 1),
            "cpu_max": max(cpu),
            "rss_max_mib": max(mem),
        }
    for name in ("generator_cpu_pct", "host_cpu_pct"):
        vals = [r[name] for r in rows if name in r]
        out[name] = {"mean": round(sum(vals) / max(1, len(vals)), 1), "max": max(vals, default=0)}
    return out


async def wait_drained(patience_s: float) -> Json:
    """Poll until the outbox is empty and no payment is pending."""
    t0 = time.time()
    while time.time() - t0 < patience_s:
        outbox, pending = (
            await psql(
                "select (select count(*) from outbox), (select count(*) from payments"
                " where delivery_state = 'pending')"
            )
        )[0]
        if int(outbox) == 0 and int(pending) == 0:
            return {"drained": True, "done_at": time.time()}
        await asyncio.sleep(1)
    return {"drained": False, "done_at": time.time()}


async def receiver_records() -> list[Json]:
    async with httpx.AsyncClient(base_url=RECEIVER_URL, timeout=60, trust_env=False) as client:
        resp = await client.get("/received", params={"compact": "1"})
        resp.raise_for_status()
        records: list[Json] = resp.json()
        return records


def save(name: str, data: Json) -> None:
    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / f"{name}.json").write_text(json.dumps(data, indent=2, default=str))
    print(f"wrote {OUT_DIR / name}.json")


def show(data: Json) -> None:
    """Print a result once, without the bulky series."""
    for key, value in data.items():
        if key not in ("samples", "backlog", "resources"):
            print(f"{key:>22}: {value}")
    for name, stats in data.get("resources", {}).items():
        print(f"{name:>22}: {stats}")


async def run_load(
    args: argparse.Namespace,
    name: str,
    with_db: bool,
    repeat: float = 0.0,
    during: Callable[[], Awaitable[Json]] | None = None,
    drain: bool = False,
) -> tuple[Json, Load]:
    """Run the load with the sampler (and `during` alongside); optionally wait for the drain."""
    load, stop = Load(args.rate, args.duration, repeat), asyncio.Event()
    sampling = asyncio.create_task(sampler(stop))
    polling = asyncio.create_task(backlog_poller(stop)) if with_db else None
    side = asyncio.ensure_future(during()) if during else None
    cpu0, started = cpu_seconds(), time.time()
    elapsed = await load.run()
    result = load.summary(elapsed) | {"scenario": name, "label": args.label, "started_at": started}
    if side:
        result["extra"] = await side
    if drain:
        result["drain"] = await wait_drained(args.drain_timeout)
    stop.set()
    rows = await sampling
    result["resources"] = stats_summary(rows)
    result["samples"] = rows
    result["backlog"] = await polling if polling else []
    result["generator_cpu_s"] = round(cpu_seconds() - cpu0, 1)
    return result, load


async def cmd_sample(args: argparse.Namespace) -> int:
    """Sample the stack for a while, beside a generator that runs elsewhere (e.g. k6)."""
    stop = asyncio.Event()
    sampling = asyncio.create_task(sampler(stop, pause_s=0, pg=True))
    await asyncio.sleep(args.seconds)
    stop.set()
    rows = await sampling
    states = {state for r in rows for state in r["pg_sessions"]}
    peaks = {st: max(r["pg_sessions"].get(st, 0) for r in rows) for st in sorted(states)}
    result = {
        "scenario": "sample",
        "label": args.label,
        "samples": rows,
        "resources": stats_summary(rows),
    }
    result["pg_sessions_max"] = peaks
    save(f"sample-{args.label}", result)
    show(result)
    return 0


async def cmd_idempotency(args: argparse.Namespace) -> int:
    result, load = await run_load(args, "idempotency", with_db=False, repeat=0.2)
    repeats = [s for s in load.samples if s["repeat"]]
    payments = int((await psql("select count(*) from payments"))[0][0])
    published = retried = left = 0
    async with httpx.AsyncClient(base_url=RABBITMQ_URL, auth=("payments", "payments")) as client:
        for _ in range(24):  # the relay empties the outbox; the management API lags by seconds
            left = int((await psql("select count(*) from outbox"))[0][0])
            stats = [(await client.get(f"/api/exchanges/%2F/{n}")).json() for n in EXCHANGES]
            published, retried = (x.get("message_stats", {}).get("publish_in", 0) for x in stats)
            if left == 0 and published - retried == payments:
                break
            await asyncio.sleep(5)
    dup_events = int((await psql("select count(*) - count(distinct payment_id) from outbox"))[0][0])
    checks = {
        "unique_keys": len(load.keys),
        "payments_in_db": payments,
        "repeat_requests": len(repeats),
        "replayed_responses": sum(1 for s in load.samples if s.get("replayed")),
        "keys_with_more_than_one_payment_id": sum(1 for v in load.key_ids.values() if len(v) > 1),
        "outbox_rows_left": left,
        "outbox_duplicate_payment_rows": dup_events,
        "broker_publish_in_payments_exchange": published,
        "broker_publish_in_retry_exchange": retried,
        "published_plus_left_equals_payments": published - retried + left == payments,
    }
    checks["pass"] = (
        payments == len(load.keys)
        and not checks["keys_with_more_than_one_payment_id"]
        and dup_events == 0
        and checks["published_plus_left_equals_payments"]
    )
    result["checks"] = checks
    save(f"idempotency-{args.label or int(args.rate)}", result)
    show(result)
    return 0 if checks["pass"] else 1


async def cmd_e2e(args: argparse.Namespace) -> int:
    result, load = await run_load(args, "e2e", with_db=True, drain=True)
    sent = {s["payment_id"]: s["sent_at"] for s in load.samples if "payment_id" in s}
    arrival: dict[str, float] = {}
    for rec in await receiver_records():
        if rec["signature_valid"] and rec["payment_id"] in sent:
            at = datetime.fromisoformat(rec["received_at"]).timestamp()
            arrival[rec["payment_id"]] = min(at, arrival.get(rec["payment_id"], at))
    lat = [(arrival[p] - sent[p]) * 1000 for p in arrival]
    per_second = Counter(int(t) for t in arrival.values())
    window = [sum(per_second[s + k] for k in range(10)) / 10 for s in per_second]
    span = max(arrival.values(), default=0) - min(sent.values(), default=0)
    result["e2e_latency_ms"] = dist(lat)
    result["webhooks_received"] = len(arrival)
    result["not_received"] = len(sent) - len(arrival)
    result["first_post_to_last_webhook_s"] = round(span, 1)
    result["throughput_per_s"] = {
        "webhooks_per_span": round(len(arrival) / max(1.0, span), 2),
        "best_10s_window": round(max(window, default=0), 2),
    }
    save(f"e2e-{args.label or int(args.rate)}", result)
    show(result)
    return 0


async def cmd_outage(args: argparse.Namespace) -> int:
    async def stop_broker() -> Json:
        await asyncio.sleep(args.outage_after)
        down = time.time()
        await sh("docker", "compose", "stop", "rabbitmq")
        await asyncio.sleep(max(0.0, args.outage_s - (time.time() - down)))
        await sh("docker", "compose", "start", "rabbitmq")
        return {"broker_down_at": down, "broker_up_at": time.time()}

    result, load = await run_load(args, "outage", with_db=True, during=stop_broker, drain=True)
    window = result["extra"]
    during = [
        s
        for s in load.samples
        if window["broker_down_at"] <= s["sent_at"] <= window["broker_up_at"]
    ]
    result["during_outage"] = {
        "requests": len(during),
        "status": dict(Counter(str(s["status"]) for s in during)),
        "latency_ms": dist([s["latency"] * 1000 for s in during if s["status"] == 202]),
    }
    drain, series = result["drain"], result["backlog"]
    up, down = window["broker_up_at"], window["broker_down_at"]
    before = [r for r in series if r["t"] < down]
    usual = {  # the largest values seen before the outage: what "caught up" means
        "outbox": max((r["outbox"] for r in before), default=0),
        "unfinished": max(((r["payments"] - r["delivered"]) for r in before), default=0),
    }
    after = [r for r in series if r["t"] >= up]
    result["catch_up_s"] = {  # seconds after the broker came back, at 2 s resolution
        "outbox_back_to_usual": next(
            (round(r["t"] - up, 1) for r in after if r["outbox"] <= usual["outbox"]), None
        ),
        "unfinished_back_to_usual": next(
            (
                round(r["t"] - up, 1)
                for r in after
                if r["payments"] - r["delivered"] <= usual["unfinished"]
            ),
            None,
        ),
        "usual_before_outage": usual,
    }
    states = await psql("select delivery_state, count(*) from payments group by 1")
    result["final_states"] = {state: int(n) for state, n in states}
    result["accepted_minus_in_db"] = result["accepted"] - sum(result["final_states"].values())
    save(f"outage-{args.label or int(args.rate)}", result)
    show(result)
    return 0 if drain["drained"] and result["accepted_minus_in_db"] == 0 else 1


async def cmd_verify(args: argparse.Namespace) -> int:
    """Reconcile the database with what the receiver saw (design §12.5, point 5)."""
    by_state = await psql(
        "select delivery_state, status, count(*) from payments group by 1, 2 order by 1, 2"
    )
    q = "select count(*) from payments where delivery_state = 'pending' and created_at < now() - "
    old_pending = int((await psql(q + f"interval '{args.pending_age_s} seconds'"))[0][0])
    over_budget = int(
        (await psql("select count(*) from payments where attempts - attempts_base > 3"))[0][0]
    )
    delivered = {
        r[0] for r in await psql("select id from payments where delivery_state = 'delivered'")
    }
    everything = await receiver_records()
    records = [r for r in everything if r["signature_valid"]]
    seen = Counter(r["payment_id"] for r in records)
    hooks = Counter(r["webhook_id"] for r in records)
    unsigned = len(everything) - len(records)
    missing, extra = delivered - set(seen), set(seen) - delivered
    checks = {
        "no_pending_older_than_threshold": old_pending == 0 or args.allow_undelivered,
        "every_delivered_payment_reached_receiver": not missing,
        "receiver_payments_equal_delivered": not (missing or extra) or args.allow_undelivered,
        # Delivery is at least once with a stable webhook-id (docs/guarantees.md), so repeats are
        # reported below; more POSTs than the attempt budget is a failure.
        "at_most_3_posts_per_webhook_id": max(hooks.values(), default=0) <= MAX_POSTS,
        "at_most_3_posts_per_payment": max(seen.values(), default=0) <= MAX_POSTS
        and over_budget == 0,
    }
    report = {
        "label": args.label,
        "by_delivery_state_and_status": [
            {"delivery_state": r[0], "status": r[1], "count": int(r[2])} for r in by_state
        ],
        "pending_older_than_s": {"threshold": args.pending_age_s, "count": old_pending},
        "delivered": len(delivered),
        "receiver": {"payments": len(seen), "posts": len(records), "invalid_signature": unsigned},
        "delivered_not_at_receiver": len(missing),
        "at_receiver_not_delivered": len(extra),
        "webhook_ids_posted_more_than_once": {w: n for w, n in hooks.items() if n > 1},
        "payments_with_repeated_webhook_id": sorted(
            {r["payment_id"] for r in records if hooks[r["webhook_id"]] > 1}
        ),
        "max_posts_per_payment": max(seen.values(), default=0),
        "payments_over_attempt_budget": over_budget,
        "checks": checks,
    }
    save(f"verify-{args.label or 'run'}", report)
    for row in report["by_delivery_state_and_status"]:
        print(f"{row['delivery_state']:>14} {row['status']:>10} {row['count']:>7}")
    for name, ok in checks.items():
        print(f"{'PASS' if ok else 'FAIL'}  {name}")
    print({k: v for k, v in report.items() if k not in ("by_delivery_state_and_status", "checks")})
    return 0 if all(checks.values()) else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name, rate, duration in (
        ("idempotency", 100, 30),
        ("e2e", 20, 30),
        ("outage", 5, 90),
    ):
        p = sub.add_parser(name)
        p.add_argument("--rate", type=float, default=rate, help="requests per second")
        p.add_argument("--duration", type=float, default=duration, help="seconds of load")
        p.add_argument("--label", default="", help="suffix of the result file")
        if name != "idempotency":
            p.add_argument("--drain-timeout", type=float, default=300, help="seconds to wait")
        if name == "outage":
            p.add_argument(
                "--outage-after", type=float, default=20, help="seconds before RabbitMQ stops"
            )
            p.add_argument(
                "--outage-s", type=float, default=30, help="seconds RabbitMQ stays stopped"
            )
    smp = sub.add_parser("sample")
    smp.add_argument("--seconds", type=float, default=60)
    smp.add_argument("--label", required=True)
    ver = sub.add_parser("verify")
    ver.add_argument("--label", default="")
    ver.add_argument("--pending-age-s", type=float, default=120)
    ver.add_argument(
        "--allow-undelivered", action="store_true", help="run stopped before the drain"
    )
    args = parser.parse_args()
    handlers = {
        "sample": cmd_sample,
        "idempotency": cmd_idempotency,
        "e2e": cmd_e2e,
        "outage": cmd_outage,
        "verify": cmd_verify,
    }
    return asyncio.run(handlers[args.cmd](args))


if __name__ == "__main__":
    sys.exit(main())
