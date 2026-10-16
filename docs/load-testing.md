# Load testing

Single runs on one disposable VPS; the numbers come from the JSON result files, condensed in
`docs/load-testing/summary.json` (the raw `loadtest-results/` are not committed). Repeated runs differ by 10 to 30%.

- **Nothing was lost.** Every accepted payment ended `delivered`; none was `pending`, `dead_lettered` or missing
  after a drain; the DLQ stayed empty; no payment got more than 2 POSTs (limit 3).
- **Intake** (k6, 45 s per rate): clean at 50 and 100 rps (p95 232 and 576 ms); 1.9% 503 at 150 rps, 35% at 200 rps,
  90% failed at 400 rps. The first limit is the single API process (about one core).
- **Idempotency:** 2000 requests, 406 repeats of in-flight keys: 1594 payments, one outbox event each.
- **End to end** (2 to 5 s gateway): POST to webhook p50 3.8 s, p99 5.5 s at 2 rps. Throughput is
  `prefetch * replicas / mean gateway delay`: 2.8, 8.1 and 12.9 payments/s for 10x1, 10x3 and 50x1.
- **Broker outage** (30 s, 5 rps): all 450 requests answered 202, all payments `delivered`; the outbox was back to
  normal 5 s after the broker returned, the unfinished payments after 28 s.

## Stand and method

- Timeweb VPS, 2 vCPU AMD EPYC-Rome, 1897 MiB RAM, no swap; Docker 29.1.3, Compose 2.40.3, PostgreSQL 17.11,
  RabbitMQ 4.1.8, Python 3.13.15. `compose.yaml` as shipped (one `api` process), defaults of `src/payments/config.py`.
- Intake: k6 (`tools/k6_ingest.js`, constant arrival rate, unique keys). The other scenarios use `tools/loadtest.py`
  (asyncio and httpx, open model, per-payment bookkeeping), which on this host fell behind its own schedule before the
  API saturated. Everything shares the 2 vCPUs: k6 used at most 45% of a core, the Python generator at most 15%.
- `tools/loadtest.py sample` records `docker stats`, host CPU and `pg_stat_activity` about every 2 s; CPU below is
  mean / max of one core over the load window. End-to-end latency is the receiver's arrival time minus the POST time.
  `verify` ran after every run (`--allow-undelivered` for the two scenarios without a consumer).

## Intake (k6, 45 s per rate, consumer scaled to zero)

| rps | Iterations | 202 / 503 / no status | Dropped | med / p95 / p99 (ms) | api CPU | host CPU | API sessions, max (idle in tx / executing) |
|---|---|---|---|---|---|---|---|
| 50 | 2250 | 2250 / 0 / 0 | 0 | 8.7 / 232 / 530 | 33 / 61 | 52 / 75 | 6 (4 / 0) |
| 100 | 4501 | 4501 / 0 / 0 | 0 | 14 / 576 / 900 | 63 / 92 | 75 / 100 | 15 (11 / 4) |
| 150 | 6637 | 6513 / 124 / 0 | 114 | 211 / 3600 / 5172 | 76 / 98 | 85 / 100 | 15 (13 / 1) |
| 200 | 8528 | 5513 / 3015 / 0 | 473 | 2051 / 5550 / 5911 | 86 / 119 | 85 / 99 | 15 (14 / 2) |
| 400 | 15805 | 1625 / 7550 / 6630 | 2196 | 221 / 7194 / 11569 | 79 / 103 | 83 / 100 | 15 (14 / 1) |

"No status" are requests without an HTTP response. From 100 rps the `api` container peaks at 92 to 119% of one core;
PostgreSQL stays at or below 31% and executes at most 4 API statements at a time; RabbitMQ (at most 84%) and the relay
(at most 49%) are not limiting. The host peaks at 99 to 100%, so a second API process would meet the host limit soon after.

**503 while Postgres is idle.** `Db.run` wraps the whole call, including the checkout from SQLAlchemy's pool (5 + 10
overflow), in `asyncio.timeout(DB_CALL_TIMEOUT_S = 5)`; from 100 rps all 15 connections are checked out and 11 to 14 sit
`idle in transaction` waiting for the API process, so the deadline fires on queueing inside the API and the response
says `database_unavailable` while the database is idle. The 503 latency is the timeout, hence p95 near 5 s from 150 rps.

## Idempotency (50 rps, 40 s, consumer scaled to zero)

About 20% of requests repeat one of the 20 most recent keys with an identical body, so the repeat usually arrives
while the original is in flight. Events are counted as outbox rows left plus the broker's `publish_in` counter.

| Requests / unique keys / repeats | `Idempotent-Replayed` | Keys with >1 `payment_id` | `count(payments)` | Outbox left / duplicate rows | Broker `publish_in` payments / retry | p50 / p95 / p99 (ms) |
|---|---|---|---|---|---|---|
| 2000 / 1594 / 406, all 202 | 406 | 0 | 1594 | 0 / 0 | 1594 / 0 | 10.4 / 55 / 287 |

## End to end (default gateway, receiver `/hooks/ok`)

Sustained: one consumer, prefetch 10, 2 rps for 60 s, 120 payments. POST p99 85 ms; POST to webhook
p50 / p95 / p99 / max = 3831 / 5117 / 5481 / 5570 ms (the gateway delay plus about 0.3 s); oldest outbox row 0.4 s;
at most 10 unfinished; 120 of 120 webhooks. CPU means: api 6%, consumer 10%, relay 6%, rabbitmq 15%, host 34%.
Throughput matrix: 20 rps offered for 30 s (600 payments), run until drained.

| Prefetch x replicas | Theory (/s) | Webhooks per span (/s) | Best 10 s | Span (s) | POST to webhook p50 / p95 / p99 (s) | Unfinished, max | Oldest outbox row |
|---|---|---|---|---|---|---|---|
| 10 x 1 | 2.9 | 2.78 | 3.4 | 215.6 | 92.5 / 176.2 / 183.4 | 521 | 0.5 s |
| 50 x 1 | 14.3 | 12.92 | 15.2 | 46.4 | 9.1 / 14.9 / 15.8 | 208 | 0.4 s |
| 10 x 3 | 8.6 | 8.10 | 9.1 | 74.1 | 23.4 / 41.4 / 43.7 | 373 | 0.5 s |
| 50 x 3 | 42.9 | 17.47 | 20.9 | 34.3 | 3.9 / 5.3 / 5.5 | 84 | 0.5 s |

The first three rows are saturated and follow `prefetch * replicas / 3.5 s` within about 10%; their latency is queueing.
The last row is not saturated: 17.47 is 600 over a span that includes the last gateway delays, the best 10 s window
reaches the offered 20 rps, and its capacity was not measured. Consumers are bound by held messages, not CPU (10 to
20% of a core each). Every run drained with one POST per payment.

## Broker outage under load (5 rps for 90 s, 450 payments, prefetch 50)

`docker compose stop rabbitmq` at 20.0 s, `start` returned at 50.4 s. Three runs; DLQ 0 messages in all of them.

| Metric | a | b | c |
|---|---|---|---|
| Answered 202 / sent during the outage (all 202, p99 ms) | 450 / 151, 35 | 450 / 151, 65 | 450 / 151, 46 |
| Peak outbox rows / oldest row (s) | 163 / 32.6 | 167 / 33.3 | 169 / 33.7 |
| Outbox / unfinished back to usual (s after the broker was up) | 4.6 / 26.8 | 5.5 / 28.1 | 5.5 / 27.9 |
| Final states; receiver POSTs / max per payment | 450 delivered; 451 / 2 | 450 delivered; 450 / 1 | 450 delivered; 450 / 1 |

"Usual" is the largest value before the outage (outbox 2 to 3 rows, 21 to 22 unfinished). In run a one payment got two
POSTs with the same `webhook-id`: attempt 1 had POSTed but not yet committed `mark_delivered` when the connection
dropped, so the unacked message was redelivered after the reconnect and `begin_attempt` granted attempt 2. This is the
"Broker connection lost" row of the failure matrix in `docs/guarantees.md` (`test_broker_connection_drop_redelivers`);
no payment exceeded the budget. Error-level log lines (relay 36, consumer 72, api 0) are the AMQP client reconnecting.

## Limitations

- Single runs on one shared VPS; the generator and the sampler run on the host under test.
- The cause of the 503 is not separated between pool checkout wait and event-loop lag (no pool counter was collected).
  In production: size the pool explicitly, report "waited for a connection" apart from "database failed", and expose
  pool checkouts, event-loop lag, outbox age (the clearest signal in the outage runs), queue depth and DLQ size.
- Not measured: the API with several workers or larger hardware; the capacity of 50 x 3 consumers; a real, slower or
  failing gateway; the outage with a larger in-flight set (it ran at a rate prefetch 50 sustains).
- The webhook receiver keeps the last 10 000 records; `verify` assumes a run stays below it.

## Reproduce

On the host that runs the stack, from the repository root (needs `uv`, Docker and the `grafana/k6` image). `reset P R`
stands for `docker compose down -v && CONSUMER_PREFETCH=P docker compose up -d --build --wait --scale consumer=R`,
`LT` for `uv run python tools/loadtest.py`; results go to `loadtest-results/` (`LOADTEST_OUT` overrides).

```bash
mkdir -p loadtest-results; chmod 777 loadtest-results          # the k6 container writes here
for r in 50 100 150 200 400; do                                 # intake
  reset 10 0; $LT sample --seconds 52 --label k6-$r & sleep 3
  docker run --rm --network host -v $PWD/tools:/s:ro -v $PWD/loadtest-results:/out \
    -e RATE=$r -e DURATION=45s -e RUN=$r grafana/k6 run --summary-export=/out/k6-$r.json /s/k6_ingest.js
  wait; $LT verify --label k6-$r --allow-undelivered
done
reset 10 0; $LT idempotency --rate 50 --duration 40 --label 50rps; $LT verify --label idempotency --allow-undelivered
reset 10 1; $LT e2e --rate 2 --duration 60 --label sustained-p10r1; $LT verify --label e2e-sustained-p10r1
for cfg in "10 1" "50 1" "10 3" "50 3"; do set -- $cfg          # throughput matrix
  reset $1 $2; $LT e2e --rate 20 --duration 30 --label sat-p$1-r$2 --drain-timeout 300; $LT verify --label e2e-sat-p$1-r$2
done
reset 50 1; $LT outage --rate 5 --duration 90 --label p50r1-5rps; $LT verify --label outage
```

`verify` exits 1 when a check fails: no `pending` payment older than `--pending-age-s` (120); the payments the receiver
saw equal the `delivered` ones; no `webhook-id` posted more than 3 times; at most 3 POSTs per payment and
`attempts - attempts_base <= 3` in the database.
