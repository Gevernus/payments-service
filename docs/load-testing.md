# Load testing

Stand: one disposable VPS (2 vCPU AMD EPYC-Rome, 1.9 GiB RAM, no swap), the whole compose stack and
the load generator on the same host. Every result below comes from the JSON files of one run each
(condensed in `docs/load-testing/summary.json`); repeated runs differ by 10 to 30%. The raw
result files (`loadtest-results/`) are not in the repository; `docs/load-testing/summary.json` condenses them.

- **Nothing was lost.** Every accepted payment ended `delivered`; none was `pending`, `dead_lettered` or
  missing after a drain, and the DLQ stayed empty. No payment received more than 2 POSTs (limit 3).
- **Intake** (k6, 45 s per rate): clean at 50 and 100 rps (p95 232 and 576 ms); 1.9% 503 at 150 rps;
  35% at 200 rps; 90% failed at 400 rps. The first limit is the single API process (about one
  core), with the host close behind.
- **Idempotency:** 2000 requests, 406 repeats of in-flight keys: 1594 payments, one outbox event each.
- **End to end** (default 2 to 5 s gateway): POST to webhook p50 3.8 s, p99 5.5 s at 2 rps. Throughput is
  `prefetch * replicas / mean gateway delay`: 2.8, 8.1 and 12.9 payments/s for 10x1, 10x3 and 50x1.
- **Broker outage** (30 s, 5 rps): all 450 requests answered 202, all payments `delivered`. The outbox was back to
  normal 5 s after the broker returned, the unfinished payments after 28 s.
- **Caveats:** single runs; the generator shares the 2 vCPUs with the stack; sampling `docker stats` costs CPU.

## 1 Stand

| Item | Value |
|---|---|
| Host | Timeweb VPS, 2 vCPU AMD EPYC-Rome, 1897 MiB RAM (`free -m`), no swap, kernel 7.0.0-31 |
| Software | Docker 29.1.3, Compose 2.40.3, PostgreSQL 17.11 (`postgres:17-alpine`), RabbitMQ 4.1.8 on Erlang/OTP 27, Python 3.13.15 |
| Topology | `compose.yaml` as shipped: one `api` (a single uvicorn process), `relay`, `consumer`, `webhook-receiver`, `postgres`, `rabbitmq` |
| Settings | defaults of design section 9 (gateway 2 to 5 s, success rate 0.9, 3 attempts); `CONSUMER_PREFETCH` 10 unless stated; `compose.e2e.yaml` not used |

## 2 Method

- **Generators.** Intake uses k6 (`tools/k6_ingest.js`, constant arrival rate, unique keys). The Python generator
  `tools/loadtest.py` (asyncio and httpx, open model: request `i` is due at `start + i / rate`, whatever happened to
  earlier ones) is used for idempotency, end-to-end and outage runs, which need per-payment bookkeeping. Intake is
  not measured with it because on this host it saturated before the API did (it fell behind its schedule by seconds at
  100 rps; `loadtest-results/superseded/`).
- **Co-location.** Ports are bound to 127.0.0.1, so the generator competes with the stack for 2 vCPUs. The k6 container
  used at most 45% of a core (505 MiB RSS at 400 rps); the Python generator at most 15% in the runs reported here.
- **Sampling.** `tools/loadtest.py sample` runs beside k6: `docker stats` for every container, host CPU from
  `/proc/stat` and `pg_stat_activity` grouped by `application_name` and state, about every 2 s. Tables give CPU as
  mean / max (percent of one core) over the load window: the samples in which the k6 container was present (22 to 23
  of 25 to 26 per run).
- **End-to-end latency** is the receiver's arrival time minus the time of the POST (same clock). Backlog figures come
  from a database poll every 2 s.
- **Reconciliation.** `verify` ran after every run. Intake and idempotency run without a consumer, so `verify` is
  told `--allow-undelivered`, which relaxes the "no old pending" and "receiver equals delivered" checks only.

## 3 Results

### 3.1 Intake

45 s per rate, consumer scaled to zero, unique keys, 202 expected.

| Rate (rps) | Iterations | 202 / 503 / no status | Dropped | k6 latency med / p95 / p99 (ms) |
|---|---|---|---|---|
| 50 | 2250 | 2250 / 0 / 0 | 0 | 8.7 / 232 / 530 |
| 100 | 4501 | 4501 / 0 / 0 | 0 | 14 / 576 / 900 |
| 150 | 6637 | 6513 / 124 / 0 | 114 | 211 / 3600 / 5172 |
| 200 | 8528 | 5513 / 3015 / 0 | 473 | 2051 / 5550 / 5911 |
| 400 | 15805 | 1625 / 7550 / 6630 | 2196 | 221 / 7194 / 11569 |

"No status" are requests that ended without an HTTP response (connection errors or k6 timeouts; not classified). The API log kept for the 400 rps run (`k6-400-api-log.txt`) counts 1625 x 202 and 7550 x 503, as k6 does.

| Rate | api CPU | postgres CPU | rabbitmq CPU | relay CPU | host CPU | API sessions in Postgres, max (idle in transaction / executing) |
|---|---|---|---|---|---|---|
| 50 | 33 / 61 | 7 / 15 | 13 / 64 | 9 / 41 | 52 / 75 | 6 (4 / 0) |
| 100 | 63 / 92 | 15 / 22 | 20 / 68 | 15 / 49 | 75 / 100 | 15 (11 / 4) |
| 150 | 76 / 98 | 21 / 31 | 22 / 84 | 16 / 40 | 85 / 100 | 15 (13 / 1) |
| 200 | 86 / 119 | 16 / 26 | 14 / 83 | 14 / 43 | 85 / 99 | 15 (14 / 2) |
| 400 | 79 / 103 | 6 / 24 | 11 / 65 | 10 / 41 | 83 / 100 | 15 (14 / 1) |

(CPU columns: mean / max; `docker stats` can report slightly above 100% for one process.)
**What the samples show.**

1. From 100 rps the `api` container peaks at 92 to 119% of one core, with a mean of 63 to 86%. PostgreSQL stays at or
   below 31% CPU and executes at most 4 statements of the API at a time. The first limit is the single API process
   (one event loop on one core).
2. The host is not far behind: 75 to 85% busy on average and 99 to 100% at peaks from 100 rps. The two vCPUs are shared
   by six services, the generator and the sampler, so a second API process would meet the host limit soon after.
   These figures do not say how much headroom a dedicated core for the API would give.
3. The k6 container is not the limit (at most 45% of a core).
4. RabbitMQ and the relay are not limiting: at most 84% and 49% of a core at peaks, and the oldest outbox row stays under
   1 s in every end-to-end run.

**The 503 `database_unavailable` while Postgres is idle.** Two mechanisms are consistent with the samples:

- *Pool checkout wait.* `Db.run` wraps the whole call, including the checkout from SQLAlchemy's default pool (5 plus 10
  overflow, 15 connections), in `asyncio.timeout(DB_CALL_TIMEOUT_S = 5)`. With the pool exhausted, a request waits for a
  connection and is cancelled after 5 s.
- *Event-loop starvation.* A saturated loop resumes the coroutine late, and the same timeout fires.

The `pg_stat_activity` samples support the first as the visible mechanism: from 100 rps the pool had grown to its
full 15 connections (5 + 10 overflow) at some sample, and 11 to 14 of them are `idle in transaction`, that is checked out with an open transaction
and waiting for the next statement from the API process, while at most 4 execute. Postgres is not the party being waited
for. They do not separate a pool queue from a starved loop as the trigger, and no pool checkout counter was collected
(SQLAlchemy's pool status is not exposed), so that distinction is open. Either way the response code is misleading:
the API answers `database_unavailable` while the database is idle, because the 5 s deadline also covers queueing inside
the API process. The 503 latency is the timeout, which is why p95 approaches 5 s from 150 rps.

### 3.2 Idempotency

50 rps for 40 s through the Python generator, consumer scaled to zero. About 20% of requests repeat one of the 20 most
recent keys with an identical body, so the repeat usually arrives while the original is in flight.

| Check | Result |
|---|---|
| Requests / unique keys / repeats | 2000 / 1594 / 406, all answered 202 |
| Responses with `Idempotent-Replayed: true` | 406 |
| Keys answered with more than one `payment_id` | 0 |
| `count(payments)` | 1594 |
| Outbox rows left / payments with a duplicate row | 0 / 0 |
| Broker `publish_in` on `payments` / `payments.retry` | 1594 / 0 |
| Latency p50 / p95 / p99 | 10.4 / 55 / 287 ms |

How the outbox events are counted is in the appendix. `verify`: 1594 payments `pending`, all checks passed.

### 3.3 End to end

Default gateway, receiver at `/hooks/ok`. A failed charge still sends a webhook, so every payment counts as delivered.

**Sustained.** One consumer, prefetch 10 (capacity about 10 / 3.5 s = 2.9 payments/s), 2 rps for 60 s, 120 payments.
POST p99 85 ms. POST to webhook p50 / p95 / p99 / max = 3831 / 5117 / 5481 / 5570 ms, which is the gateway delay plus about
0.3 s. Oldest outbox row 0.4 s, at most 10 payments unfinished, 120 of 120 webhooks. CPU means: api 6%, consumer 10%,
relay 6%, rabbitmq 15%; host 34%.

**Throughput by prefetch and replicas.** 20 rps offered for 30 s (600 payments) in every configuration, run until drained.

| Prefetch x replicas | Theory (/s) | Webhooks per span (/s) | Best 10 s | Span (s) | POST to webhook p50 / p95 / p99 (s) | Unfinished, max | Oldest outbox row |
|---|---|---|---|---|---|---|---|
| 10 x 1 | 2.9 | 2.78 | 3.4 | 215.6 | 92.5 / 176.2 / 183.4 | 521 | 0.5 s |
| 50 x 1 | 14.3 | 12.92 | 15.2 | 46.4 | 9.1 / 14.9 / 15.8 | 208 | 0.4 s |
| 10 x 3 | 8.6 | 8.10 | 9.1 | 74.1 | 23.4 / 41.4 / 43.7 | 373 | 0.5 s |
| 50 x 3 | 42.9 | 17.47 | 20.9 | 34.3 | 3.9 / 5.3 / 5.5 | 84 | 0.5 s |

"Span" is the time from the first POST to the last webhook. "Webhooks per span" divides 600 by it; "Best 10 s" is the
highest 10 s average of arrivals.

- The first three rows are saturated; their latency is queueing time. Throughput follows
  `prefetch * replicas / 3.5 s` within about 10%.
- The last row is not saturated, and 17.47 against the offered 20 is not a capacity figure: the span contains the 30 s of
  load plus the 2 to 5 s gateway delay of the last payments (600 / 34.3 s). The 10 s window reaches 20.9, the offered rate.
  The capacity of that configuration was not measured.
- Consumers are bound by held messages, not CPU: 10 to 20% of a core each; host 40 to 67% busy. The emulated gateway only sleeps.
- Every run drained; `verify` passed after each: 600 webhooks for 600 `delivered` payments, one POST per payment.

### 3.4 Broker outage under load

5 rps for 90 s (450 payments), one consumer with prefetch 50 (about 14/s, not saturated). `docker compose stop rabbitmq` at
20.0 s, `start` returned at 50.4 s. Three runs (a, b, c; a is the first).

| Metric | a | b | c |
|---|---|---|---|
| Requests / answered 202 | 450 / 450 | 450 / 450 | 450 / 450 |
| Sent during the outage, all 202, p99 (ms) | 151, 35 | 151, 65 | 151, 46 |
| Peak outbox rows / oldest row (s) | 163 / 32.6 | 167 / 33.3 | 169 / 33.7 |
| Outbox back to its usual size after the broker was up (s) | 4.6 | 5.5 | 5.5 |
| Unfinished payments back to their usual level (s) | 26.8 | 28.1 | 27.9 |
| Final states | 450 delivered | 450 delivered | 450 delivered |
| Receiver POSTs / max per payment | 451 / 2 | 450 / 1 | 450 / 1 |

"Usual" is the largest value before the outage (outbox 2 to 3 rows, 21 to 22 unfinished); resolution is 2 s. DLQ: 0 messages.
Payments in the database equal accepted requests in all runs.

- **Run a: one payment got two POSTs with the same `webhook-id`** (451 POSTs for 450 payments). This is the documented
  path for a lost broker connection (`docs/guarantees.md`, failure matrix, "Broker connection lost": the handler is
  cancelled, the message is redelivered, possible duplicate webhook; `test_broker_connection_drop_redelivers`). Attempt 1
  had POSTed but not yet committed `mark_delivered` when the connection dropped; after the reconnect the unacked message
  was redelivered, `begin_attempt` granted attempt 2 (the payment was still `pending`, 1 of 3 attempts used) and it POSTed
  again with the same `webhook-id`, which is derived from the payment id and its final status. No payment exceeded the
  budget. Only the error-line count was kept for this run, so the row's `attempts` was not recorded. Runs b and c had 450
  POSTs for 450 payments. `verify` flagged run a because its first version required unique `webhook-id`s; it now reports
  repeats and fails only above 3 POSTs per `webhook-id`.
- Error-level log lines in all three runs: relay 36, consumer 72, `api` 0 (`outage-log-excerpt.txt`,
  `outage-{b,c}-log-excerpt.txt`; run a's span 23:36:22 to 23:36:55 UTC is the outage window). They are the AMQP client reconnecting every second
  ("connection refused", name resolution failing while the container is stopped).

## 4 Limitations and open questions

- Single runs on one shared VPS; the generator and the sampler run on the host under test.
- The cause of the 503 is not separated between pool checkout wait and event-loop lag (needs a pool counter and a loop-lag probe).
- The API ceiling with several workers or on larger hardware was not measured.
- Capacity of 50 x 3 consumers and the behaviour with a real, slower or failing gateway were not measured.
- The outage was run with prefetch 50 only, at a rate the consumer sustains; a larger in-flight set during the outage would
  show more redeliveries.
- The webhook receiver keeps the last 10 000 records; `verify` assumes a run stays below it.

## 5 Production changes

- Several API workers or replicas behind a load balancer (the API is stateless); size the SQLAlchemy pool explicitly and
  separate "waited for a connection" from "database failed" in the error code and the metrics.
- Expose pool checkout count, event-loop lag, outbox age, queue depth and DLQ size as metrics and alert on them; outbox age was
  the clearest signal in the outage runs.
- Choose the consumer prefetch from `prefetch * replicas / gateway latency` once the real gateway latency is known.
- Put PostgreSQL and RabbitMQ on their own hosts and run the generator elsewhere.

## 6 Reproduce

On the host that runs the stack, from the repository root (needs `uv`, Docker, and the `grafana/k6` image for intake):

```bash
uv sync --frozen
LT="uv run python tools/loadtest.py"
reset() {  # reset <prefetch> <consumer replicas>
  docker compose down -v
  CONSUMER_PREFETCH=$1 docker compose up -d --build --wait --scale consumer=$2; sleep 5
}
mkdir -p loadtest-results; chmod 777 loadtest-results   # the k6 container writes here

# Intake (consumer scaled to zero)
for r in 50 100 150 200 400; do
  reset 10 0
  $LT sample --seconds 52 --label k6-$r &
  sleep 3
  docker run --rm --network host -v $PWD/tools:/s:ro -v $PWD/loadtest-results:/out \
    -e RATE=$r -e DURATION=45s -e RUN=$r grafana/k6 run --summary-export=/out/k6-$r.json /s/k6_ingest.js
  wait
  $LT verify --label k6-$r --allow-undelivered
done

# Idempotency
reset 10 0
$LT idempotency --rate 50 --duration 40 --label 50rps
$LT verify --label idempotency --allow-undelivered

# End to end: sustained rate, then the throughput matrix (bash: uses `set --`)
reset 10 1
$LT e2e --rate 2 --duration 60 --label sustained-p10r1
$LT verify --label e2e-sustained-p10r1
for cfg in "10 1" "50 1" "10 3" "50 3"; do set -- $cfg
  reset $1 $2
  $LT e2e --rate 20 --duration 30 --label sat-p$1-r$2 --drain-timeout 300
  $LT verify --label e2e-sat-p$1-r$2
done

# Broker outage
reset 50 1
$LT outage --rate 5 --duration 90 --label p50r1-5rps
$LT verify --label outage
docker compose down -v
```

Results go to `loadtest-results/` (override with `LOADTEST_OUT`). `verify` exits 1 when a check fails: counts by
`delivery_state` and `status` (reported); no `pending` payment older than `--pending-age-s` (120); the payments the receiver saw
equal the `delivered` ones; no `webhook-id` posted more than 3 times; at most 3 POSTs per payment, and `attempts - attempts_base`
at most 3 in the database (these runs contain no replay, so there is one epoch).

## Appendix: counting outbox events

The relay deletes an outbox row after the broker confirms it, so a row count after the run says nothing about how many events
existed. The idempotency run therefore adds the rows still in the outbox to the number of messages the broker reports as
published into the `payments` exchange (`publish_in`, management API). The stack is rebuilt with `down -v` first and the
consumer is scaled to zero, so the relay is the only publisher; `publish_in` on `payments.retry` (0) shows nothing came back
through the retry ladder. The tool polls until the outbox is empty and the counter matches, because the management API
refreshes counters every few seconds. Equal totals do not prove "one event per payment" alone (a duplicate and a missing event
would cancel). The rest of the evidence is that the payment and its outbox row are written in one transaction by the
`ON CONFLICT DO NOTHING` winner only (integration tests), that no payment has a second row waiting, and that in the
section 3.3 runs the receiver got exactly one POST per payment.
