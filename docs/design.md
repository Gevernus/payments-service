# Payment processing service: design

This document describes the design and is kept in step with the code: how each requirement is
interpreted and which mechanisms provide the delivery guarantees. Reasoning is in
[decisions.md](decisions.md), per-failure behaviour with its test in [guarantees.md](guarantees.md),
measurements in [load-testing.md](load-testing.md); usage, API examples, configuration and tests are in the README.

## 0. Goal

Accept a payment over HTTP, charge it through an emulated gateway from a queue consumer, notify
the merchant with a signed webhook. The central choice: the `payments` row is the only source of
truth about processing. Its delivery state and attempt budget change only through compare-and-set
(CAS) statements in PostgreSQL; RabbitMQ carries messages and implements delays. This bounds
processing to three attempts, and delivery to three POSTs, per payment, whatever happens to the
messages. Out of scope: metrics and tracing, multi-tenancy, Kubernetes manifests, change data
capture, RabbitMQ policies, an automatic reconciler, request body size limits (an ingress concern).

## 1. Interpretation of the requirements

| # | Requirement | Decision |
|---|---|---|
| T1 | "90% success, 10% error" | A gateway decline is a business outcome: the payment becomes `failed` and the webhook is sent. A decline is not retried (retrying would turn declines into successes). |
| T2 | "3 attempts with exponential backoff", "DLQ after 3 attempts" | Three processing attempts per payment, counted in the database. An attempt is a consumer pass that started work (a gateway call and/or a POST). Delays `base * 2^(n-1)`: 5 s, 10 s. At most 3 POSTs; a new budget only by explicit replay. |
| T3 | "Retry on delivery errors" | Retried: network errors, timeouts, DNS errors, 408, 425, 429, 5xx, a technical gateway failure, unexpected errors. DLQ after one attempt: 3xx, other 4xx, TLS errors, SSRF blocks (a permanent rejection is not fixed by repeating). |
| T3a | "DLQ after 3 attempts" | A message may be delivered more than 3 times without spending attempts: while the database is unavailable (a deferral spends nothing; an attempt that had already started still counts), or when the outcome is known and only needs writing. |
| T4 | "X-API-Key on all endpoints" | Everything under `/api/v1/*`. Not `/health/*` (probes carry no secrets), `/docs`, `/openapi.json` (the schema is not secret). |
| T5 | "compose with postgres, rabbitmq, api, consumer" | Plus `migrate` (one-shot), `relay` (separate process) and `webhook-receiver` (self-contained demo and end-to-end tests). |
| T6 | "One consumer that does everything" | One consumer on `payments.new`; the relay only publishes the outbox. |
| T7 | Response to POST | A repeat with the same key and body: 202 with the same `payment_id` and `created_at`, the current `status`, `Idempotent-Replayed: true` (Idempotency-Key draft §2.6). |
| T8 | "Idempotency-Key (required)" | Missing or malformed: 400. Same key, different body: 422 (draft §2.7). |
| T9 | Amount is "decimal" | `0 < amount <= 9 999 999 999.99`, 2 fractional digits; as a string `^\d{1,10}(\.\d{1,2})?$`. RUB, USD and EUR have two minor-unit digits; a JSON number is a float with about 15 exact digits. |
| T10 | `webhook_url` | `http` and `https`; the address must be public (SSRF policy). Compose allows `webhook-receiver` and `host.docker.internal`. |
| T11 | Webhook format | Standard Webhooks (open specification, reference verification libraries). |
| T12 | Emulation | Outcome and delay are deterministic functions of the payment's `Idempotency-Key`; about `GATEWAY_SUCCESS_RATE` (0.9) of keys succeed. No per-request overrides: a demo or test picks the outcome by choosing the key (`idempotency_key_with_outcome` in tests). |
| T13 | Tables `payments` and `outbox` | Exactly these two: one webhook per payment, so delivery state fits in the payment row. |
| T14 | `description` | Optional, default `""`, at most 1000 characters. |
| T15 | DLQ | A journal of messages that were not processed. Reprocessing is a CLI over the database through the outbox: `replay dead` (new budget) and `replay stuck --older-than T` (payments stuck in `pending`). Moving a message out of the DLQ in the UI is harmless: a dead payment is rejected again, a pending one resumes. |

## 2. Architecture

### 2.1 Processes

`api` (uvicorn, `payments.api.app:create_app`), `relay` (`payments.workers.relay:create_app`) and
`consumer` (`payments.workers.consumer:create_app`) are ASGI applications under uvicorn, the last two
through AsgiFastStream; `migrate` runs `alembic upgrade head` once; `webhook-receiver` runs
`tools/webhook_receiver.py`; `python -m payments.cli replay dead | stuck` returns dead or stuck
payments to processing. The relay and the consumer run `SELECT 1` and connect to the broker at
startup and exit on any error, so a configuration error kills the process instead of becoming an
endless wait; the CLI exits with an error if the database is unreachable; the API relies on its
readiness probe (§4.4).

### 2.2 Flow and ownership

The flow diagram and the repository layout are in the README. One consumer pass: `begin_attempt`
(CAS, budget) -> `charge(idempotency_key)` -> `finalize` (CAS) -> POST webhook -> `mark_delivered` |
`mark_dead` (CAS). A retry is a copy published to `payments.retry` and expired by TTL back to
`payments`; a dead letter is `basic.reject(requeue=false)` through `payments.dlx`. Rule: **no
database transaction is open during a network call**; safety nets are `statement_timeout`,
`lock_timeout` and `idle_in_transaction_session_timeout` in `server_settings` plus a client-side
deadline on every call (§7.1). `Processor` (`services/processing.py`) is the only consumer code
that writes to `payments`, always through a CAS statement in `db/queries.py`; the handler
(`workers/consumer.py`) owns the message (`Envelope`, `CopyPublisher`, ack, reject).

## 3. Data model

### 3.1 `payments`

Request fields as in the API (`amount numeric(18,2)`, `currency varchar(3)`, `description
varchar(1000)`, `metadata jsonb`, `webhook_url varchar(2048)`), `idempotency_key varchar(255) UNIQUE`,
`request_fingerprint varchar(64)`, `status` (`pending`, `succeeded`, `failed`), `created_at`,
`processed_at`. Delivery fields: `delivery_state` (`pending`, `delivered`, `dead_lettered`), `attempts`
(attempts started, in total), `attempts_base` (attempts at the last replay; budget used = `attempts -
attempts_base`), `last_attempt_at`, `delivered_at`, `dead_lettered_at`, `dead_letter_reason`
(`max_attempts`, `permanent`, `unroutable`), `last_error varchar(64)` (code of the last failure:
`http_<code>`, `connect_error`, `timeout`, `dns_error`, `transport_error`, `tls_error`,
`blocked:<reason>`, `redirect`, `gateway_unavailable`, `unexpected`, `crash_loop`). CHECK constraints
(migration only): `amount > 0`, the enum values, `processed_at` set exactly when the status is final,
`delivered_at` exactly when `delivered`, `dead_lettered_at` and `dead_letter_reason` exactly when
`dead_lettered`, a `delivered` payment has a final status, `attempts >= attempts_base >= 0`. ORM:
SQLAlchemy 2.0, `meta = mapped_column("metadata", JSONB)` (`metadata` is reserved); one hand-written
Alembic revision, async `env.py`, `lock_timeout='5s'`.

### 3.2 Delivery automaton

`status` is the business result; `delivery_state` the processing and notification state. All
transitions are CAS statements in `queries.py`, each `UPDATE payments ... WHERE id = :id AND ...`:

| Transition | SET ... WHERE ... |
|---|---|
| `begin_attempt(id, max)` | `attempts = attempts + 1, last_attempt_at = now()` WHERE `delivery_state = 'pending' AND attempts - attempts_base < :max` RETURNING the row, `attempt_no = attempts - attempts_base`, `epoch = attempts_base` |
| `mark_exhausted(id, max, stale)` | `delivery_state = 'dead_lettered', dead_lettered_at = now(), dead_letter_reason = 'max_attempts', last_error = COALESCE(last_error, 'crash_loop')` WHERE `delivery_state = 'pending' AND attempts - attempts_base >= :max AND last_attempt_at < now() - :stale` |
| `finalize(id, s)` | `status = :s, processed_at = now()` WHERE `status = 'pending'` RETURNING the row |
| `mark_delivered(id)` | `delivery_state = 'delivered', delivered_at = now()`, dead fields and `last_error` cleared, WHERE `delivery_state IN ('pending', 'dead_lettered')` |
| `mark_dead(id, epoch, reason, code)` | `delivery_state = 'dead_lettered', dead_lettered_at = now(), dead_letter_reason = :reason, last_error = COALESCE(:code, last_error)` WHERE `delivery_state = 'pending' AND attempts_base = :epoch` RETURNING `delivery_state`; on 0 rows the actual state is read and returned |
| `record_error(id, epoch, code)` | `last_error = :code` WHERE `delivery_state = 'pending' AND attempts_base = :epoch` |
| `reopen(id)` | `delivery_state = 'pending'`, dead fields and `last_error` cleared, `attempts_base = attempts` WHERE `delivery_state = 'dead_lettered'` (replay CLI only) |

`mark_exhausted` fires only when the budget is spent **and** the last attempt is stale (not in
flight), which separates a crash loop from a slow attempt of another chain; `COALESCE` keeps the real
last error. **Budget epoch** = `attempts_base`: `mark_dead` and `record_error` are fenced by it, so an
outcome from before a replay cannot kill the new epoch. `mark_delivered` is not fenced: "delivered
wins" in any epoch, including over `dead_lettered`.

### 3.3 `outbox`

`id uuid PK` (= AMQP `message_id`), `payment_id uuid REFERENCES payments`, `event_type`
(`payment.created` | `payment.replay_requested`; the consumer treats both alike), `routing_key`
(`payments.new`), `payload jsonb` (`event_id`, `payment_id`, `occurred_at`), `created_at`,
`available_at` (indexed), `attempts int CHECK (>= 0)`, `last_error varchar(100)`. The source of
truth per fact (payment row, retry-queue TTL, the DLQ as a journal) is tabulated in guarantees.md.

## 4. HTTP API

### 4.1 Common behaviour

Prefix `/api/v1`; errors are RFC 9457 `application/problem+json` with a stable `code` (table in the
README). Precedence: **401, then 422, then 400 (Idempotency-Key), then business errors**; a
`ValueError` or `RecursionError` while parsing the body maps to 422 `json_invalid`; any
`DatabaseUnavailable` is 503 with `Retry-After: 1`.

### 4.2 Authentication

`ApiKeyMiddleware` is plain ASGI on `/api/`, so the key is checked before the body is read, with
`secrets.compare_digest` on the raw header bytes (401 with `WWW-Authenticate: APIKey`); the router's
`APIKeyHeader` dependency only feeds the OpenAPI schema. `API_KEY` must be at least 32 characters.

### 4.3 `POST /api/v1/payments`

Validation (`schemas.py`): `amount` is a number or a string matching `^\d{1,10}(\.\d{1,2})?$`,
`Decimal(gt=0, max_digits=12, decimal_places=2)` quantised to `0.01` (`money.py`: a JSON number
arrives as a float, and 12 digits stay inside its exact range); `description` at most 1000
characters; `metadata` at most 16 KiB serialised, depth at most 10, finite numbers; no `\x00`
anywhere; `webhook_url` an `HttpUrl` of at most 2048 characters, with the address policy applied in
the service (422 `webhook_url_not_allowed`). `Idempotency-Key`: 1 to 255 characters in `\x21-\x7E`.
Fingerprint: SHA-256 of the canonical JSON (`sort_keys`, compact separators) of the validated
`amount`, `currency`, `description`, `metadata`, `webhook_url`; `100` and `"100.00"` agree, metadata
is hashed as parsed, and the explicit field list keeps fingerprints stable when a field is added.
Flow: `check_url` (no I/O, §8.4); one transaction with `INSERT ... ON CONFLICT (idempotency_key) DO
NOTHING RETURNING`, where the winner also inserts the outbox event `payment.created` and the loser
reads the row and compares fingerprints (mismatch: 422); commit; then the 202.

### 4.4 `GET /api/v1/payments/{payment_id}` and health

The request fields, `status`, `processed_at` and `delivery` (`state`, `attempts`, `last_attempt_at`,
`delivered_at`, `dead_lettered_at`, `dead_letter_reason`, `last_error`); `PaymentOut` extends
`PaymentData`, the webhook body, so both agree. `/health/live` is 200; `/health/ready` runs
`SELECT 1` under `asyncio.timeout(2)`, 503 on failure. The API does not check the broker: it does not use it.

## 5. Outbox relay

Startup: `SELECT 1` and `declare_core()`; any error terminates the process, and so does the loop task
ending for any reason other than cancellation (CRITICAL log, `SIGTERM` to itself). Each iteration:
claim (`claim_outbox`, committed at once: due rows `FOR UPDATE SKIP LOCKED`, `attempts + 1`,
`available_at = now() + :lease`; the lease hides the row from other relays, and database time makes
relay clocks irrelevant); publish outside any transaction, concurrently, with `persist`, `mandatory`,
an explicit `asyncio.timeout(RABBIT_PUBLISH_TIMEOUT_S)`, `message_id` = event id and `correlation_id`
= payment id, where success is `Basic.Ack` only and `on_return_raises=True` makes an unroutable
message a failure; record with fencing, `DELETE ... WHERE (id, attempts) IN (...)` on success,
`available_at = now() + backoff` and `last_error` on failure, so a late write to a re-claimed row is
a no-op. No broker connection, no claims; an exception in an iteration triggers a global backoff; a
row with `attempts >= 10` is logged at ERROR. Health: connection and loop alive, `now - last_tick < 60 s`.

## 6. RabbitMQ topology

RabbitMQ 4.1 (`rabbitmq:4.1.8-management-alpine`), FastStream `>=0.7,<0.8`. Only `basic.ack` and
`basic.reject(requeue=false)` are used. Exchanges (direct, durable): `payments` (the relay publishes
here; TTL queues dead-letter back here), `payments.retry` (delay steps), `payments.dlx` (DLQ).

| Queue (quorum) | Arguments | Declared by |
|---|---|---|
| `payments.new` | DLX `payments.dlx` with routing key `payments.new.dlq`, `x-dead-letter-strategy=at-least-once`, `x-overflow=reject-publish`, `x-delivery-limit=10` | `declare_core` (relay, consumer) |
| `payments.new.dlq` | `x-delivery-limit=-1` | `declare_core` |
| `payments.new.retry.{d}ms` | `x-message-ttl=d`, DLX `payments` with routing key `payments.new`, `at-least-once`, `reject-publish` | `declare_retry_ladder` (consumer) |

Core arguments are constants; changing them on an existing volume needs `docker compose down -v`.
`delays = [base * 2^(i-1) for i in 1..RETRY_MAX_ATTEMPTS-1]`; the delay is part of the queue name,
so a changed delay declares new queues instead of conflicting. `x-delivery-limit=10` is a
broker-side safety net for messages that come back unacked before `begin_attempt` (a crash before
the CAS, or the prefetch buffer during a crash); RabbitMQ 4.1 counts every previous delivery, later
versions only failed ones, and the handler neither reads nor copies `x-delivery-count`. Crashes
**after** `begin_attempt` spend the budget and end with `max_attempts` in the database.
`at-least-once` dead-lettering retains an unroutable dead letter at the source queue.

## 7. Consumer

### 7.1 Processor

`Processor.process(payment_id, *, known=None) -> Done | RetryLater(attempt_no, code) | DeadLetter`
(`services/processing.py`; the outcome table is in its docstring). A `KnownOutcome` (network result
known, only the write left) is written without network calls. Otherwise `begin_attempt`; no row means
`_no_attempt`: `mark_exhausted` (budget spent and the last attempt stale: `DeadLetter`), else by
state: `delivered` is a duplicate (`Done`), `dead_lettered` or missing is rejected (`DeadLetter`),
`pending` means another chain is working (`AttemptInFlight`, deferred without spending). With a row:
if `status` is `pending`, `charge(idempotency_key)` outside any transaction, then the CAS `finalize`
(`GatewayUnavailable` is retryable). Then one POST: `Delivered` writes `delivered`; `RetryLater(code)`
records the error and schedules the next attempt, or marks the payment dead with `max_attempts` when
`attempt_no >= max`; `Permanent(code)` marks it dead with `permanent`. Any other exception inside the
attempt is `unexpected` and spends budget. `DatabaseUnavailable` propagates with the `KnownOutcome`.
Hence at most `max_attempts` POSTs per payment per epoch (every POST follows a successful
`begin_attempt`), stale epochs cannot kill a replay, "delivered wins", and a database error never
leads to the DLQ. `Db.run` runs every query in a short transaction under
`asyncio.timeout(DB_CALL_TIMEOUT_S)` and converts driver and connection errors into
`DatabaseUnavailable`; the engine sets `statement_timeout` 4 s, `lock_timeout` 2 s,
`idle_in_transaction_session_timeout` 10 s and `pool_pre_ping=True`.

### 7.2 Handler

`workers/consumer.py::handle_payment_event`, `AckPolicy.REJECT_ON_ERROR`, `prefetch_count=
CONSUMER_PREFETCH`. `Done` acks; `RetryLater(n)` copies the message into `payments.new.retry.
{delays[n-1]}ms` and acks only after the copy is confirmed; `DeadLetter` raises `RejectMessage`
(requeue=false: DLX to the DLQ). `DatabaseUnavailable` and `AttemptInFlight` copy the message with
the deferral headers (§7.3) into the `delays[0]` queue; the wait is bounded, because within
`STALE_AFTER_S` the other attempt finishes or becomes stale. `CopyPublisher` (`messaging/copies.py`)
owns a separate connection and publishes with `persist`, `mandatory` and an explicit
`asyncio.timeout(RABBIT_PUBLISH_TIMEOUT_S)` per attempt (FastStream 0.7 does not pass `timeout=` to
aiormq, so a blocked connection would otherwise hang), retrying with backoff up to 5 s until
confirmed; a confirmation timeout may yield two copies, absorbed by the budget. An unroutable copy
marks the payment `unroutable` (unless the outcome is already known) and rejects to the DLQ.
`copies.close()` runs in `after_shutdown`, after in-flight handlers finish, under `asyncio.timeout(5)`.
A malformed body or a handler bug outside the Processor ends in the DLQ with nothing marked; `replay
stuck` recovers the payment. Health: both connections alive, subscriber running.

### 7.3 Message headers (`messaging/envelope.py`)

| Header | Type | Written by | Read by | On retry | On defer |
|---|---|---|---|---|---|
| `x-known-outcome` | JSON of `KnownOutcome` (`kind`, `epoch`, `reason`, `code`) | handler on defer | Processor | removed | set from the exception, or kept |
| `x-deferrals` | int | handler on defer | logs | reset | incremented |

No other header is copied (`x-delivery-count`, `x-death`, ...). An invalid value is ignored and the
message is processed as a normal pass; only an invalid `x-known-outcome` is logged (WARNING),
because its loss would allow a second POST.

### 7.4 Replay (`services/replay.py`, `cli.py`)

`replay dead [--reason R] [--payment-id ID] [--limit N] [--dry-run]`: per `dead_lettered` payment,
one transaction runs `reopen` (CAS, new epoch) and inserts `payment.replay_requested`. `replay stuck
--older-than 10m [--limit N] [--dry-run]`: for `pending` payments whose `last_attempt_at` (or
`created_at`) is older than the threshold and that have no outbox event waiting, insert an event
only; no new budget. A threshold well above the ladder plus `STALE_AFTER_S` avoids duplicate
messages, which are harmless anyway. A database failure mid-run exits 1 with "re-running is safe":
the CAS and the outbox check make a re-run idempotent. The DLQ is never touched.

### 7.5 Timeout chain

`Settings` derives the two "derived" rows from the other timeouts; a validator enforces the lease condition.

| Parameter | Value | Condition |
|---|---|---|
| `DB_CALL_TIMEOUT_S` | 5 | greater than `statement_timeout` (4 s) |
| `RABBIT_PUBLISH_TIMEOUT_S` | 5 | explicit `asyncio.timeout` around each publish |
| `GATEWAY_MAX_DELAY_S` | 5 | |
| `WEBHOOK_TOTAL_TIMEOUT_S` | 15 | |
| `STALE_AFTER_S` (derived) | 30 | `GATEWAY_MAX + WEBHOOK_TOTAL + 2 * DB_CALL` |
| `CONSUMER_GRACEFUL_TIMEOUT_S` (derived) | 45 | `GATEWAY_MAX + WEBHOOK_TOTAL + 4 * DB_CALL + RABBIT_PUBLISH` |
| `copies.close()` | 5 | |
| compose `stop_grace_period` (relay) | 30 s | at least `2 * RABBIT_PUBLISH_TIMEOUT_S + 5` of loop shutdown, with margin |
| compose `stop_grace_period` (consumer) | 60 s | at least graceful timeout + close + 5 |
| `OUTBOX_LEASE_S` | 30 | greater than `2 * RABBIT_PUBLISH_TIMEOUT_S` |

## 8. Gateway and webhooks

### 8.1 Gateway

`Gateway.charge(*, idempotency_key, amount, currency, metadata) -> PaymentStatus`. Contract: one key,
one outcome, a repeat with the same key is safe; a technical failure or "still processing" raises
`GatewayUnavailable`. `EmulatedGateway` seeds `random.Random` with `sha256(key)`, so outcome and
delay (`uniform(min, max)`) are functions of the key. It never raises `GatewayUnavailable`; that
branch exists for a real provider and is covered with a fake gateway. A contract test runs against both.

### 8.2 Webhook contract (Standard Webhooks)

`webhook-id = uuid5(WEBHOOK_NAMESPACE, f"{payment_id}:{status}")`, the same on every attempt and
duplicate chain; `webhook-timestamp` is the time of the attempt; `webhook-signature: v1,<base64
HMAC-SHA256(key, f"{id}.{ts}." + body)>` with `key = b64decode(WEBHOOK_SECRET.removeprefix("whsec_"))`.
Body `{"type", "timestamp", "data": PaymentData}`, sent as the exact signed bytes; unit tests verify
with the reference `standardwebhooks` library.

### 8.3 Delivery

`httpx.AsyncClient(timeout=Timeout(10, connect=3, pool=3), follow_redirects=False,
trust_env=False, limits=Limits(max_connections=50, max_keepalive_connections=0))` under
`asyncio.timeout(WEBHOOK_TOTAL_TIMEOUT_S)`. One send path: `resolve_checked(host)`, the first address
(IPv4 preferred), a request to that IP with the original `Host` header and `sni_hostname`.
Classification: 2xx `Delivered`; 408, 425, 429, 5xx, connection error, timeout, DNS error `RetryLater`;
3xx `Permanent(redirect)`; other 4xx `Permanent(http_<code>)`; `ssl.SSLCertVerificationError` in the
chain `Permanent(tls_error)`; `BlockedTarget` `Permanent(blocked:<reason>)`. `Retry-After` is logged only.

### 8.4 SSRF guard (`webhooks/guard.py`)

`is_public(ip)`: `is_global` and not multicast, after unwrapping IPv4-mapped and NAT64 addresses;
site-local, `::/96`, `3fff::/20`, `5f00::/16` and `2001:20::/28` are blocked. `check_url(url)`, no
I/O: scheme, no userinfo or fragment, IDNA host from the raw URL, internal host names, IP literals,
single-label and special-use names. `resolve_checked(host)` requires every resolved address to be
public (fail closed); the resolver is injectable. `WEBHOOK_INTERNAL_HOSTS` only widens the "address
is public" predicate, not the send path; compose sets it, production leaves it empty.

## 9. Configuration

One `Settings` class in `src/payments/config.py` holds every variable, its default and the validators
(`GATEWAY_MIN_DELAY_S <= GATEWAY_MAX_DELAY_S`, `RETRY_MAX_ATTEMPTS >= 2`, `OUTBOX_LEASE_S > 2 *
RABBIT_PUBLISH_TIMEOUT_S`); the operator-facing subset is in the README. Secrets are `SecretStr | None`,
read through `require_*()` accessors only where needed. The stale threshold and the graceful timeout
are derived (§7.5), not configured. Settings without secrets are logged as `settings.loaded`.

## 10. Logging

structlog, JSON in compose. The consumer binds `payment_id` and `event_id` as context variables, so
one payment's processing is found by payment id (also the message's `correlation_id`). One line per
processed payment, `payment.processed`, with `outcome` (done, retry, dead_letter) and, on a retry,
`attempt` and `code`; one WARNING per deferral (`consumer.deferred`) with the `x-deferrals` count;
`relay.publish_failed` and `relay.row_stuck` (10 or more attempts) in the relay.

## 11. Docker and compose

Image `python:3.13-slim`, `uv sync --frozen --no-dev` in a build stage, non-root user; only `api`
builds it, so the first run is `docker compose up -d --build`. `compose.yaml` works without `.env`:
`postgres:17-alpine`; `rabbitmq:4.1.8-management-alpine` with `hostname: rabbitmq` (quorum queue data
is tied to the node name); `migrate`, then `api`, `relay`, `consumer` (`stop_grace_period: 60s`) and
`webhook-receiver`, all with `init: true`, `restart: unless-stopped`, `LOG_FORMAT=json`, a Python
healthcheck (no curl in the image) and `extra_hosts: host-gateway`. `compose.e2e.yaml` overrides
gateway delays to 0.05 to 0.2 s, `RETRY_BASE_DELAY_MS=200`, `WEBHOOK_TOTAL_TIMEOUT_S=5` and
`WEBHOOK_INTERNAL_HOSTS=webhook-receiver`, so the SSRF scenario can use `host.docker.internal`.
`tools/webhook_receiver.py` exposes `/hooks/ok`, `/hooks/fail/{n}` (fails the first `n` POSTs per
`webhook-id`), `/hooks/status/{code}`, `/hooks/slow/{seconds}`, verifies signatures with the standard
library `hmac`, and lists received webhooks at `GET /received?payment_id=`.

## 12. Testing and load testing

Test levels (unit; integration against real PostgreSQL and RabbitMQ with failures injected by
`tests/support/tcp_proxy.py`; end-to-end on the compose stack) and the CI jobs are in the README.
The guarantee each test verifies is listed next to its name in [guarantees.md](guarantees.md);
load-test method, results and reproduction are in [load-testing.md](load-testing.md).
