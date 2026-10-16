# Design decisions

The choices that shape the service: context, decision, and why the alternatives were not taken.
Section references point to [design.md](design.md).

## 1. A gateway decline is final

**Context.** The emulated gateway fails 10% of charges; the assignment also asks for three retries
with backoff and a DLQ.
**Decision.** A decline is a business outcome: the payment becomes `failed`, the merchant gets a
webhook, nothing is retried. Only technical failures are retried (an unavailable gateway, webhook
network errors, 408, 425, 429, 5xx; design T1, T3); after three of them the payment stays `pending`
with `delivery_state = dead_lettered` for the replay CLI.
**Why not retry every gateway error.** Three independent tries would turn 99.9% of payments into
successes; against a real provider it resubmits a charge the issuer refused.

## 2. Transactional outbox with a separate relay and leased claims

**Context.** `payments.new` must be published exactly when the payment row exists; publishing inside
the request loses events (commit, then crash) or publishes for rolled-back rows.
**Decision.** The API inserts the payment and its outbox row in one transaction. A separate `relay`
claims due rows with `FOR UPDATE SKIP LOCKED`, sets a lease and commits before publishing with
confirms and `mandatory`; rows are deleted fenced by `(id, attempts)` after `Basic.Ack` (design §5).
Delivery is at least once with a stable `message_id`; a stalled relay cannot overwrite a re-claimed row.
**Why not the alternatives.** A row lock held during the publish keeps a transaction open across a
network call; publishing from the API after commit loses events on a crash; a `published_at` flag
grows the table; change data capture is an extra component.

## 3. API idempotency

**Context.** `Idempotency-Key` is required; clients retry after timeouts, and concurrent requests
with the same key must not create two payments.
**Decision.** A unique constraint and `INSERT ... ON CONFLICT DO NOTHING RETURNING`; the loser
compares a SHA-256 fingerprint of the validated request (`100` and `"100.00"` agree, metadata is
hashed as parsed). Per draft-ietf-httpapi-idempotency-key-header: missing or malformed key 400, same
key with a different body 422, matching repeat 202 with `Idempotent-Replayed: true` (design §4.3).
Keys do not expire.
**Why not the alternatives.** Check-then-insert races; a separate idempotency table duplicates what
the payment row holds; 409 for a reused key contradicts the draft.

## 4. Money parsing

**Context.** The amount is a "decimal", but JSON numbers are decoded as floats with about 15 exact digits.
**Decision.** `Decimal(gt=0, max_digits=12, decimal_places=2)` quantised to `0.01`; strings must match
`^\d{1,10}(\.\d{1,2})?$`; numbers are accepted because 12 digits stay inside the exact float range
(design §4.3). Maximum 9 999 999 999.99; RUB, USD and EUR all have two minor-unit digits; the column
is `numeric(18,2)`, leaving room to raise the limit with a string-only contract.
**Why not the alternatives.** Integer minor units change the assignment's contract; unlimited
precision silently alters a 17-digit JSON number before validation; strings only rejects ordinary clients.

## 5. The payment row is the source of truth: delivery automaton and attempt budget

**Context.** A message can be delivered more than once (relay retries, duplicate copies, redelivery
after a crash); a counter in message headers is duplicated with the message.
**Decision.** The row holds `delivery_state`, `attempts` and `attempts_base`. Every pass starts with
the CAS `begin_attempt`, which increments `attempts` only while `attempts - attempts_base < max`.
`mark_dead` and `record_error` are fenced by the epoch `attempts_base`; `mark_delivered` is not
("delivered wins"). Replay sets `attempts_base = attempts`: a new epoch with a new budget (design
§3.2, §7.1). A budget exhausted while another chain's attempt is in flight is deferred until that
attempt finishes or becomes stale (`STALE_AFTER_S`).
**Why not the alternatives.** An `x-attempt` header does not survive duplicates; the broker's delivery
limit counts deliveries, not attempts, and differs between RabbitMQ versions; a separate attempts
table is a third table for data that belongs to the payment.

## 6. Retry ladder on TTL quorum queues, copy-then-ack on a dedicated publish connection

**Context.** Retries need 5 s and 10 s delays; RabbitMQ has no per-message delay without a plugin,
and a retry must survive a crash between "schedule retry" and "ack the original".
**Decision.** One quorum queue per delay, `payments.new.retry.{d}ms`, with `x-message-ttl`, a
dead-letter route back to `payments`, `at-least-once` dead-lettering and `reject-publish`. The handler
publishes a copy with confirms and `mandatory` through a separate `CopyPublisher` connection under an
explicit timeout and acks the original only after the confirm (design §6, §7.2). A crash between copy
and ack yields two copies, absorbed by the budget; a changed delay declares new queues.
**Why not the alternatives.** The delayed-message plugin is not in the standard image and stores
messages in a non-replicated table; `nack(requeue=true)` loops at full speed; one TTL queue with
per-message expiry has head-of-line blocking; publishing on the consuming connection lets flow
control on publish block consumption.
**Why not retry rows in the outbox.** The handler could spend the attempt and insert an outbox row
with `available_at = now() + delay` in one transaction, removing the copy publisher and the ladder.
But with the database down there is nothing to write the retry into (only a hot nack loop or a sleep
in the handler); the known-outcome path (a 2xx received, the database down, the outcome written later
without a second POST) lives in message headers and would be lost; and the retry delay would depend
on the relay poll interval instead of a queue TTL.

## 7. The DLQ is a journal; native reject; replay goes through the database

**Context.** If the DLQ were the record of dead payments, moving a message out of it, losing it, or
crashing between "mark dead" and "reject" would leave the database and the queue disagreeing.
**Decision.** Messages reach `payments.new.dlq` only through `basic.reject(requeue=false)` and the
dead-letter exchange; the handler never publishes to the DLQ. The database records the dead state and
its reason (`max_attempts`, `permanent`, `unroutable`); a redelivered message of a dead payment is
rejected again. Reprocessing is `python -m payments.cli replay`, which reopens the payment and inserts
an outbox event in one transaction (design §7.4). The DLQ may hold duplicates; a malformed message
without a `payment_id` exists only there; a `pending` payment whose message hit the broker's delivery
limit is recovered with `replay stuck`.
**Why not the alternatives.** Publishing a copy to the DLQ is a second publish that fails
independently; replaying from the DLQ relies on a queue with duplicates and without the payments whose
messages were lost; `x-death` counting varies by version and any publisher can spoof it.

## 8. Database errors in the consumer always defer

**Context.** A database outage or lock timeout says nothing about the payment; treating it as a
processing failure spends attempts and can dead-letter a healthy payment while the database cannot
even record that.
**Decision.** `Db.run` runs every query under `asyncio.timeout(DB_CALL_TIMEOUT_S)` and converts any
driver or connection error into `DatabaseUnavailable`; the handler publishes a deferred copy with the
`delays[0]` delay. A known network outcome (a 2xx received) travels in `x-known-outcome`, so the next
pass only writes it (design §7.1, §7.3). A database error never leads to the DLQ; deferrals are
unbounded and visible as one WARNING each; programming errors outside the driver still spend budget.
**Why not the alternatives.** Classifying database errors into transient and permanent would push a
payment into the DLQ with no row recording it; `nack` requeues without delay, in a tight loop.

## 9. No lease on payments: the gateway idempotency contract

**Context.** Two chains for the same payment can run at once (a duplicate message, a copy published
twice); the gateway call takes 2 to 5 s and must not run inside a transaction.
**Decision.** No processing lock or lease. The gateway is called with the payment's `Idempotency-Key`,
whose contract is one key, one outcome, repeat safe. The status is written with the CAS `finalize`
(`WHERE status = 'pending'`), so only one chain changes it; a later pass that finds a final status
skips the gateway and only sends the webhook (design §7.1, §8.1). Parallel chains share the attempt
budget and carry the same `webhook-id`.
**Why not the alternatives.** A lease column is another clock to tune, and a crashed holder blocks
processing until expiry; `SELECT ... FOR UPDATE` across the gateway call is a transaction open during
a network call.

## 10. Standard Webhooks and the SSRF policy with address pinning

**Context.** The merchant must verify that a webhook came from the service and deduplicate repeats.
A client-supplied `webhook_url` is a request the service makes on the client's behalf, which can
target internal addresses.
**Decision.** Standard Webhooks: `webhook-id` (UUIDv5 of payment id and status, stable across
attempts), `webhook-timestamp`, HMAC-SHA256 `webhook-signature`. The URL is checked without I/O at
creation and again at send time: every resolved address must be public, and the request goes to the
first resolved IP with the original `Host` and TLS SNI; redirects are not followed (design §8). In
compose an allow-list admits `webhook-receiver` and `host.docker.internal`; it is empty in production.
**Why not the alternatives.** A custom signature header needs custom merchant code; checking only at
creation allows DNS rebinding between check and send; resolving once and letting the HTTP client
resolve again leaves the second resolution unchecked.

## 11. Processes and compose layout

**Context.** The assignment lists postgres, rabbitmq, api and consumer; migrations and outbox
publishing also need a place to run.
**Decision.** Separate processes: `api`, `relay`, `consumer`, a one-shot `migrate`, and a
`webhook-receiver` for the demo and end-to-end tests. The relay and the consumer run as AsgiFastStream
applications under uvicorn (health endpoints), check the database and broker at startup and exit on
configuration errors; the API relies on its readiness probe. `compose.yaml` runs without `.env`
(design §2.1, §11). Each role scales and restarts independently; the consumer gets a 60 s stop grace
period so in-flight handlers finish before the copy connection closes.
**Why not the alternatives.** A relay inside the API ties its lifecycle and scaling to HTTP workers;
migrations at application startup race between replicas.

## 12. What I would add in production

- Metrics and tracing (queue depths, DLQ size, outbox backlog age, attempt and deferral rates, webhook
  latency) with alerts on DLQ growth and payments stuck in `pending`.
- A reconciler that runs `replay stuck` on a schedule.
- A health check that verifies the consumer's queue exists (a deleted queue leaves the consumer idle
  with a green health check).
- Per-merchant webhook secrets with rotation (Standard Webhooks allows several signatures).
- Retention for idempotency keys and a cleanup policy for old payments.
- Multi-tenancy and per-client API keys instead of one static key.
- RabbitMQ policies for queue arguments instead of constants in code.
- A request body size limit at the ingress.
- Kubernetes manifests, and change data capture for the outbox if throughput requires it.
