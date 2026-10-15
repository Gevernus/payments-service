# Design decisions

Short decision records for the choices that shape the service. Each record states the context,
the decision, the alternatives that were considered, and the consequences. Section references
point to [design.md](design.md).

---

## 1. A gateway decline is final

**Context.** The emulated gateway succeeds in 90% of cases and fails in 10%. The assignment also
asks for retries with exponential backoff and a DLQ after three attempts.

**Decision.** A decline is a business outcome: the payment becomes `failed`, the merchant gets a
webhook, and the decline is never retried. Only technical failures are retried: an unavailable
gateway, webhook network errors, 408, 425, 429 and 5xx responses (design T1, T3).

**Alternatives considered.** Retrying every gateway error up to three times. With independent
outcomes, three tries would turn 99.9% of payments into successes; against a real provider it
means resubmitting a charge that the issuer refused.

**Consequences.** The gateway contract separates `failed` (an answer) from `GatewayUnavailable`
(no answer). Three consecutive technical failures leave the payment `pending` with
`delivery_state = dead_lettered`; the replay CLI can try again later.

---

## 2. Transactional outbox with a separate relay and leased claims

**Context.** Creating a payment must publish `payments.new` exactly when the payment row exists.
Publishing inside the HTTP request either loses events (commit, then crash) or publishes events
for rows that were rolled back.

**Decision.** The API inserts the payment and its outbox row in one transaction. A separate
`relay` process claims due rows with `FOR UPDATE SKIP LOCKED`, sets a lease (`available_at =
now() + lease`, `attempts + 1`) and commits before publishing. It publishes with publisher
confirms and `mandatory`, then deletes rows fenced by `(id, attempts)` after `Basic.Ack`
(section 5).

**Alternatives considered.** Holding the row lock during the publish (a database transaction
open across a network call, blocked by a slow broker). Publishing from the API after commit
(events lost on a crash). A `published_at` flag instead of deletion (the table grows and needs
cleanup). Change data capture (an extra component, out of scope).

**Consequences.** Delivery to the broker is at least once; a duplicate carries the same
`message_id`. A relay that stalls past its lease cannot overwrite the row another relay
re-claimed. The relay uses database time, so clock skew between relays does not matter. A
background task failure terminates the process instead of leaving a live process that does
nothing.

---

## 3. API idempotency

**Context.** `Idempotency-Key` is required. Clients retry after timeouts, and concurrent
requests with the same key must not create two payments.

**Decision.** A unique constraint on `idempotency_key` and `INSERT ... ON CONFLICT DO NOTHING
RETURNING`. The loser reads the existing row and compares a versioned fingerprint (`v1:` plus
SHA-256 of the canonical JSON of the validated request, so `100` and `"100.00"` agree while
metadata is hashed as parsed). Behaviour follows
draft-ietf-httpapi-idempotency-key-header: a missing or malformed key is 400; the same key with
a different body is 422; a matching repeat returns 202 with the original `payment_id` and
`created_at`, the current status and `Idempotent-Replayed: true` (section 4.3).

**Alternatives considered.** Check-then-insert (a race between the check and the insert). A
separate idempotency table with stored responses (a third table; the payment row already holds
everything the response needs). Returning 409 for a key reuse with a different body (the draft
specifies 422).

**Consequences.** Concurrent requests serialise on the unique index; a request that waits on a
transaction that later rolls back inserts the row itself. Keys do not expire.

---

## 4. Money parsing

**Context.** The amount is a "decimal". JSON numbers are decoded as floats by most clients and
by the default JSON parser; a float holds about 15 significant decimal digits exactly.

**Decision.** `Decimal(gt=0, max_digits=12, decimal_places=2)`, quantised to `0.01`. String
amounts must match `^\d{1,10}(\.\d{1,2})?$`. Numeric amounts are accepted for convenience; the
limit of 12 digits keeps them within the exact float range (section 4.3).

**Alternatives considered.** Integer minor units (a different API contract from the one in the
assignment). Unlimited precision (a 17-digit amount sent as a JSON number is altered silently
before validation sees it). Strings only (rejects ordinary JSON clients).

**Consequences.** The maximum amount is 9 999 999 999.99 in any of the three currencies, all of
which have two minor-unit digits. The column is `numeric(18,2)`, leaving room to raise the limit
together with a string-only contract.

---

## 5. The payment row is the source of truth: delivery automaton and attempt budget

**Context.** A message can be delivered more than once: relay retries, duplicate copies,
redelivery after a crash. Counting attempts in message headers lets every duplicate carry its
own counter, so the limit of three attempts does not hold.

**Decision.** The `payments` row holds `delivery_state` (`pending`, `delivered`,
`dead_lettered`), `attempts` and `attempts_base`. Every consumer pass starts with
`begin_attempt`, a CAS that increments `attempts` only while `attempts - attempts_base < max`.
`mark_dead` and `record_error` are fenced by the epoch `attempts_base`; `mark_delivered` is not
("delivered wins"). Replay sets `attempts_base = attempts`, which opens a new epoch with a new
budget (section 3.2, 7.1).

**Alternatives considered.** An `x-attempt` header (does not survive duplicates). A broker
delivery limit as the only bound (counts deliveries, not attempts, and differs between RabbitMQ
versions). A separate attempts table (a third table for data that belongs to the payment).

**Consequences.** At most three attempts, and at most three webhook POSTs, per payment per
epoch, whatever happens to the messages. Outcomes from an old epoch cannot kill a replay. A
budget exhausted while another chain's attempt is still running is deferred until the attempt
finishes or becomes stale (`STALE_AFTER_S`).

---

## 6. Retry ladder on TTL quorum queues, copy-then-ack on a dedicated publish connection

**Context.** Retries need delays of 5 s and 10 s. RabbitMQ has no native per-message delay
without a plugin, and a retry must not be lost if the consumer crashes between "schedule retry"
and "ack the original".

**Decision.** One quorum queue per delay step, `payments.new.retry.{d}ms`, with
`x-message-ttl` and a dead-letter route back to `payments`/`payments.new`, `at-least-once`
dead-lettering and `reject-publish` overflow. The handler publishes a copy to `payments.retry`
with confirms and `mandatory`, waits for the confirmation, and only then acks the original. The
copy is published through a separate connection owned by `CopyPublisher`, under an explicit
`asyncio.timeout`, and that connection is closed after in-flight handlers finish (section 6,
7.2).

**Alternatives considered.** The delayed-message exchange plugin (not part of the standard image;
messages live in a non-replicated table). `nack(requeue=true)` (no delay; loops at full speed).
A single TTL queue with per-message expiry (head-of-line blocking: a long delay at the head
holds back shorter ones). Publishing on the consuming connection (flow control on publish blocks
consumption too).

**Alternative: retry rows in the outbox.** On a failed attempt the handler would, in one
transaction, spend the attempt and insert an outbox row with `available_at = now() + delay`; the
relay would publish it when due, and the original message would be acked. That removes the copy
publisher, the TTL ladder and `x-deferrals`. It was not chosen for three reasons. With the
database down there is nothing to write the retry into, so the handler could only nack in a hot
loop that burns `x-delivery-limit`, or sleep inside the handler. The known-outcome path (the 2xx
was received, the database is down, the outcome is recorded later without a second POST) would be
lost, because that state lives in the message headers. And the retry delay would depend on the
relay poll interval instead of the queue TTL.

**Consequences.** A crash between the copy and the ack produces two copies; the budget and the
automaton absorb them. Changing the delay declares new queues instead of failing on argument
mismatch. Under `Connection.Blocked` the handler waits and retries instead of hanging forever.

---

## 7. The DLQ is a journal; native reject; replay goes through the database

**Context.** The assignment requires a DLQ for messages not processed after three attempts.
If the DLQ were the record of dead payments, moving a message out of it, losing it, or a crash
between "mark dead" and "reject" would leave the database and the queue disagreeing.

**Decision.** Messages reach `payments.new.dlq` only through `basic.reject(requeue=false)` and
the queue's dead-letter exchange; the handler never publishes to the DLQ itself. The database
records the dead state and its reason (`max_attempts`, `permanent`, `unroutable`). A redelivered
message of a dead payment is rejected again. Reprocessing is `python -m payments.cli replay`,
which reopens the payment and inserts an outbox event in one transaction (section 7.4).

**Alternatives considered.** Publishing a copy to the DLQ (a second publish that can fail
independently). Replaying from the DLQ (the queue can contain duplicates and has no record of
payments whose messages were lost). `x-death` based counting (header semantics vary and can be
spoofed by any publisher).

**Consequences.** The DLQ may contain duplicates, and a malformed message without a
`payment_id` exists only there. Moving a message from the DLQ in the management UI is harmless.
A `pending` payment whose message hit the broker's delivery limit is recovered with
`replay stuck`.

---

## 8. Database errors in the consumer always defer

**Context.** A database outage or a lock timeout says nothing about the payment. Treating it as
a processing failure spends attempts and can send a healthy payment to the DLQ while the
database cannot even record that.

**Decision.** `db_call()` wraps every query in a short transaction under
`asyncio.timeout(DB_CALL_TIMEOUT_S)` and converts any driver or connection error into
`DatabaseUnavailable`. The handler then publishes a deferred copy with the `delays[0]` delay. If
the network outcome is already known (a webhook returned 2xx), the copy carries it in
`x-known-outcome` and the next pass only writes it, without repeating the POST (section 7.1,
7.3).

**Alternatives considered.** Classifying database errors into transient and permanent (an
error that looks permanent on a single statement would push the payment into the DLQ with no
row recording it). Requeueing with `nack` (no delay; tight loop during an outage).

**Consequences.** A database error never leads to the DLQ. Deferrals are unbounded; the
`x-deferrals` counter makes long outages visible in logs (one WARNING per deferral).
Programming errors outside the driver still spend budget as `unexpected`.

---

## 9. No lease on payments: the gateway idempotency contract

**Context.** Two chains for the same payment can run at the same time (a duplicate message, a
copy published twice). The gateway call takes 2 to 5 seconds and must not run inside a
transaction.

**Decision.** There is no processing lock or lease on the payment. The gateway is called with
`idempotency_key = payment_id`, and its contract states that one key yields one outcome and a
repeat is safe. The status is written with a CAS `finalize` (`WHERE status = 'pending'`), so only
one chain changes it; a later pass that finds a final status skips the gateway and only sends
the webhook (section 7.1, 8.1).

**Alternatives considered.** A lease column with an expiry (another clock to tune, and a
crashed holder still blocks processing until expiry). `SELECT ... FOR UPDATE` across the gateway
call (a transaction open during a network call).

**Consequences.** The gateway can be called more than once for a payment, always with the same
key and the same result. Webhook POSTs from parallel chains share the attempt budget and carry
the same `webhook-id`.

---

## 10. Standard Webhooks and the SSRF policy with address pinning

**Context.** The merchant needs to verify that a webhook came from the service and to
deduplicate repeats. A `webhook_url` supplied by an API client is a request the service makes on
its behalf, which can target internal addresses.

**Decision.** Webhooks follow Standard Webhooks: `webhook-id` (a UUIDv5 of payment id and
status, stable across attempts), `webhook-timestamp`, and an HMAC-SHA256 `webhook-signature`. The
URL is checked without I/O at creation (scheme, host form, internal names, IP literals) and
again at send time: every resolved address must be public, and the request goes to the first
resolved IP with the original `Host` and TLS SNI. Redirects are not followed (section 8).

**Alternatives considered.** A custom signature header (merchants would need custom code).
Checking only at creation (DNS rebinding between check and send). Resolving once and letting the
HTTP client resolve again (the second resolution is unchecked).

**Consequences.** Merchants can use the reference libraries. In compose, an allow-list admits
`webhook-receiver` and `host.docker.internal`; it is empty in production.

---

## 11. Processes and compose layout

**Context.** The assignment lists postgres, rabbitmq, api and consumer. Migrations and outbox
publishing also need a place to run.

**Decision.** Separate processes: `api`, `relay`, `consumer`, a one-shot `migrate`, and a
`webhook-receiver` for the demo and end-to-end tests. The relay and the consumer run as
AsgiFastStream applications under uvicorn, which gives them health endpoints. Every process
checks the database (and the broker, where used) at startup and exits on configuration errors.
`compose.yaml` runs without a `.env` file (section 2.1, 11).

**Alternatives considered.** Running the relay as a background task inside the API (its
lifecycle and scaling become tied to HTTP workers). Running migrations at application startup
(several replicas race on the schema).

**Consequences.** Each role scales and restarts independently. The consumer gets a 60 s stop
grace period so in-flight handlers finish before the copy connection closes.

---

## 12. What I would add in production

Deliberately left out of this implementation:

- Metrics and tracing: queue depths, DLQ size, outbox backlog age, attempt and deferral rates,
  webhook latency; alerts on DLQ growth and on payments stuck in `pending`.
- A reconciler that runs `replay stuck` on a schedule instead of an operator.
- A health check that verifies the consumer's queue exists (today a queue deleted by an
  operator leaves the consumer idle while its health check is green).
- Per-merchant webhook secrets with rotation (Standard Webhooks allows several signatures).
- Retention for idempotency keys and a cleanup policy for old payments.
- Multi-tenancy and per-client API keys instead of one static key.
- RabbitMQ policies for queue arguments instead of constants in code, so arguments can change
  without recreating queues.
- A request body size limit at the ingress.
- Kubernetes manifests and change data capture for the outbox if throughput requires it.
