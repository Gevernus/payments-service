# Delivery guarantees

What the service guarantees at each boundary, which component is the source of truth for each
fact, how the system behaves under each failure, and which test verifies that behaviour. The
mechanisms are described in [design.md](design.md); the reasoning behind them in
[decisions.md](decisions.md).

---

## Semantics per boundary

| Boundary | Semantics | Mechanism |
|---|---|---|
| Client to API to PostgreSQL | Exactly-once effect per `Idempotency-Key` | Unique constraint and `INSERT ... ON CONFLICT DO NOTHING`; the payment and its outbox event commit in one transaction before the 202 is sent. A repeat with the same body returns the original payment; a different body returns 422. A client may safely retry after a timeout or a 503. |
| PostgreSQL to broker (relay) | At least once | Leased claims committed before publishing; publisher confirms and `mandatory`; the row is deleted only after `Basic.Ack`, fenced by `(id, attempts)`. A duplicate publish carries the same `message_id`. |
| Broker to consumer | At-least-once delivery, effectively-once effects | The status changes once (CAS `finalize`); the gateway is idempotent by `payment_id`; every attempt starts with the CAS `begin_attempt` under a budget of 3 per epoch; the delivery state changes only through CAS transitions. |
| Consumer to merchant | At least once, at most 3 POSTs per payment per epoch | Retries for transient failures; the merchant deduplicates by `webhook-id`, which is the same for every attempt and every duplicate chain of a payment's final status. Each attempt is signed again with its own `webhook-timestamp`. |

---

## Source of truth

| Fact | Source of truth |
|---|---|
| Payment status, whether the webhook was delivered, whether the payment is dead, how many attempts were made | the `payments` row (CAS transitions) |
| Delay before the next attempt | TTL of the retry queue |
| What is in the DLQ | the DLQ itself, as a journal; it may contain duplicates (a dead payment's message rejected again) |
| A malformed message without a `payment_id` | the DLQ only |

---

## Failure matrix

"Duplicate effect" names what may be observed more than once as a result of the failure.

| Failure | Behaviour | Duplicate effect | Verified by |
|---|---|---|---|
| PostgreSQL unavailable during POST | 503 | none | `test_create_returns_503_when_db_down` |
| RabbitMQ unavailable during POST | 202; the event waits in the outbox and the relay catches up | none | `test_e2e_broker_outage_outbox_catches_up` |
| Concurrent POSTs with the same key | one payment row | none | `test_concurrent_same_key_creates_one_payment` |
| Same key, different body | 422 | n/a | `test_concurrent_same_key_different_bodies` |
| Relay crashes after claiming rows | the rows become available again when the lease expires | none | `test_relay_row_reclaimed_after_lease_expiry` |
| Relay crashes after the broker confirm | the event is published again with the same `message_id` | message | `test_relay_crash_after_confirm_republishes_same_message_id` |
| Relay stalls longer than its lease | fencing by `(id, attempts)`: its late write is a no-op | message | `test_relay_lease_lost_fencing` |
| Two relays running | `SKIP LOCKED`: disjoint claims | none | `test_two_relays_disjoint` |
| Event is unroutable | `PublishError`; the row stays in the outbox | none | `test_unroutable_not_marked_published` |
| PostgreSQL unavailable for the relay | backoff, then the relay catches up | none | `test_relay_survives_db_outage` |
| Broker unavailable for the relay | no rows are claimed while disconnected | none | `test_relay_waits_while_broker_down` |
| Topology conflict at startup | the process exits | n/a | `test_relay_startup_fails_on_topology_conflict` |
| Duplicate message (two chains) | shared budget: at most 3 POSTs, one status transition | webhook (same `webhook-id`) | `test_duplicate_chains_share_attempt_budget`, `test_parallel_duplicates_single_transition` |
| Webhook returns 5xx or times out | exactly 3 POSTs, then the DLQ; GET shows `dead_lettered` / `max_attempts` | n/a | `test_retry_ladder_exactly_three_posts_then_dlq` |
| Webhook returns 4xx or 3xx, TLS error, SSRF block | 1 POST, then the DLQ; reason `permanent` | n/a | `test_permanent_goes_to_dlq_after_one_post`, `test_webhook_pinned_tls_delivery` |
| Gateway technical failure three times | DLQ; the payment stays `pending` with `delivery_state = dead_lettered` | n/a | `test_gateway_unavailable_three_times_dead_letters_pending` |
| Database unavailable in the consumer (including a silent connection) | the client deadline fires; the message is deferred; no attempt is spent | n/a | `test_db_outage_defers_without_spending_attempt` |
| Database fails after a 2xx webhook | the outcome travels in the deferred message and is written later; the POST is not repeated | none | `test_db_down_after_delivery_persists_without_resend` |
| Database fails while marking the payment dead | the outcome is deferred, written later, then the message goes to the DLQ | n/a | `test_db_down_at_dead_letter_marks_later` |
| SIGKILL between the dead mark and the reject | redelivery finds the payment dead and rejects again; the DLQ gets the entry | DLQ entry | `test_dead_payment_redelivery_rejects_again` |
| A dead payment's message moved from the DLQ in the management UI | rejected to the DLQ again; no POST | n/a | `test_ui_requeue_from_dlq_is_noop_for_dead` |
| Stale outcome arriving after a replay | the new epoch is unaffected | n/a | `test_stale_outcome_does_not_kill_replay` |
| Budget exhausted while another chain's attempt is in flight | deferred until that attempt finishes or becomes stale | n/a | `test_exhausted_budget_with_attempt_in_flight_defers` |
| Non-network database error in the consumer (lock or statement timeout) | deferred, never sent to the DLQ | n/a | `test_db_error_in_consumer_never_dead_letters` |
| Payment `pending` with its message lost (broker delivery limit, handler bug) | recovered with `replay --stuck-pending` | n/a | `test_replay_stuck_pending` |
| SIGTERM during processing: shutdown order | the copy publisher closes after in-flight handlers finish | none | `test_copies_closed_after_in_flight_handlers` |
| Publish connection blocked by the broker | explicit publish timeout; retried until confirmed | copy (on timeout) | `test_publish_timeout_enforced_when_connection_blocked` |
| Two copies: one delivers, the other dies | `delivered` wins | n/a | `test_delivered_wins_over_dead` |
| Retry copy is unroutable | `dead_lettered` / `unroutable`, then the DLQ | n/a | `test_unroutable_copy_marks_and_dead_letters` |
| Publish connection unavailable | the handler waits for the confirm, then publishes the copy | copy (on timeout) | `test_copy_waits_while_publish_connection_down` |
| Crash after an attempt starts, repeatedly | budget is spent; after 3, `dead_lettered` / `max_attempts`, `last_error = crash_loop` | n/a | `test_crash_loop_spends_budget_and_marks` |
| Crash before an attempt starts, or messages in the prefetch buffer during a crash | after 10 deliveries the broker moves the message to the DLQ without a database mark; the payment stays `pending` and is recovered with `replay --stuck-pending` | n/a | `test_replay_stuck_pending` covers the recovery. The crash before the CAS itself is not covered by an automated test: the window between delivery and `begin_attempt` cannot be hit deterministically. |
| SIGKILL of the consumer during a webhook | redelivery starts a new attempt; the gateway is not called again | webhook (same `webhook-id`) | `test_consumer_sigkill_during_webhook` |
| SIGTERM of the consumer | in-flight messages are finished | none | `test_consumer_sigterm_finishes_in_flight` |
| Broker connection lost | the handler is cancelled; the message is redelivered | possible webhook | `test_broker_connection_drop_redelivers` |
| Crash after the status CAS, before the webhook | redelivery sends only the webhook | n/a | `test_redelivery_after_finalize_does_not_recall_gateway` |
| Malformed message | DLQ; nothing is marked in the database | n/a | `test_malformed_message_dead_lettered` |
| Replay | new budget; delivery goes through the outbox | n/a | `test_replay_reopens_and_delivers` |
| Retry delays changed between deployments | new retry queues are declared; startup succeeds | n/a | `test_changing_retry_delay_does_not_break_startup` |
| Crash after a 2xx webhook, before the outcome is recorded | the next attempt POSTs again with the same `webhook-id` | webhook | Not covered by an automated test: the window between the merchant accepting the request and the consumer holding the outcome cannot be closed without a two-phase commit with the merchant. |
| Broken topology and database failure at the same time | DLQ without a database mark; the payment stays `pending` and is recovered with `replay --stuck-pending` | n/a | Not covered by an automated test: a double failure. |
| Queue deleted by an operator | the consumer receives nothing while its health check is green | n/a | Not covered by an automated test: an operator action outside the service's control. |
| `migrate` fails | the application services do not start | n/a | Not covered by an automated test: a property of the compose dependency on the migration completing successfully. |

### Other verified properties

| Property | Verified by |
|---|---|
| A request waiting on another transaction with the same key inserts the row after that transaction rolls back | `test_insert_waits_and_wins_after_holder_rollback` |
| The payment and its outbox event are written atomically | `test_create_is_atomic` |
| Error precedence: 401, then 422, then 400, then business errors | `test_error_precedence` |
| A non-public `webhook_url` is rejected with 422 | `test_webhook_url_policy_api` |
| The relay publishes an event and deletes its row | `test_relay_publishes_and_deletes` |
| The sender connects to the checked IP with the original `Host` and SNI, resolving once | `test_sender_pins_public_host` |
| The webhook body matches the GET representation | `test_webhook_body_matches_get` |
| Full flow on the compose stack: GET right after 202, then delivery | `test_e2e_happy_path` |
| A gateway decline is final and notified | `test_e2e_gateway_decline_is_final` |
| A flaky webhook endpoint is eventually delivered to | `test_e2e_webhook_flaky_then_delivered` |
| A failing webhook endpoint exhausts retries and ends in the DLQ | `test_e2e_webhook_retry_exhausted_to_dlq` |
| An internal host is rejected as a webhook target | `test_e2e_ssrf_internal_host_rejected` |
| The replay CLI on the compose stack | `test_e2e_replay_cli` |

Every test name in this file is checked against the test suite by
`test_guarantees_doc_references_existing_tests`.

---

## Not guaranteed

- **Exactly-once webhook delivery.** A merchant may receive the same notification more than
  once and must deduplicate by `webhook-id`.
- **Webhook ordering or timing.** There is one notification per final status; its arrival time
  depends on retries and outages, and there is no upper bound while the database or broker is
  down.
- **Delivery to a merchant that keeps failing.** After 3 attempts (or 1 for a permanent error)
  the payment is `dead_lettered`; only an explicit replay grants a new budget, and each replay
  allows up to 3 more POSTs.
- **A single gateway call per payment.** Parallel duplicates and crashes may call the gateway
  again; the same idempotency key makes the repeat return the same outcome.
- **A one-to-one correspondence between the DLQ and dead payments.** The DLQ may hold
  duplicates, and a message dead-lettered by the broker's delivery limit has no database mark.
- **Automatic recovery of payments stuck in `pending`.** It requires running
  `replay --stuck-pending`.
- **Reprocessing of malformed messages.** Messages without a `payment_id` require manual
  inspection of the DLQ.

---

## Known blind spots

- The crash between message delivery and `begin_attempt` is not reproduced by a test; only the
  recovery path is.
- The window between the merchant accepting a webhook and the consumer holding the outcome
  cannot be closed; a crash there produces a repeated POST with the same `webhook-id`.
- The broker's delivery limit is counted differently across RabbitMQ versions: in 4.1 every
  previous delivery counts (crashes, connection loss, consumer timeouts). The limit of 10 is a
  safety net, not an attempt counter.
- A silent (blackholed) broker connection is detected only by the AMQP heartbeat
  (`heartbeat=10`); the tests do not inject this mode for RabbitMQ.
- A deleted queue is not detected by the consumer's health check.
- Outbox rows that keep failing are only logged at ERROR after 10 attempts; nothing alerts on
  them.
- Deferrals while the database is unavailable are unbounded; long outages are visible only in
  logs (one WARNING per deferral, with the count).
- Compose-specific behaviour (service name resolution while a container restarts, restart after
  a process exits, `host-gateway`) is verified only by running the compose stack, not by the
  integration tests.
