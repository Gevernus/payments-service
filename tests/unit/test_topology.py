"""The names, delays and arguments of the RabbitMQ topology (design §6)."""

from payments.messaging.topology import (
    DEAD_LETTER_QUEUE,
    DELIVERY_LIMIT,
    DLQ_NAME,
    DLX_EXCHANGE,
    MAIN_QUEUE,
    MAIN_QUEUE_NAME,
    PAYMENTS_EXCHANGE,
    RETRY_EXCHANGE,
    retry_queue,
)


def test_retry_queue_name_contains_delay() -> None:
    assert retry_queue(5000).name == "payments.new.retry.5000ms"
    assert retry_queue(10000).name == "payments.new.retry.10000ms"


def test_retry_queue_arguments() -> None:
    queue = retry_queue(5000)

    assert queue.durable is True
    assert queue.routing_key == queue.name
    assert queue.arguments == {
        "x-queue-type": "quorum",
        "x-message-ttl": 5000,
        "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
        "x-dead-letter-routing-key": MAIN_QUEUE_NAME,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
    }


def test_core_queue_arguments() -> None:
    assert MAIN_QUEUE.name == "payments.new"
    assert MAIN_QUEUE.durable is True
    assert MAIN_QUEUE.arguments == {
        "x-queue-type": "quorum",
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new.dlq",
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": 10,
    }
    assert DELIVERY_LIMIT == 10
    assert DEAD_LETTER_QUEUE.name == DLQ_NAME == "payments.new.dlq"
    assert DEAD_LETTER_QUEUE.arguments == {"x-queue-type": "quorum", "x-delivery-limit": -1}


def test_exchange_names() -> None:
    assert [e.name for e in (PAYMENTS_EXCHANGE, RETRY_EXCHANGE, DLX_EXCHANGE)] == [
        "payments",
        "payments.retry",
        "payments.dlx",
    ]
