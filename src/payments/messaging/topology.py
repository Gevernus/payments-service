"""RabbitMQ exchanges and queues (design §6)."""

from collections.abc import Sequence

from faststream.rabbit import QueueType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments", durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", durable=True)
DLX_EXCHANGE = RabbitExchange("payments.dlx", durable=True)

MAIN_QUEUE_NAME = "payments.new"
DLQ_NAME = "payments.new.dlq"
DELIVERY_LIMIT = 10

MAIN_QUEUE = RabbitQueue(
    MAIN_QUEUE_NAME,
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=MAIN_QUEUE_NAME,
    arguments={
        "x-dead-letter-exchange": DLX_EXCHANGE.name,
        "x-dead-letter-routing-key": DLQ_NAME,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
        "x-delivery-limit": DELIVERY_LIMIT,
    },
)

DEAD_LETTER_QUEUE = RabbitQueue(
    DLQ_NAME,
    queue_type=QueueType.QUORUM,
    durable=True,
    routing_key=DLQ_NAME,
    arguments={"x-delivery-limit": -1},
)


def retry_queue(delay_ms: int) -> RabbitQueue:
    """Retry queue whose messages return to the main queue after delay_ms."""
    raise NotImplementedError


def retry_delays(base_ms: int, max_attempts: int) -> list[int]:
    """Delays of the retry ladder: base * 2**(i-1) for i in 1..max_attempts-1."""
    raise NotImplementedError


async def declare_core(broker: RabbitBroker) -> None:
    """Declare exchanges, the main queue and the dead-letter queue."""
    raise NotImplementedError


async def declare_retry_ladder(broker: RabbitBroker, delays_ms: Sequence[int]) -> None:
    """Declare the retry queues for the given delays."""
    raise NotImplementedError
