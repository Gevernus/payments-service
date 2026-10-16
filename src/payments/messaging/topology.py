"""RabbitMQ exchanges and queues."""

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
    name = f"{MAIN_QUEUE_NAME}.retry.{delay_ms}ms"
    return RabbitQueue(
        name,
        queue_type=QueueType.QUORUM,
        durable=True,
        routing_key=name,
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": MAIN_QUEUE_NAME,
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        },
    )


async def _declare_bound(
    broker: RabbitBroker, queue: RabbitQueue, exchange: RabbitExchange
) -> None:
    declared_exchange = await broker.declare_exchange(exchange)
    declared_queue = await broker.declare_queue(queue)
    await declared_queue.bind(declared_exchange, routing_key=queue.routing())


async def declare_core(broker: RabbitBroker) -> None:
    """Declare exchanges, the main queue and the dead-letter queue."""
    await broker.declare_exchange(RETRY_EXCHANGE)
    await _declare_bound(broker, MAIN_QUEUE, PAYMENTS_EXCHANGE)
    await _declare_bound(broker, DEAD_LETTER_QUEUE, DLX_EXCHANGE)


async def declare_retry_ladder(broker: RabbitBroker, delays_ms: Sequence[int]) -> None:
    """Declare the retry queues for the given delays."""
    for delay in delays_ms:
        await _declare_bound(broker, retry_queue(delay), RETRY_EXCHANGE)
