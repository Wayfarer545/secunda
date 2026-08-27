"""AMQP-топология сервиса. Единственный источник объектов exchange/queue."""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.broker.names import (
    EXCHANGE_DLX,
    EXCHANGE_PAYMENTS,
    EXCHANGE_RETRY,
    QUEUE_DLQ,
    QUEUE_PAYMENTS_NEW,
    QUEUE_RETRY_2S,
    QUEUE_RETRY_4S,
    RK_DLQ,
    RK_PAYMENTS_NEW,
    RK_RETRY_2S,
    RK_RETRY_4S,
)

exchange_payments = RabbitExchange(EXCHANGE_PAYMENTS, type=ExchangeType.DIRECT)
exchange_retry = RabbitExchange(EXCHANGE_RETRY, type=ExchangeType.DIRECT)
exchange_dlx = RabbitExchange(EXCHANGE_DLX, type=ExchangeType.DIRECT)

queue_payments_new = RabbitQueue(
    QUEUE_PAYMENTS_NEW,
    routing_key=RK_PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": EXCHANGE_DLX,
        "x-dead-letter-routing-key": RK_DLQ,
    },
)

queue_retry_2s = RabbitQueue(
    QUEUE_RETRY_2S,
    routing_key=RK_RETRY_2S,
    arguments={
        "x-message-ttl": 2000,
        "x-dead-letter-exchange": EXCHANGE_PAYMENTS,
        "x-dead-letter-routing-key": RK_PAYMENTS_NEW,
    },
)

queue_retry_4s = RabbitQueue(
    QUEUE_RETRY_4S,
    routing_key=RK_RETRY_4S,
    arguments={
        "x-message-ttl": 4000,
        "x-dead-letter-exchange": EXCHANGE_PAYMENTS,
        "x-dead-letter-routing-key": RK_PAYMENTS_NEW,
    },
)

queue_dlq = RabbitQueue(QUEUE_DLQ, routing_key=RK_DLQ)

_BINDINGS: tuple[tuple[RabbitQueue, RabbitExchange], ...] = (
    (queue_payments_new, exchange_payments),
    (queue_retry_2s, exchange_retry),
    (queue_retry_4s, exchange_retry),
    (queue_dlq, exchange_dlx),
)


async def declare_topology(broker: RabbitBroker) -> None:
    """Идемпотентно декларирует все exchange, очереди и биндинги."""
    for exchange in (exchange_payments, exchange_retry, exchange_dlx):
        await broker.declare_exchange(exchange)

    for queue, exchange in _BINDINGS:
        robust_queue = await broker.declare_queue(queue)
        await robust_queue.bind(exchange.name, routing_key=queue.routing())
