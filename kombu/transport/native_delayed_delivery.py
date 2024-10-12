"""Native Delayed Delivery API.

Only relevant for RabbitMQ.
"""
from __future__ import annotations

from kombu import Connection, Exchange, Queue
from kombu.log import get_logger

logger = get_logger(__name__)

MAX_NUMBER_OF_BITS_TO_USE = 28
MAX_LEVEL = MAX_NUMBER_OF_BITS_TO_USE - 1
CELERY_DELAYED_DELIVERY_EXCHANGE = "celery_delayed_delivery"


def level_name(level: int) -> str:
    """Generates the delayed queue/exchange name based on the level."""
    if level < 0:
        raise ValueError("level must be a non-negative number")

    return f"celery_delayed_{level}"


def declare_native_delayed_delivery_exchanges_and_queues(connection: Connection, queue_type: str) -> None:
    """Declares all native delayed delivery exchanges and queues."""
    if queue_type != "classic" and queue_type != "quorum":
        raise ValueError("queue_type must be either classic or quorum")

    channel = connection.channel()

    routing_key: str = "1.#"

    for level in range(27, -1, - 1):
        current_level = level_name(level)
        next_level = level_name(level - 1) if level > 0 else None

        delayed_exchange: Exchange = Exchange(
            current_level, type="topic").bind(channel)
        delayed_exchange.declare()

        queue_arguments = {
            "x-queue-type": queue_type,
            "x-overflow": "reject-publish",
            "x-message-ttl": pow(2, level) * 1000,
            "x-dead-letter-exchange": next_level if level > 0 else CELERY_DELAYED_DELIVERY_EXCHANGE,
        }

        if queue_type == 'quorum':
            queue_arguments["x-dead-letter-strategy"] = "at-least-once"

        delayed_queue: Queue = Queue(
            current_level,
            queue_arguments=queue_arguments
        ).bind(channel)
        delayed_queue.declare()
        delayed_queue.bind_to(current_level, routing_key)

        routing_key = "*." + routing_key

    routing_key = "0.#"
    for level in range(27, 0, - 1):
        current_level = level_name(level)
        next_level = level_name(level - 1) if level > 0 else None

        next_level_exchange: Exchange = Exchange(
            next_level, type="topic").bind(channel)

        next_level_exchange.bind_to(current_level, routing_key)

        routing_key = "*." + routing_key

    delivery_exchange: Exchange = Exchange(
        CELERY_DELAYED_DELIVERY_EXCHANGE, type="topic").bind(channel)
    delivery_exchange.declare()
    delivery_exchange.bind_to(level_name(0), routing_key)


def bind_queue_to_native_delayed_delivery_exchange(connection: Connection, queue: Queue) -> None:
    """Binds a queue to the native delayed delivery exchange."""
    channel = connection.channel()
    queue = queue.bind(channel)
    exchange: Exchange = queue.exchange.bind(channel)

    if exchange.type == 'direct':
        logger.warn(f"Exchange {exchange.name} is a direct exchange "
                    f"and native delayed delivery do not support direct exchanges.\n"
                    f"ETA tasks published to this exchange will block the worker until the ETA arrives.")
        return

    routing_key = queue.routing_key if queue.routing_key.startswith(
        '#') else f"#.{queue.routing_key}"
    exchange.bind_to(CELERY_DELAYED_DELIVERY_EXCHANGE, routing_key=routing_key)
    queue.bind_to(exchange.name, routing_key=routing_key)


def calculate_routing_key(countdown: int, routing_key: str) -> str:
    """Calculate the routing key for publishing a delayed message based on the countdown."""
    if countdown < 1:
        raise ValueError("countdown must be a positive number")

    if not routing_key:
        raise ValueError("routing_key must be non-empty")

    return '.'.join(list(f'{countdown:028b}')) + f'.{routing_key}'
