"""Native Delayed Delivery API.

Only relevant for RabbitMQ.
"""
from __future__ import annotations

import re

from kombu import Connection, Exchange, Queue, binding
from kombu.log import get_logger

logger = get_logger(__name__)

MAX_NUMBER_OF_BITS_TO_USE = 28
MAX_LEVEL = MAX_NUMBER_OF_BITS_TO_USE - 1
CELERY_DELAYED_DELIVERY_EXCHANGE = "celery_delayed_delivery"

# Matches a delayed-delivery prefix: the ``MAX_NUMBER_OF_BITS_TO_USE`` binary
# digits (each ``0`` or ``1``) that :func:`calculate_routing_key` prepends,
# joined and terminated by dots.
DELAYED_DELIVERY_PREFIX_REGEX = re.compile(
    r'^(?:[01]\.){%d}' % MAX_NUMBER_OF_BITS_TO_USE
)


def level_name(level: int, prefix: str | None = None) -> str:
    """Generates the delayed queue/exchange name based on the level."""
    if level < 0:
        raise ValueError("level must be a non-negative number")

    if prefix:
        return f"{prefix}_celery_delayed_{level}"
    return f"celery_delayed_{level}"


def _celery_delayed_delivery_exchange(prefix: str | None = None) -> str:
    if prefix:
        return f"{prefix}_{CELERY_DELAYED_DELIVERY_EXCHANGE}"
    return CELERY_DELAYED_DELIVERY_EXCHANGE


def declare_native_delayed_delivery_exchanges_and_queues(
    connection: Connection, queue_type: str, prefix: str | None = None
) -> None:
    """Declare all native delayed delivery exchanges and queues.

    The generated delayed exchange/queue names use ``prefix`` when it is a
    non-empty string. In that case, level names are created as
    ``"{prefix}_celery_delayed_{level}"`` and the delivery exchange is named
    ``"{prefix}_celery_delayed_delivery"``. If ``prefix`` is ``None`` or an
    empty string, no prefix is applied and the default names
    ``"celery_delayed_{level}"`` and ``"celery_delayed_delivery"`` are used.
    """
    if queue_type != "classic" and queue_type != "quorum":
        raise ValueError("queue_type must be either classic or quorum")

    channel = connection.channel()

    routing_key: str = "1.#"

    for level in range(27, -1, - 1):
        current_level = level_name(level, prefix)
        next_level = level_name(level - 1, prefix) if level > 0 else None

        delayed_exchange: Exchange = Exchange(
            current_level, type="topic").bind(channel)
        delayed_exchange.declare()

        queue_arguments = {
            "x-queue-type": queue_type,
            "x-overflow": "reject-publish",
            "x-message-ttl": pow(2, level) * 1000,
            "x-dead-letter-exchange": next_level if level > 0 else _celery_delayed_delivery_exchange(prefix),
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
        current_level = level_name(level, prefix)
        next_level = level_name(level - 1, prefix) if level > 0 else None

        next_level_exchange: Exchange = Exchange(
            next_level, type="topic").bind(channel)

        next_level_exchange.bind_to(current_level, routing_key)

        routing_key = "*." + routing_key

    delivery_exchange: Exchange = Exchange(
        _celery_delayed_delivery_exchange(prefix), type="topic").bind(channel)
    delivery_exchange.declare()
    delivery_exchange.bind_to(level_name(0, prefix), routing_key)


def bind_queue_to_native_delayed_delivery_exchange(
    connection: Connection, queue: Queue, prefix: str | None = None
) -> None:
    """Bind a queue to the native delayed delivery exchange.

    When a message arrives at the delivery exchange, it must be forwarded to
    the original exchange and queue. To accomplish this, the function retrieves
    the exchange or binding objects associated with the queue and binds them to
    the delivery exchange.


    :param connection: The connection object used to create and manage the channel.
    :type connection: Connection
    :param queue: The queue to be bound to the native delayed delivery exchange.
    :type queue: Queue
    :param prefix: Optional prefix for delayed delivery exchange and queue names;
        ``None`` or ``""`` means no prefix is applied.
    :type prefix: str | None

    Warning:
    -------
        If a direct exchange is detected, a warning will be logged because
        native delayed delivery does not support direct exchanges.
    """
    channel = connection.channel()
    queue = queue.bind(channel)

    bindings: set[binding] = set()

    if queue.exchange:
        bindings.add(binding(
            queue.exchange,
            routing_key=queue.routing_key,
            arguments=queue.binding_arguments
        ))
    elif queue.bindings:
        bindings = queue.bindings

    for binding_entry in bindings:
        exchange: Exchange = binding_entry.exchange.bind(channel)
        if exchange.type == 'direct':
            logger.warning(f"Exchange {exchange.name} is a direct exchange "
                           f"and native delayed delivery do not support direct exchanges.\n"
                           f"ETA tasks published to this exchange will block the worker until the ETA arrives.")
            continue

        routing_key = binding_entry.routing_key if binding_entry.routing_key.startswith(
            '#') else f"#.{binding_entry.routing_key}"
        exchange.bind_to(_celery_delayed_delivery_exchange(prefix), routing_key=routing_key)
        queue.bind_to(exchange.name, routing_key=routing_key)


def calculate_routing_key(countdown: int, routing_key: str) -> str:
    """Calculate the routing key for publishing a delayed message based on the countdown."""
    if countdown < 1:
        raise ValueError("countdown must be a positive number")

    if not routing_key:
        raise ValueError("routing_key must be non-empty")

    # On sequential delayed retries Celery re-reads the already-prefixed
    # routing key from ``delivery_info`` and passes it back here. Strip any
    # existing delayed-delivery prefix first so prefixes don't accumulate and
    # eventually exceed AMQP's 255-byte short-string limit (see issue #2556).
    routing_key = DELAYED_DELIVERY_PREFIX_REGEX.sub('', routing_key)

    return '.'.join(list(f'{countdown:028b}')) + f'.{routing_key}'
