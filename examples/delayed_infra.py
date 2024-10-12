from __future__ import annotations

from examples.experimental.async_consume import queue
from kombu import Connection, Exchange, Queue
from kombu.transport.native_delayed_delivery import (
    bind_queue_to_native_delayed_delivery_exchange, calculate_routing_key,
    declare_native_delayed_delivery_exchanges_and_queues, level_name)

with Connection('amqp://guest:guest@localhost:5672//') as connection:
    declare_native_delayed_delivery_exchanges_and_queues(connection, 'quorum')

    destination_exchange = Exchange(
        'destination', type='topic')
    destination_queue = Queue("destination", exchange=destination_exchange)
    bind_queue_to_native_delayed_delivery_exchange(connection, queue)

    channel = connection.channel()
    with connection.Producer(channel=channel) as producer:
        routing_key = calculate_routing_key(30, 'destination')
        producer.publish(
            "delayed msg",
            routing_key=routing_key,
            exchange=level_name(27)
        )
