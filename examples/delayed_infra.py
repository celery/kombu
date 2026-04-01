from __future__ import annotations

from kombu import Connection, Exchange, Queue
from kombu.transport.native_delayed_delivery import (
    bind_queue_to_native_delayed_delivery_exchange, calculate_routing_key,
    declare_native_delayed_delivery_exchanges_and_queues, level_name)

with Connection('amqp://guest:guest@localhost:5672//') as connection:
    declare_native_delayed_delivery_exchanges_and_queues(connection, 'quorum')
    channel = connection.channel()

    destination_exchange = Exchange('destination_exchange', type='topic')
    queue = Queue("destination", exchange=destination_exchange, routing_key='destination_route')
    queue.declare(channel=connection.channel())

    bind_queue_to_native_delayed_delivery_exchange(connection, queue)
    with connection.Producer(channel=channel) as producer:
        routing_key = calculate_routing_key(30, 'destination_route')
        producer.publish(
            "delayed msg",
            routing_key=routing_key,
            exchange=level_name(27)
        )
