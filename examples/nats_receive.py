from __future__ import annotations

from pprint import pformat

from kombu import Connection, Consumer, Exchange, Queue, eventloop

exchange = Exchange("exchange", "direct", durable=False)
msg_queue = Queue("queue", exchange=exchange, routing_key="messages")


def pretty(obj):
    return pformat(obj, indent=4)


def process_msg(body, message):
    print(f"Received message: {body!r}")
    print(f"  properties:\n{pretty(message.properties)}")
    print(f"  delivery_info:\n{pretty(message.delivery_info)}")
    message.ack()


with Connection("nats://localhost:4222") as connection:
    with Consumer(connection, msg_queue, callbacks=[process_msg]) as consumer:
        for msg in eventloop(connection):
            pass
