from __future__ import annotations

from kombu import Connection, Exchange, Queue

exchange = Exchange("exchange", "direct", durable=False)
msg_queue = Queue("queue", exchange=exchange, routing_key="messages")


with Connection("nats://localhost:4222") as conn:
    producer = conn.Producer()
    producer.publish(
        "hello world", exchange=exchange, routing_key="messages", declare=[msg_queue]
    )
