from __future__ import annotations

import sys

from nats.js.api import StorageType

from kombu import Connection, Exchange, Queue

LOCAL_SERVER = "localhost"
DEMO_SERVER = "demo.nats.io"

server = LOCAL_SERVER
use_demo_server = len(sys.argv) > 1 and sys.argv[1] == "--demo"
if use_demo_server:
    server = DEMO_SERVER


exchange = Exchange("exchange", "direct", durable=False)
msg_queue = Queue("kombu_demo", exchange=exchange, routing_key="messages")


with Connection(f"nats://{server}:4222", transport_options={
    "stream_config": {
        "storage": StorageType.FILE,
    }
}) as conn:
    producer = conn.Producer()
    producer.publish(
        "hello world", exchange=exchange, routing_key="messages", declare=[msg_queue]
    )
