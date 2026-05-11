from __future__ import annotations

import sys

from nats.js.api import StorageType

from kombu import Connection, Exchange, Queue

LOCAL_SERVER = "localhost"
DEMO_SERVER = "demo.nats.io"

server = LOCAL_SERVER
clean_body = False

for arg in sys.argv[1:]:
    if arg == "--demo":
        server = DEMO_SERVER
    elif arg == "--clean-body":
        clean_body = True


exchange = Exchange("exchange", "direct", durable=False)
msg_queue = Queue("kombu_demo", exchange=exchange, routing_key="messages")

transport_options = {
    "stream_config": {"storage": StorageType.FILE},
    "nats_clean_body": clean_body,
}
print(f"Publishing with nats_clean_body={clean_body} to nats://{server}:4222")

with Connection(f"nats://{server}:4222", transport_options=transport_options) as conn:
    producer = conn.Producer()
    producer.publish(
        "hello world", exchange=exchange, routing_key="messages", declare=[msg_queue]
    )
    print("Message published.")
