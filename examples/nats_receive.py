from __future__ import annotations

import sys
from pprint import pformat

from kombu import Connection, Consumer, Exchange, Queue, eventloop

LOCAL_SERVER = "localhost"
DEMO_SERVER = "demo.nats.io"

server = LOCAL_SERVER
use_demo_server = len(sys.argv) > 1 and sys.argv[1] == "--demo"
if use_demo_server:
    server = DEMO_SERVER


exchange = Exchange("exchange", "direct", durable=False)
msg_queue = Queue("kombu_demo", exchange=exchange, routing_key="messages")


def pretty(obj):
    return pformat(obj, indent=4)


def process_msg(body, message):
    print(f"Received message: {body!r}")
    print(f"  properties:\n{pretty(message.properties)}")
    print(f"  delivery_info:\n{pretty(message.delivery_info)}")
    message.ack()


with Connection(f"nats://{server}:4222") as connection:
    with Consumer(connection, msg_queue, callbacks=[process_msg]) as consumer:
        for msg in eventloop(connection):
            pass
