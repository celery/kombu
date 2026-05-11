from __future__ import annotations

import sys
from pprint import pformat

from kombu import Connection, Consumer, Exchange, Queue, eventloop

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


def pretty(obj):
    return pformat(obj, indent=4)


def process_msg(body, message):
    print(f"Received message: {body!r}")
    print(f"  properties:\n{pretty(message.properties)}")
    print(f"  delivery_info:\n{pretty(message.delivery_info)}")
    message.ack()


print(f"Consuming with nats_clean_body={clean_body} from nats://{server}:4222")

with Connection(
    f"nats://{server}:4222",
    transport_options={"nats_clean_body": clean_body},
) as connection:
    with Consumer(connection, msg_queue, callbacks=[process_msg]) as consumer:
        for _ in eventloop(connection):
            pass
