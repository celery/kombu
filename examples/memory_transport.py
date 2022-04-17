"""
Example that use memory transport for message produce.
"""
from __future__ import annotations

import time

from kombu import Connection, Consumer, Exchange, Queue

media_exchange = Exchange('media', 'direct')
video_queue = Queue('video', exchange=media_exchange, routing_key='video')
task_queues = [video_queue]


def handle_message(body, message):
    print(f"{time.time()} RECEIVED MESSAGE: {body!r}")
    message.ack()


connection = Connection("memory:///")
consumer = Consumer(connection, task_queues, callbacks=[handle_message])

producer = connection.Producer(serializer='json')
producer.publish(
    {"foo": "bar"},
    exchange=media_exchange,
    routing_key='video',
    declare=task_queues,
)
consumer.consume()
connection.drain_events()
