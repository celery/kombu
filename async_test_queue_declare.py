from __future__ import with_statement, print_function

import math

from amqp import barrier
from kombu import Connection, Exchange, Queue, Producer
from kombu.async import Hub
from kombu.five import monotonic

N = 10000
TEST_QUEUE = Queue('test3', Exchange('test3'))
FREQ = int(math.ceil(math.sqrt(N)))


def on_message_written(delivery_tag):
    if not delivery_tag % FREQ:
        print('message sent: {0:4d}/{1}'.format(delivery_tag, N))


def send_messages(connection):
    producer = Producer(connection, auto_declare=False)
    return barrier([
        producer.publish({'hello': 'x'},
                         exchange=TEST_QUEUE.exchange,
                         routing_key=TEST_QUEUE.routing_key,
                         declare=[TEST_QUEUE],
                         callback=on_message_written)
        for i in range(N)
    ])

if __name__ == '__main__':
    loop = Hub()
    connection = Connection('pyamqp://')
    connection.register_with_event_loop(loop)

    time_start = monotonic()
    sending = send_messages(connection)
    while not sending.ready:
        loop.run_once()
    print('-' * 76)
    print('total time: {0}'.format(monotonic() - time_start))
