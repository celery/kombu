from __future__ import absolute_import, print_function

import math

from amqp import promise
from kombu import Connection, Exchange, Queue, Consumer
from kombu.async import Hub
from kombu.five import monotonic

N = 1000
TEST_QUEUE = Queue('test3', Exchange('test3'))
FREQ = int(math.ceil(math.sqrt(N)))

program_finished = promise()


def on_ack_sent(message):
    if not message.delivery_tag % FREQ:
        print('acked: {0:4d}/{1}'.format(message.delivery_tag, N))
    if message.delivery_tag >= N:
        consumer.stop(callback=program_finished, close_channel=True)


def on_message(message):
    message.ack(callback=on_ack_sent)


if __name__ == '__main__':
    loop = Hub()
    connection = Connection('pyamqp://')
    consumer = Consumer(connection, [TEST_QUEUE],
                        on_message=on_message, prefetch_count=10)

    connection.register_with_event_loop(loop)
    time_start = monotonic()
    while not program_finished.ready:
        loop.run_once()
    print('-' * 76)
    print('total time: {0}'.format(monotonic() - time_start))

