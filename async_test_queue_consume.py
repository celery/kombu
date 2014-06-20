from __future__ import absolute_import, print_function

import math

from amqp import promise
from kombu import Connection, Exchange, Queue, Consumer
from kombu.async import Hub
from kombu.five import monotonic

N = 10000
TEST_QUEUE = Queue('test3', Exchange('test3'))
FREQ = int(math.ceil(math.sqrt(N)))

program_finished = promise()
time_start = [None]
time_end = [None]


def on_ack_sent(message):
    #if not message.delivery_tag % FREQ:
    #    print('acked: {0:4d}/{1}'.format(message.delivery_tag, N))
    if message.delivery_tag >= N:
        if time_end[0] is None:
            time_end[0] = monotonic()
        consumer.stop(callback=program_finished, close_channel=True)


def on_message(message):
    if time_start[0] is None:
        time_start[0] = monotonic()
    print('received: {0:4d}/{1}'.format(message.delivery_tag, N))
    message.ack(callback=on_ack_sent)


if __name__ == '__main__':
    loop = Hub()
    connection = Connection('pyamqp://')
    consumer = Consumer(connection, [TEST_QUEUE],
                        on_message=on_message, prefetch_count=0)

    connection.register_with_event_loop(loop)
    while not program_finished.ready:
        loop.run_once()
    print('-' * 76)
    print('total time: {0}'.format(time_end[0] - time_start[0]))

