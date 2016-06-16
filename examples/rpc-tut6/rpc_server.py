#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals

from kombu import Connection, Queue
from kombu.mixins import ConsumerProducerMixin

rpc_queue = Queue('rpc_queue')


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


class Worker(ConsumerProducerMixin):

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [Consumer(
            queues=[rpc_queue],
            on_message=self.on_request,
            accept={'application/json'},
            prefetch_count=1,
        )]

    def on_request(self, message):
        n = message.payload['n']
        print(' [.] fib({0})'.format(n))
        result = fib(n)

        self.producer.publish(
            {'result': result},
            exchange='', routing_key=message.properties['reply_to'],
            correlation_id=message.properties['correlation_id'],
            serializer='json',
            retry=True,
        )
        message.ack()


def start_worker(broker_url):
    connection = Connection(broker_url)
    print(' [x] Awaiting RPC requests')
    worker = Worker(connection)
    worker.run()


if __name__ == '__main__':
    try:
        start_worker('pyamqp://')
    except KeyboardInterrupt:
        pass
