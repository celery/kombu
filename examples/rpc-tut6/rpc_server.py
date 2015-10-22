#!/usr/bin/env python

from kombu import Connection, Producer, Queue
from kombu.mixins import ConsumerMixin

rpc_queue = Queue('rpc_queue')


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n-1) + fib(n-2)


class Worker(ConsumerMixin):
    _producer_connection = None

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

        with Producer(self.producer_connection) as producer:
            producer.publish(
                {'result': result},
                exchange='', routing_key=message.properties['reply_to'],
                correlation_id=message.properties['correlation_id'],
                serializer='json',
            )
        message.ack()

    def on_consume_end(self, connection, channel):
        if self._producer_connection is not None:
            self._producer_connection.close()
            self._producer_connection = None

    @property
    def producer_connection(self):
        if self._producer_connection is None:
            conn = self.connection.clone()
            conn.ensure_connection(self.on_connection_error,
                                   self.connect_max_retries)
            self._producer_connection = conn
        return self._producer_connection


def start_worker(broker_url):
    connection = Connection(broker_url)
    print " [x] Awaiting RPC requests"
    worker = Worker(connection)
    worker.run()


if __name__ == '__main__':
    start_worker('pyamqp://')
