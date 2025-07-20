#!/usr/bin/env python3

from __future__ import annotations

from kombu import Connection, Consumer, Producer, Queue, uuid


class FibonacciRpcClient:

    def __init__(self, connection):
        self.connection = connection
        self.callback_queue = Queue(uuid(), exclusive=True, auto_delete=True)

    def on_response(self, message):
        if message.properties['correlation_id'] == self.correlation_id:
            self.response = message.payload['result']

    def call(self, n):
        self.response = None
        self.correlation_id = uuid()
        with Producer(self.connection) as producer:
            producer.publish(
                {'n': n},
                exchange='',
                routing_key='rpc_queue',
                declare=[self.callback_queue],
                reply_to=self.callback_queue.name,
                correlation_id=self.correlation_id,
            )
        with Consumer(self.connection,
                      on_message=self.on_response,
                      queues=[self.callback_queue], no_ack=True):
            while self.response is None:
                self.connection.drain_events()
        return self.response


def main(broker_url):
    connection = Connection(broker_url)
    fibonacci_rpc = FibonacciRpcClient(connection)
    print(' [x] Requesting fib(30)')
    response = fibonacci_rpc.call(30)
    print(f' [.] Got {response!r}')


if __name__ == '__main__':
    main('pyamqp://')
