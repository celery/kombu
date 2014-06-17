from __future__ import absolute_import, print_function

from amqp import promise
from kombu import Connection, Exchange, Queue, Consumer, Producer
from kombu.async import Hub

N = 10000
TEST_QUEUE = Queue('test3', Exchange('test3'))

program_finished = promise()
first_message_sent_at = [0]


class Producing(object):

    def __init__(self, loop, connection):
        self.loop = loop
        self.connection = connection
        self.producer = None

    def start(self):
        self.connection.register_with_event_loop(self.loop)
        self.producer = Producer(self.connection, auto_declare=False)
        for i in range(N):
            self.producer.publish(
                {'hello': i},
                exchange=TEST_QUEUE.exchange,
                routing_key=TEST_QUEUE.routing_key,
                declare=[TEST_QUEUE],
                callback=self.on_message_sent,
            )

    def on_message_sent(self, delivery_tag):
        print('>>> message sent: {0:4d}/{1}'.format(delivery_tag, N))



class Consuming(object):

    def __init__(self, loop, connection):
        self.loop = loop
        self.connection = connection
        self.consumer = None

    def start(self):
        self.connection.register_with_event_loop(self.loop)
        self.consumer = Consumer(
            self.connection, [TEST_QUEUE],
            on_message=self.on_message, prefetch_count=10,
        )

    def stop(self):
        return self.consumer.stop(callback=program_finished,
                close_channel=True)

    def on_acknowledged(self, message):
        if message.delivery_tag >= N:
            self.stop()

    def on_message(self, message):
        print('<<< message received: {0:4d}/{1}'.format(
            message.delivery_tag, N,
        ))
        message.ack(callback=self.on_acknowledged)


if __name__ == '__main__':
    loop = Hub()
    connection = Connection('pyamqp://')

    producer = Producing(loop, connection.clone())
    producer.start()

    consumer = Consuming(loop, connection.clone())
    consumer.start()

    while not program_finished.ready:
        loop.run_once()
