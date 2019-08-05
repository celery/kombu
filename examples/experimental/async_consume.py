#!/usr/bin/env python

from kombu import Connection, Exchange, Queue, Producer, Consumer
from kombu.asynchronous import Hub

hub = Hub()
exchange = Exchange('asynt')
queue = Queue('asynt', exchange, 'asynt')


def send_message(conn):
    producer = Producer(conn)
    producer.publish('hello world', exchange=exchange, routing_key='asynt')
    print('MESSAGE SENT')


def on_message(message):
    print('RECEIVED: %r' % (message.body, ))
    message.ack()
    hub.stop()  # <-- exit after one message


if __name__ == '__main__':
    conn = Connection('amqp://')
    conn.register_with_event_loop(hub)

    with Consumer(conn, [queue], on_message=on_message):
        send_message(conn)
        hub.run_forever()
