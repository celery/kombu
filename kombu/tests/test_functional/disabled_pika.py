import socket
import time
import unittest2 as unittest

from nose import SkipTest

from kombu import BrokerConnection
from kombu import Producer, Consumer, Exchange, Queue


def consumeN(conn, consumer, n=1):
    messages = []

    def callback(message_data, message):
        messages.append(message_data)
        message.ack()

    prev, consumer.callbacks = consumer.callbacks, [callback]

    while True:
        conn.drain_events(timeout=1)
        if len(messages) >= n:
            break

    consumer.callback = prev
    return messages


class test_pika(unittest.TestCase):

    def purge(self, names):
        chan = self.connection.channel()
        map(chan.queue_purge, names)

    def setUp(self):
        self.connection = BrokerConnection(backend_cls="pika")
        try:
            self.connection.connect()
        except socket.error:
            self.connected = False
        else:
            self.connected = True
        self.exchange = Exchange("tamqplib", "direct")
        self.queue = Queue("tamqplib", self.exchange, "tamqplib")

    def test_produce__consume(self):
        if not self.connected:
            raise SkipTest("Broker not running.")
        chan1 = self.connection.channel()
        producer = Producer(chan1, self.exchange)

        producer.publish({"foo": "bar"}, routing_key="tamqplib")
        chan1.close()

        chan2 = self.connection.channel()
        consumer = Consumer(chan2, self.queue)
        message = consumeN(self.connection, consumer)
        self.assertDictEqual(message[0], {"foo": "bar"})
        chan2.close()
        self.purge(["tamqplib"])

    def test_produce__consume_multiple(self):
        if not self.connected:
            raise SkipTest("Broker not running.")
        chan1 = self.connection.channel()
        producer = Producer(chan1, self.exchange)
        b1 = Queue("pyamqplib.b1", self.exchange, "b1")
        b2 = Queue("pyamqplib.b2", self.exchange, "b2")
        b3 = Queue("pyamqplib.b3", self.exchange, "b3")

        producer.publish("b1", routing_key="b1")
        producer.publish("b2", routing_key="b2")
        producer.publish("b3", routing_key="b3")
        chan1.close()

        chan2 = self.connection.channel()
        consumer = Consumer(chan2, [b1, b2, b3])
        messages = consumeN(self.connection, consumer, 3)
        self.assertItemsEqual(messages, ["b1", "b2", "b3"])
        chan2.close()
        self.purge(["pyamqplib.b1", "pyamqplib.b2", "pyamqplib.b3"])

    def test_timeout(self):
        if not self.connected:
            raise SkipTest("Broker not running.")
        chan = self.connection.channel()
        self.purge([self.queue.name])
        consumer = Consumer(chan, self.queue)
        self.assertRaises(socket.timeout, self.connection.drain_events,
                timeout=0.3)
        consumer.cancel()

    def test_basic_get(self):
        chan1 = self.connection.channel()
        producer = Producer(chan1, self.exchange)
        producer.publish({"basic.get": "this"}, routing_key="basic_get")
        chan1.close()

        chan2 = self.connection.channel()
        queue = Queue("amqplib_basic_get", self.exchange, "basic_get")
        queue = queue(chan2)
        queue.declare()
        for i in range(50):
            m = queue.get()
            if m:
                break
            time.sleep(0.1)
        self.assertEqual(m.payload, {"basic.get": "this"})
        chan2.close()

    def tearDown(self):
        if self.connected:
            self.connection.close()
