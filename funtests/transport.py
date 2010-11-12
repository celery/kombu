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
    consumer.consume()

    while True:
        try:
            conn.drain_events(timeout=1)
        except socket.timeout:
            pass
        if len(messages) >= n:
            break

    consumer.cancel()
    consumer.callback = prev
    return messages


class TransportCase(unittest.TestCase):
    transport = None
    prefix = None
    event_loop_max = 100
    connection_options = {}

    connected = False
    skip_test_reason = None

    def before_connect(self):
        pass

    def after_connect(self, connection):
        pass

    def setUp(self):
        if self.transport:
            try:
                self.before_connect()
            except SkipTest, exc:
                self.skip_test_reason = str(exc)
            else:
                self.do_connect()
            self.exchange = Exchange(self.prefix, "direct")
            self.queue = Queue(self.prefix, self.exchange, self.prefix)

    def purge(self, names):
        chan = self.connection.channel()
        map(chan.queue_purge, names)

    def do_connect(self):
        self.connection = BrokerConnection(transport=self.transport,
                                           **self.connection_options)
        try:
            self.connection.connect()
            self.after_connect(self.connection)
        except self.connection.connection_errors:
            self.skip_test_reason = "%s transport can't connect" % (
                                       self.transport, )
        else:
            self.connected = True

    def verify_alive(self):
        if not self.connected:
            raise SkipTest(self.skip_test_reason)

    def test_produce__consume(self):
        if not self.transport:
            return
        self.verify_alive()
        chan1 = self.connection.channel()
        consumer = Consumer(chan1, self.queue)
        consumer.queues[0].purge()
        producer = Producer(chan1, self.exchange)
        producer.publish({"foo": "bar"}, routing_key=self.prefix)
        message = consumeN(self.connection, consumer)
        self.assertDictEqual(message[0], {"foo": "bar"})
        chan1.close()
        self.purge([self.queue.name])

    def P(self, rest):
        return "%s.%s" % (self.prefix, rest)

    def test_produce__consume_multiple(self):
        if not self.transport:
            return
        self.verify_alive()
        chan1 = self.connection.channel()
        producer = Producer(chan1, self.exchange)
        b1 = Queue(self.P("b1"), self.exchange, "b1")(chan1)
        b2 = Queue(self.P("b2"), self.exchange, "b2")(chan1)
        b3 = Queue(self.P("b3"), self.exchange, "b3")(chan1)
        [q.declare() for q in (b1, b2, b3)]
        [q.purge() for q in (b1, b2, b3)]

        producer.publish("b1", routing_key="b1")
        producer.publish("b2", routing_key="b2")
        producer.publish("b3", routing_key="b3")
        chan1.close()

        chan2 = self.connection.channel()
        consumer = Consumer(chan2, [b1, b2, b3])
        messages = consumeN(self.connection, consumer, 3)
        self.assertItemsEqual(messages, ["b1", "b2", "b3"])
        chan2.close()
        self.purge([self.P("b1"), self.P("b2"), self.P("b3")])

    def test_timeout(self):
        if not self.transport:
            return
        self.verify_alive()
        chan = self.connection.channel()
        self.purge([self.queue.name])
        consumer = Consumer(chan, self.queue)
        self.assertRaises(socket.timeout, self.connection.drain_events,
                timeout=0.3)
        consumer.cancel()

    def test_basic_get(self):
        if not self.transport:
            return
        self.verify_alive()
        chan1 = self.connection.channel()
        producer = Producer(chan1, self.exchange)
        chan2 = self.connection.channel()
        queue = Queue(self.P("basic_get"), self.exchange, "basic_get")
        queue = queue(chan2)
        queue.declare()
        producer.publish({"basic.get": "this"}, routing_key="basic_get")
        chan1.close()

        for i in range(self.event_loop_max):
            m = queue.get()
            if m:
                break
            time.sleep(0.1)
        self.assertEqual(m.payload, {"basic.get": "this"})
        chan2.close()

    def tearDown(self):
        if self.transport and self.connected:
            self.connection.close()
