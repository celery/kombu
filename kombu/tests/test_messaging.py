from __future__ import absolute_import

import anyjson

from ..connection import BrokerConnection
from ..exceptions import MessageStateError
from ..messaging import Consumer, Producer
from ..entity import Exchange, Queue

from .mocks import Transport
from .utils import unittest


class test_Producer(unittest.TestCase):

    def setUp(self):
        self.exchange = Exchange("foo", "direct")
        self.connection = BrokerConnection(transport=Transport)
        self.connection.connect()
        self.assertTrue(self.connection.connection.connected)
        self.assertFalse(self.exchange.is_bound)

    def test_auto_declare(self):
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, auto_declare=True)
        self.assertIsNot(p.exchange, self.exchange,
                         "creates Exchange clone at bind")
        self.assertTrue(p.exchange.is_bound)
        self.assertIn("exchange_declare", channel,
                      "auto_declare declares exchange")

    def test_manual_declare(self):
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, auto_declare=False)
        self.assertTrue(p.exchange.is_bound)
        self.assertNotIn("exchange_declare", channel,
                         "auto_declare=False does not declare exchange")
        p.declare()
        self.assertIn("exchange_declare", channel,
                      "p.declare() declares exchange")

    def test_prepare(self):
        message = {u"the quick brown fox": u"jumps over the lazy dog"}
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer="json")
        m, ctype, cencoding = p._prepare(message, headers={})
        self.assertDictEqual(message, anyjson.deserialize(m))
        self.assertEqual(ctype, "application/json")
        self.assertEqual(cencoding, "utf-8")

    def test_prepare_compression(self):
        message = {u"the quick brown fox": u"jumps over the lazy dog"}
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer="json")
        headers = {}
        m, ctype, cencoding = p._prepare(message, compression="zlib",
                                         headers=headers)
        self.assertEqual(ctype, "application/json")
        self.assertEqual(cencoding, "utf-8")
        self.assertEqual(headers["compression"], "application/x-gzip")
        import zlib
        self.assertEqual(anyjson.deserialize(
                            zlib.decompress(m).decode("utf-8")), message)

    def test_prepare_custom_content_type(self):
        message = "the quick brown fox".encode("utf-8")
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer="json")
        m, ctype, cencoding = p._prepare(message, content_type="custom")
        self.assertEqual(m, message)
        self.assertEqual(ctype, "custom")
        self.assertEqual(cencoding, "binary")
        m, ctype, cencoding = p._prepare(message, content_type="custom",
                                         content_encoding="alien")
        self.assertEqual(m, message)
        self.assertEqual(ctype, "custom")
        self.assertEqual(cencoding, "alien")

    def test_prepare_is_already_unicode(self):
        message = u"the quick brown fox"
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer="json")
        m, ctype, cencoding = p._prepare(message, content_type="text/plain")
        self.assertEqual(m, message.encode("utf-8"))
        self.assertEqual(ctype, "text/plain")
        self.assertEqual(cencoding, "utf-8")
        m, ctype, cencoding = p._prepare(message, content_type="text/plain",
                                        content_encoding="utf-8")
        self.assertEqual(m, message.encode("utf-8"))
        self.assertEqual(ctype, "text/plain")
        self.assertEqual(cencoding, "utf-8")

    def test_publish(self):
        channel = self.connection.channel()
        p = Producer(channel, self.exchange, serializer="json")
        message = {u"the quick brown fox": u"jumps over the lazy dog"}
        ret = p.publish(message, routing_key="process")
        self.assertIn("prepare_message", channel)
        self.assertIn("basic_publish", channel)

        m, exc, rkey = ret
        self.assertDictEqual(message, anyjson.deserialize(m["body"]))
        self.assertDictContainsSubset({"content_type": "application/json",
                                       "content_encoding": "utf-8",
                                       "priority": 0}, m)
        self.assertDictContainsSubset({"delivery_mode": 2}, m["properties"])
        self.assertEqual(exc, p.exchange.name)
        self.assertEqual(rkey, "process")

    def test_no_exchange(self):
        chan = self.connection.channel()
        p = Producer(chan)
        self.assertFalse(p.exchange.name)

    def test_revive(self):
        chan = self.connection.channel()
        p = Producer(chan)
        chan2 = self.connection.channel()
        p.revive(chan2)
        self.assertIs(p.channel, chan2)
        self.assertIs(p.exchange.channel, chan2)

    def test_on_return(self):
        chan = self.connection.channel()

        def on_return(exception, exchange, routing_key, message):
            pass

        p = Producer(chan, on_return=on_return)
        self.assertTrue(on_return in chan.events["basic_return"])
        self.assertTrue(p.on_return)


class test_Consumer(unittest.TestCase):

    def setUp(self):
        self.connection = BrokerConnection(transport=Transport)
        self.connection.connect()
        self.assertTrue(self.connection.connection.connected)
        self.exchange = Exchange("foo", "direct")

    def test_set_no_ack(self):
        channel = self.connection.channel()
        queue = Queue("qname", self.exchange, "rkey")
        consumer = Consumer(channel, queue, auto_declare=True, no_ack=True)
        self.assertTrue(consumer.no_ack)

    def test_set_callbacks(self):
        channel = self.connection.channel()
        queue = Queue("qname", self.exchange, "rkey")
        callbacks = [lambda x, y: x,
                     lambda x, y: x]
        consumer = Consumer(channel, queue, auto_declare=True,
                            callbacks=callbacks)
        self.assertEqual(consumer.callbacks, callbacks)

    def test_auto_declare(self):
        channel = self.connection.channel()
        queue = Queue("qname", self.exchange, "rkey")
        consumer = Consumer(channel, queue, auto_declare=True)
        consumer.consume()
        consumer.consume()  # twice is a noop
        self.assertIsNot(consumer.queues[0], queue)
        self.assertTrue(consumer.queues[0].is_bound)
        self.assertTrue(consumer.queues[0].exchange.is_bound)
        self.assertIsNot(consumer.queues[0].exchange, self.exchange)

        for meth in ("exchange_declare",
                     "queue_declare",
                     "queue_bind",
                     "basic_consume"):
            self.assertIn(meth, channel)
        self.assertEqual(channel.called.count("basic_consume"), 1)
        self.assertTrue(consumer._active_tags)

        consumer.cancel_by_queue(queue.name)
        consumer.cancel_by_queue(queue.name)
        self.assertFalse(consumer._active_tags)

    def test_manual_declare(self):
        channel = self.connection.channel()
        queue = Queue("qname", self.exchange, "rkey")
        consumer = Consumer(channel, queue, auto_declare=False)
        self.assertIsNot(consumer.queues[0], queue)
        self.assertTrue(consumer.queues[0].is_bound)
        self.assertTrue(consumer.queues[0].exchange.is_bound)
        self.assertIsNot(consumer.queues[0].exchange, self.exchange)

        for meth in ("exchange_declare",
                     "queue_declare",
                     "basic_consume"):
            self.assertNotIn(meth, channel)

        consumer.declare()
        for meth in ("exchange_declare",
                     "queue_declare",
                     "queue_bind"):
            self.assertIn(meth, channel)
        self.assertNotIn("basic_consume", channel)

        consumer.consume()
        self.assertIn("basic_consume", channel)

    def test_consume__cancel(self):
        channel = self.connection.channel()
        queue = Queue("qname", self.exchange, "rkey")
        consumer = Consumer(channel, queue, auto_declare=True)
        consumer.consume()
        consumer.cancel()
        self.assertIn("basic_cancel", channel)
        self.assertFalse(consumer._active_tags)

    def test___enter____exit__(self):
        channel = self.connection.channel()
        queue = Queue("qname", self.exchange, "rkey")
        consumer = Consumer(channel, queue, auto_declare=True)
        context = consumer.__enter__()
        self.assertIs(context, consumer)
        self.assertTrue(consumer._active_tags)
        res = consumer.__exit__(None, None, None)
        self.assertFalse(res)
        self.assertIn("basic_cancel", channel)
        self.assertFalse(consumer._active_tags)

    def test_flow(self):
        channel = self.connection.channel()
        queue = Queue("qname", self.exchange, "rkey")
        consumer = Consumer(channel, queue, auto_declare=True)
        consumer.flow(False)
        self.assertIn("flow", channel)

    def test_qos(self):
        channel = self.connection.channel()
        queue = Queue("qname", self.exchange, "rkey")
        consumer = Consumer(channel, queue, auto_declare=True)
        consumer.qos(30, 10, False)
        self.assertIn("basic_qos", channel)

    def test_purge(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        b2 = Queue("qname2", self.exchange, "rkey")
        b3 = Queue("qname3", self.exchange, "rkey")
        b4 = Queue("qname4", self.exchange, "rkey")
        consumer = Consumer(channel, [b1, b2, b3, b4], auto_declare=True)
        consumer.purge()
        self.assertEqual(channel.called.count("queue_purge"), 4)

    def test_multiple_queues(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        b2 = Queue("qname2", self.exchange, "rkey")
        b3 = Queue("qname3", self.exchange, "rkey")
        b4 = Queue("qname4", self.exchange, "rkey")
        consumer = Consumer(channel, [b1, b2, b3, b4])
        consumer.consume()
        self.assertEqual(channel.called.count("exchange_declare"), 4)
        self.assertEqual(channel.called.count("queue_declare"), 4)
        self.assertEqual(channel.called.count("queue_bind"), 4)
        self.assertEqual(channel.called.count("basic_consume"), 4)
        self.assertEqual(len(consumer._active_tags), 4)
        consumer.cancel()
        self.assertEqual(channel.called.count("basic_cancel"), 4)
        self.assertFalse(len(consumer._active_tags))

    def test_receive_callback(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])
        received = []

        def callback(message_data, message):
            received.append(message_data)
            message.ack()
            message.payload     # trigger cache

        consumer.register_callback(callback)
        consumer._receive_callback({u"foo": u"bar"})

        self.assertIn("basic_ack", channel)
        self.assertIn("message_to_python", channel)
        self.assertEqual(received[0], {u"foo": u"bar"})

    def test_basic_ack_twice(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.ack()
            message.ack()

        consumer.register_callback(callback)
        self.assertRaises(MessageStateError,
                          consumer._receive_callback, {"foo": "bar"})

    def test_basic_reject(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.reject()

        consumer.register_callback(callback)
        consumer._receive_callback({"foo": "bar"})
        self.assertIn("basic_reject", channel)

    def test_basic_reject_twice(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.reject()
            message.reject()

        consumer.register_callback(callback)
        self.assertRaises(MessageStateError,
                          consumer._receive_callback, {"foo": "bar"})
        self.assertIn("basic_reject", channel)

    def test_basic_reject__requeue(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.requeue()

        consumer.register_callback(callback)
        consumer._receive_callback({"foo": "bar"})
        self.assertIn("basic_reject:requeue", channel)

    def test_basic_reject__requeue_twice(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])

        def callback(message_data, message):
            message.requeue()
            message.requeue()

        consumer.register_callback(callback)
        self.assertRaises(MessageStateError,
                          consumer._receive_callback, {"foo": "bar"})
        self.assertIn("basic_reject:requeue", channel)

    def test_receive_without_callbacks_raises(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])
        self.assertRaises(NotImplementedError, consumer.receive, 1, 2)

    def test_decode_error(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])
        consumer.channel.throw_decode_error = True

        self.assertRaises(ValueError,
                consumer._receive_callback, {"foo": "bar"})

    def test_on_decode_error_callback(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        thrown = []

        def on_decode_error(msg, exc):
            thrown.append((msg.body, exc))

        consumer = Consumer(channel, [b1], on_decode_error=on_decode_error)
        consumer.channel.throw_decode_error = True
        consumer._receive_callback({"foo": "bar"})

        self.assertTrue(thrown)
        m, exc = thrown[0]
        self.assertEqual(anyjson.deserialize(m), {"foo": "bar"})
        self.assertIsInstance(exc, ValueError)

    def test_recover(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])
        consumer.recover()
        self.assertIn("basic_recover", channel)

    def test_revive(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        consumer = Consumer(channel, [b1])
        channel2 = self.connection.channel()
        consumer.revive(channel2)
        self.assertIs(consumer.channel, channel2)
        self.assertIs(consumer.queues[0].channel, channel2)
        self.assertIs(consumer.queues[0].exchange.channel, channel2)

    def test__repr__(self):
        channel = self.connection.channel()
        b1 = Queue("qname1", self.exchange, "rkey")
        self.assertTrue(repr(Consumer(channel, [b1])))
