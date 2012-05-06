from __future__ import absolute_import
from __future__ import with_statement

import socket

from mock import patch

from kombu import common
from kombu.common import (Broadcast, maybe_declare,
                          send_reply, isend_reply, collect_replies)

from .utils import TestCase
from .utils import ContextMock, Mock, MockPool


class test_Broadcast(TestCase):

    def test_arguments(self):
        q = Broadcast(name="test_Broadcast")
        self.assertTrue(q.name.startswith("bcast."))
        self.assertEqual(q.alias, "test_Broadcast")
        self.assertTrue(q.auto_delete)
        self.assertEqual(q.exchange.name, "test_Broadcast")
        self.assertEqual(q.exchange.type, "fanout")

        q = Broadcast("test_Broadcast", "explicit_queue_name")
        self.assertEqual(q.name, "explicit_queue_name")
        self.assertEqual(q.exchange.name, "test_Broadcast")


class test_maybe_declare(TestCase):

    def test_cacheable(self):
        channel = Mock()
        client = channel.connection.client = Mock()
        client.declared_entities = set()
        entity = Mock()
        entity.can_cache_declaration = True
        entity.is_bound = True

        maybe_declare(entity, channel)
        self.assertEqual(entity.declare.call_count, 1)
        self.assertIn(entity, channel.connection.client.declared_entities)

        maybe_declare(entity, channel)
        self.assertEqual(entity.declare.call_count, 1)

    def test_binds_entities(self):
        channel = Mock()
        channel.connection.client.declared_entities = set()
        entity = Mock()
        entity.can_cache_declaration = True
        entity.is_bound = False

        maybe_declare(entity, channel)
        entity.bind.assert_called_with(channel)

    def test_with_retry(self):
        channel = Mock()
        entity = Mock()
        entity.can_cache_declaration = True
        entity.is_bound = True

        maybe_declare(entity, channel, retry=True)
        self.assertTrue(channel.connection.client.ensure.call_count)


class test_replies(TestCase):

    def test_send_reply(self):
        req = Mock()
        req.content_type = "application/json"
        req.properties = {"reply_to": "hello",
                          "correlation_id": "world"}
        exchange = Mock()
        exchange.is_bound = True
        producer = Mock()
        producer.channel.connection.client.declared_entities = set()
        send_reply(exchange, req, {"hello": "world"}, producer)

        self.assertTrue(producer.publish.call_count)
        args = producer.publish.call_args
        self.assertDictEqual(args[0][0], {"hello": "world"})
        self.assertDictEqual(args[1], {"exchange": exchange,
                                       "routing_key": "hello",
                                       "correlation_id": "world",
                                       "serializer": "json"})

        exchange.declare.assert_called_with()

    @patch("kombu.common.ipublish")
    def test_isend_reply(self, ipublish):
        pool, exchange, req, msg, props = (Mock(), Mock(), Mock(),
                                           Mock(), Mock())

        isend_reply(pool, exchange, req, msg, props)
        ipublish.assert_called_with(pool, send_reply,
                                    (exchange, req, msg), props)

    @patch("kombu.common.itermessages")
    def test_collect_replies_with_ack(self, itermessages):
        conn, channel, queue = Mock(), Mock(), Mock()
        body, message = Mock(), Mock()
        itermessages.return_value = [(body, message)]
        it = collect_replies(conn, channel, queue, no_ack=False)
        m = it.next()
        self.assertIs(m, body)
        itermessages.assert_called_with(conn, channel, queue, no_ack=False)
        message.ack.assert_called_with()

        with self.assertRaises(StopIteration):
            it.next()

        channel.after_reply_message_received.assert_called_with(queue.name)

    @patch("kombu.common.itermessages")
    def test_collect_replies_no_ack(self, itermessages):
        conn, channel, queue = Mock(), Mock(), Mock()
        body, message = Mock(), Mock()
        itermessages.return_value = [(body, message)]
        it = collect_replies(conn, channel, queue)
        m = it.next()
        self.assertIs(m, body)
        itermessages.assert_called_with(conn, channel, queue, no_ack=True)
        self.assertFalse(message.ack.called)

    @patch("kombu.common.itermessages")
    def test_collect_replies_no_replies(self, itermessages):
        conn, channel, queue = Mock(), Mock(), Mock()
        itermessages.return_value = []
        it = collect_replies(conn, channel, queue)
        with self.assertRaises(StopIteration):
            it.next()

        self.assertFalse(channel.after_reply_message_received.called)


class test_insured(TestCase):

    @patch("kombu.common.insured_logger")
    def test_ensure_errback(self, insured_logger):
        common._ensure_errback("foo", 30)
        self.assertTrue(insured_logger.error.called)

    def test_revive_connection(self):
        on_revive = Mock()
        channel = Mock()
        common.revive_connection(Mock(), channel, on_revive)
        on_revive.assert_called_with(channel)

        common.revive_connection(Mock(), channel, None)

    def test_revive_producer(self):
        on_revive = Mock()
        channel = Mock()
        common.revive_producer(Mock(), channel, on_revive)
        on_revive.assert_called_with(channel)

        common.revive_producer(Mock(), channel, None)

    def get_insured_mocks(self, insured_returns=("works", "ignored")):
        conn = ContextMock()
        pool = MockPool(conn)
        fun = Mock()
        insured = conn.autoretry.return_value = Mock()
        insured.return_value = insured_returns
        return conn, pool, fun, insured

    def test_insured(self):
        conn, pool, fun, insured = self.get_insured_mocks()

        ret = common.insured(pool, fun, (2, 2), {"foo": "bar"})
        self.assertEqual(ret, "works")
        conn.ensure_connection.assert_called_with(
                errback=common._ensure_errback)

        self.assertTrue(insured.called)
        i_args, i_kwargs = insured.call_args
        self.assertTupleEqual(i_args, (2, 2))
        self.assertDictEqual(i_kwargs, {"foo": "bar",
                                        "connection": conn})

        self.assertTrue(conn.autoretry.called)
        ar_args, ar_kwargs = conn.autoretry.call_args
        self.assertTupleEqual(ar_args, (fun, conn.default_channel))
        self.assertTrue(ar_kwargs.get("on_revive"))
        self.assertTrue(ar_kwargs.get("errback"))

    def test_insured_custom_errback(self):
        conn, pool, fun, insured = self.get_insured_mocks()

        custom_errback = Mock()
        common.insured(pool, fun, (2, 2), {"foo": "bar"},
                       errback=custom_errback)
        conn.ensure_connection.assert_called_with(errback=custom_errback)

    def get_ipublish_args(self, ensure_returns=None):
        producer = ContextMock()
        pool = MockPool(producer)
        fun = Mock()
        ensure_returns = ensure_returns or Mock()

        producer.connection.ensure.return_value = ensure_returns

        return producer, pool, fun, ensure_returns

    def test_ipublish(self):
        producer, pool, fun, ensure_returns = self.get_ipublish_args()
        ensure_returns.return_value = "works"

        ret = common.ipublish(pool, fun, (2, 2), {"foo": "bar"})
        self.assertEqual(ret, "works")

        self.assertTrue(producer.connection.ensure.called)
        e_args, e_kwargs = producer.connection.ensure.call_args
        self.assertTupleEqual(e_args, (producer, fun))
        self.assertTrue(e_kwargs.get("on_revive"))
        self.assertEqual(e_kwargs.get("errback"), common._ensure_errback)

        ensure_returns.assert_called_with(2, 2, foo="bar", producer=producer)

    def test_ipublish_with_custom_errback(self):
        producer, pool, fun, _ = self.get_ipublish_args()

        errback = Mock()
        common.ipublish(pool, fun, (2, 2), {"foo": "bar"}, errback=errback)
        _, e_kwargs = producer.connection.ensure.call_args
        self.assertEqual(e_kwargs.get("errback"), errback)


class MockConsumer(object):
    consumers = set()

    def __init__(self, channel, queues=None, callbacks=None, **kwargs):
        self.channel = channel
        self.queues = queues
        self.callbacks = callbacks

    def __enter__(self):
        self.consumers.add(self)
        return self

    def __exit__(self, *exc_info):
        self.consumers.discard(self)


class test_itermessages(TestCase):

    class MockConnection(object):
        should_raise_timeout = False

        def drain_events(self, **kwargs):
            if self.should_raise_timeout:
                raise socket.timeout()
            for consumer in MockConsumer.consumers:
                for callback in consumer.callbacks:
                    callback("body", "message")

    def test_default(self):
        conn = self.MockConnection()
        channel = Mock()
        channel.connection.client = conn
        it = common.itermessages(conn, channel, "q", limit=1,
                                 Consumer=MockConsumer)

        ret = it.next()
        self.assertTupleEqual(ret, ("body", "message"))

        with self.assertRaises(StopIteration):
            it.next()

    def test_when_raises_socket_timeout(self):
        conn = self.MockConnection()
        conn.should_raise_timeout = True
        channel = Mock()
        channel.connection.client = conn
        it = common.itermessages(conn, channel, "q", limit=1,
                                 Consumer=MockConsumer)

        with self.assertRaises(StopIteration):
            it.next()

    @patch("kombu.common.deque")
    def test_when_raises_IndexError(self, deque):
        deque_instance = deque.return_value = Mock()
        deque_instance.popleft.side_effect = IndexError()
        conn = self.MockConnection()
        channel = Mock()
        it = common.itermessages(conn, channel, "q", limit=1,
                                 Consumer=MockConsumer)

        with self.assertRaises(StopIteration):
            it.next()
