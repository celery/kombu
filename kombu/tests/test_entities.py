from __future__ import absolute_import
from __future__ import with_statement

from .. import Connection
from ..entity import Exchange, Queue
from ..exceptions import NotBoundError

from .mocks import Transport
from .utils import TestCase
from .utils import Mock


def get_conn():
    return Connection(transport=Transport)


class test_Exchange(TestCase):

    def test_bound(self):
        exchange = Exchange("foo", "direct")
        self.assertFalse(exchange.is_bound)
        self.assertIn("<unbound", repr(exchange))

        chan = get_conn().channel()
        bound = exchange.bind(chan)
        self.assertTrue(bound.is_bound)
        self.assertIs(bound.channel, chan)
        self.assertIn("<bound", repr(bound))

    def test_hash(self):
        self.assertEqual(hash(Exchange("a")), hash(Exchange("a")))
        self.assertNotEqual(hash(Exchange("a")), hash(Exchange("b")))

    def test_can_cache_declaration(self):
        self.assertTrue(Exchange("a", durable=True).can_cache_declaration)
        self.assertFalse(Exchange("a", durable=False).can_cache_declaration)

    def test_eq(self):
        e1 = Exchange("foo", "direct")
        e2 = Exchange("foo", "direct")
        self.assertEqual(e1, e2)

        e3 = Exchange("foo", "topic")
        self.assertNotEqual(e1, e3)

        self.assertFalse(e1.__eq__(True))

    def test_revive(self):
        exchange = Exchange("foo", "direct")
        conn = get_conn()
        chan = conn.channel()

        # reviving unbound channel is a noop.
        exchange.revive(chan)
        self.assertFalse(exchange.is_bound)
        self.assertIsNone(exchange._channel)

        bound = exchange.bind(chan)
        self.assertTrue(bound.is_bound)
        self.assertIs(bound.channel, chan)

        chan2 = conn.channel()
        bound.revive(chan2)
        self.assertTrue(bound.is_bound)
        self.assertIs(bound._channel, chan2)

    def test_assert_is_bound(self):
        exchange = Exchange("foo", "direct")
        with self.assertRaises(NotBoundError):
            exchange.declare()
        conn = get_conn()

        chan = conn.channel()
        exchange.bind(chan).declare()
        self.assertIn("exchange_declare", chan)

    def test_set_transient_delivery_mode(self):
        exc = Exchange("foo", "direct", delivery_mode="transient")
        self.assertEqual(exc.delivery_mode, Exchange.TRANSIENT_DELIVERY_MODE)

    def test_set_persistent_delivery_mode(self):
        exc = Exchange("foo", "direct", delivery_mode="persistent")
        self.assertEqual(exc.delivery_mode, Exchange.PERSISTENT_DELIVERY_MODE)

    def test_bind_at_instantiation(self):
        self.assertTrue(Exchange("foo", channel=get_conn().channel()).is_bound)

    def test_create_message(self):
        chan = get_conn().channel()
        Exchange("foo", channel=chan).Message({"foo": "bar"})
        self.assertIn("prepare_message", chan)

    def test_publish(self):
        chan = get_conn().channel()
        Exchange("foo", channel=chan).publish("the quick brown fox")
        self.assertIn("basic_publish", chan)

    def test_delete(self):
        chan = get_conn().channel()
        Exchange("foo", channel=chan).delete()
        self.assertIn("exchange_delete", chan)

    def test__repr__(self):
        b = Exchange("foo", "topic")
        self.assertIn("foo(topic)", repr(b))
        self.assertIn("Exchange", repr(b))


class test_Queue(TestCase):

    def setUp(self):
        self.exchange = Exchange("foo", "direct")

    def test_hash(self):
        self.assertEqual(hash(Queue("a")), hash(Queue("a")))
        self.assertNotEqual(hash(Queue("a")), hash(Queue("b")))

    def test_when_bound_but_no_exchange(self):
        q = Queue("a")
        q.exchange = None
        self.assertIsNone(q.when_bound())

    def test_declare_but_no_exchange(self):
        q = Queue("a")
        q.queue_declare = Mock()
        q.queue_bind = Mock()
        q.exchange = None

        q.declare()
        q.queue_declare.assert_called_with(False, passive=False)
        q.queue_bind.assert_called_with(False)

    def test_can_cache_declaration(self):
        self.assertTrue(Queue("a", durable=True).can_cache_declaration)
        self.assertFalse(Queue("a", durable=False).can_cache_declaration)

    def test_eq(self):
        q1 = Queue("xxx", Exchange("xxx", "direct"), "xxx")
        q2 = Queue("xxx", Exchange("xxx", "direct"), "xxx")
        self.assertEqual(q1, q2)
        self.assertFalse(q1.__eq__(True))

        q3 = Queue("yyy", Exchange("xxx", "direct"), "xxx")
        self.assertNotEqual(q1, q3)

    def test_exclusive_implies_auto_delete(self):
        self.assertTrue(
                Queue("foo", self.exchange, exclusive=True).auto_delete)

    def test_binds_at_instantiation(self):
        self.assertTrue(Queue("foo", self.exchange,
                              channel=get_conn().channel()).is_bound)

    def test_also_binds_exchange(self):
        chan = get_conn().channel()
        b = Queue("foo", self.exchange)
        self.assertFalse(b.is_bound)
        self.assertFalse(b.exchange.is_bound)
        b = b.bind(chan)
        self.assertTrue(b.is_bound)
        self.assertTrue(b.exchange.is_bound)
        self.assertIs(b.channel, b.exchange.channel)
        self.assertIsNot(b.exchange, self.exchange)

    def test_declare(self):
        chan = get_conn().channel()
        b = Queue("foo", self.exchange, "foo", channel=chan)
        self.assertTrue(b.is_bound)
        b.declare()
        self.assertIn("exchange_declare", chan)
        self.assertIn("queue_declare", chan)
        self.assertIn("queue_bind", chan)

    def test_get(self):
        b = Queue("foo", self.exchange, "foo", channel=get_conn().channel())
        b.get()
        self.assertIn("basic_get", b.channel)

    def test_purge(self):
        b = Queue("foo", self.exchange, "foo", channel=get_conn().channel())
        b.purge()
        self.assertIn("queue_purge", b.channel)

    def test_consume(self):
        b = Queue("foo", self.exchange, "foo", channel=get_conn().channel())
        b.consume("fifafo", None)
        self.assertIn("basic_consume", b.channel)

    def test_cancel(self):
        b = Queue("foo", self.exchange, "foo", channel=get_conn().channel())
        b.cancel("fifafo")
        self.assertIn("basic_cancel", b.channel)

    def test_delete(self):
        b = Queue("foo", self.exchange, "foo", channel=get_conn().channel())
        b.delete()
        self.assertIn("queue_delete", b.channel)

    def test_unbind(self):
        b = Queue("foo", self.exchange, "foo", channel=get_conn().channel())
        b.unbind()
        self.assertIn("queue_unbind", b.channel)

    def test_as_dict(self):
        q = Queue("foo", self.exchange, "rk")
        d = q.as_dict(recurse=True)
        self.assertEqual(d["exchange"]["name"], self.exchange.name)

    def test__repr__(self):
        b = Queue("foo", self.exchange, "foo")
        self.assertIn("foo", repr(b))
        self.assertIn("Queue", repr(b))
