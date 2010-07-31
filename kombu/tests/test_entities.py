import unittest2 as unittest

from kombu.entity import Exchange, Binding
from kombu.exceptions import NotBoundError

from kombu.tests.mocks import Channel


class test_Exchange(unittest.TestCase):

    def test_bound(self):
        exchange = Exchange("foo", "direct")
        self.assertFalse(exchange.is_bound)
        self.assertIn("<unbound", repr(exchange))

        chan = Channel()
        bound = exchange.bind(chan)
        self.assertTrue(bound.is_bound)
        self.assertIs(bound.channel, chan)
        self.assertIn("<bound", repr(bound))

    def test_assert_is_bound(self):
        exchange = Exchange("foo", "direct")
        self.assertRaises(NotBoundError, exchange.declare)

        chan = Channel()
        exchange.bind(chan).declare()
        self.assertIn("exchange_declare", chan)

    def test_set_transient_delivery_mode(self):
        exc = Exchange("foo", "direct", delivery_mode="transient")
        self.assertEqual(exc.delivery_mode, Exchange.TRANSIENT_DELIVERY_MODE)

    def test_set_persistent_delivery_mode(self):
        exc = Exchange("foo", "direct", delivery_mode="persistent")
        self.assertEqual(exc.delivery_mode, Exchange.PERSISTENT_DELIVERY_MODE)

    def test_bind_at_instantiation(self):
        self.assertTrue(Exchange("foo", channel=Channel()).is_bound)

    def test_create_message(self):
        chan = Channel()
        Exchange("foo", channel=chan).Message({"foo": "bar"})
        self.assertIn("prepare_message", chan)

    def test_publish(self):
        chan = Channel()
        Exchange("foo", channel=chan).publish("the quick brown fox")
        self.assertIn("basic_publish", chan)

    def test_delete(self):
        chan = Channel()
        Exchange("foo", channel=chan).delete()
        self.assertIn("exchange_delete", chan)

    def test__repr__(self):
        b = Exchange("foo", "topic")
        self.assertIn("foo(topic)", repr(b))
        self.assertIn("Exchange", repr(b))


class test_Binding(unittest.TestCase):

    def setUp(self):
        self.exchange = Exchange("foo", "direct")

    def test_exclusive_implies_auto_delete(self):
        self.assertTrue(
                Binding("foo", self.exchange, exclusive=True).auto_delete)

    def test_binds_at_instantiation(self):
        self.assertTrue(
                Binding("foo", self.exchange, channel=Channel()).is_bound)

    def test_also_binds_exchange(self):
        chan = Channel()
        b = Binding("foo", self.exchange)
        self.assertFalse(b.is_bound)
        self.assertFalse(b.exchange.is_bound)
        b = b.bind(chan)
        self.assertTrue(b.is_bound)
        self.assertTrue(b.exchange.is_bound)
        self.assertIs(b.channel, b.exchange.channel)
        self.assertIsNot(b.exchange, self.exchange)

    def test_declare(self):
        chan = Channel()
        b = Binding("foo", self.exchange, "foo", channel=chan)
        self.assertTrue(b.is_bound)
        b.declare()
        self.assertIn("exchange_declare", chan)
        self.assertIn("queue_declare", chan)
        self.assertIn("queue_bind", chan)

    def test_get(self):
        b = Binding("foo", self.exchange, "foo", channel=Channel())
        b.get()
        self.assertIn("basic_get", b.channel)

    def test_purge(self):
        b = Binding("foo", self.exchange, "foo", channel=Channel())
        b.purge()
        self.assertIn("queue_purge", b.channel)

    def test_consume(self):
        b = Binding("foo", self.exchange, "foo", channel=Channel())
        b.consume("fifafo", None)
        self.assertIn("basic_consume", b.channel)

    def test_cancel(self):
        b = Binding("foo", self.exchange, "foo", channel=Channel())
        b.cancel("fifafo")
        self.assertIn("basic_cancel", b.channel)

    def test_delete(self):
        b = Binding("foo", self.exchange, "foo", channel=Channel())
        b.delete()
        self.assertIn("queue_delete", b.channel)

    def test__repr__(self):
        b = Binding("foo", self.exchange, "foo")
        self.assertIn("foo", repr(b))
        self.assertIn("Binding", repr(b))
