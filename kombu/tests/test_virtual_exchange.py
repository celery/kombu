from __future__ import absolute_import

from .. import BrokerConnection
from ..transport.virtual import exchange

from .mocks import Transport
from .utils import unittest


class ExchangeCase(unittest.TestCase):
    type = None

    def setUp(self):
        if self.type:
            self.e = self.type(BrokerConnection(transport=Transport)
                                               .channel())


class test_Direct(ExchangeCase):
    type = exchange.DirectExchange
    table = [("rFoo", None, "qFoo"),
             ("rFoo", None, "qFox"),
             ("rBar", None, "qBar"),
             ("rBaz", None, "qBaz")]

    def test_lookup(self):
        self.assertListEqual(self.e.lookup(
                self.table, "eFoo", "rFoo", None),
                ["qFoo", "qFox"])
        self.assertListEqual(self.e.lookup(
                self.table, "eMoz", "rMoz", "DEFAULT"),
                [])
        self.assertListEqual(self.e.lookup(
                self.table, "eBar", "rBar", None),
                ["qBar"])


class test_Fanout(ExchangeCase):
    type = exchange.FanoutExchange
    table = [(None, None, "qFoo"),
             (None, None, "qFox"),
             (None, None, "qBar")]

    def test_lookup(self):
        self.assertListEqual(self.e.lookup(
                self.table, "eFoo", "rFoo", None),
                ["qFoo", "qFox", "qBar"])


class test_Topic(ExchangeCase):
    type = exchange.TopicExchange
    table = [("stock.#", None, "rFoo"),
              ("stock.us.*", None, "rBar")]

    def setUp(self):
        super(test_Topic, self).setUp()
        self.table = [(rkey, self.e.key_to_pattern(rkey), queue)
                        for rkey, _, queue in self.table]

    def test_prepare_bind(self):
        x = self.e.prepare_bind("qFoo", "eFoo", "stock.#", {})
        self.assertTupleEqual(x, ("stock.#", r'^stock\..*?$', "qFoo"))

    def test_lookup(self):
        self.assertListEqual(self.e.lookup(
                self.table, "eFoo", "stock.us.nasdaq", None),
                ["rFoo", "rBar"])
        self.assertTrue(self.e._compiled)
        self.assertListEqual(self.e.lookup(
                self.table, "eFoo", "stock.europe.OSE", None),
                ["rFoo"])
        self.assertListEqual(self.e.lookup(
                self.table, "eFoo", "stockxeuropexOSE", None),
                [])
        self.assertListEqual(self.e.lookup(
                self.table, "eFoo", "candy.schleckpulver.snap_crackle", None),
                [])


class test_ExchangeType(ExchangeCase):
    type = exchange.ExchangeType

    def test_lookup(self):
        self.assertRaises(NotImplementedError, self.e.lookup,
                [], "eFoo", "rFoo", None)

    def test_prepare_bind(self):
        self.assertTupleEqual(self.e.prepare_bind("qFoo", "eFoo", "rFoo", {}),
                              ("rFoo", None, "qFoo"))

    def test_equivalent(self):
        e1 = dict(type="direct",
                  durable=True,
                  auto_delete=True,
                  arguments={})
        self.assertTrue(
                self.e.equivalent(e1, "eFoo", "direct", True, True, {}))
        self.assertFalse(
                self.e.equivalent(e1, "eFoo", "topic", True, True, {}))
        self.assertFalse(
                self.e.equivalent(e1, "eFoo", "direct", False, True, {}))
        self.assertFalse(
                self.e.equivalent(e1, "eFoo", "direct", True, False, {}))
        self.assertFalse(
                self.e.equivalent(e1, "eFoo", "direct", True, True, {
                    "expires": 3000}))
        e2 = dict(e1, arguments={"expires": 3000})
        self.assertTrue(
                self.e.equivalent(e2, "eFoo", "direct", True, True, {
                    "expires": 3000}))
        self.assertFalse(
                self.e.equivalent(e2, "eFoo", "direct", True, True, {
                    "expires": 6000}))
