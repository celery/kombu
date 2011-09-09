from __future__ import absolute_import

from ..transport import amqplib
from ..connection import BrokerConnection

from .utils import unittest


class MockConnection(dict):

    def __setattr__(self, key, value):
        self[key] = value


class test_amqplib(unittest.TestCase):

    def test_default_port(self):

        class Transport(amqplib.Transport):
            Connection = MockConnection

        c = BrokerConnection(port=None, transport=Transport).connect()
        self.assertEqual(c["host"],
                         "127.0.0.1:%s" % (Transport.default_port, ))

    def test_custom_port(self):

        class Transport(amqplib.Transport):
            Connection = MockConnection

        c = BrokerConnection(port=1337, transport=Transport).connect()
        self.assertEqual(c["host"], "127.0.0.1:1337")
