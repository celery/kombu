from kombu.tests.utils import unittest

from kombu.transport import pyamqplib
from kombu.connection import BrokerConnection


class MockConnection(dict):

    def __setattr__(self, key, value):
        self[key] = value


class test_amqplib(unittest.TestCase):

    def test_default_port(self):

        class Transport(pyamqplib.Transport):
            Connection = MockConnection

        c = BrokerConnection(port=None, transport=Transport).connect()
        self.assertEqual(c["host"],
                         "localhost:%s" % (Transport.default_port, ))

    def test_custom_port(self):

        class Transport(pyamqplib.Transport):
            Connection = MockConnection

        c = BrokerConnection(port=1337, transport=Transport).connect()
        self.assertEqual(c["host"], "localhost:1337")
