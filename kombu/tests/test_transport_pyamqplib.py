from kombu.tests.utils import unittest

from kombu.transport import pyamqplib
from kombu.connection import BrokerConnection


class test_amqplib(unittest.TestCase):

    def test_conninfo(self):
        c = BrokerConnection(userid=None, transport="amqplib")
        self.assertRaises(KeyError, c.connect)
        c = BrokerConnection(hostname=None, transport="amqplib")
        self.assertRaises(KeyError, c.connect)
        c = BrokerConnection(password=None, transport="amqplib")
        self.assertRaises(KeyError, c.connect)

    def test_default_port(self):

        class Transport(pyamqplib.Transport):
            Connection = dict

        c = BrokerConnection(port=None, transport=Transport).connect()
        self.assertEqual(c["host"],
                         "localhost:%s" % (Transport.default_port, ))

    def test_custom_port(self):

        class Transport(pyamqplib.Transport):
            Connection = dict

        c = BrokerConnection(port=1337, transport=Transport).connect()
        self.assertEqual(c["host"], "localhost:1337")
