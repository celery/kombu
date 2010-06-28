import unittest2 as unittest

from kombu.backends.base import BaseBackend
from kombu.connection import BrokerConnection


class Channel(object):
    open = True


class Connection(object):
    connected = True

    def channel(self):
        return Channel()


class Backend(BaseBackend):

    def establish_connection(self):
        return Connection()

    def create_channel(self, connection):
        return connection.channel()

    def drain_events(self, connection, **kwargs):
        return "event"

    def close_connection(self, connection):
        connection.connected = False


class test_Connection(unittest.TestCase):

    def test_establish_connection(self):
        conn = BrokerConnection(port=5672, backend_cls=Backend)
        conn.connect()
        self.assertTrue(conn.connection.connected)
        self.assertEqual(conn.host, "localhost:5672")
        channel = conn.channel()
        self.assertTrue(channel.open)
        self.assertEqual(conn.drain_events(), "event")
        _connection = conn.connection
        conn.close()
        self.assertFalse(_connection.connected)
        self.assertIsInstance(conn.backend, Backend)
