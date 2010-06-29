import unittest2 as unittest

from kombu.connection import BrokerConnection

from kombu.tests.mocks import Backend


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

    def test__enter____exit__(self):
        conn = BrokerConnection(backend_cls=Backend)
        context = conn.__enter__()
        self.assertIs(context, conn)
        conn.connect()
        self.assertTrue(conn.connection.connected)
        conn.__exit__()
        self.assertIsNone(conn.connection)
        conn.close() # again
