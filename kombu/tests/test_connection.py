import unittest2 as unittest

from kombu.connection import BrokerConnection

from kombu.tests.mocks import Transport


class test_Connection(unittest.TestCase):

    def test_establish_connection(self):
        conn = BrokerConnection(port=5672, transport=Transport)
        conn.connect()
        self.assertTrue(conn.connection.connected)
        self.assertEqual(conn.host, "localhost:5672")
        channel = conn.channel()
        self.assertTrue(channel.open)
        self.assertEqual(conn.drain_events(), "event")
        _connection = conn.connection
        conn.close()
        self.assertFalse(_connection.connected)
        self.assertIsInstance(conn.transport, Transport)

    def test__enter____exit__(self):
        conn = BrokerConnection(transport=Transport)
        context = conn.__enter__()
        self.assertIs(context, conn)
        conn.connect()
        self.assertTrue(conn.connection.connected)
        conn.__exit__()
        self.assertIsNone(conn.connection)
        conn.close()    # again


class ResourceCase(unittest.TestCase):
    abstract = True

    def create_resource(self, limit, preload):
        raise NotImplementedError("subclass responsibility")

    def assertState(self, P, avail, dirty):
        self.assertEqual(P._resource.qsize(), avail)
        self.assertEqual(len(P._dirty), dirty)

    def test_acquire__release(self):
        if self.abstract:
            return
        P = self.create_resource(10, 0)
        self.assertState(P, 10, 0)
        chans = [P.acquire() for _ in xrange(10)]
        self.assertState(P, 0, 10)
        self.assertRaises(P.LimitExceeded, P.acquire)
        chans.pop().release()
        self.assertState(P, 1, 9)
        [chan.release() for chan in chans]
        self.assertState(P, 10, 0)


class test_ConnectionPool(ResourceCase):
    abstract = False

    def create_resource(self, limit, preload):
        return BrokerConnection(port=5672, transport=Transport) \
                    .Pool(limit, preload)

    def test_setup(self):
        P = self.create_resource(10, 2)
        q = P._resource.queue
        self.assertIsNotNone(q[0]._connection)
        self.assertIsNotNone(q[1]._connection)
        self.assertIsNone(q[2]._connection)


class test_ChannelPool(ResourceCase):
    abstract = False

    def create_resource(self, limit, preload):
        return BrokerConnection(port=5672, transport=Transport) \
                    .ChannelPool(limit, preload)

    def test_setup(self):
        P = self.create_resource(10, 2)
        q = P._resource.queue
        self.assertTrue(q[0].basic_consume)
        self.assertTrue(q[1].basic_consume)
        self.assertRaises(AttributeError, getattr, q[2], "basic_consume")
