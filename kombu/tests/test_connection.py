from __future__ import absolute_import
import pickle

from ..connection import BrokerConnection, Resource, parse_url

from .mocks import Transport
from .utils import unittest


class test_connection_utils(unittest.TestCase):

    def setUp(self):
        self.url = "amqp://user:pass@localhost:5672/my/vhost"
        self.nopass = "amqp://user@localhost:5672/my/vhost"
        self.expected = {
            "transport": "amqp",
            "userid": "user",
            "password": "pass",
            "hostname": "localhost",
            "port": 5672,
            "virtual_host": "my/vhost",
        }

    def test_parse_url(self):
        result = parse_url(self.url)
        self.assertDictEqual(result, self.expected)

    def test_parse_generated_as_uri(self):
        conn = BrokerConnection(self.url)
        info = conn.info()
        for k, v in self.expected.items():
            self.assertEqual(info[k], v)
        # by default almost the same- no password
        self.assertEqual(conn.as_uri(), self.nopass)
        self.assertEqual(conn.as_uri(include_password=True), self.url)

    def test_bogus_scheme(self):
        conn = BrokerConnection("bogus://localhost:7421")
        # second parameter must be a callable, thus this little hack
        self.assertRaises(KeyError, lambda: conn.transport)


class test_Connection(unittest.TestCase):

    def setUp(self):
        self.conn = BrokerConnection(port=5672, transport=Transport)

    def test_establish_connection(self):
        conn = self.conn
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
        conn = self.conn
        context = conn.__enter__()
        self.assertIs(context, conn)
        conn.connect()
        self.assertTrue(conn.connection.connected)
        conn.__exit__()
        self.assertIsNone(conn.connection)
        conn.close()    # again

    def test_close_survives_connerror(self):

        class _CustomError(Exception):
            pass

        class MyTransport(Transport):
            connection_errors = (_CustomError, )

            def close_connection(self, connection):
                raise _CustomError("foo")

        conn = BrokerConnection(transport=MyTransport)
        conn.connect()
        conn.close()
        self.assertTrue(conn._closed)

    def test_ensure_connection(self):
        self.assertTrue(self.conn.ensure_connection())

    def test_ensure_success(self):
        def publish():
            return "foobar"

        ensured = self.conn.ensure(None, publish)
        self.assertEqual(ensured(), "foobar")

    def test_ensure_failure(self):
        class _CustomError(Exception):
            pass

        def publish():
            raise _CustomError("bar")

        ensured = self.conn.ensure(None, publish)
        self.assertRaises(_CustomError, ensured)

    def test_ensure_connection_failure(self):
        class _ConnectionError(Exception):
            pass

        def publish():
            raise _ConnectionError("failed connection")

        self.conn.transport.connection_errors = (_ConnectionError,)
        ensured = self.conn.ensure(self.conn, publish)
        self.assertRaises(_ConnectionError, ensured)

    def test_SimpleQueue(self):
        conn = self.conn
        q = conn.SimpleQueue("foo")
        self.assertTrue(q.channel)
        self.assertTrue(q.channel_autoclose)
        chan = conn.channel()
        q2 = conn.SimpleQueue("foo", channel=chan)
        self.assertIs(q2.channel, chan)
        self.assertFalse(q2.channel_autoclose)

    def test_SimpleBuffer(self):
        conn = self.conn
        q = conn.SimpleBuffer("foo")
        self.assertTrue(q.channel)
        self.assertTrue(q.channel_autoclose)
        chan = conn.channel()
        q2 = conn.SimpleBuffer("foo", channel=chan)
        self.assertIs(q2.channel, chan)
        self.assertFalse(q2.channel_autoclose)

    def test__repr__(self):
        self.assertTrue(repr(self.conn))

    def test__reduce__(self):
        x = pickle.loads(pickle.dumps(self.conn))
        self.assertDictEqual(x.info(), self.conn.info())

    def test_channel_errors(self):

        class MyTransport(Transport):
            channel_errors = (KeyError, ValueError)

        conn = BrokerConnection(transport=MyTransport)
        self.assertTupleEqual(conn.channel_errors, (KeyError, ValueError))

    def test_connection_errors(self):

        class MyTransport(Transport):
            connection_errors = (KeyError, ValueError)

        conn = BrokerConnection(transport=MyTransport)
        self.assertTupleEqual(conn.connection_errors, (KeyError, ValueError))


class test_Connection_with_transport_options(unittest.TestCase):

    transport_options = {"pool_recycler": 3600, "echo": True}

    def setUp(self):
        self.conn = BrokerConnection(port=5672, transport=Transport,
                                     transport_options=self.transport_options)

    def test_establish_connection(self):
        conn = self.conn
        self.assertEqual(conn.transport_options, self.transport_options)


class ResourceCase(unittest.TestCase):
    abstract = True

    def create_resource(self, limit, preload):
        raise NotImplementedError("subclass responsibility")

    def assertState(self, P, avail, dirty):
        self.assertEqual(P._resource.qsize(), avail)
        self.assertEqual(len(P._dirty), dirty)

    def test_setup(self):
        if self.abstract:
            self.assertRaises(NotImplementedError, Resource)

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
        self.assertIsNone(q[2]()._connection)


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
