from __future__ import absolute_import
from __future__ import with_statement

import pickle

from nose import SkipTest

from kombu.connection import BrokerConnection, Resource, parse_url
from kombu.messaging import Consumer, Producer

from .mocks import Transport
from .utils import TestCase
from .utils import Mock, skip_if_not_module


class test_connection_utils(TestCase):

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

    def test_parse_url_mongodb(self):
        result = parse_url("mongodb://example.com/")
        self.assertEqual(result["hostname"], "example.com/")

    def test_parse_generated_as_uri(self):
        conn = BrokerConnection(self.url)
        info = conn.info()
        for k, v in self.expected.items():
            self.assertEqual(info[k], v)
        # by default almost the same- no password
        self.assertEqual(conn.as_uri(), self.nopass)
        self.assertEqual(conn.as_uri(include_password=True), self.url)

    @skip_if_not_module("pymongo")
    def test_as_uri_when_mongodb(self):
        x = BrokerConnection("mongodb://localhost")
        self.assertTrue(x.as_uri())

    def test_bogus_scheme(self):
        with self.assertRaises(KeyError):
            BrokerConnection("bogus://localhost:7421").transport

    def assert_info(self, conn, **fields):
        info = conn.info()
        for field, expected in fields.iteritems():
            self.assertEqual(info[field], expected)

    def test_rabbitmq_example_urls(self):
        # see Appendix A of http://www.rabbitmq.com/uri-spec.html
        C = BrokerConnection

        self.assert_info(
            C("amqp://user:pass@host:10000/vhost"),
                userid="user", password="pass", hostname="host",
                port=10000, virtual_host="vhost")

        self.assert_info(
            C("amqp://user%61:%61pass@ho%61st:10000/v%2fhost"),
                userid="usera", password="apass",
                hostname="hoast", port=10000,
                virtual_host="v/host")

        self.assert_info(
            C("amqp://"),
                userid="guest", password="guest",
                hostname="localhost", port=5672,
                virtual_host="/")

        self.assert_info(
            C("amqp://:@/"),
                userid="guest", password="guest",
                hostname="localhost", port=5672,
                virtual_host="/")

        self.assert_info(
            C("amqp://user@/"),
                userid="user", password="guest",
                hostname="localhost", port=5672,
                virtual_host="/")

        self.assert_info(
            C("amqp://user:pass@/"),
                userid="user", password="pass",
                hostname="localhost", port=5672,
                virtual_host="/")

        self.assert_info(
            C("amqp://host"),
                userid="guest", password="guest",
                hostname="host", port=5672,
                virtual_host="/")

        self.assert_info(
            C("amqp://:10000"),
                userid="guest", password="guest",
                hostname="localhost", port=10000,
                virtual_host="/")

        self.assert_info(
            C("amqp:///vhost"),
                userid="guest", password="guest",
                hostname="localhost", port=5672,
                virtual_host="vhost")

        self.assert_info(
            C("amqp://host/"),
                userid="guest", password="guest",
                hostname="host", port=5672,
                virtual_host="/")

        self.assert_info(
            C("amqp://host/%2f"),
                userid="guest", password="guest",
                hostname="host", port=5672,
                virtual_host="/")

    def test_url_IPV6(self):
        C = BrokerConnection
        raise SkipTest("urllib can't parse ipv6 urls")

        self.assert_info(
            C("amqp://[::1]"),
                userid="guest", password="guest",
                hostname="[::1]", port=5672,
                virtual_host="/")


class test_Connection(TestCase):

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

    def test_close_when_default_channel(self):
        conn = self.conn
        conn._default_channel = Mock()
        conn._close()
        conn._default_channel.close.assert_called_with()

    def test_close_when_default_channel_close_raises(self):

        class Conn(BrokerConnection):

            @property
            def connection_errors(self):
                return (KeyError, )

        conn = Conn("memory://")
        conn._default_channel = Mock()
        conn._default_channel.close.side_effect = KeyError()

        conn._close()
        conn._default_channel.close.assert_called_with()

    def test_revive_when_default_channel(self):
        conn = self.conn
        defchan = conn._default_channel = Mock()
        conn.revive(Mock())

        defchan.close.assert_called_with()
        self.assertIsNone(conn._default_channel)

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
        with self.assertRaises(_CustomError):
            ensured()

    def test_ensure_connection_failure(self):
        class _ConnectionError(Exception):
            pass

        def publish():
            raise _ConnectionError("failed connection")

        self.conn.transport.connection_errors = (_ConnectionError,)
        ensured = self.conn.ensure(self.conn, publish)
        with self.assertRaises(_ConnectionError):
            ensured()

    def test_autoretry(self):
        myfun = Mock()
        myfun.__name__ = "test_autoretry"

        self.conn.transport.connection_errors = (KeyError, )

        def on_call(*args, **kwargs):
            myfun.side_effect = None
            raise KeyError("foo")

        myfun.side_effect = on_call
        insured = self.conn.autoretry(myfun)
        insured()

        self.assertTrue(myfun.called)

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

    def test_Producer(self):
        conn = self.conn
        self.assertIsInstance(conn.Producer(), Producer)
        self.assertIsInstance(conn.Producer(conn.default_channel), Producer)

    def test_Consumer(self):
        conn = self.conn
        self.assertIsInstance(conn.Consumer(queues=[]), Consumer)
        self.assertIsInstance(conn.Consumer(queues=[],
                              channel=conn.default_channel), Consumer)

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


class test_Connection_with_transport_options(TestCase):

    transport_options = {"pool_recycler": 3600, "echo": True}

    def setUp(self):
        self.conn = BrokerConnection(port=5672, transport=Transport,
                                     transport_options=self.transport_options)

    def test_establish_connection(self):
        conn = self.conn
        self.assertEqual(conn.transport_options, self.transport_options)


class xResource(Resource):

    def setup(self):
        pass


class ResourceCase(TestCase):
    abstract = True

    def create_resource(self, limit, preload):
        raise NotImplementedError("subclass responsibility")

    def assertState(self, P, avail, dirty):
        self.assertEqual(P._resource.qsize(), avail)
        self.assertEqual(len(P._dirty), dirty)

    def test_setup(self):
        if self.abstract:
            with self.assertRaises(NotImplementedError):
                Resource()

    def test_acquire__release(self):
        if self.abstract:
            return
        P = self.create_resource(10, 0)
        self.assertState(P, 10, 0)
        chans = [P.acquire() for _ in xrange(10)]
        self.assertState(P, 0, 10)
        with self.assertRaises(P.LimitExceeded):
            P.acquire()
        chans.pop().release()
        self.assertState(P, 1, 9)
        [chan.release() for chan in chans]
        self.assertState(P, 10, 0)

    def test_acquire_no_limit(self):
        if self.abstract:
            return
        P = self.create_resource(None, 0)
        P.acquire().release()

    def test_replace_when_limit(self):
        if self.abstract:
            return
        P = self.create_resource(10, 0)
        r = P.acquire()
        P._dirty = Mock()
        P.close_resource = Mock()

        P.replace(r)
        P._dirty.discard.assert_called_with(r)
        P.close_resource.assert_called_with(r)

    def test_replace_no_limit(self):
        if self.abstract:
            return
        P = self.create_resource(None, 0)
        r = P.acquire()
        P._dirty = Mock()
        P.close_resource = Mock()

        P.replace(r)
        self.assertFalse(P._dirty.discard.called)
        P.close_resource.assert_called_with(r)

    def test_interface_prepare(self):
        if not self.abstract:
            return
        x = xResource()
        self.assertEqual(x.prepare(10), 10)

    def test_force_close_all_handles_AttributeError(self):
        if self.abstract:
            return
        P = self.create_resource(10, 10)
        cr = P.close_resource = Mock()
        cr.side_effect = AttributeError("x")

        P.acquire()
        self.assertTrue(P._dirty)

        P.force_close_all()

    def test_force_close_all_no_mutex(self):
        if self.abstract:
            return
        P = self.create_resource(10, 10)
        P.close_resource = Mock()

        m = P._resource = Mock()
        m.mutex = None
        m.queue.pop.side_effect = IndexError

        P.force_close_all()

    def test_add_when_empty(self):
        if self.abstract:
            return
        P = self.create_resource(None, None)
        P._resource.queue[:] = []
        self.assertFalse(P._resource.queue)
        P._add_when_empty()
        self.assertTrue(P._resource.queue)


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

    def test_setup_no_limit(self):
        P = self.create_resource(None, None)
        self.assertFalse(P._resource.queue)
        self.assertIsNone(P.limit)

    def test_prepare_not_callable(self):
        P = self.create_resource(None, None)
        conn = BrokerConnection("memory://")
        self.assertIs(P.prepare(conn), conn)

    def test_acquire_channel(self):
        P = self.create_resource(10, 0)
        with P.acquire_channel() as (conn, channel):
            self.assertIs(channel, conn.default_channel)


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
        with self.assertRaises(AttributeError):
            getattr(q[2], "basic_consume")

    def test_setup_no_limit(self):
        P = self.create_resource(None, None)
        self.assertFalse(P._resource.queue)
        self.assertIsNone(P.limit)

    def test_prepare_not_callable(self):
        P = self.create_resource(10, 0)
        conn = BrokerConnection("memory://")
        chan = conn.default_channel
        self.assertIs(P.prepare(chan), chan)
