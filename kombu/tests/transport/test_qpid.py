from __future__ import absolute_import

import sys

from itertools import count

try:
    import amqp    # noqa
except ImportError:
    pyamqp = None  # noqa
else:
    from kombu.transport import pyamqp

from kombu.transport.qpid import QpidMessagingExceptionHandler, Base64

from kombu import Connection
from kombu.five import nextfun

from kombu.tests.case import Case, Mock, SkipTest, mask_modules, patch


class MockConnection(dict):

    def __setattr__(self, key, value):
        self[key] = value


class test_QpidMessagingExceptionHandler(Case):

    allowed_string = 'object in use'
    not_allowed_string = 'a different string'

    def setUp(self):
        self.handler = QpidMessagingExceptionHandler(self.allowed_string)

    def test_string_stored(self):
        handler_string = self.handler.allowed_exception_string
        self.assertEqual(self.allowed_string, handler_string)

    def test_exception_positive(self):
        exception_to_raise = Exception(self.allowed_string)
        def exception_raise_func():
            raise exception_to_raise
        decorated_func = self.handler(exception_raise_func)
        try:
            decorated_func()
        except:
            self.fail("QpidMessagingExceptionHandler allowed an exception "
                      "to be raised that should have been silenced!")

    def test_exception_negative(self):
        exception_to_raise = Exception(self.not_allowed_string)
        def exception_raise_func():
            raise exception_to_raise
        decorated_func = self.handler(exception_raise_func)
        self.assertRaises(Exception, decorated_func)


class test_Base64(Case):

    base64 = 'VGhpcyBpcyB0aGUgYmFzZTY0IHN0cmluZyB0byBiZSBlbmNvZGVkLg=='
    utf8 = 'This is the base64 string to be encoded.'

    def setUp(self):
        self.base_64 = Base64()

    def test_encode(self):
        encoded = self.base_64.encode(self.utf8)
        self.assertEqual(encoded, self.base64)

    def test_decode(self):
        decoded = self.base_64.decode(self.base64)
        self.assertEqual(decoded, self.utf8)


class test_Channel(Case):

    def setUp(self):
        if pyamqp is None:
            raise SkipTest('py-amqp not installed')

        class Channel(pyamqp.Channel):
            wait_returns = []

            def _x_open(self, *args, **kwargs):
                pass

            def wait(self, *args, **kwargs):
                return self.wait_returns

            def _send_method(self, *args, **kwargs):
                pass

        self.conn = Mock()
        self.conn._get_free_channel_id.side_effect = nextfun(count(0))
        self.conn.channels = {}
        self.channel = Channel(self.conn, 0)

    def test_init(self):
        self.assertFalse(self.channel.no_ack_consumers)

    def test_prepare_message(self):
        self.assertTrue(self.channel.prepare_message(
            'foobar', 10, 'application/data', 'utf-8',
            properties={},
        ))

    def test_message_to_python(self):
        message = Mock()
        message.headers = {}
        message.properties = {}
        self.assertTrue(self.channel.message_to_python(message))

    def test_close_resolves_connection_cycle(self):
        self.assertIsNotNone(self.channel.connection)
        self.channel.close()
        self.assertIsNone(self.channel.connection)

    def test_basic_consume_registers_ack_status(self):
        self.channel.wait_returns = 'my-consumer-tag'
        self.channel.basic_consume('foo', no_ack=True)
        self.assertIn('my-consumer-tag', self.channel.no_ack_consumers)

        self.channel.wait_returns = 'other-consumer-tag'
        self.channel.basic_consume('bar', no_ack=False)
        self.assertNotIn('other-consumer-tag', self.channel.no_ack_consumers)

        self.channel.basic_cancel('my-consumer-tag')
        self.assertNotIn('my-consumer-tag', self.channel.no_ack_consumers)


class test_Transport(Case):

    def setUp(self):
        if pyamqp is None:
            raise SkipTest('py-amqp not installed')
        self.connection = Connection('pyamqp://')
        self.transport = self.connection.transport

    def test_create_channel(self):
        connection = Mock()
        self.transport.create_channel(connection)
        connection.channel.assert_called_with()

    def test_driver_version(self):
        self.assertTrue(self.transport.driver_version())

    def test_drain_events(self):
        connection = Mock()
        self.transport.drain_events(connection, timeout=10.0)
        connection.drain_events.assert_called_with(timeout=10.0)

    def test_dnspython_localhost_resolve_bug(self):

        class Conn(object):

            def __init__(self, **kwargs):
                vars(self).update(kwargs)

        self.transport.Connection = Conn
        self.transport.client.hostname = 'localhost'
        conn1 = self.transport.establish_connection()
        self.assertEqual(conn1.host, '127.0.0.1:5672')

        self.transport.client.hostname = 'example.com'
        conn2 = self.transport.establish_connection()
        self.assertEqual(conn2.host, 'example.com:5672')

    def test_close_connection(self):
        connection = Mock()
        connection.client = Mock()
        self.transport.close_connection(connection)

        self.assertIsNone(connection.client)
        connection.close.assert_called_with()

    @mask_modules('ssl')
    def test_import_no_ssl(self):
        pm = sys.modules.pop('amqp.connection')
        try:
            from amqp.connection import SSLError
            self.assertEqual(SSLError.__module__, 'amqp.connection')
        finally:
            if pm is not None:
                sys.modules['amqp.connection'] = pm


class test_pyamqp(Case):

    def setUp(self):
        if pyamqp is None:
            raise SkipTest('py-amqp not installed')

    def test_default_port(self):

        class Transport(pyamqp.Transport):
            Connection = MockConnection

        c = Connection(port=None, transport=Transport).connect()
        self.assertEqual(c['host'],
                         '127.0.0.1:%s' % (Transport.default_port, ))

    def test_custom_port(self):

        class Transport(pyamqp.Transport):
            Connection = MockConnection

        c = Connection(port=1337, transport=Transport).connect()
        self.assertEqual(c['host'], '127.0.0.1:1337')

    def test_register_with_event_loop(self):
        t = pyamqp.Transport(Mock())
        conn = Mock(name='conn')
        loop = Mock(name='loop')
        t.register_with_event_loop(conn, loop)
        loop.add_reader.assert_called_with(
            conn.sock, t.on_readable, conn, loop,
        )

    def test_heartbeat_check(self):
        t = pyamqp.Transport(Mock())
        conn = Mock()
        t.heartbeat_check(conn, rate=4.331)
        conn.heartbeat_tick.assert_called_with(rate=4.331)

    def test_get_manager(self):
        with patch('kombu.transport.pyamqp.get_manager') as get_manager:
            t = pyamqp.Transport(Mock())
            t.get_manager(1, kw=2)
            get_manager.assert_called_with(t.client, 1, kw=2)
