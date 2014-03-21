from __future__ import absolute_import

import sys

from itertools import count

try:
    import amqp    # noqa
except ImportError:
    pyamqp = None  # noqa
else:
    from kombu.transport import pyamqp

from kombu.transport.qpid import QpidMessagingExceptionHandler, Base64, \
    QoS, Message, Channel, FDShimThread, FDShim, Connection, Transport

import kombu.transport.qpid

from kombu import Connection
from kombu.five import nextfun
from kombu.utils.compat import OrderedDict

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
        """Assert that the allowed_exception_string is stored correctly"""
        handler_string = self.handler.allowed_exception_string
        self.assertEqual(self.allowed_string, handler_string)

    def test_exception_positive(self):
        """Assert that an exception is silenced if it contains the
        allowed_string text"""
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
        """Assert that an exception that does not contain the
        allowed_string text is properly raised"""
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
        """Test Base64 encoding produces correct result"""
        encoded = self.base_64.encode(self.utf8)
        self.assertEqual(encoded, self.base64)

    def test_decode(self):
        """Test Base64 decoding produces correct result"""
        decoded = self.base_64.decode(self.base64)
        self.assertEqual(decoded, self.utf8)


class test_QoS(Case):

    def mock_message_factory(self):
        m_delivery_tag = self.delivery_tag_generator.next()
        m = 'message %s' % m_delivery_tag
        return (m, m_delivery_tag)

    def add_n_messages_to_qos(self, n, qos):
        for i in range(n):
            self.add_message_to_qos(qos)

    def add_message_to_qos(self, qos):
        m, m_delivery_tag = self.mock_message_factory()
        qos.append(m, m_delivery_tag)

    def setUp(self):
        self.qos_no_limit = QoS()
        self.qos_limit_2 = QoS(prefetch_count=2)
        self.delivery_tag_generator = count(1)

    def test_init_no_params(self):
        """Check that internal state is correct after initialization"""
        self.assertEqual(self.qos_no_limit.prefetch_count, 0)
        self.assertIsInstance(self.qos_no_limit._not_yet_acked, OrderedDict)

    def test_init_with_params(self):
        """Check that internal state is correct after initialization with
        prefetch_count"""
        self.assertEqual(self.qos_limit_2.prefetch_count, 2)

    def test_can_consume_no_limit(self):
        """can_consume shall always return True with no prefetch limits"""
        self.assertTrue(self.qos_no_limit.can_consume())
        self.add_n_messages_to_qos(3, self.qos_no_limit)
        self.assertTrue(self.qos_no_limit.can_consume())

    def test_can_consume_with_limit(self):
        """can_consume shall return False only when the QoS holds
        prefetch_count number of messages"""
        self.assertTrue(self.qos_limit_2.can_consume())
        self.add_n_messages_to_qos(2, self.qos_limit_2)
        self.assertFalse(self.qos_limit_2.can_consume())

    def test_can_consume_max_estimate_no_limit(self):
        """can_consume shall always return 1 with no prefetch limits"""
        self.assertEqual(self.qos_no_limit.can_consume_max_estimate(), 1)
        self.add_n_messages_to_qos(3, self.qos_no_limit)
        self.assertEqual(self.qos_no_limit.can_consume_max_estimate(), 1)

    def test_can_consume_max_estimate_with_limit(self):
        """while prefetch limits are enabled, can_consume shall return (
        prefetch_limit - #messages) as the number of messages is
        incremented from 0 to prefetch_limit"""
        self.assertEqual(self.qos_limit_2.can_consume_max_estimate(), 2)
        self.add_message_to_qos(self.qos_limit_2)
        self.assertEqual(self.qos_limit_2.can_consume_max_estimate(), 1)
        self.add_message_to_qos(self.qos_limit_2)
        self.assertEqual(self.qos_limit_2.can_consume_max_estimate(), 0)

    def test_append(self):
        """Append two messages and check inside the QoS object that they
        were put into the internal data structures correctly"""
        qos = self.qos_no_limit
        m1, m1_tag = self.mock_message_factory()
        m2, m2_tag = self.mock_message_factory()
        qos.append(m1, m1_tag)
        length_not_yet_acked = len(qos._not_yet_acked)
        self.assertEqual(length_not_yet_acked, 1)
        checked_message1 = qos._not_yet_acked[m1_tag]
        self.assertIs(m1, checked_message1)
        qos.append(m2, m2_tag)
        length_not_yet_acked = len(qos._not_yet_acked)
        self.assertEqual(length_not_yet_acked, 2)
        checked_message2 = qos._not_yet_acked[m2_tag]
        self.assertIs(m2, checked_message2)

    def test_get(self):
        """Append two messages, and use get to receive them"""
        qos = self.qos_no_limit
        m1, m1_tag = self.mock_message_factory()
        m2, m2_tag = self.mock_message_factory()
        qos.append(m1, m1_tag)
        qos.append(m2, m2_tag)
        message1 = qos.get(m1_tag)
        message2 = qos.get(m2_tag)
        self.assertEquals(m1, message1)
        self.assertEquals(m2, message2)

    def test_ack(self):
        """Load a mock message, ack the message, and ensure the right
        call is made to the acknowledge method in the qpid.messaging client
        library"""
        message = Mock()
        qos = self.qos_no_limit
        qos.append(message, 1)
        qos.ack(1)
        message._receiver.session.acknowledge.assert_called_with(
            message=message)

    def test_ack_requeue_true(self):
        """Load a mock message, reject the message with requeue=True,
        and ensure the right call to acknowledge is made"""
        message = Mock()
        mock_QpidDisposition = Mock(return_value='disposition')
        mock_RELEASED = Mock()
        kombu.transport.qpid.QpidDisposition = mock_QpidDisposition
        kombu.transport.qpid.RELEASED = mock_RELEASED
        qos = self.qos_no_limit
        qos.append(message, 1)
        qos.reject(1, requeue=True)
        mock_QpidDisposition.assert_called_with(mock_RELEASED)
        message._receiver.session.acknowledge.assert_called_with(
            message=message, disposition='disposition')

    def test_ack_requeue_false(self):
        """Load a mock message, reject the message with requeue=False,
        and ensure the right call to acknowledge is made"""
        message = Mock()
        mock_QpidDisposition = Mock(return_value='disposition')
        mock_REJECTED = Mock()
        kombu.transport.qpid.QpidDisposition = mock_QpidDisposition
        kombu.transport.qpid.REJECTED = mock_REJECTED
        qos = self.qos_no_limit
        qos.append(message, 1)
        qos.reject(1, requeue=False)
        mock_QpidDisposition.assert_called_with(mock_REJECTED)
        message._receiver.session.acknowledge.assert_called_with(
            message=message, disposition='disposition')


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
