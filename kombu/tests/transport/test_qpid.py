from __future__ import absolute_import

import datetime
import socket
import time

from itertools import count

QPID_NOT_AVAILABLE = False
try:
    import qpid.messaging.exceptions
    import qpidtoollibs     # noqa
except ImportError:
    QPID_NOT_AVAILABLE = True

import kombu.five
from kombu.transport.qpid import QpidMessagingExceptionHandler, QoS, Message
from kombu.transport.qpid import Channel
from kombu.transport.qpid import Connection, Transport
from kombu.transport.virtual import Base64
from kombu.utils.compat import OrderedDict
from kombu.tests.case import Case, Mock, SkipTest
from kombu.tests.case import patch, skip_if_not_module


class ExtraAssertionsMixin(object):
    """A mixin class adding assertDictEqual and assertDictContainsSubset"""

    def assertDictEqual(self, a, b):
        """
        Test that two dictionaries are equal.

        Implemented here because this method was not available until Python
        2.6.  This asserts that the unique set of keys are the same in a and b.
        Also asserts that the value of each key is the same in a and b using
        the is operator.
        """
        self.assertEqual(set(a.keys()), set(b.keys()))
        for key in a.keys():
            self.assertEqual(a[key], b[key])

    def assertDictContainsSubset(self, a, b):
        """
        Assert that all the key/value pairs in a exist in b.
        """
        for key in a.keys():
            self.assertTrue(key in b)
            self.assertTrue(a[key] == b[key])


class TestQpidMessagingExceptionHandler(Case):

    allowed_string = 'object in use'
    not_allowed_string = 'a different string'

    def setUp(self):
        """Create a mock ExceptionHandler for testing by this object."""
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.handler = QpidMessagingExceptionHandler(self.allowed_string)

    def test_string_stored(self):
        """Assert that the allowed_exception_string is stored correctly"""
        handler_string = self.handler.allowed_exception_string
        self.assertEqual(self.allowed_string, handler_string)

    def test_exception_positive(self):
        """Assert that an exception is silenced if it contains the
        allowed_string text
        """
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
        allowed_string text is properly raised
        """
        exception_to_raise = Exception(self.not_allowed_string)

        def exception_raise_func():
            raise exception_to_raise
        decorated_func = self.handler(exception_raise_func)
        self.assertRaises(Exception, decorated_func)


class TestQoS__init__(Case):

    def setUp(self):
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_session = Mock()
        self.qos = QoS(self.mock_session)

    def test__init__prefetch_default_set_correct_without_prefetch_value(self):
        self.assertEqual(self.qos.prefetch_count, 1)

    def test__init__prefetch_is_hard_set_to_one(self):
        qos_limit_two = QoS(self.mock_session)
        self.assertEqual(qos_limit_two.prefetch_count, 1)

    def test__init___not_yet_acked_is_initialized(self):
        self.assertTrue(isinstance(self.qos._not_yet_acked, OrderedDict))


class TestQoSCanConsume(Case):

    def setUp(self):
        session = Mock()
        self.qos = QoS(session)

    def test_True_when_prefetch_limit_is_zero(self):
        self.qos.prefetch_count = 0
        self.qos._not_yet_acked = []
        self.assertTrue(self.qos.can_consume())

    def test_True_when_len_of__not_yet_acked_is_lt_prefetch_count(self):
        self.qos.prefetch_count = 3
        self.qos._not_yet_acked = ['a', 'b']
        self.assertTrue(self.qos.can_consume())

    def test_False_when_len_of__not_yet_acked_is_eq_prefetch_count(self):
        self.qos.prefetch_count = 3
        self.qos._not_yet_acked = ['a', 'b', 'c']
        self.assertFalse(self.qos.can_consume())


class TestQoSCanConsumeMaxEstimate(Case):

    def setUp(self):
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_session = Mock()
        self.qos = QoS(self.mock_session)

    def test_return_one_when_prefetch_count_eq_zero(self):
        self.qos.prefetch_count = 0
        self.assertEqual(self.qos.can_consume_max_estimate(), 1)

    def test_return_prefetch_count_sub_len__not_yet_acked(self):
        self.qos._not_yet_acked = ['a', 'b']
        self.qos.prefetch_count = 4
        self.assertEqual(self.qos.can_consume_max_estimate(), 2)


class TestQoSAck(Case):

    def setUp(self):
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_session = Mock()
        self.qos = QoS(self.mock_session)

    def test_ack_pops__not_yet_acked(self):
        message = Mock()
        self.qos.append(message, 1)
        self.assertTrue(1 in self.qos._not_yet_acked)
        self.qos.ack(1)
        self.assertTrue(1 not in self.qos._not_yet_acked)

    def test_ack_calls_session_acknowledge_with_message(self):
        message = Mock()
        self.qos.append(message, 1)
        self.qos.ack(1)
        self.qos.session.acknowledge.assert_called_with(message=message)


class TestQoSReject(Case):

    def setUp(self):
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_session = Mock()
        self.mock_message = Mock()
        self.qos = QoS(self.mock_session)

    def test_reject_pops__not_yet_acked(self):
        self.qos.append(self.mock_message, 1)
        self.assertTrue(1 in self.qos._not_yet_acked)
        self.qos.reject(1)
        self.assertTrue(1 not in self.qos._not_yet_acked)

    @patch('qpid.messaging.Disposition')
    @patch('qpid.messaging.RELEASED')
    def test_reject_requeue_true(self, mock_RELEASED, mock_QpidDisposition):
        self.qos.append(self.mock_message, 1)
        self.qos.reject(1, requeue=True)
        mock_QpidDisposition.assert_called_with(mock_RELEASED)
        self.qos.session.acknowledge.assert_called_with(
            message=self.mock_message,
            disposition=mock_QpidDisposition.return_value)

    @patch('qpid.messaging.Disposition')
    @patch('qpid.messaging.REJECTED')
    def test_reject_requeue_false(self, mock_REJECTED, mock_QpidDisposition):
        message = Mock()
        self.qos.append(message, 1)
        self.qos.reject(1, requeue=False)
        mock_QpidDisposition.assert_called_with(mock_REJECTED)
        self.qos.session.acknowledge.assert_called_with(
            message=message, disposition=mock_QpidDisposition.return_value)


class TestQoS(Case):

    def mock_message_factory(self):
        """Create and return a mock message tag and delivery_tag."""
        m_delivery_tag = self.delivery_tag_generator.next()
        m = 'message %s' % m_delivery_tag
        return (m, m_delivery_tag)

    def add_n_messages_to_qos(self, n, qos):
        """Add N mock messages into the passed in qos object"""
        for i in range(n):
            self.add_message_to_qos(qos)

    def add_message_to_qos(self, qos):
        """Add a single mock message into the passed in qos object.

        Uses the mock_message_factory() to create the message and
        delivery_tag.
        """
        m, m_delivery_tag = self.mock_message_factory()
        qos.append(m, m_delivery_tag)

    def setUp(self):
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_session = Mock()
        self.qos_no_limit = QoS(self.mock_session)
        self.qos_limit_2 = QoS(self.mock_session, prefetch_count=2)
        self.delivery_tag_generator = count(1)

    def test_append(self):
        """Append two messages and check inside the QoS object that they
        were put into the internal data structures correctly
        """
        qos = self.qos_no_limit
        m1, m1_tag = self.mock_message_factory()
        m2, m2_tag = self.mock_message_factory()
        qos.append(m1, m1_tag)
        length_not_yet_acked = len(qos._not_yet_acked)
        self.assertEqual(length_not_yet_acked, 1)
        checked_message1 = qos._not_yet_acked[m1_tag]
        self.assertTrue(m1 is checked_message1)
        qos.append(m2, m2_tag)
        length_not_yet_acked = len(qos._not_yet_acked)
        self.assertEqual(length_not_yet_acked, 2)
        checked_message2 = qos._not_yet_acked[m2_tag]
        self.assertTrue(m2 is checked_message2)

    def test_get(self):
        """Append two messages, and use get to receive them"""
        qos = self.qos_no_limit
        m1, m1_tag = self.mock_message_factory()
        m2, m2_tag = self.mock_message_factory()
        qos.append(m1, m1_tag)
        qos.append(m2, m2_tag)
        message1 = qos.get(m1_tag)
        message2 = qos.get(m2_tag)
        self.assertTrue(m1 is message1)
        self.assertTrue(m2 is message2)


class TestConnection(ExtraAssertionsMixin, Case):

    @patch('qpid.messaging.Connection')
    def setUp(self, QpidConnection):
        """Setup a Connection with sane connection parameters."""
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.connection_options = {'host': 'localhost',
                                   'port': 5672,
                                   'username': 'guest',
                                   'password': 'guest',
                                   'transport': 'tcp',
                                   'timeout': 10,
                                   'sasl_mechanisms': 'PLAIN'}
        self.created_connection = Mock()
        QpidConnection.establish = Mock(return_value=self.created_connection)
        self.mock_qpid_connection = QpidConnection
        self.my_connection = Connection(**self.connection_options)

    def test_init_variables(self):
        """Test that all init params are internally stored correctly
        """
        self.assertDictEqual(self.connection_options,
                             self.my_connection.connection_options)
        self.assertTrue(isinstance(self.my_connection.channels, list))
        self.assertTrue(isinstance(self.my_connection._callbacks, dict))
        self.mock_qpid_connection.establish.assert_called_with(
            **self.connection_options)
        internal_conn = self.my_connection._qpid_conn
        self.assertTrue(self.created_connection is internal_conn)

    def test_verify_connection_class_attributes(self):
        """Verify that Channel class attribute is set correctly"""
        self.assertEqual(Channel, Connection.Channel)

    def test_get_qpid_connection(self):
        """Test that get_qpid_connection returns the connection."""
        mock_connection = Mock()
        self.my_connection._qpid_conn = mock_connection
        returned_connection = self.my_connection.get_qpid_connection()
        self.assertTrue(mock_connection is returned_connection)

    def test_close_channel_exists(self):
        """Test that calling close_channel() with a valid channel removes
        the channel from self.channels and sets channel.connection to None.
        """
        mock_channel = Mock()
        self.my_connection.channels = [mock_channel]
        mock_channel.connection = True
        self.my_connection.close_channel(mock_channel)
        self.assertEqual(self.my_connection.channels, [])
        self.assertTrue(mock_channel.connection is None)

    def test_close_channel_does_not_exist(self):
        """Test that calling close_channel() with an invalid channel does
        not raise a ValueError and sets channel.connection to None.
        """
        self.my_connection.channels = Mock()
        self.my_connection.channels.remove = Mock(side_effect=ValueError())
        mock_channel = Mock()
        mock_channel.connection = True
        self.my_connection.close_channel(mock_channel)
        self.assertTrue(mock_channel.connection is None)


class ChannelTestBase(Case):
    """Provides a basic setup for testing a Channel"""

    @skip_if_not_module('qpidtoollibs')
    @patch('kombu.transport.qpid.qpidtoollibs.BrokerAgent')
    def setUp(self, mock_broker_agent):
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_broker_agent = mock_broker_agent
        self.conn = Mock()
        self.transport = Mock()
        self.channel = Channel(self.conn, self.transport)


class TestChannelPut(ChannelTestBase):

    @patch('qpid.messaging.Message')
    def test_channel__put_onto_queue(self, mock_Message_cls):
        routing_key = 'routingkey'
        mock_message = Mock()

        self.channel._put(routing_key, mock_message)

        address_string = '%s; {assert: always, node: {type: queue}}' % \
                         routing_key
        self.transport.session.sender.assert_called_with(address_string)
        mock_Message_cls.assert_called_with(content=mock_message, subject=None)
        mock_sender = self.transport.session.sender.return_value
        mock_sender.send.assert_called_with(mock_Message_cls.return_value,
                                            sync=True)
        mock_sender.close.assert_called_with()

    @patch('qpid.messaging.Message')
    def test_channel__put_onto_exchange(self, mock_Message_cls):
        mock_routing_key = 'routingkey'
        mock_exchange_name = 'myexchange'
        mock_message = Mock()

        self.channel._put(mock_routing_key, mock_message, mock_exchange_name)

        address_string = '%s/%s; {assert: always, node: {type: topic}}' % \
                         (mock_exchange_name, mock_routing_key)
        self.transport.session.sender.assert_called_with(address_string)
        mock_Message_cls.assert_called_with(content=mock_message,
                                            subject=mock_routing_key)
        mock_sender = self.transport.session.sender.return_value
        mock_sender.send.assert_called_with(mock_Message_cls.return_value,
                                            sync=True)
        mock_sender.close.assert_called_with()


class TestChannelGet(ChannelTestBase):

    def test_channel__get(self):
        mock_queue = Mock()

        result = self.channel._get(mock_queue)

        self.transport.session.receiver.assert_called_once_with(mock_queue)
        mock_rx = self.transport.session.receiver.return_value
        mock_rx.fetch.assert_called_once_with(timeout=0)
        mock_rx.close.assert_called_once_with()
        self.assertTrue(mock_rx.fetch.return_value is result)


class TestChannelClose(ChannelTestBase):

    @patch.object(Channel, 'basic_cancel')
    def setUp(self, mock_basic_cancel):
        super(TestChannelClose, self).setUp()
        self.mock_basic_cancel = mock_basic_cancel
        self.mock_receiver1 = Mock()
        self.mock_receiver2 = Mock()
        self.channel._receivers = {1: self.mock_receiver1,
                                   2:self.mock_receiver2}
        self.channel.closed = False
        self.channel.close()

    def test_channel_close_sets_close_attribute(self):
        self.assertTrue(self.channel.closed)

    def test_channel_close_calls_basic_cancel_on_all_receivers(self):
        self.mock_basic_cancel.assert_any_call(1)
        self.mock_basic_cancel.assert_any_call(2)

    def test_channel_close_calls_close_channel_on_connection(self):
        self.conn.close_channel.assert_called_once_with(self.channel)

    def test_channel_calls_close_on_broker_agent(self):
        self.channel._broker.close.assert_called_once_with()


class TestChannelBasicQoS(ChannelTestBase):

    def test_channel_basic_qos_always_returns_one(self):
        self.channel.basic_qos(2)
        self.assertTrue(self.channel.qos.prefetch_count is 1)


class TestChannelBasicGet(ChannelTestBase):

    def setUp(self):
        super(TestChannelBasicGet, self).setUp()
        self.channel.Message = Mock()
        self.channel._get = Mock()

    def test_channel_basic_get_calls__get_with_queue(self):
        mock_queue = Mock()
        self.channel.basic_get(mock_queue)
        self.channel._get.assert_called_once_with(mock_queue)

    def test_channel_basic_get_creates_Message_correctly(self):
        mock_queue = Mock()
        self.channel.basic_get(mock_queue)
        mock_raw_message = self.channel._get.return_value.content
        self.channel.Message.assert_called_once_with(self.channel,
                                                     mock_raw_message)

    def test_channel_basic_get_acknowledges_message_by_default(self):
        mock_queue = Mock()
        self.channel.basic_get(mock_queue)
        mock_qpid_message = self.channel._get.return_value
        acknowledge = self.transport.session.acknowledge
        acknowledge.assert_called_once_with(message=mock_qpid_message)

    def test_channel_basic_get_acknowledges_message_with_no_ack_False(self):
        mock_queue = Mock()
        self.channel.basic_get(mock_queue, no_ack=False)
        mock_qpid_message = self.channel._get.return_value
        acknowledge = self.transport.session.acknowledge
        acknowledge.assert_called_once_with(message=mock_qpid_message)

    def test_channel_basic_get_acknowledges_message_with_no_ack_True(self):
        mock_queue = Mock()
        self.channel.basic_get(mock_queue, no_ack=True)
        mock_qpid_message = self.channel._get.return_value
        acknowledge = self.transport.session.acknowledge
        acknowledge.assert_called_once_with(message=mock_qpid_message)

    def test_channel_basic_get_returns_correct_message(self):
        mock_queue = Mock()
        basic_get_result = self.channel.basic_get(mock_queue)
        expected_message = self.channel.Message.return_value
        self.assertTrue(expected_message is basic_get_result)

    def test_basic_get_returns_None_when_channel__get_raises_Empty(self):
        mock_queue = Mock()
        self.channel._get = Mock(side_effect=kombu.five.Empty)
        basic_get_result = self.channel.basic_get(mock_queue)
        self.assertEqual(self.channel.Message.call_count, 0)
        self.assertTrue(basic_get_result is None)


class TestChannelBasicCancel(ChannelTestBase):

    def setUp(self):
        super(TestChannelBasicCancel, self).setUp()
        self.channel._receivers = {1: Mock()}

    def test_channel_basic_cancel_no_error_if_consumer_tag_not_found(self):
        self.channel.basic_cancel(2)

    def test_channel_basic_cancel_pops_receiver(self):
        self.channel.basic_cancel(1)
        self.assertTrue(1 not in self.channel._receivers)

    def test_channel_basic_cancel_pops__tag_to_queue(self):
        self.channel._tag_to_queue = Mock()
        self.channel.basic_cancel(1)
        self.channel._tag_to_queue.pop.assert_called_once_with(1, None)

    def test_channel_basic_cancel_pops_connection__callbacks(self):
        self.channel._tag_to_queue = Mock()
        self.channel.basic_cancel(1)
        mock_queue = self.channel._tag_to_queue.pop.return_value
        self.conn._callbacks.pop.assert_called_once_with(mock_queue, None)


class TestChannelInit(ChannelTestBase, ExtraAssertionsMixin):

    def test_channel___init__sets_variables_as_expected(self):
        self.assertTrue(self.conn is self.channel.connection)
        self.assertTrue(self.transport is self.channel.transport)
        self.assertFalse(self.channel.closed)
        self.conn.get_qpid_connection.assert_called_once_with()
        expected_broker_agent = self.mock_broker_agent.return_value
        self.assertTrue(self.channel._broker is expected_broker_agent)
        self.assertDictEqual(self.channel._tag_to_queue, {})
        self.assertDictEqual(self.channel._receivers, {})
        self.assertTrue(self.channel._qos is None)


class TestChannelBasicConsume(ChannelTestBase, ExtraAssertionsMixin):

    def setUp(self):
        super(TestChannelBasicConsume, self).setUp()
        self.conn._callbacks = {}

    def test_channel_basic_consume_adds_queue_to__tag_to_queue(self):
        mock_tag = Mock()
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), mock_tag)
        self.assertDictEqual({mock_tag: mock_queue}, self.channel._tag_to_queue)

    def test_channel_basic_consume_adds_entry_to_connection__callbacks(self):
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), Mock())
        self.assertTrue(mock_queue in self.conn._callbacks)
        if not hasattr(self.conn._callbacks[mock_queue], '__call__'):
            self.fail('Callback stored must be callable')

    def test_channel_basic_consume_creates_new_receiver(self):
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), Mock())
        self.transport.session.receiver.assert_called_once_with(mock_queue)

    def test_channel_basic_consume_saves_new_receiver(self):
        mock_tag = Mock()
        self.channel.basic_consume(Mock(), Mock(), Mock(), mock_tag)
        new_mock_receiver = self.transport.session.receiver.return_value
        expected_dict = {mock_tag: new_mock_receiver}
        self.assertDictEqual(expected_dict, self.channel._receivers)

    def test_channel_basic_consume_sets_capacity_on_new_receiver(self):
        mock_prefetch_count = Mock()
        self.channel.qos.prefetch_count = mock_prefetch_count
        self.channel.basic_consume(Mock(), Mock(), Mock(), Mock())
        new_receiver = self.transport.session.receiver.return_value
        self.assertTrue(new_receiver.capacity is mock_prefetch_count)

    def get_callback(self, no_ack=Mock(), original_cb=Mock()):
        self.channel.Message = Mock()
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, no_ack, original_cb, Mock())
        return self.conn._callbacks[mock_queue]

    def test_channel_basic_consume_callback_creates_Message_correctly(self):
        callback = self.get_callback()
        mock_qpid_message = Mock()
        callback(mock_qpid_message)
        mock_content = mock_qpid_message.content
        self.channel.Message.assert_called_once_with(self.channel,
                                                     mock_content)

    def test_channel_basic_consume_callback_adds_message_to_QoS(self):
        self.channel._qos = Mock()
        callback = self.get_callback()
        mock_qpid_message = Mock()
        callback(mock_qpid_message)
        mock_delivery_tag = self.channel.Message.return_value.delivery_tag
        self.channel._qos.append.assert_called_once_with(mock_qpid_message,
                                                         mock_delivery_tag)

    def test_channel_basic_consume_callback_gratuitously_acks(self):
        self.channel.basic_ack = Mock()
        callback = self.get_callback()
        mock_qpid_message = Mock()
        callback(mock_qpid_message)
        mock_delivery_tag = self.channel.Message.return_value.delivery_tag
        self.channel.basic_ack.assert_called_once_with(mock_delivery_tag)

    def test_channel_basic_consume_callback_does_not_ack_when_needed(self):
        self.channel.basic_ack = Mock()
        callback = self.get_callback(no_ack=False)
        mock_qpid_message = Mock()
        callback(mock_qpid_message)
        self.assertTrue(not self.channel.basic_ack.called)

    def test_channel_basic_consume_callback_calls_real_callback(self):
        self.channel.basic_ack = Mock()
        mock_original_callback = Mock()
        callback = self.get_callback(original_cb=mock_original_callback)
        mock_qpid_message = Mock()
        callback(mock_qpid_message)
        expected_message = self.channel.Message.return_value
        mock_original_callback.assert_called_once_with(expected_message)


class TestChannel(ExtraAssertionsMixin, Case):

    @skip_if_not_module('qpidtoollibs')
    @patch('qpidtoollibs.BrokerAgent')
    def setUp(self, mock_BrokerAgent):
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_connection = Mock()
        self.mock_qpid_connection = Mock()
        self.mock_qpid_session = Mock()
        self.mock_qpid_connection.session = \
            Mock(return_value=self.mock_qpid_session)
        self.mock_connection.get_qpid_connection = \
            Mock(return_value=self.mock_qpid_connection)
        self.mock_transport = Mock()
        self.mock_broker = Mock()
        self.mock_Message = Mock()
        self.mock_BrokerAgent = mock_BrokerAgent
        mock_BrokerAgent.return_value = self.mock_broker
        self.my_channel = Channel(self.mock_connection,
                                  self.mock_transport)
        self.my_channel.Message = self.mock_Message

    def test_verify_QoS_class_attribute(self):
        """Verify that the class attribute QoS refers to the QoS object"""
        self.assertTrue(QoS is Channel.QoS)

    def test_verify_Message_class_attribute(self):
        """Verify that the class attribute Message refers to the Message
        object
        """
        self.assertTrue(Message is Channel.Message)

    def test_body_encoding_class_attribute(self):
        """Verify that the class attribute body_encoding is set to base64"""
        self.assertEqual('base64', Channel.body_encoding)

    def test_codecs_class_attribute(self):
        """Verify that the codecs class attribute has a correct key and
        value
        """
        self.assertTrue(isinstance(Channel.codecs, dict))
        self.assertTrue('base64' in Channel.codecs)
        self.assertTrue(isinstance(Channel.codecs['base64'], Base64))

    def test_delivery_tags(self):
        """Test that _delivery_tags is using itertools"""
        self.assertTrue(isinstance(Channel._delivery_tags, count))

    def test_purge(self):
        """Test purging a queue that has messages, and verify the return
        value.
        """
        message_count = 5
        mock_queue = Mock()
        mock_queue_to_purge = Mock()
        mock_queue_to_purge.values = {'msgDepth': message_count}
        self.mock_broker.getQueue.return_value = mock_queue_to_purge
        result = self.my_channel._purge(mock_queue)
        self.mock_broker.getQueue.assert_called_with(mock_queue)
        mock_queue_to_purge.purge.assert_called_with(message_count)
        self.assertEqual(message_count, result)

    def test_size(self):
        """Test getting the number of messages in a queue specified by
        name and returning them.
        """
        message_count = 5
        mock_queue = Mock()
        mock_queue_to_check = Mock()
        mock_queue_to_check.values = {'msgDepth': message_count}
        self.mock_broker.getQueue.return_value = mock_queue_to_check
        result = self.my_channel._size(mock_queue)
        self.mock_broker.getQueue.assert_called_with(mock_queue)
        self.assertEqual(message_count, result)

    def test_delete(self):
        """Test deleting a queue calls purge and delQueue with queue name"""
        mock_queue = Mock()
        self.my_channel._purge = Mock()
        result = self.my_channel._delete(mock_queue)
        self.my_channel._purge.assert_called_with(mock_queue)
        self.mock_broker.delQueue.assert_called_with(mock_queue)
        self.assertTrue(result is None)

    def test_has_queue_true(self):
        """Test checking if a queue exists, and it does"""
        mock_queue = Mock()
        self.mock_broker.getQueue.return_value = True
        result = self.my_channel._has_queue(mock_queue)
        self.assertTrue(result)

    def test_has_queue_false(self):
        """Test checking if a queue exists, and it does not"""
        mock_queue = Mock()
        self.mock_broker.getQueue.return_value = False
        result = self.my_channel._has_queue(mock_queue)
        self.assertFalse(result)

    @patch('amqp.protocol.queue_declare_ok_t')
    def test_queue_declare_with_exception_raised(self,
                                                 mock_queue_declare_ok_t):
        """Test declare_queue, where an exception is raised and silenced"""
        mock_queue = Mock()
        mock_passive = Mock()
        mock_durable = Mock()
        mock_exclusive = Mock()
        mock_auto_delete = Mock()
        mock_nowait = Mock()
        mock_arguments = Mock()
        mock_msg_count = Mock()
        mock_queue.startswith.return_value = False
        mock_queue.endswith.return_value = False
        options = {'passive': mock_passive,
                   'durable': mock_durable,
                   'exclusive': mock_exclusive,
                   'auto-delete': mock_auto_delete,
                   'arguments': mock_arguments,
                   'qpid.auto_delete_timeout': 3}
        mock_consumer_count = Mock()
        mock_return_value = Mock()
        values_dict = {'msgDepth': mock_msg_count,
                       'consumerCount': mock_consumer_count}
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        exception_to_raise = Exception('The foo object already exists.')
        self.mock_broker.addQueue.side_effect = exception_to_raise
        self.mock_broker.getQueue.return_value = mock_queue_data
        mock_queue_declare_ok_t.return_value = mock_return_value
        result = self.my_channel.queue_declare(mock_queue,
                                               passive=mock_passive,
                                               durable=mock_durable,
                                               exclusive=mock_exclusive,
                                               auto_delete=mock_auto_delete,
                                               nowait=mock_nowait,
                                               arguments=mock_arguments,
                                               )
        self.mock_broker.addQueue.assert_called_with(mock_queue,
                                                     options=options)
        mock_queue_declare_ok_t.assert_called_with(mock_queue,
                                                   mock_msg_count,
                                                   mock_consumer_count)
        self.assertTrue(mock_return_value is result)

    def test_queue_declare_set_ring_policy_for_celeryev(self):
        """Test declare_queue sets ring_policy for celeryev"""
        mock_queue = Mock()
        mock_queue.startswith.return_value = True
        mock_queue.endswith.return_value = False
        expected_default_options = {'passive': False,
                                    'durable': False,
                                    'exclusive': False,
                                    'auto-delete': True,
                                    'arguments': None,
                                    'qpid.auto_delete_timeout': 3,
                                    'qpid.policy_type': 'ring'}
        mock_msg_count = Mock()
        mock_consumer_count = Mock()
        values_dict = {'msgDepth': mock_msg_count,
                       'consumerCount': mock_consumer_count}
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        self.mock_broker.addQueue.return_value = None
        self.mock_broker.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.startswith.assert_called_with('celeryev')

    def test_queue_declare_set_ring_policy_for_pidbox(self):
        """Test declare_queue sets ring_policy for pidbox"""
        mock_queue = Mock()
        mock_queue.startswith.return_value = False
        mock_queue.endswith.return_value = True
        expected_default_options = {'passive': False,
                                    'durable': False,
                                    'exclusive': False,
                                    'auto-delete': True,
                                    'arguments': None,
                                    'qpid.auto_delete_timeout': 3,
                                    'qpid.policy_type': 'ring'}
        mock_msg_count = Mock()
        mock_consumer_count = Mock()
        values_dict = {'msgDepth': mock_msg_count,
                       'consumerCount': mock_consumer_count}
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        self.mock_broker.addQueue.return_value = None
        self.mock_broker.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.endswith.assert_called_with('pidbox')

    def test_queue_declare_ring_policy_not_set_as_expected(self):
        """Test declare_queue does not set ring_policy as expected"""
        mock_queue = Mock()
        mock_queue.startswith.return_value = False
        mock_queue.endswith.return_value = False
        expected_default_options = {'passive': False,
                                    'durable': False,
                                    'exclusive': False,
                                    'auto-delete': True,
                                    'arguments': None,
                                    'qpid.auto_delete_timeout': 3}
        mock_msg_count = Mock()
        mock_consumer_count = Mock()
        values_dict = {'msgDepth': mock_msg_count,
                       'consumerCount': mock_consumer_count}
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        self.mock_broker.addQueue.return_value = None
        self.mock_broker.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.startswith.assert_called_with('celeryev')
        mock_queue.endswith.assert_called_with('pidbox')

    def test_queue_declare_test_defaults(self):
        """Test declare_queue defaults"""
        mock_queue = Mock()
        mock_queue.startswith.return_value = False
        mock_queue.endswith.return_value = False
        expected_default_options = {'passive': False,
                                    'durable': False,
                                    'exclusive': False,
                                    'auto-delete': True,
                                    'arguments': None,
                                    'qpid.auto_delete_timeout': 3}
        mock_msg_count = Mock()
        mock_consumer_count = Mock()
        values_dict = {'msgDepth': mock_msg_count,
                       'consumerCount': mock_consumer_count}
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        self.mock_broker.addQueue.return_value = None
        self.mock_broker.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        self.mock_broker.addQueue.assert_called_with(
            mock_queue,
            options=expected_default_options)

    def test_queue_declare_raises_exception_not_silenced(self):
        """Test declare_queue, raise an exception that is raised and not silenced"""
        unique_exception = Exception('This exception should not be silenced')
        mock_queue = Mock()
        self.mock_broker.addQueue.side_effect = unique_exception
        self.assertRaises(unique_exception.__class__,
                          self.my_channel.queue_declare,
                          mock_queue)
        self.mock_broker.addQueue.assert_called_once()

    def test_queue_delete_if_empty_param(self):
        """Test the deletion of a queue with if_empty=True"""
        mock_queue = Mock()
        self.my_channel._has_queue = Mock(return_value=True)
        self.my_channel._size = Mock(return_value=5)
        result = self.my_channel.queue_delete(mock_queue, if_empty=True)
        self.my_channel._has_queue.assert_called_with(mock_queue)
        self.my_channel._size.assert_called_with(mock_queue)
        self.assertTrue(result is None)

    def test_queue_delete_if_unused_param(self):
        """Test the deletion of a queue with if_unused=True"""
        mock_queue = Mock()
        mock_queue_obj = Mock()
        mock_queue_attributes = {'consumerCount': 5}
        mock_queue_obj.getAttributes.return_value = mock_queue_attributes
        self.my_channel._has_queue = Mock(return_value=True)
        self.my_channel._size = Mock(return_value=5)
        self.mock_broker.getQueue.return_value = mock_queue_obj
        result = self.my_channel.queue_delete(mock_queue, if_unused=True)
        self.assertTrue(result is None)

    def test_queue_delete(self):
        """Test the deletion of a queue"""
        mock_queue = Mock()
        mock_queue_obj = Mock()
        mock_queue_attributes = {'consumerCount': 5}
        mock_queue_obj.getAttributes.return_value = mock_queue_attributes
        self.my_channel._has_queue = Mock(return_value=True)
        self.my_channel._size = Mock(return_value=5)
        self.my_channel._delete = Mock()
        self.mock_broker.getQueue.return_value = mock_queue_obj
        result = self.my_channel.queue_delete(mock_queue)
        self.my_channel._delete.assert_called_with(mock_queue)
        self.assertTrue(result is None)

    def test_exchange_declare_raises_exception_and_silenced(self):
        """Create exchange where an exception is raised and then silenced"""
        self.mock_broker.addExchange.side_effect = \
            Exception('The foo object already exists.')
        self.my_channel.exchange_declare()

    def test_exchange_declare_raises_exception_not_silenced(self):
        """Create Exchange where an exception is raised and not silenced"""
        unique_exception = Exception('This exception should not be silenced')
        self.mock_broker.addExchange.side_effect = unique_exception
        self.assertRaises(unique_exception.__class__,
                          self.my_channel.exchange_declare)

    def test_exchange_declare(self):
        """Create Exchange where an exception is NOT raised"""
        mock_exchange = Mock()
        mock_type = Mock()
        mock_durable = Mock()
        options = {'durable': mock_durable}
        result = self.my_channel.exchange_declare(mock_exchange,
                                                  mock_type,
                                                  mock_durable)
        self.mock_broker.addExchange.assert_called_with(mock_type,
                                                        mock_exchange,
                                                        options)
        self.assertTrue(result is None)

    def test_exchange_delete(self):
        """Test the deletion of an exchange by name"""
        mock_exchange = Mock()
        result = self.my_channel.exchange_delete(mock_exchange)
        self.mock_broker.delExchange.assert_called_with(mock_exchange)
        self.assertTrue(result is None)

    def test_queue_bind(self):
        """Test binding a queue to an exchange using a routing key"""
        mock_queue = Mock()
        mock_exchange = Mock()
        mock_routing_key = Mock()
        self.my_channel.queue_bind(mock_queue,
                                   mock_exchange,
                                   mock_routing_key)
        self.mock_broker.bind.assert_called_with(mock_exchange,
                                                 mock_queue,
                                                 mock_routing_key)

    def test_queue_unbind(self):
        """Test unbinding a queue from an exchange using a routing key"""
        mock_queue = Mock()
        mock_exchange = Mock()
        mock_routing_key = Mock()
        self.my_channel.queue_unbind(mock_queue,
                                     mock_exchange,
                                     mock_routing_key)
        self.mock_broker.unbind.assert_called_with(mock_exchange,
                                                   mock_queue,
                                                   mock_routing_key)

    def test_queue_purge(self):
        """Test purging a queue by name"""
        mock_queue = Mock()
        purge_result = Mock()
        self.my_channel._purge = Mock(return_value=purge_result)
        result = self.my_channel.queue_purge(mock_queue)
        self.my_channel._purge.assert_called_with(mock_queue)
        self.assertTrue(purge_result is result)

    @patch('kombu.transport.qpid.Channel.qos')
    def test_basic_ack(self, mock_qos):
        """Test that basic_ack calls the QoS object properly"""
        mock_delivery_tag = Mock()
        self.my_channel.basic_ack(mock_delivery_tag)
        mock_qos.ack.assert_called_with(mock_delivery_tag)

    @patch('kombu.transport.qpid.Channel.qos')
    def test_basic_reject(self, mock_qos):
        """Test that basic_reject calls the QoS object properly"""
        mock_delivery_tag = Mock()
        mock_requeue_value = Mock()
        self.my_channel.basic_reject(mock_delivery_tag, mock_requeue_value)
        mock_qos.reject.assert_called_with(mock_delivery_tag,
                                           requeue=mock_requeue_value)

    def test_qos_manager_is_none(self):
        """Test the qos property if the QoS object did not already exist"""
        self.my_channel._qos = None
        result = self.my_channel.qos
        self.assertTrue(isinstance(result, QoS))
        self.assertEqual(result, self.my_channel._qos)

    def test_qos_manager_already_exists(self):
        """Test the qos property if the QoS object already exists"""
        mock_existing_qos = Mock()
        self.my_channel._qos = mock_existing_qos
        result = self.my_channel.qos
        self.assertTrue(mock_existing_qos is result)

    def test_prepare_message(self):
        """Test that prepare_message() returns the correct result"""
        mock_body = Mock()
        mock_priority = Mock()
        mock_content_encoding = Mock()
        mock_content_type = Mock()
        mock_header1 = Mock()
        mock_header2 = Mock()
        mock_properties1 = Mock()
        mock_properties2 = Mock()
        headers = {'header1': mock_header1, 'header2': mock_header2}
        properties = {'properties1': mock_properties1,
                      'properties2': mock_properties2}
        result = self.my_channel.prepare_message(
            mock_body,
            priority=mock_priority,
            content_type=mock_content_type,
            content_encoding=mock_content_encoding,
            headers=headers,
            properties=properties)
        self.assertTrue(mock_body is result['body'])
        self.assertTrue(mock_content_encoding is result['content-encoding'])
        self.assertTrue(mock_content_type is result['content-type'])
        self.assertDictEqual(headers, result['headers'])
        self.assertDictContainsSubset(properties, result['properties'])
        self.assertTrue(mock_priority is
                        result['properties']['delivery_info']['priority'])

    @patch('__builtin__.buffer')
    @patch('kombu.transport.qpid.Channel.body_encoding')
    @patch('kombu.transport.qpid.Channel.encode_body')
    @patch('kombu.transport.qpid.Channel._put')
    def test_basic_publish(self, mock_put,
                           mock_encode_body,
                           mock_body_encoding,
                           mock_buffer):
        """Test basic_publish()"""
        mock_original_body = Mock()
        mock_encoded_body = 'this is my encoded body'
        mock_message = {'body': mock_original_body,
                        'properties': {'delivery_info': {}}}
        mock_encode_body.return_value = (mock_encoded_body,
                                         mock_body_encoding)
        mock_exchange = Mock()
        mock_routing_key = Mock()
        mock_encoded_buffered_body = Mock()
        mock_buffer.return_value = mock_encoded_buffered_body
        self.my_channel.basic_publish(mock_message,
                                      mock_exchange,
                                      mock_routing_key)
        mock_encode_body.assert_called_once(mock_original_body,
                                            mock_body_encoding)
        mock_buffer.assert_called_once(mock_encode_body)
        self.assertTrue(mock_message['body'] is mock_encoded_buffered_body)
        self.assertTrue(mock_message['properties']['body_encoding'] is
                        mock_body_encoding)
        self.assertTrue(
            isinstance(mock_message['properties']['delivery_tag'], int))
        self.assertTrue(mock_message['properties']['delivery_info']
                        ['exchange'] is mock_exchange)
        self.assertTrue(
            mock_message['properties']['delivery_info']['routing_key'] is
            mock_routing_key)
        mock_put.assert_called_with(mock_routing_key,
                                    mock_message,
                                    mock_exchange)

    @patch('kombu.transport.qpid.Channel.codecs')
    def test_encode_body_expected_encoding(self, mock_codecs):
        """Test if encode_body() works when encoding is set correctly"""
        mock_body = Mock()
        mock_encoder = Mock()
        mock_encoded_result = Mock()
        mock_codecs.get.return_value = mock_encoder
        mock_encoder.encode.return_value = mock_encoded_result
        result = self.my_channel.encode_body(mock_body, encoding='base64')
        expected_result = (mock_encoded_result, 'base64')
        self.assertEqual(expected_result, result)

    @patch('kombu.transport.qpid.Channel.codecs')
    def test_encode_body_not_expected_encoding(self, mock_codecs):
        """Test if encode_body() works when encoding is not set correctly"""
        mock_body = Mock()
        result = self.my_channel.encode_body(mock_body,
                                             encoding=None)
        expected_result = (mock_body, None)
        self.assertEqual(expected_result, result)

    @patch('kombu.transport.qpid.Channel.codecs')
    def test_decode_body_expected_encoding(self, mock_codecs):
        """Test if decode_body() works when encoding is set correctly"""
        mock_body = Mock()
        mock_decoder = Mock()
        mock_decoded_result = Mock()
        mock_codecs.get.return_value = mock_decoder
        mock_decoder.decode.return_value = mock_decoded_result
        result = self.my_channel.decode_body(mock_body, encoding='base64')
        self.assertEqual(mock_decoded_result, result)

    @patch('kombu.transport.qpid.Channel.codecs')
    def test_decode_body_not_expected_encoding(self, mock_codecs):
        """Test if decode_body() works when encoding is not set correctly"""
        mock_body = Mock()
        result = self.my_channel.decode_body(mock_body, encoding=None)
        self.assertEqual(mock_body, result)

    def test_typeof_exchange_exists(self):
        """Test that typeof() finds an exchange that already exists"""
        mock_exchange = Mock()
        mock_qpid_exchange = Mock()
        mock_attributes = {}
        mock_type = Mock()
        mock_attributes['type'] = mock_type
        mock_qpid_exchange.getAttributes.return_value = mock_attributes
        self.mock_broker.getExchange.return_value = mock_qpid_exchange
        result = self.my_channel.typeof(mock_exchange)
        self.assertTrue(mock_type is result)

    def test_typeof_exchange_does_not_exist(self):
        """Test that typeof() finds an exchange that does not exists"""
        mock_exchange = Mock()
        mock_default = Mock()
        self.mock_broker.getExchange.return_value = None
        result = self.my_channel.typeof(mock_exchange, default=mock_default)
        self.assertTrue(mock_default is result)


class TestTransportDrainEvents(Case):

    def setUp(self):
        self.transport = Transport(Mock())
        self.transport.session = Mock()
        self.mock_queue = Mock()
        self.mock_message = Mock()
        self.mock_conn = Mock()
        self.mock_callback = Mock()
        self.mock_conn._callbacks = {self.mock_queue: self.mock_callback}

    def mock_next_receiver(self, timeout):
        time.sleep(0.3)
        mock_receiver = Mock()
        mock_receiver.source = self.mock_queue
        mock_receiver.fetch.return_value = self.mock_message
        return mock_receiver

    def test_socket_timeout_raised_when_all_receivers_empty(self):
        qpid_Empty = qpid.messaging.exceptions.Empty
        self.transport.session.next_receiver.side_effect = qpid_Empty()
        self.assertRaises(socket.timeout, self.transport.drain_events, Mock())

    def test_socket_timeout_raised_when_by_timeout(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        self.assertRaises(socket.timeout, self.transport.drain_events,
                          self.mock_conn, timeout=1)

    def test_timeout_returns_no_earlier_then_asked_for(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        start_time = datetime.datetime.now()
        try:
            self.transport.drain_events(self.mock_conn, timeout=1)
        except socket.timeout:
            pass
        end_time = datetime.datetime.now()
        td = end_time - start_time
        elapsed_time_in_s = (td.microseconds + td.seconds * 10**6) / 10**6
        self.assertTrue(elapsed_time_in_s >= 1)

    def test_callback_is_called(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        try:
            self.transport.drain_events(self.mock_conn, timeout=1)
        except socket.timeout:
            pass
        self.mock_callback.assert_called_with(self.mock_message)


class TestTransportCreateChannel(Case):

    def setUp(self):
        self.transport = Transport(Mock())
        self.mock_conn = Mock()
        self.mock_new_channel = Mock()
        self.mock_conn.Channel.return_value = self.mock_new_channel
        self.returned_channel = self.transport.create_channel(self.mock_conn)

    def test_new_channel_created_from_connection(self):
        self.assertTrue(self.mock_new_channel is self.returned_channel)
        self.mock_conn.Channel.assert_called_with(self.mock_conn,
                                                  self.transport)

    def test_new_channel_added_to_connection_channel_list(self):
        append_method = self.mock_conn.channels.append
        append_method.assert_called_with(self.mock_new_channel)


class TestTransport(ExtraAssertionsMixin, Case):

    def setUp(self):
        """Creates a mock_client to be used in testing."""
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_client = Mock()

    def test_verify_Connection_attribute(self):
        """Verify that class attribute Connection refers to the connection
        object
        """
        self.assertTrue(Connection is Transport.Connection)

    def test_verify_default_port(self):
        """Verify that the class attribute default_port refers to the 5672
        properly
        """
        self.assertEqual(5672, Transport.default_port)

    def test_verify_polling_disabled(self):
        """Verify that polling is disabled"""
        self.assertTrue(Transport.polling_interval is None)

    def test_verify_does_not_support_asynchronous_events(self):
        """Verify that the Transport advertises that it does not support
        an asynchronous event model
        """
        self.assertFalse(Transport.supports_ev)

    def test_verify_driver_type_and_name(self):
        """Verify that the driver and type are correctly labeled on the
        class
        """
        self.assertEqual('qpid', Transport.driver_type)
        self.assertEqual('qpid', Transport.driver_name)

    def test_establish_connection_no_ssl(self):
        """Test that a call to establish connection creates a connection
        object with sane parameters and returns it.
        """
        self.mock_client.ssl = False
        self.mock_client.transport_options = []
        my_transport = Transport(self.mock_client)
        new_connection = Mock()
        my_transport.Connection = Mock(return_value=new_connection)
        my_transport.establish_connection()
        my_transport.Connection.assert_called_once()
        self.assertTrue(new_connection.client is self.mock_client)

    def test_close_connection(self):
        """Test that close_connection calls close on each channel in the
        list of channels on the connection object.
        """
        my_transport = Transport(self.mock_client)
        mock_connection = Mock()
        mock_channel_1 = Mock()
        mock_channel_2 = Mock()
        mock_connection.channels = [mock_channel_1, mock_channel_2]
        my_transport.close_connection(mock_connection)
        mock_channel_1.close.assert_called_with()
        mock_channel_2.close.assert_called_with()

    def test_default_connection_params(self):
        """Test that the default_connection_params are correct"""
        correct_params = {'userid': 'guest', 'password': 'guest',
                          'port': 5672, 'virtual_host': '',
                          'hostname': 'localhost',
                          'sasl_mechanisms': 'PLAIN'}
        my_transport = Transport(self.mock_client)
        result_params = my_transport.default_connection_params
        self.assertDictEqual(correct_params, result_params)
