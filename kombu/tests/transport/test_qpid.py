from __future__ import absolute_import, unicode_literals

import datetime
import select
import ssl
import socket
import sys
import threading
import time

from collections import OrderedDict
from itertools import count

from kombu.five import Empty, bytes_if_py2
from kombu.transport.qpid import AuthenticationFailure, QoS, Message
from kombu.transport.qpid import QpidMessagingExceptionHandler, Channel
from kombu.transport.qpid import Connection, ReceiversMonitor, Transport
from kombu.transport.qpid import ConnectionError
from kombu.transport.virtual import Base64
from kombu.tests.case import Case, Mock, call, skip
from kombu.tests.case import patch

QPID_MODULE = 'kombu.transport.qpid'


@skip.if_python3()
@skip.if_pypy()
class QPidCase(Case):
    pass


class MockException(Exception):
    pass


class BreakOutException(Exception):
    pass


class test_QPidMessagingExceptionHandler(QPidCase):
    allowed_string = 'object in use'
    not_allowed_string = 'a different string'

    def setup(self):
        # Create a mock ExceptionHandler for testing by this object.
        self.handler = QpidMessagingExceptionHandler(self.allowed_string)

    def test_string_stored(self):
        handler_string = self.handler.allowed_exception_string
        self.assertEqual(self.allowed_string, handler_string)

    def test_exception_positive(self):
        exception_to_raise = Exception(self.allowed_string)

        def exception_raise_fun():
            raise exception_to_raise
        decorated_fun = self.handler(exception_raise_fun)
        try:
            decorated_fun()
        except:
            self.fail('QpidMessagingExceptionHandler allowed an exception '
                      'to be raised that should have been silenced!')

    def test_exception_negative(self):
        exception_to_raise = Exception(self.not_allowed_string)

        def exception_raise_fun():
            raise exception_to_raise
        decorated_fun = self.handler(exception_raise_fun)
        with self.assertRaises(Exception):
            decorated_fun()


class test_QoS__init__(QPidCase):

    def setup(self):
        self.session = Mock(name='session')
        self.qos = QoS(self.session)

    def test_prefetch_default_set_correct_without_prefetch_value(self):
        self.assertEqual(self.qos.prefetch_count, 1)

    def test_prefetch_is_hard_set_to_one(self):
        qos_limit_two = QoS(self.session)
        self.assertEqual(qos_limit_two.prefetch_count, 1)

    def test_not_yet_acked_is_initialized(self):
        self.assertIsInstance(self.qos._not_yet_acked, OrderedDict)


class test_QoS__can_consume(QPidCase):

    def setup(self):
        session = Mock(name='session')
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


class test_QoS__can_consume_max_estimate(QPidCase):

    def setup(self):
        self.session = Mock(name='session')
        self.qos = QoS(self.session)

    def test_return_one_when_prefetch_count_eq_zero(self):
        self.qos.prefetch_count = 0
        self.assertEqual(self.qos.can_consume_max_estimate(), 1)

    def test_return_prefetch_count_sub_len__not_yet_acked(self):
        self.qos._not_yet_acked = ['a', 'b']
        self.qos.prefetch_count = 4
        self.assertEqual(self.qos.can_consume_max_estimate(), 2)


class test_QoS__ack(QPidCase):

    def setup(self):
        self.session = Mock(name='session')
        self.qos = QoS(self.session)

    def test_pops__not_yet_acked(self):
        message = Mock(name='message')
        self.qos.append(message, 1)
        self.assertIn(1, self.qos._not_yet_acked)
        self.qos.ack(1)
        self.assertNotIn(1, self.qos._not_yet_acked)

    def test_calls_session_acknowledge_with_message(self):
        message = Mock(name='message')
        self.qos.append(message, 1)
        self.qos.ack(1)
        self.qos.session.acknowledge.assert_called_with(message=message)


class test_QoS__reject(QPidCase):

    def setup(self):
        self.session = Mock(name='session')
        self.message = Mock(name='message')
        self.qos = QoS(self.session)
        self.patch_qpid = patch(QPID_MODULE + '.qpid')
        self.qpid = self.patch_qpid.start()
        self.Disposition = self.qpid.messaging.Disposition
        self.RELEASED = self.qpid.messaging.RELEASED
        self.REJECTED = self.qpid.messaging.REJECTED

    def teardown(self):
        self.patch_qpid.stop()

    def test_pops__not_yet_acked(self):
        self.qos.append(self.message, 1)
        self.assertIn(1, self.qos._not_yet_acked)
        self.qos.reject(1)
        self.assertNotIn(1, self.qos._not_yet_acked)

    def test_requeue_true(self):
        self.qos.append(self.message, 1)
        self.qos.reject(1, requeue=True)
        self.Disposition.assert_called_with(self.RELEASED)
        self.qos.session.acknowledge.assert_called_with(
            message=self.message,
            disposition=self.Disposition.return_value)

    def test_requeue_false(self):
        message = Mock(name='message')
        self.qos.append(message, 1)
        self.qos.reject(1, requeue=False)
        self.Disposition.assert_called_with(self.REJECTED)
        self.qos.session.acknowledge.assert_called_with(
            message=message, disposition=self.Disposition.return_value)


class test_QoS(QPidCase):

    def setup(self):
        self.session = Mock(name='session')
        self.qos_no_limit = QoS(self.session)
        self.qos_limit_2 = QoS(self.session, prefetch_count=2)
        self.delivery_tag_generator = count(1)

    def mock_message_factory(self):
        # Create and return a mock message tag and delivery_tag.
        m_delivery_tag = self.delivery_tag_generator.next()
        m = 'message %s' % m_delivery_tag
        return (m, m_delivery_tag)

    def add_n_messages_to_qos(self, n, qos):
        # Add N mock messages into the passed in qos object.
        for i in range(n):
            self.add_message_to_qos(qos)

    def add_message_to_qos(self, qos):
        # Add a single mock message into the passed in qos object.
        #
        # Uses the mock_message_factory() to create the message and
        # delivery_tag.
        m, m_delivery_tag = self.mock_message_factory()
        qos.append(m, m_delivery_tag)

    def test_append(self):
        # Append two messages and check inside the QoS object that
        # were put into the internal data structures correctly.
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
        # Append two messages, and use get to receive them.
        qos = self.qos_no_limit
        m1, m1_tag = self.mock_message_factory()
        m2, m2_tag = self.mock_message_factory()
        qos.append(m1, m1_tag)
        qos.append(m2, m2_tag)
        message1 = qos.get(m1_tag)
        message2 = qos.get(m2_tag)
        self.assertIs(m1, message1)
        self.assertIs(m2, message2)


@skip.if_python3()
@skip.if_pypy()
class ConnectionCase(QPidCase):

    @patch(QPID_MODULE + '.qpid')
    def setUp(self, qpid):
        self.connection_options = {
            'host': 'localhost',
            'port': 5672,
            'username': 'guest',
            'password': '',
            'transport': 'tcp',
            'timeout': 10,
            'sasl_mechanisms': 'ANONYMOUS PLAIN',
        }
        self.qpid_connection = qpid.messaging.Connection
        self.conn = Connection(**self.connection_options)
        super(ConnectionCase, self).setUp()


class test_Connection__init__(ConnectionCase):

    def test_stores_connection_options(self):
        # ensure that only one mech was passed into connection. The other
        # options should all be passed through as-is
        modified_conn_opts = self.connection_options
        modified_conn_opts['sasl_mechanisms'] = 'PLAIN'
        self.assertDictEqual(modified_conn_opts,
                             self.conn.connection_options)

    def test_variables(self):
        self.assertIsInstance(self.conn.channels, list)
        self.assertIsInstance(self.conn._callbacks, dict)

    def test_establishes_connection(self):
        modified_conn_opts = self.connection_options
        modified_conn_opts['sasl_mechanisms'] = 'PLAIN'
        self.qpid_connection.establish.assert_called_with(
            **modified_conn_opts)

    def test_saves_established_connection(self):
        created_conn = self.qpid_connection.establish.return_value
        self.assertIs(self.conn._qpid_conn, created_conn)

    @patch(QPID_MODULE + '.ConnectionError', new=(MockException,))
    @patch(QPID_MODULE + '.sys.exc_info')
    @patch(QPID_MODULE + '.qpid')
    def test_mutates_ConnError_by_message(self, qpid, exc_info):
        my_conn_error = MockException()
        my_conn_error.text = bytes_if_py2(
            'connection-forced: Authentication failed(320)')
        qpid.messaging.Connection.establish.side_effect = my_conn_error
        exc_info.return_value = (bytes_if_py2('a'), bytes_if_py2('b'), None)
        try:
            self.conn = Connection(**self.connection_options)
        except AuthenticationFailure as error:
            exc_info = sys.exc_info()
            self.assertNotIsInstance(error, MockException)
            self.assertEqual(exc_info[1], 'b')
            self.assertIsNone(exc_info[2])
        else:
            self.fail('ConnectionError type was not mutated correctly')

    @patch(QPID_MODULE + '.ConnectionError', new=(MockException,))
    @patch(QPID_MODULE + '.sys.exc_info')
    @patch(QPID_MODULE + '.qpid')
    def test_mutates_ConnError_by_code(self, qpid, exc_info):
        my_conn_error = MockException()
        my_conn_error.code = 320
        my_conn_error.text = bytes_if_py2('someothertext')
        qpid.messaging.Connection.establish.side_effect = my_conn_error
        exc_info.return_value = (bytes_if_py2('a'), bytes_if_py2('b'), None)
        try:
            self.conn = Connection(**self.connection_options)
        except AuthenticationFailure as error:
            exc_info = sys.exc_info()
            self.assertNotIsInstance(error, MockException)
            self.assertEqual(exc_info[1], bytes_if_py2('b'))
            self.assertIsNone(exc_info[2])
        else:
            self.fail('ConnectionError type was not mutated correctly')

    @patch(QPID_MODULE + '.ConnectionError', new=(MockException,))
    @patch(QPID_MODULE + '.sys.exc_info')
    @patch(QPID_MODULE + '.qpid')
    def test_unknown_connection_error(self, qpid, exc_info):
        # If we get a connection error that we don't understand,
        # bubble it up as-is
        my_conn_error = MockException()
        my_conn_error.code = 999
        my_conn_error.text = bytes_if_py2('someothertext')
        qpid.messaging.Connection.establish.side_effect = my_conn_error
        exc_info.return_value = ('a', 'b', None)
        try:
            self.conn = Connection(**self.connection_options)
        except Exception as error:
            self.assertEqual(error.code, 999)
        else:
            self.fail('Connection should have thrown an exception')

    @patch.object(Transport, 'channel_errors', new=(MockException,))
    @patch(QPID_MODULE + '.qpid')
    @patch(QPID_MODULE + '.ConnectionError', new=IOError)
    def test_non_qpid_error_raises(self, qpid):
        Qpid_Connection = qpid.messaging.Connection
        my_conn_error = SyntaxError()
        my_conn_error.text = bytes_if_py2(
            'some non auth related error message')
        Qpid_Connection.establish.side_effect = my_conn_error
        with self.assertRaises(SyntaxError):
            Connection(**self.connection_options)

    @patch(QPID_MODULE + '.qpid')
    @patch(QPID_MODULE + '.ConnectionError', new=IOError)
    def test_non_auth_conn_error_raises(self, qpid):
        Qpid_Connection = qpid.messaging.Connection
        my_conn_error = IOError()
        my_conn_error.text = bytes_if_py2(
            'some non auth related error message')
        Qpid_Connection.establish.side_effect = my_conn_error
        with self.assertRaises(IOError):
            Connection(**self.connection_options)


class test_Connection__class_attributes(ConnectionCase):

    def test_class_attributes(self):
        self.assertEqual(Channel, Connection.Channel)


class test_Connection__get_qpid_connection(ConnectionCase):

    def test_get_qpid_connection(self):
        self.conn._qpid_conn = Mock('qpid_conn')
        returned_connection = self.conn.get_qpid_connection()
        self.assertIs(self.conn._qpid_conn, returned_connection)


class test_Connection__close_channel(ConnectionCase):

    def setup(self):
        self.conn.channels = Mock(name='conn.channels')

    def test_removes_channel_from_channel_list(self):
        channel = Mock(name='channel')
        self.conn.close_channel(channel)
        self.conn.channels.remove.assert_called_once_with(channel)

    def test_handles_ValueError_being_raised(self):
        self.conn.channels.remove = Mock(
            name='conn.channels', side_effect=ValueError(),
        )
        try:
            self.conn.close_channel(Mock())
        except ValueError:
            self.fail('ValueError should not have been raised')

    def test_set_channel_connection_to_None(self):
        channel = Mock(name='channel')
        channel.connection = False
        self.conn.channels.remove = Mock(
            name='conn.channels', side_effect=ValueError(),
        )
        self.conn.close_channel(channel)
        self.assertIsNone(channel.connection)


@skip.if_python3()
@skip.if_pypy()
class ChannelCase(QPidCase):

    def setUp(self):
        self.patch_qpidtoollibs = patch(QPID_MODULE + '.qpidtoollibs')
        self.qpidtoollibs = self.patch_qpidtoollibs.start()
        self.broker_agent = self.qpidtoollibs.BrokerAgent
        self.conn = Mock(name='conn')
        self.transport = Mock(name='transport')
        self.channel = Channel(self.conn, self.transport)
        super(ChannelCase, self).setUp()

    def tearDown(self):
        self.patch_qpidtoollibs.stop()
        super(ChannelCase, self).tearDown()


class test_Channel__purge(ChannelCase):

    def setup(self):
        self.queue = Mock(name='queue')

    def test_gets_queue(self):
        self.channel._purge(self.queue)
        getQueue = self.broker_agent.return_value.getQueue
        getQueue.assert_called_once_with(self.queue)

    def test_does_not_call_purge_if_message_count_is_zero(self):
        values = {'msgDepth': 0}
        queue_obj = self.broker_agent.return_value.getQueue.return_value
        queue_obj.values = values
        self.channel._purge(self.queue)
        queue_obj.purge.assert_not_called()

    def test_purges_all_messages_from_queue(self):
        values = {'msgDepth': 5}
        queue_obj = self.broker_agent.return_value.getQueue.return_value
        queue_obj.values = values
        self.channel._purge(self.queue)
        queue_obj.purge.assert_called_with(5)

    def test_returns_message_count(self):
        values = {'msgDepth': 5}
        queue_obj = self.broker_agent.return_value.getQueue.return_value
        queue_obj.values = values
        result = self.channel._purge(self.queue)
        self.assertEqual(result, 5)


class test_Channel__put(ChannelCase):

    @patch(QPID_MODULE + '.qpid')
    def test_onto_queue(self, qpid_module):
        routing_key = 'routingkey'
        message = Mock(name='message')
        Message_cls = qpid_module.messaging.Message

        self.channel._put(routing_key, message)

        address_string = '%s; {assert: always, node: {type: queue}}' % (
            routing_key,
        )
        self.transport.session.sender.assert_called_with(address_string)
        Message_cls.assert_called_with(content=message, subject=None)
        sender = self.transport.session.sender.return_value
        sender.send.assert_called_with(Message_cls.return_value, sync=True)
        sender.close.assert_called_with()

    @patch(QPID_MODULE + '.qpid')
    def test_onto_exchange(self, qpid_module):
        routing_key = 'routingkey'
        exchange_name = 'myexchange'
        message = Mock(name='message')
        Message_cls = qpid_module.messaging.Message

        self.channel._put(routing_key, message, exchange_name)

        address_string = '%s/%s; {assert: always, node: {type: topic}}' % (
            exchange_name, routing_key,
        )
        self.transport.session.sender.assert_called_with(address_string)
        Message_cls.assert_called_with(content=message, subject=routing_key)
        sender = self.transport.session.sender.return_value
        sender.send.assert_called_with(Message_cls.return_value, sync=True)
        sender.close.assert_called_with()


class test_Channel__get(ChannelCase):

    def test_get(self):
        queue = Mock(name='queue')

        result = self.channel._get(queue)

        self.transport.session.receiver.assert_called_once_with(queue)
        rx = self.transport.session.receiver.return_value
        rx.fetch.assert_called_once_with(timeout=0)
        rx.close.assert_called_once_with()
        self.assertIs(rx.fetch.return_value, result)


class test_Channel__close(ChannelCase):

    def setup(self):
        self.patch_basic_cancel = patch.object(self.channel, 'basic_cancel')
        self.basic_cancel = self.patch_basic_cancel.start()
        self.receiver1 = Mock(name='receiver1')
        self.receiver2 = Mock(name='receiver2')
        self.channel._receivers = {1: self.receiver1,
                                   2: self.receiver2}
        self.channel.closed = False

    def teardown(self):
        self.patch_basic_cancel.stop()

    def test_sets_close_attribute(self):
        self.channel.close()
        self.assertTrue(self.channel.closed)

    def test_calls_basic_cancel_on_all_receivers(self):
        self.channel.close()
        self.basic_cancel.assert_has_calls([call(1), call(2)])

    def test_calls_close_channel_on_connection(self):
        self.channel.close()
        self.conn.close_channel.assert_called_once_with(self.channel)

    def test_calls_close_on_broker_agent(self):
        self.channel.close()
        self.channel._broker.close.assert_called_once_with()

    def test_does_nothing_if_already_closed(self):
        self.channel.closed = True
        self.channel.close()
        self.basic_cancel.assert_not_called()

    def test_does_not_call_close_channel_if_conn_is_None(self):
        self.channel.connection = None
        self.channel.close()
        self.conn.close_channel.assert_not_called()


class test_Channel__basic_qos(ChannelCase):

    def test_always_returns_one(self):
        self.channel.basic_qos(2)
        self.assertEqual(self.channel.qos.prefetch_count, 1)


class test_Channel_basic_get(ChannelCase):

    def setup(self):
        self.channel.Message = Mock(name='channel.Message')
        self.channel._get = Mock(name='channel._get')

    def test_calls__get_with_queue(self):
        queue = Mock(name='queue')
        self.channel.basic_get(queue)
        self.channel._get.assert_called_once_with(queue)

    def test_creates_Message_correctly(self):
        queue = Mock(name='queue')
        self.channel.basic_get(queue)
        raw_message = self.channel._get.return_value.content
        self.channel.Message.assert_called_once_with(self.channel,
                                                     raw_message)

    def test_acknowledges_message_by_default(self):
        queue = Mock(name='queue')
        self.channel.basic_get(queue)
        qpid_message = self.channel._get.return_value
        acknowledge = self.transport.session.acknowledge
        acknowledge.assert_called_once_with(message=qpid_message)

    def test_acknowledges_message_with_no_ack_False(self):
        queue = Mock(name='queue')
        self.channel.basic_get(queue, no_ack=False)
        qpid_message = self.channel._get.return_value
        acknowledge = self.transport.session.acknowledge
        acknowledge.assert_called_once_with(message=qpid_message)

    def test_get_acknowledges_message_with_no_ack_True(self):
        queue = Mock(name='queue')
        self.channel.basic_get(queue, no_ack=True)
        qpid_message = self.channel._get.return_value
        acknowledge = self.transport.session.acknowledge
        acknowledge.assert_called_once_with(message=qpid_message)

    def test_get_returns_correct_message(self):
        queue = Mock(name='queue')
        basic_get_result = self.channel.basic_get(queue)
        expected_message = self.channel.Message.return_value
        self.assertIs(expected_message, basic_get_result)

    def test_returns_None_when_channel__get_raises_Empty(self):
        queue = Mock(name='queue')
        self.channel._get = Mock(
            name='channel._get', side_effect=Empty,
        )
        basic_get_result = self.channel.basic_get(queue)
        self.assertEqual(self.channel.Message.call_count, 0)
        self.assertIsNone(basic_get_result)


class test_Channel__basic_cancel(ChannelCase):

    def setup(self):
        self.channel._receivers = {1: Mock()}

    def test_no_error_if_consumer_tag_not_found(self):
        self.channel.basic_cancel(2)

    def test_pops_receiver(self):
        self.channel.basic_cancel(1)
        self.assertNotIn(1, self.channel._receivers)

    def test_closes_receiver(self):
        receiver = self.channel._receivers[1]
        self.channel.basic_cancel(1)
        receiver.close.assert_called_once_with()

    def test_pops__tag_to_queue(self):
        self.channel._tag_to_queue = Mock(name='_tag_to_queue')
        self.channel.basic_cancel(1)
        self.channel._tag_to_queue.pop.assert_called_once_with(1, None)

    def test_pops_connection__callbacks(self):
        self.channel._tag_to_queue = Mock(name='_tag_to_queue')
        self.channel.basic_cancel(1)
        queue = self.channel._tag_to_queue.pop.return_value
        self.conn._callbacks.pop.assert_called_once_with(queue, None)


class test_Channel__init__(ChannelCase):

    def test_sets_variables_as_expected(self):
        self.assertIs(self.conn, self.channel.connection)
        self.assertIs(self.transport, self.channel.transport)
        self.assertFalse(self.channel.closed)
        self.conn.get_qpid_connection.assert_called_once_with()
        expected_broker_agent = self.broker_agent.return_value
        self.assertIs(self.channel._broker, expected_broker_agent)
        self.assertDictEqual(self.channel._tag_to_queue, {})
        self.assertDictEqual(self.channel._receivers, {})
        self.assertIsNone(self.channel._qos)


class test_Channel__basic_consume(ChannelCase):

    def setup(self):
        self.conn._callbacks = {}

    def test_adds_queue_to__tag_to_queue(self):
        tag = Mock(name='tag')
        queue = Mock(name='queue')
        self.channel.basic_consume(queue, Mock(), Mock(), tag)
        expected_dict = {tag: queue}
        self.assertDictEqual(expected_dict, self.channel._tag_to_queue)

    def test_adds_entry_to_connection__callbacks(self):
        queue = Mock(name='queue')
        self.channel.basic_consume(queue, Mock(), Mock(), Mock())
        self.assertIn(queue, self.conn._callbacks)
        if not hasattr(self.conn._callbacks[queue], '__call__'):
            self.fail('Callback stored must be callable')

    def test_creates_new_receiver(self):
        queue = Mock(name='queue')
        self.channel.basic_consume(queue, Mock(), Mock(), Mock())
        self.transport.session.receiver.assert_called_once_with(queue)

    def test_saves_new_receiver(self):
        tag = Mock(name='tag')
        self.channel.basic_consume(Mock(), Mock(), Mock(), tag)
        new_receiver = self.transport.session.receiver.return_value
        expected_dict = {tag: new_receiver}
        self.assertDictEqual(expected_dict, self.channel._receivers)

    def test_sets_capacity_on_new_receiver(self):
        prefetch_count = Mock(name='prefetch_count')
        self.channel.qos.prefetch_count = prefetch_count
        self.channel.basic_consume(Mock(), Mock(), Mock(), Mock())
        new_receiver = self.transport.session.receiver.return_value
        self.assertIs(new_receiver.capacity, prefetch_count)

    def get_callback(self, no_ack=Mock(), original_cb=Mock()):
        self.channel.Message = Mock(name='Message')
        queue = Mock(name='queue')
        self.channel.basic_consume(queue, no_ack, original_cb, Mock())
        return self.conn._callbacks[queue]

    def test_callback_creates_Message_correctly(self):
        callback = self.get_callback()
        qpid_message = Mock(name='qpid_message')
        callback(qpid_message)
        content = qpid_message.content
        self.channel.Message.assert_called_once_with(self.channel,
                                                     content)

    def test_callback_adds_message_to_QoS(self):
        self.channel._qos = Mock(name='_qos')
        callback = self.get_callback()
        qpid_message = Mock(name='qpid_message')
        callback(qpid_message)
        delivery_tag = self.channel.Message.return_value.delivery_tag
        self.channel._qos.append.assert_called_once_with(qpid_message,
                                                         delivery_tag)

    def test_callback_gratuitously_acks(self):
        self.channel.basic_ack = Mock(name='basic_ack')
        callback = self.get_callback()
        qpid_message = Mock(name='qpid_message')
        callback(qpid_message)
        delivery_tag = self.channel.Message.return_value.delivery_tag
        self.channel.basic_ack.assert_called_once_with(delivery_tag)

    def test_callback_does_not_ack_when_needed(self):
        self.channel.basic_ack = Mock(name='basic_ack')
        callback = self.get_callback(no_ack=False)
        qpid_message = Mock(name='qpid_message')
        callback(qpid_message)
        self.channel.basic_ack.assert_not_called()

    def test_callback_calls_real_callback(self):
        self.channel.basic_ack = Mock(name='basic_ack')
        original_callback = Mock(name='original_callback')
        callback = self.get_callback(original_cb=original_callback)
        qpid_message = Mock(name='qpid_message')
        callback(qpid_message)
        expected_message = self.channel.Message.return_value
        original_callback.assert_called_once_with(expected_message)


class test_Channel__queue_delete(ChannelCase):

    def setup(self):
        has_queue_patcher = patch.object(self.channel, '_has_queue')
        self.has_queue = has_queue_patcher.start()
        self.addCleanup(has_queue_patcher.stop)

        size_patcher = patch.object(self.channel, '_size')
        self.size = size_patcher.start()
        self.addCleanup(size_patcher.stop)

        delete_patcher = patch.object(self.channel, '_delete')
        self.delete = delete_patcher.start()
        self.addCleanup(delete_patcher.stop)

        self.queue = Mock(name='queue')

    def test_checks_if_queue_exists(self):
        self.channel.queue_delete(self.queue)
        self.has_queue.assert_called_once_with(self.queue)

    def test_does_nothing_if_queue_does_not_exist(self):
        self.has_queue.return_value = False
        self.channel.queue_delete(self.queue)
        self.delete.assert_not_called()

    def test_not_empty_and_if_empty_True_no_delete(self):
        self.size.return_value = 1
        self.channel.queue_delete(self.queue, if_empty=True)
        broker = self.broker_agent.return_value
        broker.getQueue.assert_not_called()

    def test_calls_get_queue(self):
        self.channel.queue_delete(self.queue)
        getQueue = self.broker_agent.return_value.getQueue
        getQueue.assert_called_once_with(self.queue)

    def test_gets_queue_attribute(self):
        self.channel.queue_delete(self.queue)
        queue_obj = self.broker_agent.return_value.getQueue.return_value
        queue_obj.getAttributes.assert_called_once_with()

    def test_queue_in_use_and_if_unused_no_delete(self):
        queue_obj = self.broker_agent.return_value.getQueue.return_value
        queue_obj.getAttributes.return_value = {'consumerCount': 1}
        self.channel.queue_delete(self.queue, if_unused=True)
        self.delete.assert_not_called()

    def test_calls__delete_with_queue(self):
        self.channel.queue_delete(self.queue)
        self.delete.assert_called_once_with(self.queue)


class test_Channel(QPidCase):

    @patch(QPID_MODULE + '.qpidtoollibs')
    def setup(self, qpidtoollibs):
        self.connection = Mock(name='connection')
        self.qpid_connection = Mock(name='qpid_connection')
        self.qpid_session = Mock(name='qpid_session')
        self.qpid_connection.session = Mock(
            name='session',
            return_value=self.qpid_session,
        )
        self.connection.get_qpid_connection = Mock(
            name='get_qpid_connection',
            return_value=self.qpid_connection,
        )
        self.transport = Mock(name='transport')
        self.broker = Mock(name='broker')
        self.Message = Mock(name='Message')
        self.BrokerAgent = qpidtoollibs.BrokerAgent
        self.BrokerAgent.return_value = self.broker
        self.my_channel = Channel(self.connection,
                                  self.transport)
        self.my_channel.Message = self.Message

    def test_verify_QoS_class_attribute(self):
        # Verify that the class attribute QoS refers to the QoS object
        self.assertIs(QoS, Channel.QoS)

    def test_verify_Message_class_attribute(self):
        # Verify that the class attribute Message refers
        # to the Message object
        self.assertIs(Message, Channel.Message)

    def test_body_encoding_class_attribute(self):
        # Verify that the class attribute body_encoding is set to base64
        self.assertEqual('base64', Channel.body_encoding)

    def test_codecs_class_attribute(self):
        # Verify that the codecs class attribute has a correct key and
        # value
        self.assertIsInstance(Channel.codecs, dict)
        self.assertIn('base64', Channel.codecs)
        self.assertIsInstance(Channel.codecs['base64'], Base64)

    def test_delivery_tags(self):
        # Test that _delivery_tags is using itertools
        self.assertTrue(isinstance(Channel._delivery_tags, count))

    def test_size(self):
        # Test getting the number of messages in a queue specified by
        # name and returning them.
        message_count = 5
        queue = Mock(name='queue')
        queue_to_check = Mock(name='queue_to_check')
        queue_to_check.values = {'msgDepth': message_count}
        self.broker.getQueue.return_value = queue_to_check
        result = self.my_channel._size(queue)
        self.broker.getQueue.assert_called_with(queue)
        self.assertEqual(message_count, result)

    def test_delete(self):
        # Test deleting a queue calls purge and delQueue with queue name
        queue = Mock(name='queue')
        self.my_channel._purge = Mock(name='_purge')
        result = self.my_channel._delete(queue)
        self.my_channel._purge.assert_called_with(queue)
        self.broker.delQueue.assert_called_with(queue)
        self.assertIsNone(result)

    def test_has_queue_true(self):
        # Test checking if a queue exists, and it does
        queue = Mock(name='queue')
        self.broker.getQueue.return_value = True
        result = self.my_channel._has_queue(queue)
        self.assertTrue(result)

    def test_has_queue_false(self):
        # Test checking if a queue exists, and it does not
        queue = Mock(name='queue')
        self.broker.getQueue.return_value = False
        result = self.my_channel._has_queue(queue)
        self.assertFalse(result)

    @patch('amqp.protocol.queue_declare_ok_t')
    def test_queue_declare_with_exception_raised(self,
                                                 queue_declare_ok_t):
        # Test declare_queue, where an exception is raised and silenced
        queue = Mock(name='queue')
        passive = Mock(name='passive')
        durable = Mock(name='durable')
        exclusive = Mock(name='exclusive')
        auto_delete = Mock(name='auto_delete')
        nowait = Mock(name='nowait')
        arguments = Mock(name='arguments')
        msg_count = Mock(name='msg_count')
        queue.startswith.return_value = False
        queue.endswith.return_value = False
        options = {
            'passive': passive,
            'durable': durable,
            'exclusive': exclusive,
            'auto-delete': auto_delete,
            'arguments': arguments,
        }
        consumer_count = Mock(name='consumer_count')
        expected_return_value = Mock(name='expected_return_value')
        values_dict = {
            'msgDepth': msg_count,
            'consumerCount': consumer_count,
        }
        queue_data = Mock(name='queue_data')
        queue_data.values = values_dict
        exception_to_raise = Exception('The foo object already exists.')
        self.broker.addQueue.side_effect = exception_to_raise
        self.broker.getQueue.return_value = queue_data
        queue_declare_ok_t.return_value = expected_return_value
        result = self.my_channel.queue_declare(
            queue,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            nowait=nowait,
            arguments=arguments,
        )
        self.broker.addQueue.assert_called_with(queue, options=options)
        queue_declare_ok_t.assert_called_with(queue, msg_count, consumer_count)
        self.assertIs(result, expected_return_value)

    def test_queue_declare_set_ring_policy_for_celeryev(self):
        # Test declare_queue sets ring_policy for celeryev.
        queue = Mock(name='queue')
        queue.startswith.return_value = True
        queue.endswith.return_value = False
        expected_default_options = {
            'passive': False,
            'durable': False,
            'exclusive': False,
            'auto-delete': True,
            'arguments': None,
            'qpid.policy_type': 'ring',
        }
        msg_count = Mock(name='msg_count')
        consumer_count = Mock(name='consumer_count')
        values_dict = {
            'msgDepth': msg_count,
            'consumerCount': consumer_count,
        }
        queue_data = Mock(name='queue_data')
        queue_data.values = values_dict
        self.broker.addQueue.return_value = None
        self.broker.getQueue.return_value = queue_data
        self.my_channel.queue_declare(queue)
        queue.startswith.assert_called_with('celeryev')
        self.broker.addQueue.assert_called_with(
            queue, options=expected_default_options,
        )

    def test_queue_declare_set_ring_policy_for_pidbox(self):
        # Test declare_queue sets ring_policy for pidbox.
        queue = Mock(name='queue')
        queue.startswith.return_value = False
        queue.endswith.return_value = True
        expected_default_options = {
            'passive': False,
            'durable': False,
            'exclusive': False,
            'auto-delete': True,
            'arguments': None,
            'qpid.policy_type': 'ring',
        }
        msg_count = Mock(name='msg_count')
        consumer_count = Mock(name='consumer_count')
        values_dict = {
            'msgDepth': msg_count,
            'consumerCount': consumer_count,
        }
        queue_data = Mock(name='queue_data')
        queue_data.values = values_dict
        self.broker.addQueue.return_value = None
        self.broker.getQueue.return_value = queue_data
        self.my_channel.queue_declare(queue)
        queue.endswith.assert_called_with('pidbox')
        self.broker.addQueue.assert_called_with(
            queue, options=expected_default_options)

    def test_queue_declare_ring_policy_not_set_as_expected(self):
        # Test declare_queue does not set ring_policy as expected
        queue = Mock(name='queue')
        queue.startswith.return_value = False
        queue.endswith.return_value = False
        expected_default_options = {
            'passive': False,
            'durable': False,
            'exclusive': False,
            'auto-delete': True,
            'arguments': None,
        }
        msg_count = Mock(name='msg_count')
        consumer_count = Mock(name='consumer_count')
        values_dict = {
            'msgDepth': msg_count,
            'consumerCount': consumer_count,
        }
        queue_data = Mock(name='queue_data')
        queue_data.values = values_dict
        self.broker.addQueue.return_value = None
        self.broker.getQueue.return_value = queue_data
        self.my_channel.queue_declare(queue)
        queue.startswith.assert_called_with('celeryev')
        queue.endswith.assert_called_with('pidbox')
        self.broker.addQueue.assert_called_with(
            queue, options=expected_default_options)

    def test_queue_declare_test_defaults(self):
        # Test declare_queue defaults
        queue = Mock(name='queue')
        queue.startswith.return_value = False
        queue.endswith.return_value = False
        expected_default_options = {
            'passive': False,
            'durable': False,
            'exclusive': False,
            'auto-delete': True,
            'arguments': None,
        }
        msg_count = Mock(name='msg_count')
        consumer_count = Mock(name='consumer_count')
        values_dict = {
            'msgDepth': msg_count,
            'consumerCount': consumer_count,
        }
        queue_data = Mock(name='queue_data')
        queue_data.values = values_dict
        self.broker.addQueue.return_value = None
        self.broker.getQueue.return_value = queue_data
        self.my_channel.queue_declare(queue)
        self.broker.addQueue.assert_called_with(
            queue, options=expected_default_options)

    def test_queue_declare_raises_exception_not_silenced(self):
        unique_exception = Exception('This exception should not be silenced')
        queue = Mock(name='queue')
        self.broker.addQueue.side_effect = unique_exception
        with self.assertRaises(unique_exception.__class__):
            self.my_channel.queue_declare(queue)
        self.broker.addQueue.assert_called_once_with(queue, options={
            'exclusive': False,
            'durable': False,
            'qpid.policy_type': 'ring',
            'passive': False,
            'arguments': None,
            'auto-delete': True,
        })

    def test_exchange_declare_raises_exception_and_silenced(self):
        # Create exchange where an exception is raised and then silenced.
        self.broker.addExchange.side_effect = Exception(
            'The foo object already exists.',
        )
        self.my_channel.exchange_declare()

    def test_exchange_declare_raises_exception_not_silenced(self):
        # Create Exchange where an exception is raised and not silenced
        unique_exception = Exception('This exception should not be silenced')
        self.broker.addExchange.side_effect = unique_exception
        with self.assertRaises(unique_exception.__class__):
            self.my_channel.exchange_declare()

    def test_exchange_declare(self):
        # Create Exchange where an exception is NOT raised.
        exchange = Mock(name='exchange')
        exchange_type = Mock(name='exchange_type')
        durable = Mock(name='durable')
        options = {'durable': durable}
        result = self.my_channel.exchange_declare(
            exchange, exchange_type, durable,
        )
        self.broker.addExchange.assert_called_with(
            exchange_type, exchange, options,
        )
        self.assertIsNone(result)

    def test_exchange_delete(self):
        # Test the deletion of an exchange by name.
        exchange = Mock(name='exchange')
        result = self.my_channel.exchange_delete(exchange)
        self.broker.delExchange.assert_called_with(exchange)
        self.assertIsNone(result)

    def test_queue_bind(self):
        # Test binding a queue to an exchange using a routing key.
        queue = Mock(name='queue')
        exchange = Mock(name='exchange')
        routing_key = Mock(name='routing_key')
        self.my_channel.queue_bind(queue, exchange, routing_key)
        self.broker.bind.assert_called_with(exchange, queue, routing_key)

    def test_queue_unbind(self):
        # Test unbinding a queue from an exchange using a routing key.
        queue = Mock(name='queue')
        exchange = Mock(name='exchange')
        routing_key = Mock(name='routing_key')
        self.my_channel.queue_unbind(queue, exchange, routing_key)
        self.broker.unbind.assert_called_with(exchange, queue, routing_key)

    def test_queue_purge(self):
        # Test purging a queue by name.
        queue = Mock(name='queue')
        purge_result = Mock(name='purge_result')
        self.my_channel._purge = Mock(name='_purge', return_value=purge_result)
        result = self.my_channel.queue_purge(queue)
        self.my_channel._purge.assert_called_with(queue)
        self.assertIs(purge_result, result)

    @patch(QPID_MODULE + '.Channel.qos')
    def test_basic_ack(self, qos):
        # Test that basic_ack calls the QoS object properly.
        delivery_tag = Mock(name='delivery_tag')
        self.my_channel.basic_ack(delivery_tag)
        qos.ack.assert_called_with(delivery_tag)

    @patch(QPID_MODULE + '.Channel.qos')
    def test_basic_reject(self, qos):
        # Test that basic_reject calls the QoS object properly.
        delivery_tag = Mock(name='delivery_tag')
        requeue_value = Mock(name='requeue_value')
        self.my_channel.basic_reject(delivery_tag, requeue_value)
        qos.reject.assert_called_with(delivery_tag, requeue=requeue_value)

    def test_qos_manager_is_none(self):
        # Test the qos property if the QoS object did not already exist.
        self.my_channel._qos = None
        result = self.my_channel.qos
        self.assertIsInstance(result, QoS)
        self.assertEqual(result, self.my_channel._qos)

    def test_qos_manager_already_exists(self):
        # Test the qos property if the QoS object already exists.
        existing_qos = Mock(name='existing_qos')
        self.my_channel._qos = existing_qos
        result = self.my_channel.qos
        self.assertIs(existing_qos, result)

    def test_prepare_message(self):
        # Test that prepare_message() returns the correct result.
        body = Mock(name='body')
        priority = Mock(name='priority')
        content_encoding = Mock(name='content_encoding')
        content_type = Mock(name='content_type')
        header1 = Mock(name='header1')
        header2 = Mock(name='header2')
        property1 = Mock(name='property1')
        property2 = Mock(name='property2')
        headers = {'header1': header1, 'header2': header2}
        properties = {
            'property1': property1,
            'property2': property2,
        }
        result = self.my_channel.prepare_message(
            body,
            priority=priority,
            content_type=content_type,
            content_encoding=content_encoding,
            headers=headers,
            properties=properties)
        self.assertIs(result['body'], body)
        self.assertIs(result['content-encoding'], content_encoding)
        self.assertIs(result['content-type'], content_type)
        self.assertDictEqual(result['headers'], headers)
        self.assertDictContainsSubset(result['properties'], properties)
        self.assertIs(
            result['properties']['delivery_info']['priority'],
            priority,
        )

    @patch('__builtin__.buffer')
    @patch(QPID_MODULE + '.Channel.body_encoding')
    @patch(QPID_MODULE + '.Channel.encode_body')
    @patch(QPID_MODULE + '.Channel._put')
    def test_basic_publish(self, put, encode_body, body_encoding, buffer):
        # Test basic_publish()
        original_body = Mock(name='original_body')
        encoded_body = 'this is my encoded body'
        message = {
            'body': original_body,
            'properties': {'delivery_info': {}},
        }
        encode_body.return_value = (
            encoded_body, body_encoding,
        )
        exchange = Mock(name='exchange')
        routing_key = Mock(name='routing_key')
        encoded_buffered_body = Mock(name='encoded_buffered_body')
        buffer.return_value = encoded_buffered_body
        self.my_channel.basic_publish(message, exchange, routing_key)
        encode_body.assert_called_once_with(original_body, body_encoding)
        buffer.assert_called_once_with(encoded_body)
        self.assertIs(message['body'], encoded_buffered_body)
        self.assertIs(message['properties']['body_encoding'],
                      body_encoding)
        self.assertIsInstance(
            message['properties']['delivery_tag'], int,
        )
        self.assertIs(
            message['properties']['delivery_info']['exchange'],
            exchange,
        )
        self.assertIs(
            message['properties']['delivery_info']['routing_key'],
            routing_key,
        )
        put.assert_called_with(
            routing_key, message, exchange,
        )

    @patch(QPID_MODULE + '.Channel.codecs')
    def test_encode_body_expected_encoding(self, codecs):
        # Test if encode_body() works when encoding is set correctly.
        body = Mock(name='body')
        encoder = Mock(name='encoder')
        encoded_result = Mock(name='encoded_result')
        codecs.get.return_value = encoder
        encoder.encode.return_value = encoded_result
        result = self.my_channel.encode_body(body, encoding='base64')
        expected_result = (encoded_result, 'base64')
        self.assertEqual(expected_result, result)

    @patch(QPID_MODULE + '.Channel.codecs')
    def test_encode_body_not_expected_encoding(self, codecs):
        # Test if encode_body() works when encoding is not set correctly.
        body = Mock(name='body')
        result = self.my_channel.encode_body(body, encoding=None)
        expected_result = (body, None)
        self.assertEqual(expected_result, result)

    @patch(QPID_MODULE + '.Channel.codecs')
    def test_decode_body_expected_encoding(self, codecs):
        # Test if decode_body() works when encoding is set correctly.
        body = Mock(name='body')
        decoder = Mock(name='decoder')
        decoded_result = Mock(name='decoded_result')
        codecs.get.return_value = decoder
        decoder.decode.return_value = decoded_result
        result = self.my_channel.decode_body(body, encoding='base64')
        self.assertEqual(decoded_result, result)

    @patch(QPID_MODULE + '.Channel.codecs')
    def test_decode_body_not_expected_encoding(self, codecs):
        # Test if decode_body() works when encoding is not set correctly.
        body = Mock(name='body')
        result = self.my_channel.decode_body(body, encoding=None)
        self.assertEqual(body, result)

    def test_typeof_exchange_exists(self):
        # Test that typeof() finds an exchange that already exists.
        exchange = Mock(name='exchange')
        qpid_exchange = Mock(name='qpid_exchange')
        attributes = {}
        exchange_type = Mock(name='exchange_type')
        attributes['type'] = exchange_type
        qpid_exchange.getAttributes.return_value = attributes
        self.broker.getExchange.return_value = qpid_exchange
        result = self.my_channel.typeof(exchange)
        self.assertIs(exchange_type, result)

    def test_typeof_exchange_does_not_exist(self):
        # Test that typeof() finds an exchange that does not exists.
        exchange = Mock(name='exchange')
        default = Mock(name='default')
        self.broker.getExchange.return_value = None
        result = self.my_channel.typeof(exchange, default=default)
        self.assertIs(default, result)


@skip.if_python3()
@skip.if_pypy()
class ReceiversMonitorCase(QPidCase):

    def setUp(self):
        self.session = Mock(name='session')
        self.w = Mock(name='w')
        self.monitor = ReceiversMonitor(self.session, self.w)
        super(ReceiversMonitorCase, self).setUp()


class test_ReceiversMonitor__type(ReceiversMonitorCase):

    def test_is_subclass_of_threading(self):
        self.assertIsInstance(self.monitor, threading.Thread)


class test_ReceiversMonitor__init__(ReceiversMonitorCase):

    def setUp(self):
        self.Thread___init__ = self.patch(
            QPID_MODULE + '.threading.Thread.__init__',
        )
        super(test_ReceiversMonitor__init__, self).setUp()

    def test_saves_session(self):
        self.assertIs(self.monitor._session, self.session)

    def test_saves_fd(self):
        self.assertIs(self.monitor._w_fd, self.w)

    def test_calls_parent__init__(self):
        self.Thread___init__.assert_called_once_with()


class test_ReceiversMonitor_run(ReceiversMonitorCase):

    @patch.object(ReceiversMonitor, 'monitor_receivers')
    @patch(QPID_MODULE + '.time.sleep')
    def test_calls_monitor_receivers(
            self, sleep, monitor_receivers):
        sleep.side_effect = BreakOutException()
        with self.assertRaises(BreakOutException):
            self.monitor.run()
        monitor_receivers.assert_called_once_with()

    @patch.object(Transport, 'connection_errors', new=(BreakOutException,))
    @patch.object(ReceiversMonitor, 'monitor_receivers')
    @patch(QPID_MODULE + '.time.sleep')
    @patch(QPID_MODULE + '.logger')
    def test_calls_logs_exception_and_sleeps(
            self, logger, sleep, monitor_receivers):
        exc_to_raise = IOError()
        monitor_receivers.side_effect = exc_to_raise
        sleep.side_effect = BreakOutException()
        with self.assertRaises(BreakOutException):
            self.monitor.run()
        logger.error.assert_called_once_with(exc_to_raise, exc_info=1)
        sleep.assert_called_once_with(10)

    @patch.object(ReceiversMonitor, 'monitor_receivers')
    @patch(QPID_MODULE + '.time.sleep')
    def test_loops_when_exception_is_raised(
            self, sleep, monitor_receivers):

        def return_once_raise_on_second_call(*args):
            sleep.side_effect = BreakOutException()
        sleep.side_effect = return_once_raise_on_second_call

        with self.assertRaises(BreakOutException):
            self.monitor.run()
        monitor_receivers.has_calls([call(), call()])

    @patch.object(Transport, 'connection_errors', new=(MockException,))
    @patch.object(ReceiversMonitor, 'monitor_receivers')
    @patch(QPID_MODULE + '.time.sleep')
    @patch(QPID_MODULE + '.logger')
    @patch(QPID_MODULE + '.os.write')
    @patch(QPID_MODULE + '.sys.exc_info')
    def test_exits_when_recoverable_exception_raised(
            self, sys_exc_info, os_write, logger, sleep, monitor_receivers):
        monitor_receivers.side_effect = MockException()
        sleep.side_effect = BreakOutException()
        try:
            self.monitor.run()
        except Exception:
            self.fail('ReceiversMonitor.run() should exit normally when '
                      'recoverable error is caught')
        logger.error.assert_not_called()

    @patch.object(Transport, 'connection_errors', new=(MockException,))
    @patch.object(ReceiversMonitor, 'monitor_receivers')
    @patch(QPID_MODULE + '.time.sleep')
    @patch(QPID_MODULE + '.logger')
    @patch(QPID_MODULE + '.os.write')
    def test_saves_exception_when_recoverable_exc_raised(
            self, os_write, logger, sleep, monitor_receivers):
        monitor_receivers.side_effect = MockException()
        sleep.side_effect = BreakOutException()
        try:
            self.monitor.run()
        except Exception:
            self.fail('ReceiversMonitor.run() should exit normally when '
                      'recoverable error is caught')
        self.assertIs(
            self.session.saved_exception,
            monitor_receivers.side_effect,
        )

    @patch.object(Transport, 'connection_errors', new=(MockException,))
    @patch.object(ReceiversMonitor, 'monitor_receivers')
    @patch(QPID_MODULE + '.time.sleep')
    @patch(QPID_MODULE + '.logger')
    @patch(QPID_MODULE + '.os.write')
    @patch(QPID_MODULE + '.sys.exc_info')
    def test_writes_e_to_pipe_when_recoverable_exc_raised(
            self, sys_exc_info, os_write, logger, sleep, monitor_receivers):
        monitor_receivers.side_effect = MockException()
        sleep.side_effect = BreakOutException()
        try:
            self.monitor.run()
        except Exception:
            self.fail('ReceiversMonitor.run() should exit normally when '
                      'recoverable error is caught')
        os_write.assert_called_once_with(self.w, 'e')


class test_ReceiversMonitor_monitor_receivers(ReceiversMonitorCase):

    def test_calls_next_receivers(self):
        self.session.next_receiver.side_effect = BreakOutException()
        with self.assertRaises(BreakOutException):
            self.monitor.monitor_receivers()
        self.session.next_receiver.assert_called_once_with()

    def test_writes_to_fd(self):
        with patch(QPID_MODULE + '.os.write') as os_write:
            os_write.side_effect = BreakOutException()
            with self.assertRaises(BreakOutException):
                self.monitor.monitor_receivers()
            os_write.assert_called_once_with(self.w, '0')


class test_Transport__init(QPidCase):

    def setup(self):
        self.patch_a = patch.object(Transport, 'verify_runtime_environment')
        self.verify_runtime_environment = self.patch_a.start()

        self.patch_b = patch(QPID_MODULE + '.base.Transport.__init__')
        self.base_Transport__init__ = self.patch_b.start()

        self.patch_c = patch(QPID_MODULE + '.os')
        self.os = self.patch_c.start()
        self.r = Mock(name='r')
        self.w = Mock(name='w')
        self.os.pipe.return_value = (self.r, self.w)

        self.patch_d = patch(QPID_MODULE + '.fcntl')
        self.fcntl = self.patch_d.start()

    def teardown(self):
        self.patch_a.stop()
        self.patch_b.stop()
        self.patch_c.stop()
        self.patch_d.stop()

    def test_calls_verify_runtime_environment(self):
        Transport(Mock())
        self.verify_runtime_environment.assert_called_once_with()

    def test_calls_os_pipe(self):
        Transport(Mock())
        self.os.pipe.assert_called_once_with()

    def test_saves_os_pipe_file_descriptors(self):
        transport = Transport(Mock())
        self.assertIs(transport.r, self.r)
        self.assertIs(transport._w, self.w)

    def test_sets_non_blocking_behavior_on_r_fd(self):
        Transport(Mock())
        self.fcntl.fcntl.assert_called_once_with(
            self.r, self.fcntl.F_SETFL, self.os.O_NONBLOCK)


class test_Transport__drain_events(QPidCase):

    def setup(self):
        self.transport = Transport(Mock())
        self.transport.session = Mock(name='session')
        self.queue = Mock(name='queue')
        self.message = Mock(name='message')
        self.conn = Mock(name='conn')
        self.callback = Mock(name='callback')
        self.conn._callbacks = {self.queue: self.callback}

    def mock_next_receiver(self, timeout):
        time.sleep(0.3)
        receiver = Mock(name='receiver')
        receiver.source = self.queue
        receiver.fetch.return_value = self.message
        return receiver

    def test_socket_timeout_raised_when_all_receivers_empty(self):
        with patch(QPID_MODULE + '.QpidEmpty', new=MockException):
            self.transport.session.next_receiver.side_effect = MockException()
            with self.assertRaises(socket.timeout):
                self.transport.drain_events(Mock())

    def test_socket_timeout_raised_when_by_timeout(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        with self.assertRaises(socket.timeout):
            self.transport.drain_events(self.conn, timeout=1)

    def test_timeout_returns_no_earlier_then_asked_for(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        start_time = datetime.datetime.now()
        try:
            self.transport.drain_events(self.conn, timeout=1)
        except socket.timeout:
            pass
        end_time = datetime.datetime.now()
        elapsed_time = end_time - start_time
        self.assertGreaterEqual(elapsed_time.total_seconds(), 1)

    def test_callback_is_called(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        try:
            self.transport.drain_events(self.conn, timeout=1)
        except socket.timeout:
            pass
        self.callback.assert_called_with(self.message)


class test_Transport__create_channel(QPidCase):

    def setup(self):
        self.transport = Transport(Mock())
        self.conn = Mock(name='conn')
        self.new_channel = Mock(name='new_channel')
        self.conn.Channel.return_value = self.new_channel
        self.returned_channel = self.transport.create_channel(self.conn)

    def test_new_channel_created_from_connection(self):
        self.assertIs(self.new_channel, self.returned_channel)
        self.conn.Channel.assert_called_with(self.conn, self.transport)

    def test_new_channel_added_to_connection_channel_list(self):
        append_method = self.conn.channels.append
        append_method.assert_called_with(self.new_channel)


class test_Transport__establish_connection(QPidCase):

    def setup(self):

        class MockClient(object):
            pass

        self.client = MockClient()
        self.client.connect_timeout = 4
        self.client.ssl = False
        self.client.transport_options = {}
        self.transport = Transport(self.client)
        self.conn = Mock(name='conn')
        self.transport.Connection = self.conn
        path_to_mock = QPID_MODULE + '.ReceiversMonitor'
        self.patcher = patch(path_to_mock)
        self.ReceiverMonitor = self.patcher.start()

    def teardown(self):
        self.patcher.stop()

    def test_new_option_overwrites_default(self):
        new_userid_string = 'new-userid'
        self.client.userid = new_userid_string
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username=new_userid_string,
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='127.0.0.1',
            timeout=4,
            password='',
            port=5672,
            transport='tcp',
        )

    def test_sasl_mech_sorting(self):
        self.client.sasl_mechanisms = 'MECH1 MECH2'
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            sasl_mechanisms='MECH1 MECH2',
            host='127.0.0.1',
            timeout=4,
            password='',
            port=5672,
            transport='tcp',
        )

    def test_empty_client_is_default(self):
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='127.0.0.1',
            timeout=4,
            password='',
            port=5672,
            transport='tcp',
        )

    def test_additional_transport_option(self):
        new_param_value = 'mynewparam'
        self.client.transport_options['new_param'] = new_param_value
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='127.0.0.1',
            timeout=4,
            new_param=new_param_value,
            password='',
            port=5672,
            transport='tcp',
        )

    def test_transform_localhost_to_127_0_0_1(self):
        self.client.hostname = 'localhost'
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='127.0.0.1',
            timeout=4,
            password='',
            port=5672,
            transport='tcp',
        )

    def test_set_password(self):
        self.client.password = 'somepass'
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='127.0.0.1',
            timeout=4,
            password='somepass',
            port=5672,
            transport='tcp',
        )

    def test_no_ssl_sets_transport_tcp(self):
        self.client.ssl = False
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='127.0.0.1',
            timeout=4,
            password='',
            port=5672,
            transport='tcp',
        )

    def test_with_ssl_with_hostname_check(self):
        self.client.ssl = {
            'keyfile': 'my_keyfile',
            'certfile': 'my_certfile',
            'ca_certs': 'my_cacerts',
            'cert_reqs': ssl.CERT_REQUIRED,
        }
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            ssl_certfile='my_certfile',
            ssl_trustfile='my_cacerts',
            timeout=4,
            ssl_skip_hostname_check=False,
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='127.0.0.1',
            ssl_keyfile='my_keyfile',
            password='',
            port=5672, transport='ssl',
        )

    def test_with_ssl_skip_hostname_check(self):
        self.client.ssl = {
            'keyfile': 'my_keyfile',
            'certfile': 'my_certfile',
            'ca_certs': 'my_cacerts',
            'cert_reqs': ssl.CERT_OPTIONAL,
        }
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            ssl_certfile='my_certfile',
            ssl_trustfile='my_cacerts',
            timeout=4,
            ssl_skip_hostname_check=True,
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='127.0.0.1',
            ssl_keyfile='my_keyfile',
            password='',
            port=5672, transport='ssl',
        )

    def test_sets_client_on_connection_object(self):
        self.transport.establish_connection()
        self.assertIs(self.conn.return_value.client, self.client)

    def test_creates_session_on_transport(self):
        self.transport.establish_connection()
        qpid_conn = self.conn.return_value.get_qpid_connection
        new_session = qpid_conn.return_value.session.return_value
        self.assertIs(self.transport.session, new_session)

    def test_returns_new_connection_object(self):
        new_conn = self.transport.establish_connection()
        self.assertIs(new_conn, self.conn.return_value)

    def test_creates_ReceiversMonitor(self):
        self.transport.establish_connection()
        self.ReceiverMonitor.assert_called_once_with(
            self.transport.session, self.transport._w)

    def test_daemonizes_thread(self):
        self.transport.establish_connection()
        self.assertTrue(self.ReceiverMonitor.return_value.daemon)

    def test_starts_thread(self):
        self.transport.establish_connection()
        new_receiver_monitor = self.ReceiverMonitor.return_value
        new_receiver_monitor.start.assert_called_once_with()

    def test_ignores_hostname_if_not_localhost(self):
        self.client.hostname = 'some_other_hostname'
        self.transport.establish_connection()
        self.conn.assert_called_once_with(
            username='guest',
            sasl_mechanisms='PLAIN ANONYMOUS',
            host='some_other_hostname',
            timeout=4, password='',
            port=5672, transport='tcp',
        )


class test_Transport__class_attributes(QPidCase):

    def test_Connection_attribute(self):
        self.assertIs(Connection, Transport.Connection)

    def test_default_port(self):
        self.assertEqual(Transport.default_port, 5672)

    def test_polling_disabled(self):
        self.assertIsNone(Transport.polling_interval)

    def test_supports_asynchronous_events(self):
        self.assertTrue(Transport.supports_ev)

    def test_verify_driver_type_and_name(self):
        self.assertEqual(Transport.driver_type, 'qpid')
        self.assertEqual(Transport.driver_name, 'qpid')

    def test_channel_error_contains_qpid_ConnectionError(self):
        self.assertIn(ConnectionError, Transport.connection_errors)

    def test_channel_error_contains_socket_error(self):
        self.assertIn(select.error, Transport.connection_errors)


class test_Transport__register_with_event_loop(QPidCase):

    def test_calls_add_reader(self):
        transport = Transport(Mock())
        connection = Mock(name='connection')
        loop = Mock(name='loop')
        transport.register_with_event_loop(connection, loop)
        loop.add_reader.assert_called_with(
            transport.r,
            transport.on_readable,
            connection, loop,
        )


class test_Transport__on_readable(QPidCase):

    def setup(self):
        self.patch_a = patch(QPID_MODULE + '.os.read')
        self.os_read = self.patch_a.start()
        self.patch_b = patch.object(Transport, 'drain_events')
        self.drain_events = self.patch_b.start()
        self.transport = Transport(Mock())

    def teardown(self):
        self.patch_a.stop()

    def test_reads_symbol_from_fd(self):
        self.transport.on_readable(Mock(), Mock())
        self.os_read.assert_called_once_with(self.transport.r, 1)

    def test_calls_drain_events(self):
        connection = Mock(name='connection')
        self.transport.on_readable(connection, Mock())
        self.drain_events.assert_called_with(connection)

    def test_catches_socket_timeout(self):
        self.drain_events.side_effect = socket.timeout()
        try:
            self.transport.on_readable(Mock(), Mock())
        except Exception:
            self.fail('Transport.on_readable did not catch socket.timeout()')

    def test_ignores_non_socket_timeout_exception(self):
        self.drain_events.side_effect = IOError()
        with self.assertRaises(IOError):
            self.transport.on_readable(Mock(), Mock())

    def test_reads_e_off_of_pipe_raises_exc_info(self):
        self.transport.session = Mock(name='session')
        try:
            raise IOError()
        except Exception as error:
            self.transport.session.saved_exception = error
        self.os_read.return_value = 'e'
        with self.assertRaises(IOError):
            self.transport.on_readable(Mock(), Mock())


class test_Transport__verify_runtime_environment(QPidCase):

    def setup(self):
        self.verify_runtime_environment = Transport.verify_runtime_environment
        self.patch_a = patch.object(Transport, 'verify_runtime_environment')
        self.patch_a.start()
        self.transport = Transport(Mock())

    def teardown(self):
        self.patch_a.stop()

    @patch(QPID_MODULE + '.PY3', new=True)
    def test_raises_exception_for_Python3(self):
        with self.assertRaises(RuntimeError):
            self.verify_runtime_environment(self.transport)

    @patch('__builtin__.getattr')
    def test_raises_exc_for_PyPy(self, mock_getattr):
        mock_getattr.return_value = True
        with self.assertRaises(RuntimeError):
            self.verify_runtime_environment(self.transport)

    def test_raises_no_exception(self):
        try:
            self.verify_runtime_environment(self.transport)
        except Exception:
            self.fail(
                'verify_runtime_environment raised an unexpected Exception')


class test_Transport(QPidCase):

    def setup(self):
        # Creates a mock client to be used in testing.
        self.client = Mock(name='client')

    def test_close_connection(self):
        # Test that close_connection calls close on each channel in the
        # list of channels on the connection object.
        my_transport = Transport(self.client)
        connection = Mock(name='connection')
        channel1 = Mock(name='channel1')
        channel2 = Mock(name='channel2')
        connection.channels = [channel1, channel2]
        my_transport.close_connection(connection)
        channel1.close.assert_called_with()
        channel2.close.assert_called_with()

    def test_default_connection_params(self):
        # Test that the default_connection_params are correct.
        correct_params = {
            'userid': 'guest', 'password': '',
            'port': 5672, 'virtual_host': '',
            'hostname': 'localhost',
            'sasl_mechanisms': 'PLAIN ANONYMOUS',
        }
        my_transport = Transport(self.client)
        result_params = my_transport.default_connection_params
        self.assertDictEqual(correct_params, result_params)
