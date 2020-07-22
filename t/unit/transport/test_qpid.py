from __future__ import absolute_import, unicode_literals

import pytest
import select
import ssl
import socket
import sys
import time
import uuid

from collections import OrderedDict

try:
    from collections.abc import Callable
except ImportError:
    from collections import Callable

from itertools import count

from case import Mock, call, patch, skip

from kombu.five import Empty, range, monotonic
from kombu.transport.qpid import (AuthenticationFailure, Channel, Connection,
                                  ConnectionError, Message, NotFound, QoS,
                                  Transport)
from kombu.transport.virtual import Base64


QPID_MODULE = 'kombu.transport.qpid'


@pytest.fixture
def disable_runtime_dependency_check(patching):
    mock_dependency_is_none = patching(QPID_MODULE + '.dependency_is_none')
    mock_dependency_is_none.return_value = False
    return mock_dependency_is_none


class QpidException(Exception):
    """
    An object used to mock Exceptions provided by qpid.messaging.exceptions
    """

    def __init__(self, code=None, text=None):
        super(Exception, self).__init__(self)
        self.code = code
        self.text = text


class BreakOutException(Exception):
    pass


@skip.if_python3()
@skip.if_pypy()
class test_QoS__init__(object):

    def setup(self):
        self.mock_session = Mock()
        self.qos = QoS(self.mock_session)

    def test__init__prefetch_default_set_correct_without_prefetch_value(self):
        assert self.qos.prefetch_count == 1

    def test__init__prefetch_is_hard_set_to_one(self):
        qos_limit_two = QoS(self.mock_session)
        assert qos_limit_two.prefetch_count == 1

    def test__init___not_yet_acked_is_initialized(self):
        assert isinstance(self.qos._not_yet_acked, OrderedDict)


@skip.if_python3()
@skip.if_pypy()
class test_QoS_can_consume(object):

    def setup(self):
        session = Mock()
        self.qos = QoS(session)

    def test_True_when_prefetch_limit_is_zero(self):
        self.qos.prefetch_count = 0
        self.qos._not_yet_acked = []
        assert self.qos.can_consume()

    def test_True_when_len_of__not_yet_acked_is_lt_prefetch_count(self):
        self.qos.prefetch_count = 3
        self.qos._not_yet_acked = ['a', 'b']
        assert self.qos.can_consume()

    def test_False_when_len_of__not_yet_acked_is_eq_prefetch_count(self):
        self.qos.prefetch_count = 3
        self.qos._not_yet_acked = ['a', 'b', 'c']
        assert not self.qos.can_consume()


@skip.if_python3()
@skip.if_pypy()
class test_QoS_can_consume_max_estimate(object):

    def setup(self):
        self.mock_session = Mock()
        self.qos = QoS(self.mock_session)

    def test_return_one_when_prefetch_count_eq_zero(self):
        self.qos.prefetch_count = 0
        assert self.qos.can_consume_max_estimate() == 1

    def test_return_prefetch_count_sub_len__not_yet_acked(self):
        self.qos._not_yet_acked = ['a', 'b']
        self.qos.prefetch_count = 4
        assert self.qos.can_consume_max_estimate() == 2


@skip.if_python3()
@skip.if_pypy()
class test_QoS_ack(object):

    def setup(self):
        self.mock_session = Mock()
        self.qos = QoS(self.mock_session)

    def test_ack_pops__not_yet_acked(self):
        message = Mock()
        self.qos.append(message, 1)
        assert 1 in self.qos._not_yet_acked
        self.qos.ack(1)
        assert 1 not in self.qos._not_yet_acked

    def test_ack_calls_session_acknowledge_with_message(self):
        message = Mock()
        self.qos.append(message, 1)
        self.qos.ack(1)
        self.qos.session.acknowledge.assert_called_with(message=message)


@skip.if_python3()
@skip.if_pypy()
class test_QoS_reject(object):

    @pytest.fixture(autouse=True)
    def setup_qpid(self, patching):
        self.mock_qpid = patching(QPID_MODULE + '.qpid')
        self.mock_Disposition = self.mock_qpid.messaging.Disposition
        self.mock_RELEASED = self.mock_qpid.messaging.RELEASED
        self.mock_REJECTED = self.mock_qpid.messaging.REJECTED

    def setup(self):
        self.mock_session = Mock()
        self.mock_message = Mock()
        self.qos = QoS(self.mock_session)

    def test_reject_pops__not_yet_acked(self):
        self.qos.append(self.mock_message, 1)
        assert 1 in self.qos._not_yet_acked
        self.qos.reject(1)
        assert 1 not in self.qos._not_yet_acked

    def test_reject_requeue_true(self):
        self.qos.append(self.mock_message, 1)
        self.qos.reject(1, requeue=True)
        self.mock_Disposition.assert_called_with(self.mock_RELEASED)
        self.qos.session.acknowledge.assert_called_with(
            message=self.mock_message,
            disposition=self.mock_Disposition.return_value,
        )

    def test_reject_requeue_false(self):
        message = Mock()
        self.qos.append(message, 1)
        self.qos.reject(1, requeue=False)
        self.mock_Disposition.assert_called_with(self.mock_REJECTED)
        self.qos.session.acknowledge.assert_called_with(
            message=message, disposition=self.mock_Disposition.return_value,
        )


@skip.if_python3()
@skip.if_pypy()
class test_QoS(object):

    def mock_message_factory(self):
        """Create and return a mock message tag and delivery_tag."""
        m_delivery_tag = self.delivery_tag_generator.next()
        m = 'message %s' % (m_delivery_tag, )
        return m, m_delivery_tag

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

    def setup(self):
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
        assert length_not_yet_acked == 1
        checked_message1 = qos._not_yet_acked[m1_tag]
        assert m1 is checked_message1
        qos.append(m2, m2_tag)
        length_not_yet_acked = len(qos._not_yet_acked)
        assert length_not_yet_acked == 2
        checked_message2 = qos._not_yet_acked[m2_tag]
        assert m2 is checked_message2

    def test_get(self):
        """Append two messages, and use get to receive them"""
        qos = self.qos_no_limit
        m1, m1_tag = self.mock_message_factory()
        m2, m2_tag = self.mock_message_factory()
        qos.append(m1, m1_tag)
        qos.append(m2, m2_tag)
        message1 = qos.get(m1_tag)
        message2 = qos.get(m2_tag)
        assert m1 is message1
        assert m2 is message2


@skip.if_python3()
@skip.if_pypy()
class ConnectionTestBase(object):

    @patch(QPID_MODULE + '.qpid')
    def setup(self, mock_qpid):
        self.connection_options = {
            'host': 'localhost',
            'port': 5672,
            'transport': 'tcp',
            'timeout': 10,
            'sasl_mechanisms': 'ANONYMOUS',
        }
        self.mock_qpid_connection = mock_qpid.messaging.Connection
        self.conn = Connection(**self.connection_options)


@skip.if_python3()
@skip.if_pypy()
class test_Connection__init__(ConnectionTestBase):

    def test_stores_connection_options(self):
        # ensure that only one mech was passed into connection. The other
        # options should all be passed through as-is
        modified_conn_opts = self.connection_options
        assert modified_conn_opts == self.conn.connection_options

    def test_class_variables(self):
        assert isinstance(self.conn.channels, list)
        assert isinstance(self.conn._callbacks, dict)

    def test_establishes_connection(self):
        modified_conn_opts = self.connection_options
        self.mock_qpid_connection.establish.assert_called_with(
            **modified_conn_opts
        )

    def test_saves_established_connection(self):
        created_conn = self.mock_qpid_connection.establish.return_value
        assert self.conn._qpid_conn is created_conn

    @patch(QPID_MODULE + '.ConnectionError', new=(QpidException, ))
    @patch(QPID_MODULE + '.sys.exc_info')
    @patch(QPID_MODULE + '.qpid')
    def test_mutates_ConnError_by_message(self, mock_qpid, mock_exc_info):
        text = 'connection-forced: Authentication failed(320)'
        my_conn_error = QpidException(text=text)
        mock_qpid.messaging.Connection.establish.side_effect = my_conn_error
        mock_exc_info.return_value = 'a', 'b', None
        try:
            self.conn = Connection(**self.connection_options)
        except AuthenticationFailure as error:
            exc_info = sys.exc_info()
            assert not isinstance(error, QpidException)
            assert exc_info[1] == 'b'
            assert exc_info[2] is None
        else:
            self.fail('ConnectionError type was not mutated correctly')

    @patch(QPID_MODULE + '.ConnectionError', new=(QpidException, ))
    @patch(QPID_MODULE + '.sys.exc_info')
    @patch(QPID_MODULE + '.qpid')
    def test_mutates_ConnError_by_code(self, mock_qpid, mock_exc_info):
        my_conn_error = QpidException(code=320, text='someothertext')
        mock_qpid.messaging.Connection.establish.side_effect = my_conn_error
        mock_exc_info.return_value = 'a', 'b', None
        try:
            self.conn = Connection(**self.connection_options)
        except AuthenticationFailure as error:
            exc_info = sys.exc_info()
            assert not isinstance(error, QpidException)
            assert exc_info[1] == 'b'
            assert exc_info[2] is None
        else:
            self.fail('ConnectionError type was not mutated correctly')

    @patch(QPID_MODULE + '.ConnectionError', new=(QpidException, ))
    @patch(QPID_MODULE + '.sys.exc_info')
    @patch(QPID_MODULE + '.qpid')
    def test_connection__init__mutates_ConnError_by_message2(self, mock_qpid,
                                                             mock_exc_info):
        """
        Test for PLAIN connection via python-saslwrapper, sans cyrus-sasl-plain

        This test is specific for what is returned when we attempt to connect
        with PLAIN mech and python-saslwrapper is installed, but
        cyrus-sasl-plain is not installed.
        """
        my_conn_error = QpidException()
        my_conn_error.text = 'Error in sasl_client_start (-4) SASL(-4): no '\
                             'mechanism available'
        mock_qpid.messaging.Connection.establish.side_effect = my_conn_error
        mock_exc_info.return_value = ('a', 'b', None)
        try:
            self.conn = Connection(**self.connection_options)
        except AuthenticationFailure as error:
            exc_info = sys.exc_info()
            assert not isinstance(error, QpidException)
            assert exc_info[1] == 'b'
            assert exc_info[2] is None
        else:
            self.fail('ConnectionError type was not mutated correctly')

    @patch(QPID_MODULE + '.ConnectionError', new=(QpidException, ))
    @patch(QPID_MODULE + '.sys.exc_info')
    @patch(QPID_MODULE + '.qpid')
    def test_unknown_connection_error(self, mock_qpid, mock_exc_info):
        # If we get a connection error that we don't understand,
        # bubble it up as-is
        my_conn_error = QpidException(code=999, text='someothertext')
        mock_qpid.messaging.Connection.establish.side_effect = my_conn_error
        mock_exc_info.return_value = 'a', 'b', None
        try:
            self.conn = Connection(**self.connection_options)
        except Exception as error:
            assert error.code == 999
        else:
            self.fail('Connection should have thrown an exception')

    @patch.object(Transport, 'channel_errors', new=(QpidException, ))
    @patch(QPID_MODULE + '.qpid')
    @patch(QPID_MODULE + '.ConnectionError', new=IOError)
    def test_non_qpid_error_raises(self, mock_qpid):
        mock_Qpid_Connection = mock_qpid.messaging.Connection
        my_conn_error = SyntaxError()
        my_conn_error.text = 'some non auth related error message'
        mock_Qpid_Connection.establish.side_effect = my_conn_error
        with pytest.raises(SyntaxError):
            Connection(**self.connection_options)

    @patch(QPID_MODULE + '.qpid')
    @patch(QPID_MODULE + '.ConnectionError', new=IOError)
    def test_non_auth_conn_error_raises(self, mock_qpid):
        mock_Qpid_Connection = mock_qpid.messaging.Connection
        my_conn_error = IOError()
        my_conn_error.text = 'some non auth related error message'
        mock_Qpid_Connection.establish.side_effect = my_conn_error
        with pytest.raises(IOError):
            Connection(**self.connection_options)


@skip.if_python3()
@skip.if_pypy()
class test_Connection_class_attributes(ConnectionTestBase):

    def test_connection_verify_class_attributes(self):
        assert Channel == Connection.Channel


@skip.if_python3()
@skip.if_pypy()
class test_Connection_get_Qpid_connection(ConnectionTestBase):

    def test_connection_get_qpid_connection(self):
        self.conn._qpid_conn = Mock()
        returned_connection = self.conn.get_qpid_connection()
        assert self.conn._qpid_conn is returned_connection


@skip.if_python3()
@skip.if_pypy()
class test_Connection_close(ConnectionTestBase):

    def test_connection_close(self):
        self.conn._qpid_conn = Mock()
        self.conn.close()
        self.conn._qpid_conn.close.assert_called_once_with()


@skip.if_python3()
@skip.if_pypy()
class test_Connection_close_channel(ConnectionTestBase):

    def setup(self):
        super(test_Connection_close_channel, self).setup()
        self.conn.channels = Mock()

    def test_connection_close_channel_removes_channel_from_channel_list(self):
        mock_channel = Mock()
        self.conn.close_channel(mock_channel)
        self.conn.channels.remove.assert_called_once_with(mock_channel)

    def test_connection_close_channel_handles_ValueError_being_raised(self):
        self.conn.channels.remove = Mock(side_effect=ValueError())
        self.conn.close_channel(Mock())

    def test_connection_close_channel_set_channel_connection_to_None(self):
        mock_channel = Mock()
        mock_channel.connection = False
        self.conn.channels.remove = Mock(side_effect=ValueError())
        self.conn.close_channel(mock_channel)
        assert mock_channel.connection is None


@skip.if_python3()
@skip.if_pypy()
class ChannelTestBase(object):

    @pytest.fixture(autouse=True)
    def setup_channel(self, patching):
        self.mock_qpidtoollibs = patching(QPID_MODULE + '.qpidtoollibs')
        self.mock_broker_agent = self.mock_qpidtoollibs.BrokerAgent
        self.conn = Mock()
        self.transport = Mock()
        self.channel = Channel(self.conn, self.transport)


@skip.if_python3()
@skip.if_pypy()
class test_Channel_purge(ChannelTestBase):

    def setup(self):
        self.mock_queue = Mock()

    def test_gets_queue(self):
        self.channel._purge(self.mock_queue)
        getQueue = self.mock_broker_agent.return_value.getQueue
        getQueue.assert_called_once_with(self.mock_queue)

    def test_does_not_call_purge_if_message_count_is_zero(self):
        values = {'msgDepth': 0}
        queue_obj = self.mock_broker_agent.return_value.getQueue.return_value
        queue_obj.values = values
        self.channel._purge(self.mock_queue)
        assert not queue_obj.purge.called

    def test_purges_all_messages_from_queue(self):
        values = {'msgDepth': 5}
        queue_obj = self.mock_broker_agent.return_value.getQueue.return_value
        queue_obj.values = values
        self.channel._purge(self.mock_queue)
        queue_obj.purge.assert_called_with(5)

    def test_returns_message_count(self):
        values = {'msgDepth': 5}
        queue_obj = self.mock_broker_agent.return_value.getQueue.return_value
        queue_obj.values = values
        result = self.channel._purge(self.mock_queue)
        assert result == 5

    @patch(QPID_MODULE + '.NotFound', new=QpidException)
    def test_raises_channel_error_if_queue_does_not_exist(self):
        self.mock_broker_agent.return_value.getQueue.return_value = None
        with pytest.raises(QpidException):
            self.channel._purge(self.mock_queue)


@skip.if_python3()
@skip.if_pypy()
class test_Channel_put(ChannelTestBase):

    @patch(QPID_MODULE + '.qpid')
    def test_channel__put_onto_queue(self, mock_qpid):
        routing_key = 'routingkey'
        mock_message = Mock()
        mock_Message_cls = mock_qpid.messaging.Message

        self.channel._put(routing_key, mock_message)

        address_str = '{0}; {{assert: always, node: {{type: queue}}}}'.format(
            routing_key,
        )
        self.transport.session.sender.assert_called_with(address_str)
        mock_Message_cls.assert_called_with(
            content=mock_message, subject=None, durable=True
        )
        mock_sender = self.transport.session.sender.return_value
        mock_sender.send.assert_called_with(
            mock_Message_cls.return_value, sync=True,
        )
        mock_sender.close.assert_called_with()

    @patch(QPID_MODULE + '.qpid')
    def test_channel__put_onto_exchange(self, mock_qpid):
        mock_routing_key = 'routingkey'
        mock_exchange_name = 'myexchange'
        mock_message = Mock()
        mock_Message_cls = mock_qpid.messaging.Message

        self.channel._put(mock_routing_key, mock_message, mock_exchange_name)

        addrstr = '{0}/{1}; {{assert: always, node: {{type: topic}}}}'.format(
            mock_exchange_name, mock_routing_key,
        )
        self.transport.session.sender.assert_called_with(addrstr)
        mock_Message_cls.assert_called_with(
            content=mock_message, subject=mock_routing_key, durable=True
        )
        mock_sender = self.transport.session.sender.return_value
        mock_sender.send.assert_called_with(
            mock_Message_cls.return_value, sync=True,
        )
        mock_sender.close.assert_called_with()


@skip.if_python3()
@skip.if_pypy()
class test_Channel_get(ChannelTestBase):

    def test_channel__get(self):
        mock_queue = Mock()

        result = self.channel._get(mock_queue)

        self.transport.session.receiver.assert_called_once_with(mock_queue)
        mock_rx = self.transport.session.receiver.return_value
        mock_rx.fetch.assert_called_once_with(timeout=0)
        mock_rx.close.assert_called_once_with()
        assert mock_rx.fetch.return_value is result


@skip.if_python3()
@skip.if_pypy()
class test_Channel_close(ChannelTestBase):

    @pytest.fixture(autouse=True)
    def setup_basic_cancel(self, patching, setup_channel):
        self.mock_basic_cancel = patching.object(self.channel, 'basic_cancel')
        self.channel.closed = False

    @pytest.fixture(autouse=True)
    def setup_receivers(self, setup_channel):
        self.mock_receiver1 = Mock()
        self.mock_receiver2 = Mock()
        self.channel._receivers = {
            1: self.mock_receiver1, 2: self.mock_receiver2,
        }

    def test_channel_close_sets_close_attribute(self):
        self.channel.close()
        assert self.channel.closed

    def test_channel_close_calls_basic_cancel_on_all_receivers(self):
        self.channel.close()
        self.mock_basic_cancel.assert_has_calls([call(1), call(2)])

    def test_channel_close_calls_close_channel_on_connection(self):
        self.channel.close()
        self.conn.close_channel.assert_called_once_with(self.channel)

    def test_channel_close_calls_close_on_broker_agent(self):
        self.channel.close()
        self.channel._broker.close.assert_called_once_with()

    def test_channel_close_does_nothing_if_already_closed(self):
        self.channel.closed = True
        self.channel.close()
        self.mock_basic_cancel.assert_not_called()

    def test_channel_close_does_not_call_close_channel_if_conn_is_None(self):
        self.channel.connection = None
        self.channel.close()
        self.conn.close_channel.assert_not_called()


@skip.if_python3()
@skip.if_pypy()
class test_Channel_basic_qos(ChannelTestBase):

    def test_channel_basic_qos_always_returns_one(self):
        self.channel.basic_qos(2)
        assert self.channel.qos.prefetch_count == 1


@skip.if_python3()
@skip.if_pypy()
class test_Channel_basic_get(ChannelTestBase):

    @pytest.fixture(autouse=True)
    def setup_channel_attributes(self, setup_channel):
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
        self.channel.Message.assert_called_once_with(
            mock_raw_message, channel=self.channel,
        )

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
        assert expected_message is basic_get_result

    def test_basic_get_returns_None_when_channel__get_raises_Empty(self):
        mock_queue = Mock()
        self.channel._get = Mock(side_effect=Empty)
        basic_get_result = self.channel.basic_get(mock_queue)
        assert self.channel.Message.call_count == 0
        assert basic_get_result is None


@skip.if_python3()
@skip.if_pypy()
class test_Channel_basic_cancel(ChannelTestBase):

    @pytest.fixture(autouse=True)
    def setup_receivers(self, setup_channel):
        self.channel._receivers = {1: Mock()}

    def test_channel_basic_cancel_no_error_if_consumer_tag_not_found(self):
        self.channel.basic_cancel(2)

    def test_channel_basic_cancel_pops_receiver(self):
        self.channel.basic_cancel(1)
        assert 1 not in self.channel._receivers

    def test_channel_basic_cancel_closes_receiver(self):
        mock_receiver = self.channel._receivers[1]
        self.channel.basic_cancel(1)
        mock_receiver.close.assert_called_once_with()

    def test_channel_basic_cancel_pops__tag_to_queue(self):
        self.channel._tag_to_queue = Mock()
        self.channel.basic_cancel(1)
        self.channel._tag_to_queue.pop.assert_called_once_with(1, None)

    def test_channel_basic_cancel_pops_connection__callbacks(self):
        self.channel._tag_to_queue = Mock()
        self.channel.basic_cancel(1)
        mock_queue = self.channel._tag_to_queue.pop.return_value
        self.conn._callbacks.pop.assert_called_once_with(mock_queue, None)


@skip.if_python3()
@skip.if_pypy()
class test_Channel__init__(ChannelTestBase):

    def test_channel___init__sets_variables_as_expected(self):
        assert self.conn is self.channel.connection
        assert self.transport is self.channel.transport
        assert not self.channel.closed
        self.conn.get_qpid_connection.assert_called_once_with()
        expected_broker_agent = self.mock_broker_agent.return_value
        assert self.channel._broker is expected_broker_agent
        assert self.channel._tag_to_queue == {}
        assert self.channel._receivers == {}
        assert self.channel._qos is None


@skip.if_python3()
@skip.if_pypy()
class test_Channel_basic_consume(ChannelTestBase):

    @pytest.fixture(autouse=True)
    def setup_callbacks(self, setup_channel):
        self.conn._callbacks = {}

    def test_channel_basic_consume_adds_queue_to__tag_to_queue(self):
        mock_tag = Mock()
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), mock_tag)
        expected_dict = {mock_tag: mock_queue}
        assert expected_dict == self.channel._tag_to_queue

    def test_channel_basic_consume_adds_entry_to_connection__callbacks(self):
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), Mock())
        assert mock_queue in self.conn._callbacks
        assert isinstance(self.conn._callbacks[mock_queue], Callable)

    def test_channel_basic_consume_creates_new_receiver(self):
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), Mock())
        self.transport.session.receiver.assert_called_once_with(mock_queue)

    def test_channel_basic_consume_saves_new_receiver(self):
        mock_tag = Mock()
        self.channel.basic_consume(Mock(), Mock(), Mock(), mock_tag)
        new_mock_receiver = self.transport.session.receiver.return_value
        expected_dict = {mock_tag: new_mock_receiver}
        assert expected_dict == self.channel._receivers

    def test_channel_basic_consume_sets_capacity_on_new_receiver(self):
        mock_prefetch_count = Mock()
        self.channel.qos.prefetch_count = mock_prefetch_count
        self.channel.basic_consume(Mock(), Mock(), Mock(), Mock())
        new_receiver = self.transport.session.receiver.return_value
        assert new_receiver.capacity is mock_prefetch_count

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
        self.channel.Message.assert_called_once_with(
            mock_content, channel=self.channel,
        )

    def test_channel_basic_consume_callback_adds_message_to_QoS(self):
        self.channel._qos = Mock()
        callback = self.get_callback()
        mock_qpid_message = Mock()
        callback(mock_qpid_message)
        mock_delivery_tag = self.channel.Message.return_value.delivery_tag
        self.channel._qos.append.assert_called_once_with(
            mock_qpid_message, mock_delivery_tag,
        )

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
        self.channel.basic_ack.assert_not_called()

    def test_channel_basic_consume_callback_calls_real_callback(self):
        self.channel.basic_ack = Mock()
        mock_original_callback = Mock()
        callback = self.get_callback(original_cb=mock_original_callback)
        mock_qpid_message = Mock()
        callback(mock_qpid_message)
        expected_message = self.channel.Message.return_value
        mock_original_callback.assert_called_once_with(expected_message)


@skip.if_python3()
@skip.if_pypy()
class test_Channel_queue_delete(ChannelTestBase):

    @pytest.fixture(autouse=True)
    def setup_channel_patches(self, patching, setup_channel):
        self.mock__has_queue = patching.object(self.channel, '_has_queue')
        self.mock__size = patching.object(self.channel, '_size')
        self.mock__delete = patching.object(self.channel, '_delete')
        self.mock_queue = Mock()

    def test_checks_if_queue_exists(self):
        self.channel.queue_delete(self.mock_queue)
        self.mock__has_queue.assert_called_once_with(self.mock_queue)

    def test_does_nothing_if_queue_does_not_exist(self):
        self.mock__has_queue.return_value = False
        self.channel.queue_delete(self.mock_queue)
        self.mock__delete.assert_not_called()

    def test_not_empty_and_if_empty_True_no_delete(self):
        self.mock__size.return_value = 1
        self.channel.queue_delete(self.mock_queue, if_empty=True)
        mock_broker = self.mock_broker_agent.return_value
        mock_broker.getQueue.assert_not_called()

    def test_calls_get_queue(self):
        self.channel.queue_delete(self.mock_queue)
        getQueue = self.mock_broker_agent.return_value.getQueue
        getQueue.assert_called_once_with(self.mock_queue)

    def test_gets_queue_attribute(self):
        self.channel.queue_delete(self.mock_queue)
        queue_obj = self.mock_broker_agent.return_value.getQueue.return_value
        queue_obj.getAttributes.assert_called_once_with()

    def test_queue_in_use_and_if_unused_no_delete(self):
        queue_obj = self.mock_broker_agent.return_value.getQueue.return_value
        queue_obj.getAttributes.return_value = {'consumerCount': 1}
        self.channel.queue_delete(self.mock_queue, if_unused=True)
        self.mock__delete.assert_not_called()

    def test_calls__delete_with_queue(self):
        self.channel.queue_delete(self.mock_queue)
        self.mock__delete.assert_called_once_with(self.mock_queue)


@skip.if_python3()
@skip.if_pypy()
class test_Channel(object):

    @patch(QPID_MODULE + '.qpidtoollibs')
    def setup(self, mock_qpidtoollibs):
        self.mock_connection = Mock()
        self.mock_qpid_connection = Mock()
        self.mock_qpid_session = Mock()
        self.mock_qpid_connection.session = Mock(
            return_value=self.mock_qpid_session,
        )
        self.mock_connection.get_qpid_connection = Mock(
            return_value=self.mock_qpid_connection,
        )
        self.mock_transport = Mock()
        self.mock_broker = Mock()
        self.mock_Message = Mock()
        self.mock_BrokerAgent = mock_qpidtoollibs.BrokerAgent
        self.mock_BrokerAgent.return_value = self.mock_broker
        self.my_channel = Channel(
            self.mock_connection, self.mock_transport,
        )
        self.my_channel.Message = self.mock_Message

    def test_verify_QoS_class_attribute(self):
        """Verify that the class attribute QoS refers to the QoS object"""
        assert QoS is Channel.QoS

    def test_verify_Message_class_attribute(self):
        """Verify that the class attribute Message refers to the Message
        object."""
        assert Message is Channel.Message

    def test_body_encoding_class_attribute(self):
        """Verify that the class attribute body_encoding is set to base64"""
        assert Channel.body_encoding == 'base64'

    def test_codecs_class_attribute(self):
        """Verify that the codecs class attribute has a correct key and
        value."""
        assert isinstance(Channel.codecs, dict)
        assert 'base64' in Channel.codecs
        assert isinstance(Channel.codecs['base64'], Base64)

    def test_size(self):
        """Test getting the number of messages in a queue specified by
        name and returning them."""
        message_count = 5
        mock_queue = Mock()
        mock_queue_to_check = Mock()
        mock_queue_to_check.values = {'msgDepth': message_count}
        self.mock_broker.getQueue.return_value = mock_queue_to_check
        result = self.my_channel._size(mock_queue)
        self.mock_broker.getQueue.assert_called_with(mock_queue)
        assert message_count == result

    def test_delete(self):
        """Test deleting a queue calls purge and delQueue with queue name."""
        mock_queue = Mock()
        self.my_channel._purge = Mock()
        result = self.my_channel._delete(mock_queue)
        self.my_channel._purge.assert_called_with(mock_queue)
        self.mock_broker.delQueue.assert_called_with(mock_queue)
        assert result is None

    def test_has_queue_true(self):
        """Test checking if a queue exists, and it does."""
        mock_queue = Mock()
        self.mock_broker.getQueue.return_value = True
        result = self.my_channel._has_queue(mock_queue)
        assert result

    def test_has_queue_false(self):
        """Test checking if a queue exists, and it does not."""
        mock_queue = Mock()
        self.mock_broker.getQueue.return_value = False
        result = self.my_channel._has_queue(mock_queue)
        assert not result

    @patch('amqp.protocol.queue_declare_ok_t')
    def test_queue_declare_with_exception_raised(self,
                                                 mock_queue_declare_ok_t):
        """Test declare_queue, where an exception is raised and silenced."""
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
        options = {
            'passive': mock_passive,
            'durable': mock_durable,
            'exclusive': mock_exclusive,
            'auto-delete': mock_auto_delete,
            'arguments': mock_arguments,
        }
        mock_consumer_count = Mock()
        mock_return_value = Mock()
        values_dict = {
            'msgDepth': mock_msg_count,
            'consumerCount': mock_consumer_count,
        }
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        exception_to_raise = Exception('The foo object already exists.')
        self.mock_broker.addQueue.side_effect = exception_to_raise
        self.mock_broker.getQueue.return_value = mock_queue_data
        mock_queue_declare_ok_t.return_value = mock_return_value
        result = self.my_channel.queue_declare(
            mock_queue,
            passive=mock_passive,
            durable=mock_durable,
            exclusive=mock_exclusive,
            auto_delete=mock_auto_delete,
            nowait=mock_nowait,
            arguments=mock_arguments,
        )
        self.mock_broker.addQueue.assert_called_with(
            mock_queue, options=options,
        )
        mock_queue_declare_ok_t.assert_called_with(
            mock_queue, mock_msg_count, mock_consumer_count,
        )
        assert mock_return_value is result

    def test_queue_declare_set_ring_policy_for_celeryev(self):
        """Test declare_queue sets ring_policy for celeryev."""
        mock_queue = Mock()
        mock_queue.startswith.return_value = True
        mock_queue.endswith.return_value = False
        expected_default_options = {
            'passive': False,
            'durable': False,
            'exclusive': False,
            'auto-delete': True,
            'arguments': None,
            'qpid.policy_type': 'ring',
        }
        mock_msg_count = Mock()
        mock_consumer_count = Mock()
        values_dict = {
            'msgDepth': mock_msg_count,
            'consumerCount': mock_consumer_count,
        }
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        self.mock_broker.addQueue.return_value = None
        self.mock_broker.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.startswith.assert_called_with('celeryev')
        self.mock_broker.addQueue.assert_called_with(
            mock_queue, options=expected_default_options,
        )

    def test_queue_declare_set_ring_policy_for_pidbox(self):
        """Test declare_queue sets ring_policy for pidbox."""
        mock_queue = Mock()
        mock_queue.startswith.return_value = False
        mock_queue.endswith.return_value = True
        expected_default_options = {
            'passive': False,
            'durable': False,
            'exclusive': False,
            'auto-delete': True,
            'arguments': None,
            'qpid.policy_type': 'ring',
        }
        mock_msg_count = Mock()
        mock_consumer_count = Mock()
        values_dict = {
            'msgDepth': mock_msg_count,
            'consumerCount': mock_consumer_count,
        }
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        self.mock_broker.addQueue.return_value = None
        self.mock_broker.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.endswith.assert_called_with('pidbox')
        self.mock_broker.addQueue.assert_called_with(
            mock_queue, options=expected_default_options,
        )

    def test_queue_declare_ring_policy_not_set_as_expected(self):
        """Test declare_queue does not set ring_policy as expected."""
        mock_queue = Mock()
        mock_queue.startswith.return_value = False
        mock_queue.endswith.return_value = False
        expected_default_options = {
            'passive': False,
            'durable': False,
            'exclusive': False,
            'auto-delete': True,
            'arguments': None,
        }
        mock_msg_count = Mock()
        mock_consumer_count = Mock()
        values_dict = {
            'msgDepth': mock_msg_count,
            'consumerCount': mock_consumer_count,
        }
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        self.mock_broker.addQueue.return_value = None
        self.mock_broker.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.startswith.assert_called_with('celeryev')
        mock_queue.endswith.assert_called_with('pidbox')
        self.mock_broker.addQueue.assert_called_with(
            mock_queue, options=expected_default_options,
        )

    def test_queue_declare_test_defaults(self):
        """Test declare_queue defaults."""
        mock_queue = Mock()
        mock_queue.startswith.return_value = False
        mock_queue.endswith.return_value = False
        expected_default_options = {
            'passive': False,
            'durable': False,
            'exclusive': False,
            'auto-delete': True,
            'arguments': None,
        }
        mock_msg_count = Mock()
        mock_consumer_count = Mock()
        values_dict = {
            'msgDepth': mock_msg_count,
            'consumerCount': mock_consumer_count,
        }
        mock_queue_data = Mock()
        mock_queue_data.values = values_dict
        self.mock_broker.addQueue.return_value = None
        self.mock_broker.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        self.mock_broker.addQueue.assert_called_with(
            mock_queue,
            options=expected_default_options,
        )

    def test_queue_declare_raises_exception_not_silenced(self):
        unique_exception = Exception('This exception should not be silenced')
        mock_queue = Mock()
        self.mock_broker.addQueue.side_effect = unique_exception
        with pytest.raises(unique_exception.__class__):
            self.my_channel.queue_declare(mock_queue)
        self.mock_broker.addQueue.assert_called_once_with(
            mock_queue,
            options={
                'exclusive': False,
                'durable': False,
                'qpid.policy_type': 'ring',
                'passive': False,
                'arguments': None,
                'auto-delete': True
            })

    def test_exchange_declare_raises_exception_and_silenced(self):
        """Create exchange where an exception is raised and then silenced"""
        self.mock_broker.addExchange.side_effect = Exception(
            'The foo object already exists.',
        )
        self.my_channel.exchange_declare()

    def test_exchange_declare_raises_exception_not_silenced(self):
        """Create Exchange where an exception is raised and not silenced."""
        unique_exception = Exception('This exception should not be silenced')
        self.mock_broker.addExchange.side_effect = unique_exception
        with pytest.raises(unique_exception.__class__):
            self.my_channel.exchange_declare()

    def test_exchange_declare(self):
        """Create Exchange where an exception is NOT raised."""
        mock_exchange = Mock()
        mock_type = Mock()
        mock_durable = Mock()
        options = {'durable': mock_durable}
        result = self.my_channel.exchange_declare(
            mock_exchange, mock_type, mock_durable,
        )
        self.mock_broker.addExchange.assert_called_with(
            mock_type, mock_exchange, options,
        )
        assert result is None

    def test_exchange_delete(self):
        """Test the deletion of an exchange by name."""
        mock_exchange = Mock()
        result = self.my_channel.exchange_delete(mock_exchange)
        self.mock_broker.delExchange.assert_called_with(mock_exchange)
        assert result is None

    def test_queue_bind(self):
        """Test binding a queue to an exchange using a routing key."""
        mock_queue = Mock()
        mock_exchange = Mock()
        mock_routing_key = Mock()
        self.my_channel.queue_bind(
            mock_queue, mock_exchange, mock_routing_key,
        )
        self.mock_broker.bind.assert_called_with(
            mock_exchange, mock_queue, mock_routing_key,
        )

    def test_queue_unbind(self):
        """Test unbinding a queue from an exchange using a routing key."""
        mock_queue = Mock()
        mock_exchange = Mock()
        mock_routing_key = Mock()
        self.my_channel.queue_unbind(
            mock_queue, mock_exchange, mock_routing_key,
        )
        self.mock_broker.unbind.assert_called_with(
            mock_exchange, mock_queue, mock_routing_key,
        )

    def test_queue_purge(self):
        """Test purging a queue by name."""
        mock_queue = Mock()
        purge_result = Mock()
        self.my_channel._purge = Mock(return_value=purge_result)
        result = self.my_channel.queue_purge(mock_queue)
        self.my_channel._purge.assert_called_with(mock_queue)
        assert purge_result is result

    @patch(QPID_MODULE + '.Channel.qos')
    def test_basic_ack(self, mock_qos):
        """Test that basic_ack calls the QoS object properly."""
        mock_delivery_tag = Mock()
        self.my_channel.basic_ack(mock_delivery_tag)
        mock_qos.ack.assert_called_with(mock_delivery_tag)

    @patch(QPID_MODULE + '.Channel.qos')
    def test_basic_reject(self, mock_qos):
        """Test that basic_reject calls the QoS object properly."""
        mock_delivery_tag = Mock()
        mock_requeue_value = Mock()
        self.my_channel.basic_reject(mock_delivery_tag, mock_requeue_value)
        mock_qos.reject.assert_called_with(
            mock_delivery_tag, requeue=mock_requeue_value,
        )

    def test_qos_manager_is_none(self):
        """Test the qos property if the QoS object did not already exist."""
        self.my_channel._qos = None
        result = self.my_channel.qos
        assert isinstance(result, QoS)
        assert result == self.my_channel._qos

    def test_qos_manager_already_exists(self):
        """Test the qos property if the QoS object already exists."""
        mock_existing_qos = Mock()
        self.my_channel._qos = mock_existing_qos
        result = self.my_channel.qos
        assert mock_existing_qos is result

    def test_prepare_message(self):
        """Test that prepare_message() returns the correct result."""
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
        assert mock_body is result['body']
        assert mock_content_encoding is result['content-encoding']
        assert mock_content_type is result['content-type']
        assert headers == result['headers']
        assert properties == result['properties']
        assert (mock_priority is
                result['properties']['delivery_info']['priority'])

    @patch('__builtin__.buffer')
    @patch(QPID_MODULE + '.Channel.body_encoding')
    @patch(QPID_MODULE + '.Channel.encode_body')
    @patch(QPID_MODULE + '.Channel._put')
    def test_basic_publish(self, mock_put,
                           mock_encode_body,
                           mock_body_encoding,
                           mock_buffer):
        """Test basic_publish()."""
        mock_original_body = Mock()
        mock_encoded_body = 'this is my encoded body'
        mock_message = {'body': mock_original_body,
                        'properties': {'delivery_info': {}}}
        mock_encode_body.return_value = (
            mock_encoded_body, mock_body_encoding,
        )
        mock_exchange = Mock()
        mock_routing_key = Mock()
        mock_encoded_buffered_body = Mock()
        mock_buffer.return_value = mock_encoded_buffered_body
        self.my_channel.basic_publish(
            mock_message, mock_exchange, mock_routing_key,
        )
        mock_encode_body.assert_called_once_with(
            mock_original_body, mock_body_encoding,
        )
        mock_buffer.assert_called_once_with(mock_encoded_body)
        assert mock_message['body'] is mock_encoded_buffered_body
        assert (mock_message['properties']['body_encoding'] is
                mock_body_encoding)
        assert isinstance(
            mock_message['properties']['delivery_tag'], uuid.UUID)
        assert (mock_message['properties']['delivery_info']['exchange'] is
                mock_exchange)
        assert (mock_message['properties']['delivery_info']['routing_key'] is
                mock_routing_key)
        mock_put.assert_called_with(
            mock_routing_key, mock_message, mock_exchange,
        )

    @patch(QPID_MODULE + '.Channel.codecs')
    def test_encode_body_expected_encoding(self, mock_codecs):
        """Test if encode_body() works when encoding is set correctly"""
        mock_body = Mock()
        mock_encoder = Mock()
        mock_encoded_result = Mock()
        mock_codecs.get.return_value = mock_encoder
        mock_encoder.encode.return_value = mock_encoded_result
        result = self.my_channel.encode_body(mock_body, encoding='base64')
        expected_result = (mock_encoded_result, 'base64')
        assert expected_result == result

    @patch(QPID_MODULE + '.Channel.codecs')
    def test_encode_body_not_expected_encoding(self, mock_codecs):
        """Test if encode_body() works when encoding is not set correctly."""
        mock_body = Mock()
        result = self.my_channel.encode_body(mock_body, encoding=None)
        expected_result = mock_body, None
        assert expected_result == result

    @patch(QPID_MODULE + '.Channel.codecs')
    def test_decode_body_expected_encoding(self, mock_codecs):
        """Test if decode_body() works when encoding is set correctly."""
        mock_body = Mock()
        mock_decoder = Mock()
        mock_decoded_result = Mock()
        mock_codecs.get.return_value = mock_decoder
        mock_decoder.decode.return_value = mock_decoded_result
        result = self.my_channel.decode_body(mock_body, encoding='base64')
        assert mock_decoded_result == result

    @patch(QPID_MODULE + '.Channel.codecs')
    def test_decode_body_not_expected_encoding(self, mock_codecs):
        """Test if decode_body() works when encoding is not set correctly."""
        mock_body = Mock()
        result = self.my_channel.decode_body(mock_body, encoding=None)
        assert mock_body == result

    def test_typeof_exchange_exists(self):
        """Test that typeof() finds an exchange that already exists."""
        mock_exchange = Mock()
        mock_qpid_exchange = Mock()
        mock_attributes = {}
        mock_type = Mock()
        mock_attributes['type'] = mock_type
        mock_qpid_exchange.getAttributes.return_value = mock_attributes
        self.mock_broker.getExchange.return_value = mock_qpid_exchange
        result = self.my_channel.typeof(mock_exchange)
        assert mock_type is result

    def test_typeof_exchange_does_not_exist(self):
        """Test that typeof() finds an exchange that does not exists."""
        mock_exchange = Mock()
        mock_default = Mock()
        self.mock_broker.getExchange.return_value = None
        result = self.my_channel.typeof(mock_exchange, default=mock_default)
        assert mock_default is result


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport__init__(object):

    @pytest.fixture(autouse=True)
    def mock_verify_runtime_environment(self, patching):
        self.mock_verify_runtime_environment = patching.object(
            Transport, 'verify_runtime_environment')

    @pytest.fixture(autouse=True)
    def mock_transport_init(self, patching):
        self.mock_base_Transport__init__ = patching(
            QPID_MODULE + '.base.Transport.__init__')

    def test_Transport___init___calls_verify_runtime_environment(self):
        Transport(Mock())
        self.mock_verify_runtime_environment.assert_called_once_with()

    def test_transport___init___calls_parent_class___init__(self):
        m = Mock()
        Transport(m)
        self.mock_base_Transport__init__.assert_called_once_with(m)

    def test_transport___init___sets_use_async_interface_False(self):
        transport = Transport(Mock())
        assert not transport.use_async_interface


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport_drain_events(object):

    @pytest.fixture(autouse=True)
    def setup_self(self, disable_runtime_dependency_check):
        # ^^ disable_runtime.. must be called before this fixture.
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
        with patch(QPID_MODULE + '.QpidEmpty', new=QpidException):
            self.transport.session.next_receiver.side_effect = QpidException()
            with pytest.raises(socket.timeout):
                self.transport.drain_events(Mock())

    def test_socket_timeout_raised_when_by_timeout(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        with pytest.raises(socket.timeout):
            self.transport.drain_events(self.mock_conn, timeout=1)

    def test_timeout_returns_no_earlier_then_asked_for(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        start_time = monotonic()
        try:
            self.transport.drain_events(self.mock_conn, timeout=1)
        except socket.timeout:
            pass
        elapsed_time_in_s = monotonic() - start_time
        assert elapsed_time_in_s >= 1.0

    def test_callback_is_called(self):
        self.transport.session.next_receiver = self.mock_next_receiver
        try:
            self.transport.drain_events(self.mock_conn, timeout=1)
        except socket.timeout:
            pass
        self.mock_callback.assert_called_with(self.mock_message)


@skip.if_python3()
@skip.if_pypy()
class test_Transport_create_channel(object):

    @pytest.fixture(autouse=True)
    def setup_self(self, disable_runtime_dependency_check):
        # ^^ disable runtime MUST be called before this fixture
        self.transport = Transport(Mock())
        self.mock_conn = Mock()
        self.mock_new_channel = Mock()
        self.mock_conn.Channel.return_value = self.mock_new_channel
        self.returned_channel = self.transport.create_channel(self.mock_conn)

    def test_new_channel_created_from_connection(self):
        assert self.mock_new_channel is self.returned_channel
        self.mock_conn.Channel.assert_called_with(
            self.mock_conn, self.transport,
        )

    def test_new_channel_added_to_connection_channel_list(self):
        append_method = self.mock_conn.channels.append
        append_method.assert_called_with(self.mock_new_channel)


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport_establish_connection(object):

    @pytest.fixture(autouse=True)
    def setup_self(self, disable_runtime_dependency_check):

        class MockClient(object):
            pass

        self.client = MockClient()
        self.client.connect_timeout = 4
        self.client.ssl = False
        self.client.transport_options = {}
        self.client.userid = None
        self.client.password = None
        self.client.login_method = None
        self.transport = Transport(self.client)
        self.mock_conn = Mock()
        self.transport.Connection = self.mock_conn

    def test_transport_establish_conn_new_option_overwrites_default(self):
        self.client.userid = 'new-userid'
        self.client.password = 'new-password'
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            username=self.client.userid,
            password=self.client.password,
            sasl_mechanisms='PLAIN',
            host='localhost',
            timeout=4,
            port=5672,
            transport='tcp',
        )

    def test_transport_establish_conn_empty_client_is_default(self):
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            sasl_mechanisms='ANONYMOUS',
            host='localhost',
            timeout=4,
            port=5672,
            transport='tcp',
        )

    def test_transport_establish_conn_additional_transport_option(self):
        new_param_value = 'mynewparam'
        self.client.transport_options['new_param'] = new_param_value
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            sasl_mechanisms='ANONYMOUS',
            host='localhost',
            timeout=4,
            new_param=new_param_value,
            port=5672,
            transport='tcp',
        )

    def test_transport_establish_conn_transform_localhost_to_127_0_0_1(self):
        self.client.hostname = 'localhost'
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            sasl_mechanisms='ANONYMOUS',
            host='localhost',
            timeout=4,
            port=5672,
            transport='tcp',
        )

    def test_transport_password_no_userid_raises_exception(self):
        self.client.password = 'somepass'
        with pytest.raises(Exception):
            self.transport.establish_connection()

    def test_transport_userid_no_password_raises_exception(self):
        self.client.userid = 'someusername'
        with pytest.raises(Exception):
            self.transport.establish_connection()

    def test_transport_overrides_sasl_mech_from_login_method(self):
        self.client.login_method = 'EXTERNAL'
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            sasl_mechanisms='EXTERNAL',
            host='localhost',
            timeout=4,
            port=5672,
            transport='tcp',
        )

    def test_transport_overrides_sasl_mech_has_username(self):
        self.client.userid = 'new-userid'
        self.client.login_method = 'EXTERNAL'
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            username=self.client.userid,
            sasl_mechanisms='EXTERNAL',
            host='localhost',
            timeout=4,
            port=5672,
            transport='tcp',
        )

    def test_transport_establish_conn_set_password(self):
        self.client.userid = 'someuser'
        self.client.password = 'somepass'
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            username='someuser',
            password='somepass',
            sasl_mechanisms='PLAIN',
            host='localhost',
            timeout=4,
            port=5672,
            transport='tcp',
        )

    def test_transport_establish_conn_no_ssl_sets_transport_tcp(self):
        self.client.ssl = False
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            sasl_mechanisms='ANONYMOUS',
            host='localhost',
            timeout=4,
            port=5672,
            transport='tcp',
        )

    def test_transport_establish_conn_with_ssl_with_hostname_check(self):
        self.client.ssl = {
            'keyfile': 'my_keyfile',
            'certfile': 'my_certfile',
            'ca_certs': 'my_cacerts',
            'cert_reqs': ssl.CERT_REQUIRED,
        }
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            ssl_certfile='my_certfile',
            ssl_trustfile='my_cacerts',
            timeout=4,
            ssl_skip_hostname_check=False,
            sasl_mechanisms='ANONYMOUS',
            host='localhost',
            ssl_keyfile='my_keyfile',
            port=5672, transport='ssl',
        )

    def test_transport_establish_conn_with_ssl_skip_hostname_check(self):
        self.client.ssl = {
            'keyfile': 'my_keyfile',
            'certfile': 'my_certfile',
            'ca_certs': 'my_cacerts',
            'cert_reqs': ssl.CERT_OPTIONAL,
        }
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            ssl_certfile='my_certfile',
            ssl_trustfile='my_cacerts',
            timeout=4,
            ssl_skip_hostname_check=True,
            sasl_mechanisms='ANONYMOUS',
            host='localhost',
            ssl_keyfile='my_keyfile',
            port=5672, transport='ssl',
        )

    def test_transport_establish_conn_sets_client_on_connection_object(self):
        self.transport.establish_connection()
        assert self.mock_conn.return_value.client is self.client

    def test_transport_establish_conn_creates_session_on_transport(self):
        self.transport.establish_connection()
        qpid_conn = self.mock_conn.return_value.get_qpid_connection
        new_mock_session = qpid_conn.return_value.session.return_value
        assert self.transport.session is new_mock_session

    def test_transport_establish_conn_returns_new_connection_object(self):
        new_conn = self.transport.establish_connection()
        assert new_conn is self.mock_conn.return_value

    def test_transport_establish_conn_uses_hostname_if_not_default(self):
        self.client.hostname = 'some_other_hostname'
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            sasl_mechanisms='ANONYMOUS',
            host='some_other_hostname',
            timeout=4,
            port=5672,
            transport='tcp',
        )

    def test_transport_sets_qpid_message_ready_handler(self):
        self.transport.establish_connection()
        qpid_conn_call = self.mock_conn.return_value.get_qpid_connection
        mock_session = qpid_conn_call.return_value.session.return_value
        mock_set_callback = mock_session.set_message_received_notify_handler
        expected_msg_callback = self.transport._qpid_message_ready_handler
        mock_set_callback.assert_called_once_with(expected_msg_callback)

    def test_transport_sets_session_exception_handler(self):
        self.transport.establish_connection()
        qpid_conn_call = self.mock_conn.return_value.get_qpid_connection
        mock_session = qpid_conn_call.return_value.session.return_value
        mock_set_callback = mock_session.set_async_exception_notify_handler
        exc_callback = self.transport._qpid_async_exception_notify_handler
        mock_set_callback.assert_called_once_with(exc_callback)

    def test_transport_sets_connection_exception_handler(self):
        self.transport.establish_connection()
        qpid_conn_call = self.mock_conn.return_value.get_qpid_connection
        qpid_conn = qpid_conn_call.return_value
        mock_set_callback = qpid_conn.set_async_exception_notify_handler
        exc_callback = self.transport._qpid_async_exception_notify_handler
        mock_set_callback.assert_called_once_with(exc_callback)


@skip.if_python3()
@skip.if_pypy()
class test_Transport_class_attributes(object):

    def test_verify_Connection_attribute(self):
        assert Connection is Transport.Connection

    def test_verify_polling_disabled(self):
        assert Transport.polling_interval is None

    def test_verify_driver_type_and_name(self):
        assert Transport.driver_type == 'qpid'
        assert Transport.driver_name == 'qpid'

    def test_verify_implements_exchange_types(self):
        assert 'fanout' in Transport.implements.exchange_type
        assert 'direct' in Transport.implements.exchange_type
        assert 'topic' in Transport.implements.exchange_type
        assert 'frobnitz' not in Transport.implements.exchange_type

    def test_transport_verify_recoverable_connection_errors(self):
        connection_errors = Transport.recoverable_connection_errors
        assert ConnectionError in connection_errors
        assert select.error in connection_errors

    def test_transport_verify_recoverable_channel_errors(self):
        channel_errors = Transport.recoverable_channel_errors
        assert NotFound in channel_errors

    def test_transport_verify_pre_kombu_3_0_exception_labels(self):
        assert (Transport.recoverable_channel_errors ==
                Transport.channel_errors)
        assert (Transport.recoverable_connection_errors ==
                Transport.connection_errors)


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport_register_with_event_loop(object):

    def test_transport_register_with_event_loop_calls_add_reader(self):
        transport = Transport(Mock())
        mock_connection = Mock()
        mock_loop = Mock()
        transport.register_with_event_loop(mock_connection, mock_loop)
        mock_loop.add_reader.assert_called_with(
            transport.r, transport.on_readable, mock_connection, mock_loop,
        )


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport_Qpid_callback_handlers_async(object):

    @pytest.fixture(autouse=True)
    def setup_self(self, patching, disable_runtime_dependency_check):
        self.mock_os_write = patching(QPID_MODULE + '.os.write')
        self.transport = Transport(Mock())
        self.transport.register_with_event_loop(Mock(), Mock())

    def test__qpid_message_ready_handler_writes_symbol_to_fd(self):
        self.transport._qpid_message_ready_handler(Mock())
        self.mock_os_write.assert_called_once_with(self.transport._w, '0')

    def test__qpid_async_exception_notify_handler_writes_symbol_to_fd(self):
        self.transport._qpid_async_exception_notify_handler(Mock(), Mock())
        self.mock_os_write.assert_called_once_with(self.transport._w, 'e')


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport_Qpid_callback_handlers_sync(object):

    @pytest.fixture(autouse=True)
    def setup(self, patching, disable_runtime_dependency_check):
        self.mock_os_write = patching(QPID_MODULE + '.os.write')
        self.transport = Transport(Mock())

    def test__qpid_message_ready_handler_dows_not_write(self):
        self.transport._qpid_message_ready_handler(Mock())
        self.mock_os_write.assert_not_called()

    def test__qpid_async_exception_notify_handler_does_not_write(self):
        self.transport._qpid_async_exception_notify_handler(Mock(), Mock())
        self.mock_os_write.assert_not_called()


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport_on_readable(object):

    @pytest.fixture(autouse=True)
    def setup_self(self, patching, disable_runtime_dependency_check):
        self.mock_os_read = patching(QPID_MODULE + '.os.read')

        self.mock_drain_events = patching.object(Transport, 'drain_events')
        self.transport = Transport(Mock())
        self.transport.register_with_event_loop(Mock(), Mock())

    def test_transport_on_readable_reads_symbol_from_fd(self):
        self.transport.on_readable(Mock(), Mock())
        self.mock_os_read.assert_called_once_with(self.transport.r, 1)

    def test_transport_on_readable_calls_drain_events(self):
        mock_connection = Mock()
        self.transport.on_readable(mock_connection, Mock())
        self.mock_drain_events.assert_called_with(mock_connection)

    def test_transport_on_readable_catches_socket_timeout(self):
        self.mock_drain_events.side_effect = socket.timeout()
        self.transport.on_readable(Mock(), Mock())

    def test_transport_on_readable_ignores_non_socket_timeout_exception(self):
        self.mock_drain_events.side_effect = IOError()
        with pytest.raises(IOError):
            self.transport.on_readable(Mock(), Mock())


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport_verify_runtime_environment(object):

    @pytest.fixture(autouse=True)
    def setup_self(self, patching):
        self.verify_runtime_environment = Transport.verify_runtime_environment
        patching.object(Transport, 'verify_runtime_environment')
        self.transport = Transport(Mock())

    @patch(QPID_MODULE + '.PY3', new=True)
    def test_raises_exception_for_Python3(self):
        with pytest.raises(RuntimeError):
            self.verify_runtime_environment(self.transport)

    @patch('__builtin__.getattr')
    def test_raises_exc_for_PyPy(self, mock_getattr):
        mock_getattr.return_value = True
        with pytest.raises(RuntimeError):
            self.verify_runtime_environment(self.transport)

    @patch(QPID_MODULE + '.dependency_is_none')
    def test_raises_exc_dep_missing(self, mock_dep_is_none):
        mock_dep_is_none.return_value = True
        with pytest.raises(RuntimeError):
            self.verify_runtime_environment(self.transport)

    @patch(QPID_MODULE + '.dependency_is_none')
    def test_calls_dependency_is_none(self, mock_dep_is_none):
        mock_dep_is_none.return_value = False
        self.verify_runtime_environment(self.transport)
        mock_dep_is_none.assert_called()

    def test_raises_no_exception(self):
        self.verify_runtime_environment(self.transport)


@skip.if_python3()
@skip.if_pypy()
@pytest.mark.usefixtures('disable_runtime_dependency_check')
class test_Transport(object):

    def setup(self):
        """Creates a mock_client to be used in testing."""
        self.mock_client = Mock()

    def test_supports_ev(self):
        """Test that the transport claims to support async event loop"""
        assert Transport(self.mock_client).supports_ev

    def test_close_connection(self):
        """Test that close_connection calls close on the connection."""
        my_transport = Transport(self.mock_client)
        mock_connection = Mock()
        my_transport.close_connection(mock_connection)
        mock_connection.close.assert_called_once_with()

    def test_default_connection_params(self):
        """Test that the default_connection_params are correct"""
        correct_params = {
            'hostname': 'localhost',
            'port': 5672,
        }
        my_transport = Transport(self.mock_client)
        result_params = my_transport.default_connection_params
        assert correct_params == result_params

    @patch(QPID_MODULE + '.os.close')
    def test_del_sync(self, close):
        my_transport = Transport(self.mock_client)
        my_transport.__del__()
        close.assert_not_called()

    @patch(QPID_MODULE + '.os.close')
    def test_del_async(self, close):
        my_transport = Transport(self.mock_client)
        my_transport.register_with_event_loop(Mock(), Mock())
        my_transport.__del__()
        close.assert_called()

    @patch(QPID_MODULE + '.os.close')
    def test_del_async_failed(self, close):
        close.side_effect = OSError()
        my_transport = Transport(self.mock_client)
        my_transport.register_with_event_loop(Mock(), Mock())
        my_transport.__del__()
        close.assert_called()
