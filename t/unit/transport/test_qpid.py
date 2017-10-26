from __future__ import absolute_import, unicode_literals

import Queue
import select
import ssl
import socket
import uuid

from collections import Callable, OrderedDict
from itertools import count

from case import Mock, call, patch, skip

from kombu.five import range, monotonic
from kombu.transport.qpid import (Channel, Connection, Message, QoS,
                                  Transport)
from kombu.transport.virtual import Base64
from kombu.tests.case import Case, Mock, case_no_pypy
from kombu.tests.case import patch
from kombu.utils.compat import OrderedDict
try:
    from proton.utils import ConnectionException
except ImportError:  # pragma: no cover
    ConnectionException = None


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


@case_no_pypy
class TestQoS__init__(Case):

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


@case_no_pypy
class TestQoSCanConsume(Case):

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


@case_no_pypy
class TestQoSCanConsumeMaxEstimate(Case):

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


@case_no_pypy
class QoSSetup(Case):

    def setUp(self):
        self.mock_main_thread_commands = Mock()
        self.qos = QoS(self.mock_main_thread_commands)
        self.patch_Event = patch(QPID_MODULE + '.threading.Event')
        self.mock_Event = self.patch_Event.start()

    def tearDown(self):
        self.patch_Event.stop()


@case_no_pypy
class TestQoSAck(QoSSetup):

    def setUp(self):
        super(TestQoSAck, self).setUp()

        self.message = Mock()
        self.qos.append(self.message, 1)

    def test_ack_pops__not_yet_acked(self):
        self.assertIn(1, self.qos._not_yet_acked)
        self.qos.ack(1)
        assert 1 not in self.qos._not_yet_acked

    def test_ack_waits_for_backend_event(self):
        self.qos.ack(1)
        self.mock_Event.return_value.wait.assert_called_once_with()

    def test_ack_sends_AckMessage_to_backend(self):
        patch_AckMessage = patch(QPID_MODULE + '.AckMessage')
        mock_AckMessage = patch_AckMessage.start()
        self.qos.ack(1)
        mock_AckMessage.assert_called_once_with(
            delivery=self.message, ack_complete=self.mock_Event.return_value
        )
        exp_msg = mock_AckMessage.return_value
        self.qos.main_thread_commands.put.assert_called_once_with(exp_msg)
        patch_AckMessage.stop()


@case_no_pypy
class TestQoSReject(QoSSetup):

    def setUp(self):
        super(TestQoSReject, self).setUp()

        self.patch_RejectMessage = patch(QPID_MODULE + '.RejectMessage')
        self.mock_RejectMessage = self.patch_RejectMessage.start()

        self.patch_ReleaseMessage = patch(QPID_MODULE + '.ReleaseMessage')
        self.mock_ReleaseMessage = self.patch_ReleaseMessage.start()

        self.message = Mock()
        self.qos.append(self.message, 1)

    def tearDown(self):
        self.patch_RejectMessage.stop()
        self.patch_ReleaseMessage.stop()

    def test_reject_pops__not_yet_acked(self):
        self.assertIn(1, self.qos._not_yet_acked)
        self.qos.reject(1)
        assert 1 not in self.qos._not_yet_acked

    def test_reject_requeue_true(self):
        self.qos.reject(1, requeue=True)
        self.mock_ReleaseMessage.assert_called_once_with(
            delivery=self.message,
            release_complete=self.mock_Event.return_value
        )
        self.assertFalse(self.mock_RejectMessage.called)
        exp_cmd = self.mock_ReleaseMessage.return_value
        self.mock_main_thread_commands.put.assert_called_once_with(exp_cmd)
        self.mock_Event.return_value.wait.assert_called_once_with()

    def requeue_false_assertions(self):
        self.mock_RejectMessage.assert_called_once_with(
            delivery=self.message,
            reject_complete=self.mock_Event.return_value
        )
        self.assertFalse(self.mock_ReleaseMessage.called)
        exp_cmd = self.mock_RejectMessage.return_value
        self.mock_main_thread_commands.put.assert_called_once_with(exp_cmd)
        self.mock_Event.return_value.wait.assert_called_once_with()

    def test_reject_requeue_false(self):
        self.qos.reject(1, requeue=False)
        self.requeue_false_assertions()

    def test_reject_requeue_default_is_False(self):
        self.qos.reject(1)
        self.requeue_false_assertions()


@case_no_pypy
class TestQoSFunctional(Case):

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

    def setUp(self):
        self.mock_main_thread_commands = Mock()
        self.qos = QoS(self.mock_main_thread_commands)
        self.delivery_tag_generator = count(1)

    def test_append(self):
        """Append two messages and check inside the QoS object that they
        were put into the internal data structures correctly
        """
        m1, m1_tag = self.mock_message_factory()
        m2, m2_tag = self.mock_message_factory()
        self.qos.append(m1, m1_tag)
        length_not_yet_acked = len(self.qos._not_yet_acked)
        self.assertEqual(length_not_yet_acked, 1)
        checked_message1 = self.qos._not_yet_acked[m1_tag]
        self.assertIs(m1, checked_message1)
        self.qos.append(m2, m2_tag)
        length_not_yet_acked = len(self.qos._not_yet_acked)
        self.assertEqual(length_not_yet_acked, 2)
        checked_message2 = self.qos._not_yet_acked[m2_tag]
        self.assertIs(m2, checked_message2)

    @patch(QPID_MODULE + '.threading.Event')
    def test_ack_removes_delivery_reference(self, mock_event):
        m1, m1_tag = self.mock_message_factory()
        self.qos.append(m1, m1_tag)
        self.qos.ack(m1_tag)
        self.assertIs(len(self.qos._not_yet_acked), 0)


@case_no_pypy
class ConnectionTestBase(Case):

    @patch(QPID_MODULE + '.BrokerAgent')
    def setUp(self, mock_BrokerAgent):
        self.connection_options = {
            'host': 'localhost',
            'port': 5672,
            'transport': 'tcp',
            'timeout': 10,
            'sasl_mechanisms': 'ANONYMOUS',
        }
        self.mock_broker_agent = mock_BrokerAgent
        self.conn = Connection(self.connection_options)


@case_no_pypy
class TestConnectionInit(ConnectionTestBase):

    def test_class_variables(self):
        assert isinstance(self.conn.channels, list)
        assert isinstance(self.conn._callbacks, dict)

    def test_establishes_connection(self):
        modified_conn_opts = self.connection_options
        self.mock_broker_agent.connect.assert_called_with(
            **modified_conn_opts
        )

    def test_saves_established_connection(self):
        created_conn = self.mock_broker_agent.connect.return_value
        self.assertIs(self.conn._broker_agent, created_conn)


@case_no_pypy
class TestConnectionClassAttributes(ConnectionTestBase):

    def test_connection_verify_class_attributes(self):
        assert Channel == Connection.Channel


@case_no_pypy
class TestConnectionGetBrokerAgent(ConnectionTestBase):

    def test_connection_get_broker_agent(self):
        self.conn._broker_agent = Mock()
        returned_connection = self.conn.get_broker_agent()
        self.assertIs(self.conn._broker_agent, returned_connection)


@case_no_pypy
class TestConnectionCloseChannel(ConnectionTestBase):

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


@case_no_pypy
class ChannelTestBase(Case):

    def setUp(self):
        self.conn = Mock()
        self.transport = Mock()
        self.channel = Channel(self.conn, self.transport)


@case_no_pypy
class TestChannelClose(ChannelTestBase):

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


@case_no_pypy
class TestChannelBasicQoS(ChannelTestBase):

    def test_channel_basic_qos_always_returns_one(self):
        self.channel.basic_qos(2)
        assert self.channel.qos.prefetch_count == 1


@case_no_pypy
class TestChannelBasicGet(ChannelTestBase):

    @pytest.fixture(autouse=True)
    def setup_channel_attributes(self, setup_channel):
        self.channel.Message = Mock()
        self.channel.qos = Mock()

        self.patch_queue = patch(QPID_MODULE + '.Queue')
        self.mock_queue = self.patch_queue.start()

    def tearDown(self):
        self.patch_queue.stop()

    def test_channel_basic_get_creates_Message_correctly(self):
        mock_queue = Mock()
        self.channel.basic_get(mock_queue)
        pm = self.mock_queue.Queue.return_value.get.return_value.message
        expected_dict = {
            'body': pm.body,
            'properties': pm.properties,
            'content-encoding': pm.content_encoding,
            'content-type': pm.content_type,
            'headers': pm.properties.pop('headers', {})
        }
        self.channel.Message.assert_called_once_with(
            self.channel, expected_dict
        )

    def test_channel_basic_get_acknowledges_message_by_default(self):
        mock_queue = Mock()
        self.channel.basic_get(mock_queue)
        self.assertIs(self.channel.qos.ack.call_count, 1)

    def test_channel_basic_get_acknowledges_message_with_no_ack_False(self):
        self.channel.basic_get(Mock(), no_ack=False)
        self.assertIs(self.channel.qos.ack.call_count, 1)

    def test_channel_basic_get_acknowledges_message_with_no_ack_True(self):
        self.channel.basic_get(Mock(), no_ack=True)
        self.assertIs(self.channel.qos.ack.call_count, 0)

    def test_channel_basic_get_returns_correct_message(self):
        mock_queue = Mock()
        basic_get_result = self.channel.basic_get(mock_queue)
        expected_message = self.channel.Message.return_value
        assert expected_message is basic_get_result

    def test_basic_get_returns_None_when_return_queue_returns_None(self):
        mock_queue = Mock()
        self.mock_queue.Queue.return_value.get.return_value = None
        basic_get_result = self.channel.basic_get(mock_queue)
        self.assertIsNone(basic_get_result)


@case_no_pypy
class TestChannelBasicCancel(ChannelTestBase):

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


@case_no_pypy
class TestChannelInit(ChannelTestBase):

    def test_channel___init__sets_variables_as_expected(self):
        self.assertIs(self.conn, self.channel.connection)
        self.assertIs(self.transport, self.channel.transport)
        self.assertFalse(self.channel.closed)
        self.conn.get_broker_agent.assert_called_once_with()
        expected_broker_agent = self.conn.get_broker_agent.return_value
        self.assertIs(self.channel._broker, expected_broker_agent)
        self.assertDictEqual(self.channel._tag_to_queue, {})
        self.assertDictEqual(self.channel._receivers, {})
        self.assertIsInstance(self.channel.qos, QoS)


@case_no_pypy
class TestChannelBasicConsume(ChannelTestBase):

    def setUp(self):
        super(TestChannelBasicConsume, self).setUp()

        self.patch_basic_ack = patch.object(self.channel, 'basic_ack')
        self.mock_basic_ack = self.patch_basic_ack.start()

        self.conn._callbacks = {}

    def tearDown(self):
        self.patch_basic_ack.stop()

    def test_channel_basic_consume_adds_queue_to__tag_to_queue(self):
        mock_tag = Mock()
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), mock_tag)
        expected_dict = {mock_tag: mock_queue}
        self.assertDictEqual(expected_dict, self.channel._tag_to_queue)

    def test_channel_basic_consume_adds_entry_to_connection__callbacks(self):
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), Mock())
        assert mock_queue in self.conn._callbacks
        assert isinstance(self.conn._callbacks[mock_queue], Callable)

    @patch(QPID_MODULE + '.StartConsumer')
    def test_channel_basic_consume_requests_new_receiver(self,
                                                         mock_start_consumer):
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, Mock(), Mock(), Mock())
        mock_start_consumer.assert_called_once_with(mock_queue)
        receiver_request = mock_start_consumer.return_value
        self.transport.main_thread_commands.put.assert_called_once_with(
            receiver_request
        )

    def get_callback(self, no_ack=Mock(), original_cb=Mock()):
        self.channel.Message = Mock()
        mock_queue = Mock()
        self.channel.basic_consume(mock_queue, no_ack, original_cb, Mock())
        return self.conn._callbacks[mock_queue]

    def test_channel_basic_consume_callback_transforms_to_proton(self):
        patch_msg_transform = patch.object(self.channel,
                                           '_make_kombu_message_from_proton')
        mock_msg_transform = patch_msg_transform.start()
        callback = self.get_callback()
        mock_proton_event = Mock()
        callback(mock_proton_event)
        mock_msg_transform.assert_called_once_with(mock_proton_event.message)
        patch_msg_transform.stop()

    def test_channel_basic_consume_callback_adds_message_to_QoS(self):
        self.channel.qos = Mock()
        callback = self.get_callback()
        mock_proton_message = Mock()
        callback(mock_proton_message)
        mock_delivery_tag = self.channel.Message.return_value.delivery_tag
        self.channel.qos.append.assert_called_once_with(
            mock_proton_message.delivery, mock_delivery_tag,
        )

    def test_channel_basic_consume_callback_gratuitously_acks(self):
        self.channel.basic_ack = Mock()
        callback = self.get_callback()
        mock_proton_message = Mock()
        callback(mock_proton_message)
        mock_delivery_tag = self.channel.Message.return_value.delivery_tag
        self.channel.basic_ack.assert_called_once_with(mock_delivery_tag)

    def test_channel_basic_consume_callback_does_not_ack_when_needed(self):
        self.channel.basic_ack = Mock()
        callback = self.get_callback(no_ack=False)
        mock_proton_message = Mock()
        callback(mock_proton_message)
        self.assertFalse(self.channel.basic_ack.called)

    def test_channel_basic_consume_callback_calls_real_callback(self):
        self.channel.basic_ack = Mock()
        mock_original_callback = Mock()
        callback = self.get_callback(original_cb=mock_original_callback)
        mock_proton_message = Mock()
        callback(mock_proton_message)
        expected_message = self.channel.Message.return_value
        mock_original_callback.assert_called_once_with(expected_message)


@case_no_pypy
class TestChannelQueueDelete(ChannelTestBase):

    def setUp(self):
        super(TestChannelQueueDelete, self).setUp()
        self.patch__size = patch.object(self.channel, '_size')
        self.mock__size = self.patch__size.start()

        self.patch_queue_purge = patch.object(self.channel, 'queue_purge')
        self.mock_queue_purge = self.patch_queue_purge.start()

        self.mock_queue_obj = Mock()
        self.mock_queue_obj.getAttributes.return_value = {'consumerCount': 1}

        self.mock_broker_agent = self.channel._broker
        self.mock_broker_agent.getQueue.return_value = self.mock_queue_obj

        self.mock_queue = Mock()

    def tearDown(self):
        self.patch__size.stop()
        self.patch_queue_purge.stop()
        super(TestChannelQueueDelete, self).tearDown()

    def test_not_empty_and_if_empty_True_no_delete(self):
        self.mock__size.return_value = 1
        self.channel.queue_delete(self.mock_queue, if_empty=True)
        mock_broker = self.mock_broker_agent.return_value
        mock_broker.getQueue.assert_not_called()

    def test_calls_get_queue(self):
        self.channel.queue_delete(self.mock_queue)
        self.mock_broker_agent.getQueue.assert_called_once_with(
            self.mock_queue
        )

    def test_gets_queue_attribute(self):
        self.channel.queue_delete(self.mock_queue)
        self.mock_queue_obj.getAttributes.assert_called_once_with()

    def test_queue_in_use_and_if_unused_no_delete(self):
        queue_obj = self.mock_broker_agent.return_value.getQueue.return_value
        queue_obj.getAttributes.return_value = {'consumerCount': 1}
        self.channel.queue_delete(self.mock_queue, if_unused=True)
        self.assertFalse(self.mock_broker_agent.delQueue.called)

    def test_calls_delQueue_with_queue(self):
        self.channel.queue_delete(self.mock_queue)
        self.mock_broker_agent.delQueue.assert_called_once_with(
            self.mock_queue
        )


@case_no_pypy
class TestChannel(Case):

    def setUp(self):
        self.mock_connection = Mock()
        self.mock_broker_agent = Mock()
        self.mock_connection.get_broker_agent = Mock(
            return_value=self.mock_broker_agent,
        )
        self.mock_transport = Mock()
        self.mock_Message = Mock()
        self.my_channel = Channel(
            self.mock_connection, self.mock_transport,
        )
        self.my_channel.Message = self.mock_Message
        self.mock_qos = Mock()
        self.my_channel.qos = self.mock_qos

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
        """Test the size."""
        message_count = 5
        mock_queue = Mock()
        mock_queue_to_check = Mock()
        mock_queue_to_check.values = {'msgDepth': message_count}
        self.mock_broker_agent.getQueue.return_value = mock_queue_to_check
        result = self.my_channel._size(mock_queue)
        self.mock_broker_agent.getQueue.assert_called_with(mock_queue)
        self.assertEqual(message_count, result)

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
        self.mock_broker_agent.addQueue.side_effect = exception_to_raise
        self.mock_broker_agent.getQueue.return_value = mock_queue_data
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
        self.mock_broker_agent.addQueue.assert_called_with(
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
        self.mock_broker_agent.addQueue.return_value = None
        self.mock_broker_agent.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.startswith.assert_called_with('celeryev')
        self.mock_broker_agent.addQueue.assert_called_with(
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
        self.mock_broker_agent.addQueue.return_value = None
        self.mock_broker_agent.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.endswith.assert_called_with('pidbox')
        self.mock_broker_agent.addQueue.assert_called_with(
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
        self.mock_broker_agent.addQueue.return_value = None
        self.mock_broker_agent.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        mock_queue.startswith.assert_called_with('celeryev')
        mock_queue.endswith.assert_called_with('pidbox')
        self.mock_broker_agent.addQueue.assert_called_with(
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
        self.mock_broker_agent.addQueue.return_value = None
        self.mock_broker_agent.getQueue.return_value = mock_queue_data
        self.my_channel.queue_declare(mock_queue)
        self.mock_broker_agent.addQueue.assert_called_with(
            mock_queue,
            options=expected_default_options,
        )

    def test_queue_declare_raises_exception_not_silenced(self):
        unique_exception = Exception('This exception should not be silenced')
        mock_queue = Mock()
        self.mock_broker_agent.addQueue.side_effect = unique_exception
        with self.assertRaises(unique_exception.__class__):
            self.my_channel.queue_declare(mock_queue)
        self.mock_broker_agent.addQueue.assert_called_once_with(
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
        self.mock_broker_agent.addExchange.side_effect = Exception(
            'The foo object already exists.',
        )
        self.my_channel.exchange_declare()

    def test_exchange_declare_raises_exception_not_silenced(self):
        """Create Exchange where an exception is raised and not silenced."""
        unique_exception = Exception('This exception should not be silenced')
        self.mock_broker_agent.addExchange.side_effect = unique_exception
        with self.assertRaises(unique_exception.__class__):
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
        self.mock_broker_agent.addExchange.assert_called_with(
            mock_type, mock_exchange, options,
        )
        assert result is None

    def test_exchange_delete(self):
        """Test the deletion of an exchange by name."""
        mock_exchange = Mock()
        result = self.my_channel.exchange_delete(mock_exchange)
        self.mock_broker_agent.delExchange.assert_called_with(mock_exchange)
        self.assertIsNone(result)

    def test_queue_bind(self):
        """Test binding a queue to an exchange using a routing key."""
        mock_queue = Mock()
        mock_exchange = Mock()
        mock_routing_key = Mock()
        self.my_channel.queue_bind(
            mock_queue, mock_exchange, mock_routing_key,
        )
        self.mock_broker_agent.bind.assert_called_with(
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
        self.mock_broker_agent.unbind.assert_called_with(
            mock_exchange, mock_queue, mock_routing_key,
        )

    def test_queue_purge(self):
        """Test purging a queue by name."""
        mock_queue = Mock()
        mock_get_queue_result = Mock()
        mock_get_queue_result.values = {'msgDepth': 2}
        self.mock_broker_agent.getQueue.return_value = mock_get_queue_result
        result = self.my_channel.queue_purge(mock_queue)
        mock_get_queue_result.purge.assert_called_with(2)
        self.assertIs(2, result)

    def test_basic_ack(self):
        """Test that basic_ack calls the QoS object properly."""
        mock_delivery_tag = Mock()
        self.my_channel.basic_ack(mock_delivery_tag)
        self.mock_qos.ack.assert_called_with(mock_delivery_tag)

    def test_basic_reject(self):
        """Test that basic_reject calls the QoS object properly."""
        mock_delivery_tag = Mock()
        mock_requeue_value = Mock()
        self.my_channel.basic_reject(mock_delivery_tag, mock_requeue_value)
        self.mock_qos.reject.assert_called_with(
            mock_delivery_tag, requeue=mock_requeue_value,
        )

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
        self.assertIs(mock_body, result['body'])
        self.assertIs(mock_content_encoding, result['content_encoding'])
        self.assertIs(mock_content_type, result['content_type'])
        self.assertDictEqual(headers, result['properties']['headers'])
        self.assertDictContainsSubset(properties, result['properties'])
        assert (mock_priority is
                result['properties']['delivery_info']['priority'])

    @patch(QPID_MODULE + '.Channel.body_encoding')
    @patch(QPID_MODULE + '.Channel.encode_body')
    @patch(QPID_MODULE + '.proton')
    @patch(QPID_MODULE + '.threading.Event')
    def test_basic_publish(self, mock_threading_Event, mock_proton,
                           mock_encode_body, mock_body_encoding):
        """Test basic_publish()."""
        mock_original_body = Mock()
        mock_encoded_body = Mock()
        mock_message = {'body': mock_original_body,
                        'properties': {'delivery_info': {}}}
        mock_encode_body.return_value = (
            mock_encoded_body, mock_body_encoding,
        )
        mock_exchange = Mock()
        mock_routing_key = Mock()
        self.my_channel.basic_publish(
            mock_message, mock_exchange, mock_routing_key,
        )
        mock_encode_body.assert_called_once_with(
            mock_original_body, mock_body_encoding,
        )
        self.assertIs(mock_message['body'], mock_encoded_body)
        self.assertIs(
            mock_message['properties']['body_encoding'], mock_body_encoding,
        )
        self.assertIsInstance(
            mock_message['properties']['delivery_tag'], uuid.UUID,
        )
        self.assertIs(
            mock_message['properties']['delivery_info']['exchange'],
            mock_exchange,
        )
        self.assertIs(
            mock_message['properties']['delivery_info']['routing_key'],
            mock_routing_key,
        )
        self.assertIs(mock_proton.Message.call_count, 1)
        self.assertIs(
            self.mock_transport.main_thread_commands.put.call_count, 1
        )
        mock_threading_Event.return_value.wait.assert_called_once_with()

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
        mock_type = Mock()
        mock_attributes = {'type': mock_type}
        mock_qpid_exch = Mock()
        mock_qpid_exch.getAttributes.return_value = mock_attributes
        self.mock_broker_agent.getExchange.return_value = mock_qpid_exch
        result = self.my_channel.typeof(mock_exchange)
        assert mock_type is result

    def test_typeof_exchange_does_not_exist(self):
        """Test that typeof() finds an exchange that does not exists."""
        mock_exchange = Mock()
        mock_default = Mock()
        self.mock_broker_agent.getExchange.return_value = None
        result = self.my_channel.typeof(mock_exchange, default=mock_default)
        assert mock_default is result

@case_no_pypy
@disable_runtime_dependency_check
class TestTransportInit(Case):

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


@case_no_pypy
@disable_runtime_dependency_check
class TestTransportDrainEvents(Case):

    def setUp(self):
        self.mock_proton_event = Mock()

        self.transport = Transport(Mock())
        self.transport.recv_messages = Mock()
        self.transport.recv_messages.get.return_value = self.mock_proton_event

        self.mock_message = Mock()
        self.mock_conn = Mock()
        self.mock_callback = Mock()
        self.mock_conn._callbacks = {
            self.mock_proton_event.queue:
            self.mock_callback
        }

    def test_socket_timeout_raised_when_no_messages(self):
        self.transport.recv_messages.get.side_effect = Queue.Empty()
        with self.assertRaises(socket.timeout):
            self.transport.drain_events(Mock())

    def test_timeout_returns_no_earlier_then_asked_for(self):
        start_time = monotonic()
        with self.assertRaises(socket.timeout):
            self.transport.drain_events( self.mock_conn, timeout=1)
        elapsed_time_in_s = monotonic() - start_time

        # Assert the timeout is working
        self.assertGreaterEqual(elapsed_time_in_s, 1.0)

        # Assert the callback was called
        self.mock_callback.assert_called_with(self.mock_proton_event)


@case_no_pypy
@disable_runtime_dependency_check
class TestTransportCreateChannel(Case):

    @pytest.fixture(autouse=True)
    def setup_self(self, disable_runtime_dependency_check):
        # ^^ disable runtime MUST be called before this fixture
        self.transport = Transport(Mock())
        self.mock_conn = Mock()

        self.patch_Channel = patch(QPID_MODULE + '.Channel')
        self.mock_Channel = self.patch_Channel.start()

        self.returned_channel = self.transport.create_channel(self.mock_conn)

    def tearDown(self):
        self.patch_Channel.stop()

    def test_new_channel_created_from_connection(self):
        self.assertIs(self.mock_Channel.return_value, self.returned_channel)
        self.mock_Channel.assert_called_with(
            self.mock_conn, self.transport,
        )

    def test_new_channel_added_to_connection_channel_list(self):
        append_method = self.mock_conn.channels.append
        append_method.assert_called_with(self.mock_Channel.return_value)


@case_no_pypy
@disable_runtime_dependency_check
class TestTransportEstablishConnection(Case):

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

        self.patch_a = patch(QPID_MODULE + '.ReconnectDelays')
        self.mock_reconnect_delays = self.patch_a.start()

        self.patch_b = patch(QPID_MODULE + '.SASL_obj')
        self.mock_sasl_obj = self.patch_b.start()

        self.patch_c = patch(QPID_MODULE + '.ProtonThread')
        self.mock_proton_thread = self.patch_c.start()

    def tearDown(self):
        self.patch_a.stop()
        self.patch_b.stop()
        self.patch_c.stop()

    def test_transport_establish_conn_no_options(self):
        self.transport.establish_connection()
        {
            'url': 'amqp://localhost:5672',
            'reconnect_delays': self.mock_reconnect_delays.return_value,
        }

    def test_transport_establish_conn_with_username_password(self):
        self.client.userid = 'new-userid'
        self.client.password = 'new-password'
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            {
                'url': 'amqp://localhost:5672',
                'reconnect_delays': self.mock_reconnect_delays.return_value,
                'sasl': self.mock_sasl_obj.return_value
            }
        )
        self.assertIs(self.mock_sasl_obj.return_value.user,
                      self.client.userid)
        self.assertIs(self.mock_sasl_obj.return_value.password,
                      self.client.password)
        self.assertIs(self.mock_sasl_obj.return_value.mechs, 'PLAIN')

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
        self.assertIs(self.mock_sasl_obj.return_value.mechs,
                      self.client.login_method)

    def test_transport_overrides_sasl_mech_has_username(self):
        self.client.userid = 'new-userid'
        self.client.login_method = 'EXTERNAL'
        self.transport.establish_connection()
        self.assertIs(self.mock_sasl_obj.return_value.user,
                      self.client.userid)
        self.assertIs(self.mock_sasl_obj.return_value.mechs,
                      self.client.login_method)

    @patch(QPID_MODULE + '.proton')
    def test_transport_establish_conn_with_ssl_with_hostname_check(self,
                                                                   m_proton):
        self.client.ssl = {
            'keyfile': 'my_keyfile',
            'certfile': 'my_certfile',
            'ca_certs': 'my_cacerts',
            'cert_reqs': ssl.CERT_REQUIRED,
        }
        self.transport.establish_connection()
        creds = [self.client.ssl['certfile'], self.client.ssl['keyfile'],None]
        mock_ssl = m_proton.SSLDomain
        mock_ssl.return_value.set_credentials.assert_called_once_with(*creds)

        mock_ssl.return_value.set_trusted_ca_db.assert_called_once_with(
            self.client.ssl['ca_certs']
        )
        mock_ssl.return_value.set_peer_authentication.assert_called_once_with(
            mock_ssl.VERIFY_PEER_NAME
        )

    def test_transport_establish_conn_sets_client_on_connection_object(self):
        self.transport.establish_connection()
        assert self.mock_conn.return_value.client is self.client

    def test_transport_establish_conn_returns_new_connection_object(self):
        new_conn = self.transport.establish_connection()
        assert new_conn is self.mock_conn.return_value

    def test_transport_establish_conn_uses_hostname_if_not_default(self):
        self.client.hostname = 'some_other_hostname'
        self.transport.establish_connection()
        self.mock_conn.assert_called_once_with(
            {
                'url': 'amqp://some_other_hostname:5672',
                'reconnect_delays': self.mock_reconnect_delays.return_value,
            }
        )


@case_no_pypy
class TestTransportClassAttributes(Case):

    def test_verify_Connection_attribute(self):
        assert Connection is Transport.Connection

    def test_verify_polling_disabled(self):
        assert Transport.polling_interval is None

    def test_transport_verify_supports_asynchronous_events(self):
        assert Transport.supports_ev

    def test_verify_driver_type_and_name(self):
        assert Transport.driver_type == 'qpid'
        assert Transport.driver_name == 'qpid'

    def test_transport_verify_recoverable_connection_errors(self):
        connection_errors = Transport.recoverable_connection_errors
        self.assertIn(ConnectionException, connection_errors)
        self.assertIn(select.error, connection_errors)

    def test_transport_verify_recoverable_channel_errors(self):
        channel_errors = Transport.recoverable_channel_errors
        self.assertIs(channel_errors, ())

    def test_transport_verify_pre_kombu_3_0_exception_labels(self):
        assert (Transport.recoverable_channel_errors ==
                Transport.channel_errors)
        assert (Transport.recoverable_connection_errors ==
                Transport.connection_errors)


@case_no_pypy
@disable_runtime_dependency_check
class TestTransportRegisterWithEventLoop(Case):

    def test_transport_register_with_event_loop_calls_add_reader(self):
        transport = Transport(Mock())
        mock_connection = Mock()
        mock_loop = Mock()
        transport.register_with_event_loop(mock_connection, mock_loop)
        mock_loop.add_reader.assert_called_with(
            transport.r, transport.on_readable, mock_connection, mock_loop,
        )


@case_no_pypy
@disable_runtime_dependency_check
class TestTransportOnReadable(Case):

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


@case_no_pypy
@disable_runtime_dependency_check
class TestTransportVerifyRuntimeEnvironment(Case):

    @pytest.fixture(autouse=True)
    def setup_self(self, patching):
        self.verify_runtime_environment = Transport.verify_runtime_environment
        patching.object(Transport, 'verify_runtime_environment')
        self.transport = Transport(Mock())

    def tearDown(self):
        self.patch_a.stop()

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


@case_no_pypy
@disable_runtime_dependency_check
class TestTransport(Case):

    def setup(self):
        """Creates a mock_client to be used in testing."""
        self.mock_client = Mock()

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
        self.assertDictEqual(correct_params, result_params)

    @patch(QPID_MODULE + '.os.close')
    def test_del_sync(self, close):
        my_transport = Transport(self.mock_client)
        my_transport.__del__()
        self.assertTrue(close.called)

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
