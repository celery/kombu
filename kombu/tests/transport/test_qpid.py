from __future__ import absolute_import

import Queue
import socket
import threading
import time

from itertools import count

QPID_NOT_AVAILABLE = False
try:
    import qpid.messaging.exceptions
    import qpidtoollibs
except ImportError:
    QPID_NOT_AVAILABLE = True

import kombu.five
from kombu.transport.qpid import QpidMessagingExceptionHandler, QoS, Message
from kombu.transport.qpid import Channel, FDShimThread, FDShim
from kombu.transport.qpid import Connection, Transport
from kombu.transport.virtual import Base64
from kombu.utils.compat import OrderedDict
from kombu.tests.case import Case, Mock, SkipTest
from kombu.tests.case import mask_modules, patch, skip_if_not_module


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
        """Create two QoS objects for use by this test class.

        One with no prefetch_limit, and the other with a prefetch limit of
        2.
        """
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.qos_no_limit = QoS()
        self.qos_limit_2 = QoS(prefetch_count=2)
        self.delivery_tag_generator = count(1)

    def test_init_no_params(self):
        """Check that internal state is correct after initialization"""
        self.assertEqual(self.qos_no_limit.prefetch_count, 0)
        self.assertIsInstance(self.qos_no_limit._not_yet_acked, OrderedDict)

    def test_init_with_params(self):
        """Check that internal state is correct after initialization with
        prefetch_count.
        """
        self.assertEqual(self.qos_limit_2.prefetch_count, 2)

    def test_can_consume_no_limit(self):
        """can_consume shall always return True with no prefetch limits"""
        self.assertTrue(self.qos_no_limit.can_consume())
        self.add_n_messages_to_qos(3, self.qos_no_limit)
        self.assertTrue(self.qos_no_limit.can_consume())

    def test_can_consume_with_limit(self):
        """can_consume shall return False only when the QoS holds
        prefetch_count number of messages
        """
        self.assertTrue(self.qos_limit_2.can_consume())
        self.add_n_messages_to_qos(2, self.qos_limit_2)
        self.assertFalse(self.qos_limit_2.can_consume())

    def test_can_consume_max_estimate_no_limit(self):
        """can_consume_max_estimate shall always return 1 with no prefetch
        limits
        """
        self.assertEqual(self.qos_no_limit.can_consume_max_estimate(), 1)
        self.add_n_messages_to_qos(3, self.qos_no_limit)
        self.assertEqual(self.qos_no_limit.can_consume_max_estimate(), 1)

    def test_can_consume_max_estimate_with_limit(self):
        """while prefetch limits are enabled, can_consume_max_estimate shall
        return (prefetch_limit - #messages) as the number of messages is
        incremented from 0 to prefetch_limit
        """
        self.assertEqual(self.qos_limit_2.can_consume_max_estimate(), 2)
        self.add_message_to_qos(self.qos_limit_2)
        self.assertEqual(self.qos_limit_2.can_consume_max_estimate(), 1)
        self.add_message_to_qos(self.qos_limit_2)
        self.assertEqual(self.qos_limit_2.can_consume_max_estimate(), 0)

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
        self.assertIs(m1, message1)
        self.assertIs(m2, message2)

    def test_ack(self):
        """Load a mock message, ack the message, and ensure the right
        call is made to the acknowledge method in the qpid.messaging client
        library
        """
        message = Mock()
        qos = self.qos_no_limit
        qos.append(message, 1)
        qos.ack(1)
        message._receiver.session.acknowledge.assert_called_with(
            message=message)

    @patch('qpid.messaging.Disposition')
    @patch('qpid.messaging.RELEASED')
    def test_ack_requeue_true(self, mock_RELEASED, mock_QpidDisposition):
        """Load a mock message, reject the message with requeue=True,
        and ensure the right call to acknowledge is made.
        """
        message = Mock()
        mock_QpidDisposition.return_value = 'disposition'
        qos = self.qos_no_limit
        qos.append(message, 1)
        qos.reject(1, requeue=True)
        mock_QpidDisposition.assert_called_with(mock_RELEASED)
        message._receiver.session.acknowledge.assert_called_with(
            message=message, disposition='disposition')

    @patch('qpid.messaging.Disposition')
    @patch('qpid.messaging.REJECTED')
    def test_ack_requeue_false(self, mock_REJECTED, mock_QpidDisposition):
        """Load a mock message, reject the message with requeue=False,
        and ensure the right call to acknowledge is made.
        """
        message = Mock()
        mock_QpidDisposition.return_value = 'disposition'
        qos = self.qos_no_limit
        qos.append(message, 1)
        qos.reject(1, requeue=False)
        mock_QpidDisposition.assert_called_with(mock_REJECTED)
        message._receiver.session.acknowledge.assert_called_with(
            message=message, disposition='disposition')


class TestFDShimThread(Case):

    def setUp(self):
        """Create a mock FDShimThread object and associated objects."""
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_create_qpid_connection = Mock()
        self.mock_session = Mock()
        self.mock_create_qpid_connection().session = Mock(
            return_value=self.mock_session)
        self.mock_receiver = Mock()
        self.mock_session.receiver = Mock(return_value=self.mock_receiver)
        self.mock_queue = Mock()
        self.mock_delivery_queue = Mock()
        self.my_thread = FDShimThread(self.mock_create_qpid_connection,
                                      self.mock_queue,
                                      self.mock_delivery_queue)

    def test_init_variables(self):
        """Test that all simple init params are internally stored
        correctly
        """
        self.assertIs(self.my_thread._queue, self.mock_queue)
        self.assertIs(self.my_thread._delivery_queue,
                      self.mock_delivery_queue)
        self.assertFalse(self.my_thread._is_killed)

    def test_session_gets_made(self):
        """Test that a session is created"""
        self.mock_create_qpid_connection().session.assert_called_with()

    def test_session_gets_stored(self):
        """Test that a session is saved properly after being created"""
        self.assertIs(self.mock_session, self.my_thread._session)

    def test_receiver_gets_made(self):
        """Test that a receiver is created listening on the queue"""
        self.mock_session.receiver.assert_called_with(self.mock_queue)

    def test_receiver_gets_stored(self):
        """Test that a receiver is saved properly after being created"""
        self.assertIs(self.mock_receiver, self.my_thread._receiver)

    def test_kill(self):
        """Ensure that a call to the thread main method run() will exit
        properly when kill() has been called.
        """
        self.my_thread.kill()
        self.my_thread.run()

    def test_receiver_cleanup(self):
        """Ensure that when a thread main method exits, that close() is
        called on the receiver.
        """
        self.my_thread.kill()
        self.my_thread.run()
        self.mock_receiver.close.assert_called_with()

    def test_session_cleanup(self):
        """Ensure that when a thread main method exits, that close() is
        called on the session.
        """
        self.my_thread.kill()
        self.my_thread.run()
        self.mock_session.close.assert_called_with()

    def test_call_to_fetch_raise_empty(self):
        """Ensure the call to fetch() occurs, and with the proper timeout.
        Raises an Empty exception.
        """
        QpidEmpty = qpid.messaging.exceptions.Empty
        exception_causing_exit = Exception('Exit run() method')
        self.mock_receiver.fetch = Mock(side_effect=[QpidEmpty(),
                                                     exception_causing_exit])
        try:
            self.my_thread.run()
        except Exception as err:
            self.assertEqual(exception_causing_exit, err)
        self.mock_receiver.fetch.assert_called_with(
            timeout=FDShimThread.block_timeout)

    def test_call_to_fetch_return_message(self):
        """Ensure the call to fetch() occurs, that the response bundle is
        built correctly is called as the only argument to receiver.put()
        """
        mock_response = Mock()
        mock_source = Mock()
        mock_put = Mock()
        exception_causing_exit = Exception('Exit run() method')
        response_bundle = (mock_source, mock_response)
        self.mock_receiver.fetch = Mock(return_value=mock_response)
        self.mock_receiver.source = mock_source
        self.mock_delivery_queue.put.side_effect = exception_causing_exit
        try:
            self.my_thread.run()
        except Exception as err:
            self.assertEqual(exception_causing_exit, err)
        self.mock_receiver.fetch.assert_called_with(
            timeout=FDShimThread.block_timeout)
        self.mock_delivery_queue.put.assert_called_with(response_bundle)


class TestFDShim(Case):

    def setUp(self):
        """Create a test shim to use """
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_queue_from_fdshim = Mock()
        self.mock_delivery_queue = Mock()
        self.my_fdshim = FDShim(self.mock_queue_from_fdshim,
                               self.mock_delivery_queue)

    def test_init_variables(self):
        """Test that all simple init params are internally stored
        correctly
        """
        self.assertIs(self.my_fdshim.queue_from_fdshim,
                      self.mock_queue_from_fdshim)
        self.assertIs(self.my_fdshim.delivery_queue,
                      self.mock_delivery_queue)
        self.assertFalse(self.my_fdshim._is_killed)

    def test_kill(self):
        """Ensure that a call to the thread main method run() will exit
        properly when kill() has been called.
        """
        self.my_fdshim.kill()
        self.my_fdshim.monitor_consumers()

    def test_call_to_get_raise_empty(self):
        """Ensure the call to delivery_queue.get() occurs, and with
        block=True.  Raises a Queue.Empty exception.
        """
        exception_causing_exit = Exception('Exit monitor_consumers() method')
        self.mock_delivery_queue.get = Mock(
            side_effect=[Queue.Empty(), exception_causing_exit])
        try:
            self.my_fdshim.monitor_consumers()
        except Exception as err:
            self.assertEqual(exception_causing_exit, err)
        self.mock_delivery_queue.get.assert_called_with(block=True)

    @patch('os.write')
    def test_call_to_get_return_response_bundle(self, write_method):
        """Ensure the call to get() occurs, and when the response bundle
        is received from delivery_queue, that the response bundle is put
        into queue_from_fdshim using put().

        This method patches os.write to ensure that a '0' is written to the
        _w file descriptor attribute of FDShim.
        """
        response_bundle = Mock()
        exception_causing_exit = Exception('Exit monitor_consumers() method')
        self.mock_delivery_queue.get = Mock(return_value=response_bundle)
        write_method.side_effect = exception_causing_exit
        try:
            self.my_fdshim.monitor_consumers()
        except Exception as err:
            self.assertEqual(exception_causing_exit, err)
        self.mock_delivery_queue.get.assert_called_with(block=True)
        self.mock_queue_from_fdshim.put.assert_called_with(
            response_bundle)
        write_method.assert_called_with(self.my_fdshim._w, '0')


class TestConnection(Case):

    def setUp(self):
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
        self.my_connection = Connection(**self.connection_options)

    def test_init_variables(self):
        """Test that all simple init params are internally stored
        correctly
        """
        self.assertDictEqual(self.connection_options,
                             self.my_connection.connection_options)
        self.assertIsInstance(self.my_connection.channels, list)
        self.assertIsInstance(self.my_connection._callbacks, dict)

    def test_verify_connection_class_attributes(self):
        """Verify that Channel class attribute is set correctly"""
        self.assertEqual(Channel, Connection.Channel)

    @patch('qpid.messaging.Connection')
    def test_create_qpid_connection(self, QpidConnection):
        """Test that create_qpid_connection calls establish with the
        connection_options, and then returns the result.
        """
        new_connection = 'connection'
        QpidConnection.establish = Mock(return_value=new_connection)
        conn_from_func = self.my_connection.create_qpid_connection()
        QpidConnection.establish.assert_called_with(
            **self.connection_options)
        self.assertEqual(new_connection, conn_from_func)

    def test_close_channel_exists(self):
        """Test that calling close_channel() with a valid channel removes
        the channel from self.channels and sets channel.connection to None.
        """
        mock_channel = Mock()
        self.my_connection.channels = [mock_channel]
        mock_channel.connection = True
        self.my_connection.close_channel(mock_channel)
        self.assertEqual(self.my_connection.channels, [])
        self.assertIsNone(mock_channel.connection)


    def test_close_channel_does_not_exist(self):
        """Test that calling close_channel() with an invalid channel does
        not raise a ValueError and sets channel.connection to None.
        """
        self.my_connection.channels = Mock()
        self.my_connection.channels.remove = Mock(side_effect=ValueError())
        mock_channel = Mock()
        mock_channel.connection = True
        self.my_connection.close_channel(mock_channel)
        self.assertIsNone(mock_channel.connection)


class TestChannel(Case):

    @skip_if_not_module('qpidtoollibs')
    @patch('qpidtoollibs.BrokerAgent')
    def setUp(self, mock_BrokerAgent):
        """Set up objects for use in testing a Channel.

        Create a mock Channel, and all associated mock objects."""
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_connection = Mock()
        self.mock_qpid_connection = Mock()
        self.mock_qpid_session = Mock()
        self.mock_qpid_connection.session = \
            Mock(return_value=self.mock_qpid_session)
        self.mock_connection.create_qpid_connection = \
            Mock(return_value=self.mock_qpid_connection)
        self.mock_transport = Mock()
        self.mock_delivery_queue = Mock()
        self.mock_broker = Mock()
        self.mock_Message = Mock()
        self.mock_BrokerAgent = mock_BrokerAgent
        mock_BrokerAgent.return_value = self.mock_broker
        self.my_channel = Channel(self.mock_connection,
                                  self.mock_transport,
                                  self.mock_delivery_queue)
        self.my_channel.Message = self.mock_Message

    def test_verify_QoS_class_attribute(self):
        """Verify that the class attribute QoS refers to the QoS object"""
        self.assertIs(QoS, Channel.QoS)

    def test_verify_Message_class_attribute(self):
        """Verify that the class attribute Message refers to the Message
        object
        """
        self.assertIs(Message, Channel.Message)

    def test_body_encoding_class_attribute(self):
        """Verify that the class attribute body_encoding is set to base64"""
        self.assertEqual('base64', Channel.body_encoding)

    def test_codecs_class_attribute(self):
        """Verify that the codecs class attribute has a correct key and
        value
        """
        self.assertIsInstance(Channel.codecs, dict)
        self.assertIn('base64', Channel.codecs)
        self.assertIsInstance(Channel.codecs['base64'], Base64)

    def test_delivery_tags(self):
        """Test that _delivery_tags is using itertools"""
        self.assertIsInstance(Channel._delivery_tags, count)

    def test_init_variables(self):
        """Test init variables"""
        self.assertIs(self.mock_connection, self.my_channel.connection)
        self.assertIs(self.mock_transport, self.my_channel.transport)
        self.assertIs(self.mock_delivery_queue,
                      self.my_channel.delivery_queue)
        self.assertIsInstance(self.my_channel._tag_to_queue, dict)
        self.assertIsInstance(self.my_channel._consumer_threads, dict)
        self.assertIs(self.mock_qpid_session, self.my_channel._qpid_session)
        self.assertIs(self.mock_broker, self.my_channel._broker)
        self.assertIsNone(self.my_channel._qos)
        self.assertIsInstance(self.my_channel._consumers, set)
        self.assertFalse(self.my_channel.closed)
        self.mock_connection.create_qpid_connection.assert_called_with()
        self.mock_qpid_connection.session.assert_called_with()
        self.mock_BrokerAgent.assert_called_with(self.mock_qpid_connection)

    def test_get(self):
        """Test _get() for return value from call to receiver, and check
        that receiver has closed() called on it.
        """
        mock_queue = Mock()
        mock_rx = Mock()
        mock_message = Mock()
        mock_rx.fetch.return_value = mock_message
        self.mock_qpid_session.receiver.return_value = mock_rx
        result = self.my_channel._get(mock_queue)
        self.mock_qpid_session.receiver.assert_called_with(mock_queue)
        mock_rx.close.assert_called_with()
        self.assertIs(mock_message, result)

    @patch('qpid.messaging.Message')
    def test_put_queue(self, mock_qpid_Message_obj):
        """Test putting a messages directly into a queue."""
        mock_routing_key = 'routingkey'
        mock_message = Mock()
        mock_sender = Mock()
        mock_new_qpid_message_obj = Mock()
        self.mock_qpid_session.sender.return_value = mock_sender
        mock_qpid_Message_obj.return_value = mock_new_qpid_message_obj
        self.my_channel._put(mock_routing_key, mock_message)
        address_string = '%s; {assert: always, node: {type: queue}}' % \
                         mock_routing_key
        self.mock_qpid_session.sender.assert_called_with(address_string)
        mock_qpid_Message_obj.assert_called_with(content=mock_message,
                                                 subject=None)
        mock_sender.send.assert_called_with(mock_new_qpid_message_obj,
                                            sync=True)
        mock_sender.close.assert_called_with()

    @patch('qpid.messaging.Message')
    def test_put_exchange(self, mock_qpid_Message_obj):
        """Test putting a message directly into an exchange."""
        mock_routing_key = 'routingkey'
        mock_exchange_name = 'myexchange'
        mock_message = Mock()
        mock_sender = Mock()
        mock_new_qpid_message_obj = Mock()
        self.mock_qpid_session.sender.return_value = mock_sender
        mock_qpid_Message_obj.return_value = mock_new_qpid_message_obj
        self.my_channel._put(mock_routing_key, mock_message,
                             mock_exchange_name)
        address_string = '%s/%s; {assert: always, node: {type: topic}}' % \
                         (mock_exchange_name, mock_routing_key)
        self.mock_qpid_session.sender.assert_called_with(address_string)
        mock_qpid_Message_obj.assert_called_with(content=mock_message,
                                                 subject=mock_routing_key)
        mock_sender.send.assert_called_with(mock_new_qpid_message_obj,
                                            sync=True)
        mock_sender.close.assert_called_with()

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
        self.assertIsNone(result)

    def test_new_queue_raises_exception_and_silenced(self):
        """Test new queue, where an exception is raised and then silenced"""
        mock_queue = Mock()
        exception_to_raise = Exception('The foo object already exists.')
        self.mock_broker.addQueue.side_effect = exception_to_raise
        result = self.my_channel._new_queue(mock_queue)
        self.mock_broker.addQueue.assert_called_with(mock_queue)
        self.assertIsNone(result)

    def test_new_queue_raises_exception_not_silenced(self):
        """Test new queue, where an exception is raised and not silenced"""
        unique_exception = Exception('This exception should not be silenced')
        mock_queue = Mock()
        self.mock_broker.addQueue.side_effect = unique_exception
        self.assertRaises(unique_exception.__class__,
                          self.my_channel._new_queue,
                          mock_queue)
        self.mock_broker.addQueue.assert_called_with(mock_queue)

    def test_new_queue_no_exception(self):
        """Test creation of a new queue, where an exception is NOT raised"""
        mock_queue = Mock()
        result = self.my_channel._new_queue(mock_queue)
        self.mock_broker.addQueue.assert_called_with(mock_queue)
        self.assertIsNone(result)

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
    def test_queue_declare(self, mock_queue_declare_ok_t):
        """Test the declaration of a queue, and the return value"""
        mock_queue = Mock()
        message_count = 5
        self.my_channel._new_queue = Mock()
        self.my_channel._size = Mock(return_value=message_count)
        mock_queue_declare_ok_t.return_value = Mock()
        result = self.my_channel.queue_declare(mock_queue)
        self.my_channel._new_queue.assert_called_with(mock_queue)
        self.my_channel._size.assert_called_with(mock_queue)
        mock_queue_declare_ok_t.assert_called_with(mock_queue,
                                                   message_count, 0)
        self.assertIs(mock_queue_declare_ok_t.return_value, result)

    def test_queue_delete_if_empty_param(self):
        """Test the deletion of a queue with if_empty=True"""
        mock_queue = Mock()
        self.my_channel._has_queue = Mock(return_value=True)
        self.my_channel._size = Mock(return_value=5)
        result = self.my_channel.queue_delete(mock_queue, if_empty=True)
        self.my_channel._has_queue.assert_called_with(mock_queue)
        self.my_channel._size.assert_called_with(mock_queue)
        self.assertIsNone(result)

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
        self.assertIsNone(result)

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
        self.assertIsNone(result)

    def test_exchange_declare_raises_exception_and_silenced(self):
        """Create exchange where an exception is raised and then silenced"""
        mock_exchange = Mock()
        self.mock_broker.addExchange.side_effect = \
            Exception('The foo object already exists.')
        self.my_channel.exchange_declare()

    def test_exchange_declare_raises_exception_not_silenced(self):
        """Create Exchange where an exception is raised and not silenced"""
        mock_exchange = Mock()
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
        self.assertIsNone(result)

    def test_exchange_delete(self):
        """Test the deletion of an exchange by name"""
        mock_exchange = Mock()
        result = self.my_channel.exchange_delete(mock_exchange)
        self.mock_broker.delExchange.assert_called_with(mock_exchange)
        self.assertIsNone(result)

    def test_after_reply_message_received_exception_and_silenced(self):
        """Call after_reply_message_received exception raised and silenced"""
        mock_queue = Mock()
        normal_exception = Exception('The queue in use is foo')
        self.my_channel._delete = Mock(side_effect=normal_exception)
        self.my_channel.after_reply_message_received(mock_queue)

    def test_after_reply_message_received_exception_not_silenced(self):
        """Call after_reply_message_received exception raised not silenced"""
        mock_queue = Mock()
        abnormal_exception = Exception('This exception should pass through')
        self.my_channel._delete = Mock(side_effect=abnormal_exception)
        self.assertRaises(abnormal_exception.__class__,
                          self.my_channel.after_reply_message_received,
                          mock_queue)

    def test_after_reply_message_received(self):
        """Call after_reply_message_received"""
        mock_queue = Mock()
        self.my_channel._delete = Mock()
        self.my_channel.after_reply_message_received(mock_queue)
        self.my_channel._delete.assert_called_with(mock_queue)

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
        self.assertIs(purge_result, result)

    def test_basic_get_happy_path(self):
        """Test a basic_get with the most common case"""
        mock_queue = Mock()
        mock_qpid_message = Mock()
        new_message = Mock()
        self.my_channel._get = Mock(return_value=mock_qpid_message)
        self.mock_Message.return_value = new_message
        result = self.my_channel.basic_get(mock_queue)
        self.mock_qpid_session.acknowledge.assert_called_with(
            message=mock_qpid_message)
        self.assertIs(new_message, result)

    def test_basic_get_no_ack_equals_True(self):
        """Test a basic_get where no_ack equals True"""
        mock_queue = Mock()
        mock_qpid_message = Mock()
        new_message = Mock()
        self.my_channel._get = Mock(return_value=mock_qpid_message)
        self.mock_Message.return_value = new_message
        result = self.my_channel.basic_get(mock_queue, no_ack=True)
        self.assertEqual(self.mock_qpid_session.acknowledge.call_count, 0)
        self.assertIs(new_message, result)

    def test_basic_get_raises_Empty(self):
        """Test a basic_get where _get() raises an Empty exception"""
        mock_queue = Mock()
        self.my_channel._get = Mock(side_effect=kombu.five.Empty)
        result = self.my_channel.basic_get(mock_queue)
        self.assertEqual(self.my_channel.Message.call_count, 0)
        self.assertIsNone(result)

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

    @patch('kombu.transport.qpid.FDShimThread')
    def test_basic_consume(self, mock_FDShimThread):
        """Test a basic_consume() to fetch a message."""
        mock_queue = Mock()
        mock_no_ack = False
        mock_callback = Mock()
        mock_consumer_tag = Mock()
        self.mock_connection._callbacks = {}
        mock_my_thread = Mock()
        mock_FDShimThread.return_value = mock_my_thread
        self.my_channel.basic_consume(mock_queue, mock_no_ack,
                                      mock_callback, mock_consumer_tag)
        self.assertIn(mock_consumer_tag, self.my_channel._tag_to_queue)
        queue_reference = self.my_channel._tag_to_queue[mock_consumer_tag]
        self.assertIs(queue_reference, mock_queue)
        self.assertIn(mock_queue, self.mock_connection._callbacks)
        _callback_reference = self.mock_connection._callbacks[mock_queue]
        if not hasattr(_callback_reference, '__call__'):
            self.fail('Callback stored must be callable')
        self.basic_consume_callback_helper(_callback_reference,
                                           mock_callback)
        self.assertIn(mock_consumer_tag, self.my_channel._consumers)
        mock_FDShimThread.assert_called_with(
            self.mock_connection.create_qpid_connection,
            mock_queue,
            self.mock_delivery_queue)
        self.assertIn(mock_queue, self.my_channel._consumer_threads)
        my_thread_reference = self.my_channel._consumer_threads[mock_queue]
        self.assertIs(mock_my_thread, my_thread_reference)
        self.assertTrue(mock_my_thread.daemon)
        mock_my_thread.start.assert_called_with()

    @patch('kombu.transport.qpid.Channel.qos')
    def basic_consume_callback_helper(self, new_callback,
                                      original_callback,
                                      mock_qos):
        """Test the dynamically built callback inside basic_consume()"""
        mock_qpid_message = Mock()
        mock_raw_message = Mock()
        new_mock_message = Mock()
        new_mock_delivery_tag = Mock()
        original_callback_result = Mock()
        original_callback.return_value = original_callback_result
        new_mock_message.delivery_tag = new_mock_delivery_tag
        mock_qpid_message.content = mock_raw_message
        self.mock_Message.return_value = new_mock_message
        result = new_callback(mock_qpid_message)
        self.mock_Message.assert_called_with(self.my_channel,
                                             mock_raw_message)
        mock_qos.append.assert_called_with(mock_qpid_message,
                                           new_mock_delivery_tag)
        original_callback.assert_called_with(new_mock_message)
        self.assertIs(original_callback_result, result)

    def test_basic_cancel(self):
        """Test basic_cancel() by consumer tag"""
        mock_consumer_tag = Mock()
        mock_queue = Mock()
        mock_consumer_thread = Mock()
        self.my_channel._consumers = set([mock_consumer_tag])
        self.my_channel._tag_to_queue = Mock()
        self.my_channel._tag_to_queue.pop.return_value = mock_queue
        self.my_channel._consumer_threads = Mock()
        self.my_channel._consumer_threads.pop.return_value = \
            mock_consumer_thread
        self.my_channel.basic_cancel(mock_consumer_tag)
        self.assertNotIn(mock_consumer_tag, self.my_channel._consumers)
        self.my_channel._tag_to_queue.pop.assert_called_with(
            mock_consumer_tag, None)
        self.my_channel._consumer_threads.pop.assert_called_with(
            mock_queue, None)
        mock_consumer_thread.kill.assert_called_once()
        self.mock_connection._callbacks.pop.assert_called_once(mock_queue,
                                                               None)

    @patch('kombu.transport.qpid.Channel.basic_cancel')
    def test_close(self, mock_basic_cancel):
        """Test close() on Channel"""
        mock_consumer1 = Mock()
        mock_consumer2 = Mock()
        self.my_channel._consumers = set([mock_consumer1, mock_consumer2])
        self.my_channel.closed = False
        self.my_channel.close()
        self.assertTrue(self.my_channel.closed)
        mock_basic_cancel.assert_any_call(mock_consumer2)
        mock_basic_cancel.assert_any_call(mock_consumer1)
        self.mock_connection.close_channel.assert_called_once(
            self.my_channel)
        self.mock_qpid_session.close.assert_called_once()
        self.mock_broker.close.assert_called_once()

    def test_qos_manager_is_none(self):
        """Test the qos property if the QoS object did not already exist"""
        self.my_channel._qos = None
        result = self.my_channel.qos
        self.assertIsInstance(result, QoS)
        self.assertEqual(result, self.my_channel._qos)

    def test_qos_manager_already_exists(self):
        """Test the qos property if the QoS object already exists"""
        mock_existing_qos = Mock()
        self.my_channel._qos = mock_existing_qos
        result = self.my_channel.qos
        self.assertIs(mock_existing_qos, result)

    @patch('kombu.transport.qpid.Channel.qos')
    def test_basic_qos(self, mock_qos):
        """Verify the basic_qos() sets prefetch_count on the QoS object"""
        mock_prefetch_count = Mock()
        self.my_channel.basic_qos(mock_prefetch_count)
        self.assertIs(mock_prefetch_count, mock_qos.prefetch_count)

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
        self.assertIs(mock_body, result['body'])
        self.assertIs(mock_content_encoding, result['content-encoding'])
        self.assertIs(mock_content_type, result['content-type'])
        self.assertDictEqual(headers, result['headers'])
        self.assertDictContainsSubset(properties, result['properties'])
        self.assertIs(mock_priority,
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
                        'properties': {'delivery_info': {}}
                       }
        mock_encode_body.return_value = (mock_encoded_body,
                                         mock_body_encoding)
        mock_exchange = Mock()
        mock_routing_key = Mock()
        mock_encoded_buffered_body = Mock()
        mock_buffer.return_value = mock_encoded_buffered_body
        result = self.my_channel.basic_publish(mock_message,
                                               mock_exchange,
                                               mock_routing_key)
        mock_encode_body.assert_called_once(mock_original_body,
                                            mock_body_encoding)
        mock_buffer.assert_called_once(mock_encode_body)
        self.assertIs(mock_message['body'], mock_encoded_buffered_body)
        self.assertIs(mock_message['properties']['body_encoding'],
                      mock_body_encoding)
        self.assertIsInstance(mock_message['properties']['delivery_tag'],
                              int)
        self.assertIs(mock_message['properties']['delivery_info'][
                          'exchange'], mock_exchange)
        self.assertIs(
            mock_message['properties']['delivery_info']['routing_key'],
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
        mock_attributes['type'] =  mock_type
        mock_qpid_exchange.getAttributes.return_value = mock_attributes
        self.mock_broker.getExchange.return_value = mock_qpid_exchange
        result = self.my_channel.typeof(mock_exchange)
        self.assertIs(mock_type, result)

    def test_typeof_exchange_does_not_exist(self):
        """Test that typeof() finds an exchange that does not exists"""
        mock_exchange = Mock()
        mock_default = Mock()
        self.mock_broker.getExchange.return_value = None
        result = self.my_channel.typeof(mock_exchange, default=mock_default)
        self.assertIs(mock_default, result)


class TestTransport(Case):

    def setUp(self):
        """Creates a mock_client to be used in testing."""
        if QPID_NOT_AVAILABLE:
            raise SkipTest('qpid.messaging not installed')
        self.mock_client = Mock()

    @patch('kombu.transport.qpid.FDShim')
    @patch('threading.Thread')
    def test_init_variables(self, mock_thread, mock_FDShim):
        """Test that all simple init params are internally stored
        correctly
        """
        new_fdshim = Mock()
        mock_FDShim.return_value = new_fdshim
        new_thread = Mock()
        mock_thread.return_value = new_thread
        my_transport = Transport(self.mock_client)
        self.assertIs(my_transport.client, self.mock_client)
        self.assertIsInstance(my_transport.queue_from_fdshim, Queue.Queue)
        self.assertIsInstance(my_transport.delivery_queue, Queue.Queue)
        mock_FDShim.assert_called_with(my_transport.queue_from_fdshim,
                                       my_transport.delivery_queue)
        self.assertIs(new_fdshim, my_transport.fd_shim)
        mock_thread.assert_called_with(
            target=my_transport.fd_shim.monitor_consumers)
        self.assertTrue(new_thread.daemon)
        new_thread.start.assert_called_with()

    def test_verify_Connection_attribute(self):
        """Verify that class attribute Connection refers to the connection
        object
        """
        self.assertIs(Connection, Transport.Connection)

    def test_verify_default_port(self):
        """Verify that the class attribute default_port refers to the 5672
        properly
        """
        self.assertEqual(5672, Transport.default_port)

    def test_verify_polling_disabled(self):
        """Verify that polling is disabled"""
        self.assertIsNone(Transport.polling_interval)

    def test_verify_supports_asynchronous_events(self):
        """Verify that the Transport advertises that it supports
        an asynchronous event model
        """
        self.assertTrue(Transport.supports_ev)

    def test_verify_driver_type_and_name(self):
        """Verify that the driver and type are correctly labeled on the
        class
        """
        self.assertEqual('qpid', Transport.driver_type)
        self.assertEqual('qpid', Transport.driver_name)

    def test_register_with_event_loop(self):
        """Test that the file descriptor to monitor, and the on_readable
        callbacks properly register with the event loop
        """
        my_transport = Transport(self.mock_client)
        mock_connection = Mock()
        mock_loop = Mock()
        my_transport.register_with_event_loop(mock_connection, mock_loop)
        mock_loop.add_reader.assert_called_with(my_transport.fd_shim.r,
                                                my_transport.on_readable,
                                                mock_connection, mock_loop)

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
        self.assertIs(new_connection.client, self.mock_client)

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

    def test_drain_events_get_raises_empty_no_timeout(self):
        """Test drain_events() to ensure that a socket.timeout is raised
        once the get() method on queue_from_fdshim raises a Queue.Empty.
        """
        my_transport = Transport(self.mock_client)
        my_transport.queue_from_fdshim = Mock()
        my_transport.queue_from_fdshim.get = Mock(side_effect=Queue.Empty())
        mock_connection = Mock()
        self.assertRaises(socket.timeout, my_transport.drain_events,
                          mock_connection)

    def test_drain_events_and_raise_timeout(self):
        """Test drain_events() drains properly and also exits after the
        timeout is reached even if the queue isn't empty.
        """
        my_transport = Transport(self.mock_client)
        my_transport.queue_from_fdshim = Mock()
        mock_queue = Mock()
        mock_message = Mock()
        def sleep_and_return_message(block, timeout):
            time.sleep(0.5)
            return (mock_queue, mock_message)
        my_transport.queue_from_fdshim.get = sleep_and_return_message
        mock_connection = Mock()
        mock_callback = Mock()
        mock_connection._callbacks = {mock_queue: mock_callback}
        self.assertRaises(socket.timeout, my_transport.drain_events,
                          mock_connection, timeout=2)
        mock_callback.assert_called_with(mock_message)

    def test_create_channel(self):
        """Test that a Channel is created, and appended to the list of
        channels the connection maintains.
        """
        my_transport = Transport(self.mock_client)
        mock_connection = Mock()
        mock_new_channel = Mock()
        mock_connection.Channel.return_value = mock_new_channel
        returned_channel = my_transport.create_channel(mock_connection)
        self.assertIs(mock_new_channel, returned_channel)
        mock_connection.Channel.assert_called_with(
            mock_connection,
            my_transport,
            my_transport.delivery_queue)
        mock_connection.channels.append.assert_called_with(mock_new_channel)

    @patch('os.read')
    def test_on_readable(self, mock_os_read):
        """Test on_readable() reads an available message."""
        my_transport = Transport(self.mock_client)
        mock_drain_events = Mock(side_effect=socket.timeout())
        my_transport.drain_events = mock_drain_events
        mock_connection = Mock()
        mock_loop = Mock()
        mock_os_read.return_value = '0'
        result = my_transport.on_readable(mock_connection, mock_loop)
        mock_os_read.assert_called_with(my_transport.fd_shim.r, 1)
        mock_drain_events.assert_called_with(mock_connection)
        self.assertIsNone(result)

    def test_default_connection_params(self):
        """Test that the default_connection_params are correct"""
        correct_params =  {'userid': 'guest', 'password': 'guest',
                           'port': 5672, 'virtual_host': '',
                           'hostname': 'localhost',
                           'sasl_mechanisms': 'PLAIN'}
        my_transport = Transport(self.mock_client)
        result_params = my_transport.default_connection_params
        self.assertDictEqual(correct_params, result_params)