from __future__ import absolute_import

import threading
import Queue
import socket
import time

from itertools import count


from kombu.transport.qpid import QpidMessagingExceptionHandler, Base64, \
    QoS, Message, Channel, FDShimThread, FDShim, Connection, Transport

import qpid.messaging.exceptions

from kombu.utils.compat import OrderedDict

from kombu.tests.case import Case, Mock, SkipTest, mask_modules, patch


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
        self.assertIs(m1, message1)
        self.assertIs(m2, message2)

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

    @patch('qpid.messaging.Disposition')
    @patch('qpid.messaging.RELEASED')
    def test_ack_requeue_true(self, mock_RELEASED, mock_QpidDisposition):
        """Load a mock message, reject the message with requeue=True,
        and ensure the right call to acknowledge is made"""
        message = Mock()
        mock_QpidDisposition.return_value='disposition'
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
        and ensure the right call to acknowledge is made"""
        message = Mock()
        mock_QpidDisposition.return_value='disposition'
        qos = self.qos_no_limit
        qos.append(message, 1)
        qos.reject(1, requeue=False)
        mock_QpidDisposition.assert_called_with(mock_REJECTED)
        message._receiver.session.acknowledge.assert_called_with(
            message=message, disposition='disposition')


class test_FDShimThread(Case):

    def setUp(self):
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
        self.my_thread.daemon = True

    def test_init_variables(self):
        """Test that all simple init params are internally stored
        correctly"""
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
        """Start a thread, and then kill it using the kill() method. Ensure
        that it exits properly within the expected amount of time."""
        self.my_thread.start()
        self.my_thread.kill()
        self.my_thread.join(timeout=FDShimThread.block_timeout)
        self.assertFalse(self.my_thread.is_alive())

    def test_receiver_cleanup(self):
        """Ensure that when a thread exits normally after a call to kill()
        that close() is called on the receiver.
        """
        self.my_thread.start()
        self.my_thread.kill()
        self.my_thread.join(timeout=FDShimThread.block_timeout)
        self.mock_receiver.close.assert_called_with()

    def test_session_cleanup(self):
        """Ensure that when a thread exits normally after a call to kill()
        that close() is called on the session.
        """
        self.my_thread.start()
        self.my_thread.kill()
        self.my_thread.join(timeout=FDShimThread.block_timeout)
        self.mock_session.close.assert_called_with()

    def test_call_to_fetch_raise_empty(self):
        """Ensure the call to fetch() occurs, and with the proper timeout.
        Raises an Empty exception."""
        QpidEmpty = qpid.messaging.exceptions.Empty
        self.mock_receiver.fetch = Mock(side_effect=QpidEmpty())
        self.my_thread.start()
        self.mock_receiver.fetch.assert_called_with(
            timeout=FDShimThread.block_timeout)

    def test_call_to_fetch_return_message(self):
        """Ensure the call to fetch() occurs, that the response bundle is
        built correctly is called as the only argument to receiver.put()"""
        mock_response = Mock()
        mock_source = Mock()
        mock_put = Mock()
        response_bundle = (mock_source, mock_response)
        self.mock_receiver.fetch = Mock(return_value=mock_response)
        self.mock_receiver.source = mock_source
        self.mock_delivery_queue.put = mock_put
        self.my_thread.start()
        self.mock_receiver.fetch.assert_called_with(
            timeout=FDShimThread.block_timeout)
        mock_put.assert_called_with(response_bundle)


class test_FDShim(Case):
    def setUp(self):
        self.mock_queue_from_fdshim = Mock()
        self.mock_delivery_queue = Mock()
        self.my_fdshim = FDShim(self.mock_queue_from_fdshim,
                               self.mock_delivery_queue)
        self.my_thread = threading.Thread(
            target=self.my_fdshim.monitor_consumers)
        self.my_thread.daemon = True

    def test_init_variables(self):
        """Test that all simple init params are internally stored
        correctly"""
        self.assertIs(self.my_fdshim.queue_from_fdshim,
                      self.mock_queue_from_fdshim)
        self.assertIs(self.my_fdshim.delivery_queue,
                      self.mock_delivery_queue)
        self.assertFalse(self.my_fdshim._is_killed)

    def test_kill(self):
        """Start a thread, and then kill it using the kill() method. Ensure
        that it exits properly within the expected amount of time."""
        self.my_thread.start()
        self.my_fdshim.kill()
        self.my_thread.join(timeout=3)
        self.assertFalse(self.my_thread.is_alive())

    def test_call_to_get_raise_empty(self):
        """Ensure the call to delivery_queue.get() occurs, and with
        block=True.  Raises an Queue.Empty exception."""
        self.mock_delivery_queue.get = Mock(side_effect=Queue.Empty())
        self.my_thread.start()
        self.mock_delivery_queue.get.assert_called_with(block=True)

    @patch('os.write')
    def test_call_to_get_return_response_bundle(self, write_method):
        """Ensure the call to get() occurs, and when the response bundle
        is received from delivery_queue, that the response bundle is put
        into queue_from_fdshim using put().

        This method patches os.write to ensure that a '0' is written to the
        _w file descriptor attribute of FDShim."""
        response_bundle = Mock()
        self.mock_delivery_queue.get = Mock(return_value=response_bundle)
        self.my_thread.start()
        self.mock_delivery_queue.get.assert_called_with(block=True)
        self.mock_queue_from_fdshim.put.assert_called_with(
            response_bundle)
        write_method.assert_called_with(self.my_fdshim._w, '0')


class test_Connection(Case):

    def setUp(self):
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
        correctly"""
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
        connection_options, and then returns the result."""
        new_connection = 'connection'
        QpidConnection.establish = Mock(return_value=new_connection)
        conn_from_func = self.my_connection.create_qpid_connection()
        QpidConnection.establish.assert_called_with(
            **self.connection_options)
        self.assertEqual(new_connection, conn_from_func)

    def test_close_channel_exists(self):
        """Test that calling close_channel() with a valid channel removes
        the channel from self.channels and sets channel.connection to None"""
        mock_channel = Mock()
        self.my_connection.channels = [mock_channel]
        mock_channel.connection = True
        self.my_connection.close_channel(mock_channel)
        self.assertEqual(self.my_connection.channels, [])
        self.assertIsNone(mock_channel.connection)


    def test_close_channel_does_not_exist(self):
        """Test that calling close_channel() with an invalid channel does
        not raise a ValueError and sets channel.connection to None."""
        self.my_connection.channels = Mock()
        self.my_connection.channels.remove = Mock(side_effect=ValueError())
        mock_channel = Mock()
        mock_channel.connection = True
        self.my_connection.close_channel(mock_channel)
        self.assertIsNone(mock_channel.connection)


class test_Transport(Case):

    def setUp(self):
        self.mock_client = Mock()

    @patch('kombu.transport.qpid.FDShim')
    @patch('threading.Thread')
    def test_init_variables(self, mock_thread, mock_FDShim):
        """Test that all simple init params are internally stored
        correctly"""
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


    def test_verify_connection_attribute(self):
        """Verify that static class attribute Connection refers to the
        connection object"""
        self.assertIs(Connection, Transport.Connection)

    def test_verify_default_port(self):
        """Verify that the static class attribute default_port refers to
        the 5672 properly"""
        self.assertEqual(5672, Transport.default_port)

    def test_verify_polling_disabled(self):
        """Verify that polling is disabled"""
        self.assertIsNone(Transport.polling_interval)

    def test_verify_supports_asynchronous_events(self):
        """Verify that the Transport advertises that it supports
        an asynchronous event model"""
        self.assertTrue(Transport.supports_ev)

    def test_verify_driver_type_and_name(self):
        """Verify that the driver and type are correctly labeled on the
        class"""
        self.assertEqual('qpid', Transport.driver_type)
        self.assertEqual('qpid', Transport.driver_name)

    def test_register_with_event_loop(self):
        """Test that the file descriptor to monitor, and the on_readable
        callbacks properly register with the event loop"""
        my_transport = Transport(self.mock_client)
        mock_connection = Mock()
        mock_loop = Mock()
        my_transport.register_with_event_loop(mock_connection, mock_loop)
        mock_loop.add_reader.assert_called_with(my_transport.fd_shim.r,
                                                my_transport.on_readable,
                                                mock_connection, mock_loop)

    def test_establish_connection_no_ssl(self):
        """Test that a call to establish connection creates a connection
        object with sane parameters and returns it"""
        self.mock_client.ssl = False
        self.mock_client.transport_options = []
        my_transport = Transport(self.mock_client)
        new_connection = Mock()
        my_transport.Connection = Mock(return_value = new_connection)
        my_transport.establish_connection()
        my_transport.Connection.assert_called_once()
        self.assertIs(new_connection.client, self.mock_client)

    def test_close_connection(self):
        """Test that close_connection calls close on each channel in the
        list of channels on the connection object"""
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
        once the get() method on queue_from_fdshim raises a Queue.Empty"""
        my_transport = Transport(self.mock_client)
        my_transport.queue_from_fdshim = Mock()
        my_transport.queue_from_fdshim.get = Mock(side_effect=Queue.Empty())
        mock_connection = Mock()
        self.assertRaises(socket.timeout, my_transport.drain_events,
                          mock_connection)

    def test_drain_events_and_raise_timeout(self):
        """Test drain_events() drains properly and also exits after the
        timeout is reached even if the queue isn't empty"""
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
        channels the connection maintains"""
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

    def test_default_conneciton_parapms(self):
        """Test that the default_connection_params are correct"""
        correct_params =  {'userid': 'guest', 'password': 'guest',
                           'port': 5672, 'virtual_host': '',
                           'hostname': 'localhost',
                           'sasl_mechanisms': 'PLAIN'}
        my_transport = Transport(self.mock_client)
        result_params = my_transport.default_connection_params
        self.assertDictEqual(correct_params, result_params)