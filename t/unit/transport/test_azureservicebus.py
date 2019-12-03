from __future__ import absolute_import, unicode_literals

import pytest

from case import skip, patch
from kombu import messaging
from kombu import Connection, Exchange, Queue
from kombu.five import Empty
from kombu.transport import azureservicebus

try:
    # azure-servicebus version >= 0.50.0
    from azure.servicebus.control_client import Message, ServiceBusService
except ImportError:
    try:
        # azure-servicebus version <= 0.21.1
        from azure.servicebus import Message, ServiceBusService
    except ImportError:
        ServiceBusService = Message = None


class QueueMock(object):
    """ Hold information about a queue. """

    def __init__(self, name):
        self.name = name
        self.messages = []
        self.message_count = len(self.messages)

    def __repr__(self):
        return 'QueueMock: {} messages'.format(len(self.messages))


def _create_mock_connection(url='', **kwargs):

    class _Channel(azureservicebus.Channel):
        # reset _fanout_queues for each instance
        queues = []
        _queue_service = None

        def list_queues(self):
            return self.queues

        @property
        def queue_service(self):
            if self._queue_service is None:
                self._queue_service = AzureServiceBusClientMock()
            return self._queue_service

    class Transport(azureservicebus.Transport):
        Channel = _Channel

    return Connection(url, transport=Transport, **kwargs)


class AzureServiceBusClientMock(object):

    def __init__(self):
        """
        Imitate the ServiceBus Client.
        """
        # queues doesn't exist on the real client, here for testing.
        self.queues = []
        self._queue_cache = {}
        self.queues.append(self.create_queue(queue_name='unittest_queue'))

    def create_queue(self, queue_name, queue=None, fail_on_exist=False):
        queue = QueueMock(name=queue_name)
        self.queues.append(queue)
        self._queue_cache[queue_name] = queue
        return queue

    def get_queue(self, queue_name=None):
        for queue in self.queues:
            if queue.name == queue_name:
                return queue

    def list_queues(self):
        return self.queues

    def send_queue_message(self, queue_name=None, message=None):
        queue = self.get_queue(queue_name)
        queue.messages.append(message)

    def receive_queue_message(self, queue_name, peek_lock=True, timeout=60):
        queue = self.get_queue(queue_name)
        if queue and len(queue.messages):
            return queue.messages.pop(0)
        return Message()

    def read_delete_queue_message(self, queue_name, timeout='60'):
        return self.receive_queue_message(queue_name, timeout=timeout)

    def delete_queue(self, queue_name=None):
        queue = self.get_queue(queue_name)
        if queue:
            del queue


@skip.unless_module('azure.servicebus')
class test_Channel:

    def handleMessageCallback(self, message):
        self.callback_message = message

    def setup(self):
        self.url = 'azureservicebus://'
        self.queue_name = 'unittest_queue'

        self.exchange = Exchange('test_servicebus', type='direct')
        self.queue = Queue(self.queue_name, self.exchange, self.queue_name)
        self.connection = _create_mock_connection(self.url)
        self.channel = self.connection.default_channel
        self.queue(self.channel).declare()

        self.producer = messaging.Producer(self.channel,
                                           self.exchange,
                                           routing_key=self.queue_name)

        self.channel.basic_consume(self.queue_name,
                                   no_ack=False,
                                   callback=self.handleMessageCallback,
                                   consumer_tag='unittest')

    def teardown(self):
        # Removes QoS reserved messages so we don't restore msgs on shutdown.
        try:
            qos = self.channel._qos
        except AttributeError:
            pass
        else:
            if qos:
                qos._dirty.clear()
                qos._delivered.clear()

    def test_queue_service(self):
        # Test gettings queue service without credentials
        conn = Connection(self.url, transport=azureservicebus.Transport)
        with pytest.raises(ValueError) as exc:
            conn.channel()
            assert exc == 'You need to provide servicebus namespace'

        # Test getting queue service when queue_service is not setted
        with patch('kombu.transport.azureservicebus.ServiceBusService') as m:
            channel = conn.channel()

            # Remove queue service to get from service bus again
            channel._queue_service = None
            channel.queue_service

            assert m.call_count == 2

            # Calling queue_service again needs to reuse ServiceBus instance
            channel.queue_service
            assert m.call_count == 2

    def test_conninfo(self):
        conninfo = self.channel.conninfo
        assert conninfo is self.connection

    def test_transport_type(self):
        transport_options = self.channel.transport_options
        assert transport_options == {}

    def test_visibility_timeout(self):
        # Test getting default visibility timeout
        assert (
            self.channel.visibility_timeout ==
            azureservicebus.Channel.default_visibility_timeout
        )

        # Test getting value setted in transport options
        del self.channel.visibility_timeout
        self.channel.transport_options['visibility_timeout'] = 10
        assert self.channel.visibility_timeout == 10

    def test_wait_timeout_seconds(self):
        # Test getting default wait timeout seconds
        assert (
            self.channel.wait_time_seconds ==
            azureservicebus.Channel.default_wait_time_seconds
        )

        # Test getting value setted in transport options
        del self.channel.wait_time_seconds
        self.channel.transport_options['wait_time_seconds'] = 10
        assert self.channel.wait_time_seconds == 10

    def test_peek_lock(self):
        # Test getting default peek lock
        assert (
            self.channel.peek_lock ==
            azureservicebus.Channel.default_peek_lock
        )

        # Test getting value setted in transport options
        del self.channel.peek_lock
        self.channel.transport_options['peek_lock'] = True
        assert self.channel.peek_lock is True

    def test_get_from_azure(self):
        # Test getting a single message
        message = 'my test message'
        self.producer.publish(message)
        result = self.channel._get(self.queue_name)
        assert 'body' in result.keys()

        # Test getting multiple messages
        for i in range(3):
            message = 'message: {0}'.format(i)
            self.producer.publish(message)

        queue_service = self.channel.queue_service
        assert len(queue_service.get_queue(self.queue_name).messages) == 3

        for i in range(3):
            result = self.channel._get(self.queue_name)

        assert len(queue_service.get_queue(self.queue_name).messages) == 0

    def test_get_with_empty_list(self):
        with pytest.raises(Empty):
            self.channel._get(self.queue_name)

    def test_put_and_get(self):
        message = 'my test message'
        self.producer.publish(message)
        results = self.queue(self.channel).get().payload
        assert message == results

    def test_delete_queue(self):
        # Test deleting queue without message
        queue_name = 'new_unittest_queue'
        self.channel._new_queue(queue_name)

        assert queue_name in self.channel._queue_cache
        self.channel._delete(queue_name)
        assert queue_name not in self.channel._queue_cache

        # Test deleting queue with message
        message = 'my test message'
        self.producer.publish(message)
        self.channel._delete(self.queue_name)
        assert queue_name not in self.channel._queue_cache
