from __future__ import absolute_import, unicode_literals

import pytest

from case import skip
from kombu import messaging
from kombu import Connection, Exchange, Queue
from kombu.five import Empty
from kombu.transport import azureservicebus

try:
    # azure-servicebus version >= 0.50.0
    from azure.servicebus.control_client import Message
except ImportError:
    try:
        # azure-servicebus version <= 0.21.1
        from azure.servicebus import Message
    except ImportError:
        Message = None


class QueueMock(object):
    """ Hold information about a queue. """

    def __init__(self, name):
        self.name = name
        self.messages = []
        self.message_count = 0

    def __repr__(self):
        return 'QueueMock: {} messages'.format(len(self.messages))


def _create_mock_connection(url='', **kwargs):

    class _Channel(azureservicebus.Channel):
        # reset _fanout_queues for each instance
        queues = []
        _service_queue = None

        def list_queues(self):
            return self.queues

        @property
        def queue_service(self):
            if self._service_queue is None:
                self._service_queue = AzureServiceBusClientMock()
            return self._service_queue

        @property
        def transport_options(self):
            return {'wait_time_seconds': 0.1}

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
        if queue:
            try:
                return queue.messages.pop(0)
            except IndexError:
                return Message()


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
