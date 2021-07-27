import base64
import json
import random
from collections import namedtuple
from queue import Empty
from unittest.mock import MagicMock, patch

import pytest

from kombu import Connection, Exchange, Queue, messaging

pytest.importorskip('azure.servicebus')
import azure.core.exceptions  # noqa
import azure.servicebus.exceptions  # noqa
from azure.servicebus import ServiceBusMessage, ServiceBusReceiveMode  # noqa

from kombu.transport import azureservicebus  # noqa


class ASBQueue:
    def __init__(self, kwargs):
        self.options = kwargs
        self.items = []
        self.waiting_ack = []
        self.send_calls = []
        self.recv_calls = []

    def get_receiver(self, kwargs):
        receive_mode = kwargs.get(
            'receive_mode', ServiceBusReceiveMode.PEEK_LOCK)

        class Receiver:
            def close(self):
                pass

            def receive_messages(_self, **kwargs2):
                max_message_count = kwargs2.get('max_message_count', 1)
                result = []
                if self.items:
                    while self.items or len(result) > max_message_count:
                        item = self.items.pop(0)
                        if receive_mode is ServiceBusReceiveMode.PEEK_LOCK:
                            self.waiting_ack.append(item)
                        result.append(item)

                self.recv_calls.append({
                    'receiver_options': kwargs,
                    'receive_messages_options': kwargs2,
                    'messages': result
                })
                return result
        return Receiver()

    def get_sender(self):
        class Sender:
            def close(self):
                pass

            def send_messages(_self, msg):
                self.send_calls.append(msg)
                self.items.append(msg)
        return Sender()


class ASBMock:
    def __init__(self):
        self.queues = {}

    def get_queue_receiver(self, queue_name, **kwargs):
        return self.queues[queue_name].get_receiver(kwargs)

    def get_queue_sender(self, queue_name, **kwargs):
        return self.queues[queue_name].get_sender()


class ASBMgmtMock:
    def __init__(self, queues):
        self.queues = queues

    def create_queue(self, queue_name, **kwargs):
        if queue_name in self.queues:
            raise azure.core.exceptions.ResourceExistsError()
        self.queues[queue_name] = ASBQueue(kwargs)

    def delete_queue(self, queue_name):
        self.queues.pop(queue_name, None)

    def get_queue_runtime_properties(self, queue_name):
        count = len(self.queues[queue_name].items)
        mock = MagicMock()
        mock.total_message_count = count
        return mock


URL_NOCREDS = 'azureservicebus://'
URL_CREDS = 'azureservicebus://policyname:ke/y@hostname'


def test_queue_service_nocredentials():
    conn = Connection(URL_NOCREDS, transport=azureservicebus.Transport)
    with pytest.raises(ValueError) as exc:
        conn.channel()
        assert exc == 'Need an URI like azureservicebus://{SAS policy name}:{SAS key}@{ServiceBus Namespace}'   # noqa


def test_queue_service():
    # Test gettings queue service without credentials
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport)
    with patch('kombu.transport.azureservicebus.ServiceBusClient') as m:
        channel = conn.channel()

        # Check the SAS token "ke/y" has been parsed from the url correctly
        assert channel._sas_key == 'ke/y'

        m.from_connection_string.return_value = 'test'

        # Remove queue service to get from service bus again
        channel._queue_service = None
        assert channel.queue_service == 'test'
        assert m.from_connection_string.call_count == 1

        # Ensure that queue_service is cached
        assert channel.queue_service == 'test'
        assert m.from_connection_string.call_count == 1


def test_conninfo():
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport)
    channel = conn.channel()
    assert channel.conninfo is conn


def test_transport_type():
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport)
    channel = conn.channel()
    assert not channel.transport_options


def test_default_wait_timeout_seconds():
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport)
    channel = conn.channel()

    assert channel.wait_time_seconds == \
        azureservicebus.Channel.default_wait_time_seconds


def test_custom_wait_timeout_seconds():
    conn = Connection(
        URL_CREDS,
        transport=azureservicebus.Transport,
        transport_options={'wait_time_seconds': 10}
    )
    channel = conn.channel()

    assert channel.wait_time_seconds == 10


def test_default_peek_lock_seconds():
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport)
    channel = conn.channel()

    assert channel.peek_lock_seconds == \
        azureservicebus.Channel.default_peek_lock_seconds


def test_custom_peek_lock_seconds():
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport,
                      transport_options={'peek_lock_seconds': 65})
    channel = conn.channel()

    assert channel.peek_lock_seconds == 65


def test_invalid_peek_lock_seconds():
    # Max is 300
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport,
                      transport_options={'peek_lock_seconds': 900})
    channel = conn.channel()

    assert channel.peek_lock_seconds == 300


@pytest.fixture
def random_queue():
    return f'azureservicebus_queue_{random.randint(1000, 9999)}'


@pytest.fixture
def mock_asb():
    return ASBMock()


@pytest.fixture
def mock_asb_management(mock_asb):
    return ASBMgmtMock(queues=mock_asb.queues)


MockQueue = namedtuple(
    'MockQueue',
    ['queue_name', 'asb', 'asb_mgmt', 'conn', 'channel', 'producer', 'queue']
)


@pytest.fixture
def mock_queue(mock_asb, mock_asb_management, random_queue) -> MockQueue:
    exchange = Exchange('test_servicebus', type='direct')
    queue = Queue(random_queue, exchange, random_queue)
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport)
    channel = conn.channel()
    channel._queue_service = mock_asb
    channel._queue_mgmt_service = mock_asb_management

    queue(channel).declare()
    producer = messaging.Producer(channel, exchange, routing_key=random_queue)

    return MockQueue(
        random_queue,
        mock_asb,
        mock_asb_management,
        conn,
        channel,
        producer,
        queue
    )


def test_basic_put_get(mock_queue: MockQueue):
    text_message = "test message"

    # This ends up hitting channel._put
    mock_queue.producer.publish(text_message)

    assert len(mock_queue.asb.queues[mock_queue.queue_name].items) == 1
    azure_msg = mock_queue.asb.queues[mock_queue.queue_name].items[0]
    assert isinstance(azure_msg, ServiceBusMessage)

    message = mock_queue.channel._get(mock_queue.queue_name)
    azure_msg_decoded = json.loads(str(azure_msg))

    assert message['body'] == azure_msg_decoded['body']

    # Check the message has been annotated with the azure message object
    # which is used to ack later
    assert message['properties']['delivery_info']['azure_message'] is azure_msg

    assert base64.b64decode(message['body']).decode() == text_message

    # Ack is on by default, check an ack is waiting
    assert len(mock_queue.asb.queues[mock_queue.queue_name].waiting_ack) == 1


def test_empty_queue_get(mock_queue: MockQueue):
    with pytest.raises(Empty):
        mock_queue.channel._get(mock_queue.queue_name)


def test_delete_empty_queue(mock_queue: MockQueue):
    chan = mock_queue.channel
    queue_name = f'random_queue_{random.randint(1000, 9999)}'

    chan._new_queue(queue_name)
    assert queue_name in chan._queue_cache
    chan._delete(queue_name)
    assert queue_name not in chan._queue_cache


def test_delete_populated_queue(mock_queue: MockQueue):
    mock_queue.producer.publish('test1234')

    mock_queue.channel._delete(mock_queue.queue_name)
    assert mock_queue.queue_name not in mock_queue.channel._queue_cache


def test_purge(mock_queue: MockQueue):
    mock_queue.producer.publish('test1234')
    mock_queue.producer.publish('test1234')
    mock_queue.producer.publish('test1234')
    mock_queue.producer.publish('test1234')

    size = mock_queue.channel._size(mock_queue.queue_name)
    assert size == 4

    assert mock_queue.channel._purge(mock_queue.queue_name) == 4

    size = mock_queue.channel._size(mock_queue.queue_name)
    assert size == 0
    assert len(mock_queue.asb.queues[mock_queue.queue_name].waiting_ack) == 0


def test_custom_queue_name_prefix():
    conn = Connection(
        URL_CREDS,
        transport=azureservicebus.Transport,
        transport_options={'queue_name_prefix': 'test-queue'}
    )
    channel = conn.channel()

    assert channel.queue_name_prefix == 'test-queue'


def test_custom_entity_name():
    conn = Connection(URL_CREDS, transport=azureservicebus.Transport)
    channel = conn.channel()

    # dashes allowed and dots replaced by dashes
    assert channel.entity_name('test-celery') == 'test-celery'
    assert channel.entity_name('test.celery') == 'test-celery'

    # all other punctuations replaced by underscores
    assert channel.entity_name('test_celery') == 'test_celery'
    assert channel.entity_name('test:celery') == 'test_celery'
    assert channel.entity_name('test+celery') == 'test_celery'
