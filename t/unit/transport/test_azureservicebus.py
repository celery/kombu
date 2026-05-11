from __future__ import annotations

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

try:
    from azure.identity import (DefaultAzureCredential,
                                ManagedIdentityCredential)
except ImportError:
    DefaultAzureCredential = None
    ManagedIdentityCredential = None

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
URL_CREDS_SAS = 'azureservicebus://policyname:ke/y@hostname'
URL_CREDS_SAS_FQ = 'azureservicebus://policyname:ke/y@hostname.servicebus.windows.net'
URL_CREDS_DA = 'azureservicebus://DefaultAzureCredential@hostname'
URL_CREDS_DA_FQ = 'azureservicebus://DefaultAzureCredential@hostname.servicebus.windows.net' # noqa
URL_CREDS_MI = 'azureservicebus://ManagedIdentityCredential@hostname'
URL_CREDS_MI_FQ = 'azureservicebus://ManagedIdentityCredential@hostname.servicebus.windows.net' # noqa


def test_queue_service_nocredentials():
    conn = Connection(URL_NOCREDS, transport=azureservicebus.Transport)
    with pytest.raises(ValueError) as exc:
        conn.channel()
        assert exc == 'Need an URI like azureservicebus://{SAS policy name}:{SAS key}@{ServiceBus Namespace}'   # noqa


def test_queue_service_sas():
    # Test getting queue service without credentials
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
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
        assert channel._namespace == 'hostname.servicebus.windows.net'


def test_queue_service_da():
    conn = Connection(URL_CREDS_DA, transport=azureservicebus.Transport)
    channel = conn.channel()

    # Check the DefaultAzureCredential has been parsed from the url correctly
    # and the credential is a ManagedIdentityCredential
    assert isinstance(channel._credential, DefaultAzureCredential)
    assert channel._namespace == 'hostname.servicebus.windows.net'


def test_queue_service_mi():
    conn = Connection(URL_CREDS_MI, transport=azureservicebus.Transport)
    channel = conn.channel()

    # Check the ManagedIdentityCredential has been parsed from the url
    # correctly and the credential is a ManagedIdentityCredential
    assert isinstance(channel._credential, ManagedIdentityCredential)
    assert channel._namespace == 'hostname.servicebus.windows.net'


def test_conninfo():
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    channel = conn.channel()
    assert channel.conninfo is conn


def test_transport_type():
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    channel = conn.channel()
    assert not channel.transport_options


def test_default_wait_timeout_seconds():
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    channel = conn.channel()

    assert channel.wait_time_seconds == \
        azureservicebus.Channel.default_wait_time_seconds


def test_custom_wait_timeout_seconds():
    conn = Connection(
        URL_CREDS_SAS,
        transport=azureservicebus.Transport,
        transport_options={'wait_time_seconds': 10}
    )
    channel = conn.channel()

    assert channel.wait_time_seconds == 10


def test_default_peek_lock_seconds():
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    channel = conn.channel()

    assert channel.peek_lock_seconds == \
        azureservicebus.Channel.default_peek_lock_seconds


def test_custom_peek_lock_seconds():
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport,
                      transport_options={'peek_lock_seconds': 65})
    channel = conn.channel()

    assert channel.peek_lock_seconds == 65


def test_invalid_peek_lock_seconds():
    # Max is 300
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport,
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


@pytest.fixture(autouse=True)
def sbac_class_patch():
    with patch('kombu.transport.azureservicebus.ServiceBusAdministrationClient') as sbac: # noqa
        yield sbac


@pytest.fixture(autouse=True)
def sbc_class_patch():
    with patch('kombu.transport.azureservicebus.ServiceBusClient') as sbc: # noqa
        yield sbc


@pytest.fixture(autouse=True)
def mock_clients(
    sbc_class_patch,
    sbac_class_patch,
    mock_asb,
    mock_asb_management
):
    sbc_class_patch.from_connection_string.return_value = mock_asb
    sbac_class_patch.from_connection_string.return_value = mock_asb_management


@pytest.fixture
def mock_queue(mock_asb, mock_asb_management, random_queue) -> MockQueue:
    exchange = Exchange('test_servicebus', type='direct')
    queue = Queue(random_queue, exchange, random_queue)
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    channel = conn.channel()

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
        URL_CREDS_SAS,
        transport=azureservicebus.Transport,
        transport_options={'queue_name_prefix': 'test-queue'}
    )
    channel = conn.channel()

    assert channel.queue_name_prefix == 'test-queue'


def test_custom_entity_name():
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    channel = conn.channel()

    # dashes allowed
    assert channel.entity_name('test-celery') == 'test-celery'

    # dots allowed
    # cf. https://learn.microsoft.com/en-us/azure/azure-resource-manager/management/resource-name-rules
    assert channel.entity_name('test.celery') == 'test.celery'

    # all other punctuation is replaced by underscores
    assert channel.entity_name('test_celery') == 'test_celery'
    assert channel.entity_name('test:celery') == 'test_celery'
    assert channel.entity_name('test+celery') == 'test_celery'


def test_basic_ack_complete_message(mock_queue: MockQueue):
    mock_queue.producer.publish("test message")
    message = mock_queue.channel._get(mock_queue.queue_name)
    mock_queue.channel.qos.get = MagicMock(
        return_value=mock_queue.channel.Message(
            message, mock_queue.channel
        )
    )
    receiver_mock = MagicMock()
    receiver_mock.complete_message = MagicMock(return_value=None)
    queue_object_mock = MagicMock()
    queue_object_mock.receiver = receiver_mock
    mock_queue.channel._get_asb_receiver = MagicMock(
        return_value=queue_object_mock)
    with patch(
        'kombu.transport.virtual.base.Channel.basic_ack'
    ) as super_basic_ack:
        mock_queue.channel.basic_ack("test_delivery_tag")
        assert mock_queue.channel.qos.get.call_count == 1
        assert mock_queue.channel._get_asb_receiver.call_count == 1
        assert queue_object_mock.receiver.complete_message.call_count == 1
        assert super_basic_ack.call_count == 1


def test_basic_ack_when_already_settled(mock_queue: MockQueue):
    mock_queue.producer.publish("test message")
    message = mock_queue.channel._get(mock_queue.queue_name)
    mock_queue.channel.qos.get = MagicMock(
        return_value=mock_queue.channel.Message(
            message, mock_queue.channel
        )
    )
    receiver_mock = MagicMock()
    receiver_mock.complete_message = MagicMock(
        side_effect=azure.servicebus.exceptions.MessageAlreadySettled())
    queue_object_mock = MagicMock()
    queue_object_mock.receiver = receiver_mock
    mock_queue.channel._get_asb_receiver = MagicMock(
        return_value=queue_object_mock)
    with patch(
        'kombu.transport.virtual.base.Channel.basic_ack'
    ) as super_basic_ack:
        mock_queue.channel.basic_ack("test_delivery_tag")
        assert mock_queue.channel.qos.get.call_count == 1
        assert mock_queue.channel._get_asb_receiver.call_count == 1
        assert queue_object_mock.receiver.complete_message.call_count == 1
        assert super_basic_ack.call_count == 1


def test_basic_ack_when_qos_raises_keyerror(mock_queue: MockQueue):
    """Test that basic_ack calls super method when keyerror"""
    mock_queue.channel.qos.get = MagicMock(side_effect=KeyError())
    with patch(
        'kombu.transport.virtual.base.Channel.basic_ack'
    ) as super_basic_ack:
        mock_queue.channel.basic_ack("invented_delivery_tag")
        assert super_basic_ack.call_count == 1
        assert mock_queue.channel.qos.get.call_count == 1


def test_basic_ack_reject_message_when_raises_exception(
    mock_queue: MockQueue
):
    mock_queue.producer.publish("test message")
    message = mock_queue.channel._get(mock_queue.queue_name)
    mock_queue.channel.qos.get = MagicMock(
        return_value=mock_queue.channel.Message(
            message, mock_queue.channel
        )
    )
    receiver_mock = MagicMock()
    receiver_mock.complete_message = MagicMock(side_effect=Exception())
    queue_object_mock = MagicMock()
    queue_object_mock.receiver = receiver_mock
    mock_queue.channel._get_asb_receiver = MagicMock(
        return_value=queue_object_mock)
    with patch(
        'kombu.transport.virtual.base.Channel.basic_reject'
    ) as super_basic_reject:
        mock_queue.channel.basic_ack("test_delivery_tag")
        assert mock_queue.channel.qos.get.call_count == 1
        assert mock_queue.channel._get_asb_receiver.call_count == 1
        assert queue_object_mock.receiver.complete_message.call_count == 1
        assert super_basic_reject.call_count == 1


def test_separate_connections_get_separate_queue_caches(
    mock_asb, mock_asb_management
):
    """Regression: separate Connections must not share _queue_cache."""
    conn_a = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    conn_b = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    chan_a = conn_a.channel()
    chan_b = conn_b.channel()

    assert chan_a._queue_cache is not chan_b._queue_cache

    chan_a._add_queue_to_cache('shared-name', sender=MagicMock())
    assert 'shared-name' in chan_a._queue_cache
    assert 'shared-name' not in chan_b._queue_cache


def test_separate_connections_get_separate_noack_queues(
    mock_asb, mock_asb_management
):
    """Regression: separate Connections must not share no_ack state."""
    conn_a = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    conn_b = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    chan_a = conn_a.channel()
    chan_b = conn_b.channel()

    chan_a.basic_consume('shared-name', True, lambda m: None, 'tag-x')
    assert 'shared-name' in chan_a._noack_queues
    assert 'shared-name' not in chan_b._noack_queues


def test_channels_on_same_connection_share_queue_cache(mock_queue: MockQueue):
    """Channels on one Connection share Transport-scoped state."""
    chan_a = mock_queue.channel
    chan_b = mock_queue.conn.channel()

    assert chan_a._queue_cache is chan_b._queue_cache
    assert (chan_a.connection._noack_consumer_tags
            is chan_b.connection._noack_consumer_tags)


def test_channel_close_does_not_evict_queue_cache(mock_queue: MockQueue):
    """Regression: Channel.close() must not evict siblings' cache entries."""
    chan_a = mock_queue.channel
    chan_b = mock_queue.conn.channel()

    sender = MagicMock()
    chan_a._add_queue_to_cache('q', sender=sender)
    assert 'q' in chan_b._queue_cache

    chan_a.close()

    # Sibling still sees the cached entry; SDK object was not closed.
    assert 'q' in chan_b._queue_cache
    assert chan_b._queue_cache['q'].sender is sender
    sender.close.assert_not_called()


def test_close_connection_clears_queue_cache(mock_queue: MockQueue):
    """Transport.close_connection() tears down the per-Connection cache."""
    transport = mock_queue.conn.transport
    sender = MagicMock()
    receiver = MagicMock()
    mock_queue.channel._add_queue_to_cache(
        'q1', sender=sender, receiver=receiver)
    transport._noack_consumer_tags.add('tag-x')

    transport.close_connection(mock_queue.conn)

    assert transport._queue_cache == {}
    assert transport._noack_consumer_tags == set()
    sender.close.assert_called_once()
    receiver.close.assert_called_once()


def test_close_connection_continues_on_per_entry_exception(
    mock_queue: MockQueue
):
    """A failing SendReceive.close() must not strand sibling cleanup."""
    transport = mock_queue.conn.transport

    bad = MagicMock()
    bad.close.side_effect = RuntimeError("boom")
    good_sender = MagicMock()

    mock_queue.channel._add_queue_to_cache('bad', sender=bad)
    mock_queue.channel._add_queue_to_cache('good', sender=good_sender)

    transport.close_connection(mock_queue.conn)

    good_sender.close.assert_called_once()
    assert transport._queue_cache == {}


def test_basic_ack_lock_lost_logs_warning_and_rejects(
    mock_queue: MockQueue, caplog
):
    """Regression: lock-lost ack failures must be logged, not silent."""
    mock_queue.producer.publish("test message")
    message = mock_queue.channel._get(mock_queue.queue_name)
    mock_queue.channel.qos.get = MagicMock(
        return_value=mock_queue.channel.Message(
            message, mock_queue.channel
        )
    )
    receiver_mock = MagicMock()
    receiver_mock.complete_message = MagicMock(
        side_effect=azure.servicebus.exceptions.MessageLockLostError())
    queue_object_mock = MagicMock()
    queue_object_mock.receiver = receiver_mock
    mock_queue.channel._get_asb_receiver = MagicMock(
        return_value=queue_object_mock)
    with patch(
        'kombu.transport.virtual.base.Channel.basic_reject'
    ) as super_basic_reject, caplog.at_level(
        'WARNING', logger='kombu.transport.azureservicebus'
    ):
        mock_queue.channel.basic_ack("test_delivery_tag")

    assert super_basic_reject.call_count == 1
    assert any(
        'MessageLockLostError' in record.message
        for record in caplog.records
    )


def test_basic_ack_unknown_exception_logs_and_rejects(
    mock_queue: MockQueue, caplog
):
    """Regression: unknown ack failures must be logged with traceback."""
    mock_queue.producer.publish("test message")
    message = mock_queue.channel._get(mock_queue.queue_name)
    mock_queue.channel.qos.get = MagicMock(
        return_value=mock_queue.channel.Message(
            message, mock_queue.channel
        )
    )
    receiver_mock = MagicMock()
    receiver_mock.complete_message = MagicMock(
        side_effect=RuntimeError("transient broker error"))
    queue_object_mock = MagicMock()
    queue_object_mock.receiver = receiver_mock
    mock_queue.channel._get_asb_receiver = MagicMock(
        return_value=queue_object_mock)
    with patch(
        'kombu.transport.virtual.base.Channel.basic_reject'
    ) as super_basic_reject, caplog.at_level(
        'ERROR', logger='kombu.transport.azureservicebus'
    ):
        mock_queue.channel.basic_ack("test_delivery_tag")

    assert super_basic_reject.call_count == 1
    error_records = [r for r in caplog.records if r.levelname == 'ERROR']
    assert error_records, "expected an ERROR-level log from logger.exception"
    assert error_records[0].exc_info is not None


def test_returning_sas():
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    assert conn.as_uri(True) == URL_CREDS_SAS_FQ


def test_returning_da():
    conn = Connection(URL_CREDS_DA, transport=azureservicebus.Transport)
    assert conn.as_uri(True) == URL_CREDS_DA_FQ


def test_returning_mi():
    conn = Connection(URL_CREDS_MI, transport=azureservicebus.Transport)
    assert conn.as_uri(True) == URL_CREDS_MI_FQ


def test_lock_renewal_default_config():
    """use_lock_renewal defaults to False; duration defaults to 3600s."""
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport)
    channel = conn.channel()

    assert channel.use_lock_renewal is False
    assert channel.max_lock_renewal_duration == \
        azureservicebus.Channel.default_max_lock_renewal_duration


def test_lock_renewal_config_initialization():
    """Transport options override the renewal defaults."""
    options = {
        'use_lock_renewal': True,
        'max_lock_renewal_duration': 1234,
    }
    conn = Connection(URL_CREDS_SAS, transport=azureservicebus.Transport,
                      transport_options=options)
    channel = conn.channel()

    assert channel.use_lock_renewal is True
    assert channel.max_lock_renewal_duration == 1234


@patch('kombu.transport.azureservicebus.AutoLockRenewer')
def test_get_asb_receiver_default_does_not_create_renewer(
    mock_renewer_cls, mock_queue,
):
    """Renewer is not instantiated when use_lock_renewal is unset."""
    channel = mock_queue.channel
    channel.queue_service.get_queue_receiver = MagicMock()

    channel._get_asb_receiver(
        'some_queue', recv_mode=ServiceBusReceiveMode.PEEK_LOCK)

    mock_renewer_cls.assert_not_called()
    assert channel.queue_service.get_queue_receiver.call_args.kwargs[
        'auto_lock_renewer'] is None
    assert channel.connection._renewer is None


@patch('kombu.transport.azureservicebus.AutoLockRenewer')
def test_get_asb_receiver_creates_and_reuses_renewer(
    mock_renewer_cls, mock_queue,
):
    """Renewer is created on first PEEK_LOCK call, reused, and gated."""
    conn = Connection(
        URL_CREDS_SAS,
        transport=azureservicebus.Transport,
        transport_options={'use_lock_renewal': True},
    )
    channel = conn.channel()
    channel.queue_service.get_queue_receiver = MagicMock()

    # Renewer is created on first PEEK_LOCK call.
    channel._get_asb_receiver(
        'first_queue', recv_mode=ServiceBusReceiveMode.PEEK_LOCK)
    mock_renewer_cls.assert_called_once_with(
        max_lock_renewal_duration=channel.max_lock_renewal_duration)
    assert channel.queue_service.get_queue_receiver.call_args.kwargs[
        'auto_lock_renewer'] is mock_renewer_cls.return_value
    assert channel.connection._renewer is mock_renewer_cls.return_value

    # Subsequent PEEK_LOCK calls reuse the same renewer.
    channel._get_asb_receiver(
        'second_queue', recv_mode=ServiceBusReceiveMode.PEEK_LOCK)
    assert mock_renewer_cls.call_count == 1

    # RECEIVE_AND_DELETE never gets a renewer attached.
    channel._get_asb_receiver(
        'third_queue', recv_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE)
    assert channel.queue_service.get_queue_receiver.call_args.kwargs[
        'auto_lock_renewer'] is None


@patch('kombu.transport.azureservicebus.AutoLockRenewer')
def test_separate_connections_get_separate_renewers(
    mock_renewer_cls, mock_asb, mock_asb_management,
):
    """Regression: renewer is per-Connection, not process-wide."""
    options = {'use_lock_renewal': True}
    conn_a = Connection(
        URL_CREDS_SAS, transport=azureservicebus.Transport,
        transport_options=options,
    )
    conn_b = Connection(
        URL_CREDS_SAS, transport=azureservicebus.Transport,
        transport_options=options,
    )
    chan_a = conn_a.channel()
    chan_b = conn_b.channel()
    chan_a.queue_service.get_queue_receiver = MagicMock()
    chan_b.queue_service.get_queue_receiver = MagicMock()

    mock_renewer_cls.side_effect = [MagicMock(), MagicMock()]

    chan_a._get_asb_receiver(
        'q', recv_mode=ServiceBusReceiveMode.PEEK_LOCK)
    chan_b._get_asb_receiver(
        'q', recv_mode=ServiceBusReceiveMode.PEEK_LOCK)

    assert chan_a.connection._renewer is not None
    assert chan_b.connection._renewer is not None
    assert chan_a.connection._renewer is not chan_b.connection._renewer
    assert mock_renewer_cls.call_count == 2


def test_close_connection_without_renewer_is_safe(mock_queue: MockQueue):
    """Connection release with no renewer ever created is a no-op."""
    transport = mock_queue.conn.transport
    assert transport._renewer is None

    mock_queue.conn.release()

    assert transport._renewer is None


def test_close_connection_closes_renewer(mock_queue: MockQueue):
    """Connection release closes the renewer exactly once."""
    transport = mock_queue.conn.transport
    renewer = MagicMock()
    transport._renewer = renewer

    mock_queue.conn.release()

    renewer.close.assert_called_once()
    assert transport._renewer is None


def test_close_connection_continues_on_renewer_close_exception(
    mock_queue: MockQueue, caplog,
):
    """A failing renewer.close() must not strand cache teardown."""
    transport = mock_queue.conn.transport
    renewer = MagicMock()
    renewer.close.side_effect = RuntimeError("boom")
    transport._renewer = renewer

    sender = MagicMock()
    mock_queue.channel._add_queue_to_cache('q1', sender=sender)

    with caplog.at_level(
        'ERROR', logger='kombu.transport.azureservicebus'
    ):
        mock_queue.conn.release()

    renewer.close.assert_called_once()
    assert transport._renewer is None
    sender.close.assert_called_once()
    assert transport._queue_cache == {}
    assert any(
        'AutoLockRenewer' in record.message for record in caplog.records
    )


def test_basic_cancel_does_not_de_noack_when_another_no_ack_consumer_remains(
    mock_queue: MockQueue,
):
    """Regression: cancelling an ack consumer must not remove the queue
    from the no_ack view while a no_ack consumer is still subscribed."""
    channel = mock_queue.channel
    queue = mock_queue.queue_name

    channel.basic_consume(queue, True, lambda m: None, 'tag_noack')
    channel.basic_consume(queue, False, lambda m: None, 'tag_ack')
    assert queue in channel._noack_queues

    channel.basic_cancel('tag_ack')

    assert queue in channel._noack_queues


def test_get_asb_receiver_creates_separate_receiver_per_recv_mode(
    mock_queue: MockQueue,
):
    """Regression: a receiver created in one recv_mode must not be reused
    for callers requesting the other mode (silently skips renewal wiring
    and applies the wrong receive semantics)."""
    channel = mock_queue.channel
    recv_a = MagicMock(name='peek_lock_receiver')
    recv_b = MagicMock(name='receive_and_delete_receiver')
    channel.queue_service.get_queue_receiver = MagicMock(
        side_effect=[recv_a, recv_b])

    queue_obj_a = channel._get_asb_receiver(
        'q', recv_mode=ServiceBusReceiveMode.PEEK_LOCK)
    queue_obj_b = channel._get_asb_receiver(
        'q', recv_mode=ServiceBusReceiveMode.RECEIVE_AND_DELETE)

    assert channel.queue_service.get_queue_receiver.call_count == 2
    assert channel.queue_service.get_queue_receiver.call_args_list[0].kwargs[
        'receive_mode'] == ServiceBusReceiveMode.PEEK_LOCK
    assert channel.queue_service.get_queue_receiver.call_args_list[1].kwargs[
        'receive_mode'] == ServiceBusReceiveMode.RECEIVE_AND_DELETE
    assert queue_obj_a.receiver is recv_a
    assert queue_obj_b.receiver is recv_b
