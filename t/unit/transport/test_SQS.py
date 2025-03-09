"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""
from __future__ import annotations

import base64
import os
import random
import string
from datetime import datetime, timedelta
from queue import Empty
from unittest.mock import Mock, patch

import pytest

from kombu import Connection, Exchange, Queue, messaging

boto3 = pytest.importorskip('boto3')

from botocore.exceptions import ClientError  # noqa

from kombu.transport import SQS  # noqa

SQS_Channel_sqs = SQS.Channel.sqs


example_predefined_queues = {
    'queue-1': {
        'url': 'https://sqs.us-east-1.amazonaws.com/xxx/queue-1',
        'access_key_id': 'a',
        'secret_access_key': 'b',
        'backoff_tasks': ['svc.tasks.tasks.task1'],
        'backoff_policy': {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640}
    },
    'queue-2': {
        'url': 'https://sqs.us-east-1.amazonaws.com/xxx/queue-2',
        'access_key_id': 'c',
        'secret_access_key': 'd',
    },
    'queue-3.fifo': {
        'url': 'https://sqs.us-east-1.amazonaws.com/xxx/queue-3.fifo',
        'access_key_id': 'e',
        'secret_access_key': 'f',
    }
}


class SQSMessageMock:
    def __init__(self):
        """
        Imitate the SQS Message from boto3.
        """
        self.body = ""
        self.receipt_handle = "receipt_handle_xyz"


class QueueMock:
    """ Hold information about a queue. """

    def __init__(self, url, creation_attributes=None):
        self.url = url
        # arguments of boto3.sqs.create_queue
        self.creation_attributes = creation_attributes
        self.attributes = {'ApproximateNumberOfMessages': '0'}

        self.messages = []

    def __repr__(self):
        return f'QueueMock: {self.url} {len(self.messages)} messages'


class SQSClientMock:

    def __init__(self, QueueName='unittest_queue'):
        """
        Imitate the SQS Client from boto3.
        """
        self._receive_messages_calls = 0
        # _queues doesn't exist on the real client, here for testing.
        self._queues = {}
        url = self.create_queue(QueueName=QueueName)['QueueUrl']
        self.send_message(QueueUrl=url, MessageBody='hello')

    def _get_q(self, url):
        """ Helper method to quickly get a queue. """
        for q in self._queues.values():
            if q.url == url:
                return q
        raise Exception(f"Queue url {url} not found")

    def create_queue(self, QueueName=None, Attributes=None):
        q = self._queues[QueueName] = QueueMock(
            'https://sqs.us-east-1.amazonaws.com/xxx/' + QueueName,
            Attributes,
        )
        return {'QueueUrl': q.url}

    def list_queues(self, QueueNamePrefix=None):
        """ Return a list of queue urls """
        urls = (val.url for key, val in self._queues.items()
                if key.startswith(QueueNamePrefix))
        return {'QueueUrls': urls}

    def get_queue_url(self, QueueName=None):
        return self._queues[QueueName]

    def send_message(self, QueueUrl=None, MessageBody=None,
                     MessageAttributes=None):
        for q in self._queues.values():
            if q.url == QueueUrl:
                handle = ''.join(random.choice(string.ascii_lowercase) for
                                 x in range(10))
                q.messages.append({'Body': MessageBody,
                                   'ReceiptHandle': handle,
                                   'MessageAttributes': MessageAttributes})
                break

    def receive_message(self, QueueUrl=None, MaxNumberOfMessages=1,
                        WaitTimeSeconds=10):
        self._receive_messages_calls += 1
        for q in self._queues.values():
            if q.url == QueueUrl:
                msgs = q.messages[:MaxNumberOfMessages]
                q.messages = q.messages[MaxNumberOfMessages:]
                return {'Messages': msgs} if msgs else {}

    def get_queue_attributes(self, QueueUrl=None, AttributeNames=None):
        if 'ApproximateNumberOfMessages' in AttributeNames:
            count = len(self._get_q(QueueUrl).messages)
            return {'Attributes': {'ApproximateNumberOfMessages': count}}

    def purge_queue(self, QueueUrl=None):
        for q in self._queues.values():
            if q.url == QueueUrl:
                q.messages = []

    def delete_queue(self, QueueUrl=None):
        queue_name = None
        for key, val in self._queues.items():
            if val.url == QueueUrl:
                queue_name = key
                break
        if queue_name is None:
            raise Exception(f"Queue url {QueueUrl} not found")
        del self._queues[queue_name]


class test_Channel:

    def handleMessageCallback(self, message):
        self.callback_message = message

    def setup_method(self):
        """Mock the back-end SQS classes"""
        # Sanity check... if SQS is None, then it did not import and we
        # cannot execute our tests.
        SQS.Channel._queue_cache.clear()

        # Common variables used in the unit tests
        self.queue_name = 'unittest'

        # Mock the sqs() method that returns an SQSConnection object and
        # instead return an SQSConnectionMock() object.
        sqs_conn_mock = SQSClientMock()
        self.sqs_conn_mock = sqs_conn_mock

        predefined_queues_sqs_conn_mocks = {
            'queue-1': SQSClientMock(QueueName='queue-1'),
            'queue-2': SQSClientMock(QueueName='queue-2'),
            'queue-3.fifo': SQSClientMock(QueueName='queue-3.fifo')
        }

        def mock_sqs():
            def sqs(self, queue=None):
                if queue in predefined_queues_sqs_conn_mocks:
                    return predefined_queues_sqs_conn_mocks[queue]
                return sqs_conn_mock

            return sqs

        SQS.Channel.sqs = mock_sqs()

        # Set up a task exchange for passing tasks through the queue
        self.exchange = Exchange('test_SQS', type='direct')
        self.queue = Queue(self.queue_name, self.exchange, self.queue_name)

        # Mock up a test SQS Queue with the QueueMock class (and always
        # make sure its a clean empty queue)
        self.sqs_queue_mock = QueueMock('sqs://' + self.queue_name)

        # Now, create our Connection object with the SQS Transport and store
        # the connection/channel objects as references for use in these tests.
        self.connection = Connection(transport=SQS.Transport)
        self.channel = self.connection.channel()

        self.queue(self.channel).declare()
        self.producer = messaging.Producer(self.channel,
                                           self.exchange,
                                           routing_key=self.queue_name)

        # Lastly, make sure that we're set up to 'consume' this queue.
        self.channel.basic_consume(self.queue_name,
                                   no_ack=False,
                                   callback=self.handleMessageCallback,
                                   consumer_tag='unittest')

    def teardown_method(self):
        # Removes QoS reserved messages so we don't restore msgs on shutdown.
        try:
            qos = self.channel._qos
        except AttributeError:
            pass
        else:
            if qos:
                qos._dirty.clear()
                qos._delivered.clear()

    def test_init(self):
        """kombu.SQS.Channel instantiates correctly with mocked queues"""
        assert self.queue_name in self.channel._queue_cache

    def test_region(self):
        _environ = dict(os.environ)

        # when the region is unspecified
        connection = Connection(transport=SQS.Transport)
        channel = connection.channel()
        assert channel.transport_options.get('region') is None
        # the default region is us-east-1
        assert channel.region == 'us-east-1'

        # when boto3 picks a region
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-2'
        assert boto3.Session().region_name == 'us-east-2'
        # the default region should match
        connection = Connection(transport=SQS.Transport)
        channel = connection.channel()
        assert channel.region == 'us-east-2'

        # when transport_options are provided
        connection = Connection(transport=SQS.Transport, transport_options={
            'region': 'us-west-2'
        })
        channel = connection.channel()
        assert channel.transport_options.get('region') == 'us-west-2'
        # the specified region should be used
        assert connection.channel().region == 'us-west-2'

        os.environ.clear()
        os.environ.update(_environ)

    def test_endpoint_url(self):
        url = 'sqs://@localhost:5493'
        self.connection = Connection(hostname=url, transport=SQS.Transport)
        self.channel = self.connection.channel()
        self.channel._sqs = None
        expected_endpoint_url = 'http://localhost:5493'
        assert self.channel.endpoint_url == expected_endpoint_url
        boto3_sqs = SQS_Channel_sqs.__get__(self.channel, SQS.Channel)
        assert boto3_sqs()._endpoint.host == expected_endpoint_url

    def test_none_hostname_persists(self):
        conn = Connection(hostname=None, transport=SQS.Transport)
        assert conn.hostname == conn.clone().hostname

    def test_entity_name(self):
        assert self.channel.entity_name('foo') == 'foo'
        assert self.channel.entity_name('foo.bar-baz*qux_quux') == \
            'foo-bar-baz_qux_quux'
        assert self.channel.entity_name('abcdef.fifo') == 'abcdef.fifo'

    def test_resolve_queue_url(self):
        queue_name = 'unittest_queue'
        assert self.sqs_conn_mock._queues[queue_name].url == \
            self.channel._resolve_queue_url(queue_name)

    def test_new_queue(self):
        queue_name = 'new_unittest_queue'
        self.channel._new_queue(queue_name)
        assert queue_name in self.sqs_conn_mock._queues.keys()
        # For cleanup purposes, delete the queue and the queue file
        self.channel._delete(queue_name)

    def test_new_queue_custom_creation_attributes(self):
        self.connection.transport_options['sqs-creation-attributes'] = {
            'KmsMasterKeyId': 'alias/aws/sqs',
        }
        queue_name = 'new_custom_attribute_queue'
        self.channel._new_queue(queue_name)

        assert queue_name in self.sqs_conn_mock._queues.keys()
        queue = self.sqs_conn_mock._queues[queue_name]

        assert 'KmsMasterKeyId' in queue.creation_attributes
        assert queue.creation_attributes['KmsMasterKeyId'] == 'alias/aws/sqs'

        # For cleanup purposes, delete the queue and the queue file
        self.channel._delete(queue_name)

    def test_botocore_config_override(self):
        expected_connect_timeout = 5
        client_config = {'connect_timeout': expected_connect_timeout}
        self.connection = Connection(
            transport=SQS.Transport,
            transport_options={'client-config': client_config},
        )
        self.channel = self.connection.channel()
        self.channel._sqs = None
        boto3_sqs = SQS_Channel_sqs.__get__(self.channel, SQS.Channel)
        botocore_config = boto3_sqs()._client_config
        assert botocore_config.connect_timeout == expected_connect_timeout

    def test_dont_create_duplicate_new_queue(self):
        # All queue names start with "q", except "unittest_queue".
        # which is definitely out of cache when get_all_queues returns the
        # first 1000 queues sorted by name.
        queue_name = 'unittest_queue'
        # This should not create a new queue.
        self.channel._new_queue(queue_name)
        assert queue_name in self.sqs_conn_mock._queues.keys()
        queue = self.sqs_conn_mock._queues[queue_name]
        # The queue originally had 1 message in it.
        assert 1 == len(queue.messages)
        assert 'hello' == queue.messages[0]['Body']

    def test_delete(self):
        queue_name = 'new_unittest_queue'
        self.channel._new_queue(queue_name)
        self.channel._delete(queue_name)
        assert queue_name not in self.channel._queue_cache
        assert queue_name not in self.sqs_conn_mock._queues

    def test_get_from_sqs(self):
        # Test getting a single message
        message = 'my test message'
        self.producer.publish(message)
        result = self.channel._get(self.queue_name)
        assert 'body' in result.keys()

        # Now test getting many messages
        for i in range(3):
            message = f'message: {i}'
            self.producer.publish(message)

        self.channel._get_bulk(self.queue_name, max_if_unlimited=3)
        assert len(self.sqs_conn_mock._queues[self.queue_name].messages) == 0

    def test_get_with_empty_list(self):
        with pytest.raises(Empty):
            self.channel._get(self.queue_name)

    def test_get_bulk_raises_empty(self):
        with pytest.raises(Empty):
            self.channel._get_bulk(self.queue_name)

    def test_optional_b64_decode(self):
        raw = b'{"id": "4cc7438e-afd4-4f8f-a2f3-f46567e7ca77","task": "celery.task.PingTask",' \
              b'"args": [],"kwargs": {},"retries": 0,"eta": "2009-11-17T12:30:56.527191"}'
        b64_enc = base64.b64encode(raw)
        assert self.channel._optional_b64_decode(b64_enc) == raw
        assert self.channel._optional_b64_decode(raw) == raw
        assert self.channel._optional_b64_decode(b"test123") == b"test123"

    def test_messages_to_python(self):
        from kombu.asynchronous.aws.sqs.message import Message

        kombu_message_count = 3
        json_message_count = 3
        # Create several test messages and publish them
        for i in range(kombu_message_count):
            message = 'message: %s' % i
            self.producer.publish(message)

        # json formatted message NOT created by kombu
        for i in range(json_message_count):
            message = {'foo': 'bar'}
            self.channel._put(self.producer.routing_key, message)

        q_url = self.channel._new_queue(self.queue_name)
        # Get the messages now
        kombu_messages = []
        for m in self.sqs_conn_mock.receive_message(
                QueueUrl=q_url,
                MaxNumberOfMessages=kombu_message_count)['Messages']:
            m['Body'] = Message(body=m['Body']).decode()
            kombu_messages.append(m)
        json_messages = []
        for m in self.sqs_conn_mock.receive_message(
                QueueUrl=q_url,
                MaxNumberOfMessages=json_message_count)['Messages']:
            m['Body'] = Message(body=m['Body']).decode()
            json_messages.append(m)

        # Now convert them to payloads
        kombu_payloads = self.channel._messages_to_python(
            kombu_messages, self.queue_name,
        )
        json_payloads = self.channel._messages_to_python(
            json_messages, self.queue_name,
        )

        # We got the same number of payloads back, right?
        assert len(kombu_payloads) == kombu_message_count
        assert len(json_payloads) == json_message_count

        # Make sure they're payload-style objects
        for p in kombu_payloads:
            assert 'properties' in p
        for p in json_payloads:
            assert 'properties' in p

    def test_put_and_get(self):
        message = 'my test message'
        self.producer.publish(message)
        results = self.queue(self.channel).get().payload
        assert message == results

    def test_redelivered(self):
        self.channel.sqs().change_message_visibility = \
            Mock(name='change_message_visibility')
        message = {
            'redelivered': True,
            'properties': {'delivery_tag': 'test_message_id'}
        }
        self.channel._put(self.producer.routing_key, message)
        self.sqs_conn_mock.change_message_visibility.assert_called_once()

    def test_put_and_get_bulk(self):
        # With QoS.prefetch_count = 0
        message = 'my test message'
        self.producer.publish(message)
        self.channel.connection._deliver = Mock(name='_deliver')
        self.channel._get_bulk(self.queue_name)
        self.channel.connection._deliver.assert_called_once()

    def test_puts_and_get_bulk(self):
        # Generate 8 messages
        message_count = 8

        # Set the prefetch_count to 5
        self.channel.qos.prefetch_count = 5

        # Now, generate all the messages
        for i in range(message_count):
            message = 'message: %s' % i
            self.producer.publish(message)

        # Count how many messages are retrieved the first time. Should
        # be 5 (message_count).
        self.channel.connection._deliver = Mock(name='_deliver')
        self.channel._get_bulk(self.queue_name)
        assert self.channel.connection._deliver.call_count == 5
        for i in range(5):
            self.channel.qos.append(Mock(name=f'message{i}'), i)

        # Now, do the get again, the number of messages returned should be 1.
        self.channel.connection._deliver.reset_mock()
        self.channel._get_bulk(self.queue_name)
        self.channel.connection._deliver.assert_called_once()

    # hub required for successful instantiation of AsyncSQSConnection
    @pytest.mark.usefixtures('hub')
    def test_get_async(self):
        """Basic coverage of async code typically used via:
        basic_consume > _loop1 > _schedule_queue > _get_bulk_async"""
        # Prepare
        for i in range(3):
            message = 'message: %s' % i
            self.producer.publish(message)

        # SQS.Channel.asynsqs constructs AsyncSQSConnection using self.sqs
        # which is already a mock thanks to `setup` above, we just need to
        # mock the async-specific methods (as test_AsyncSQSConnection does)
        async_sqs_conn = self.channel.asynsqs(self.queue_name)
        async_sqs_conn.get_list = Mock(name='X.get_list')

        # Call key method
        self.channel._get_bulk_async(self.queue_name)

        assert async_sqs_conn.get_list.call_count == 1
        get_list_args = async_sqs_conn.get_list.call_args[0]
        get_list_kwargs = async_sqs_conn.get_list.call_args[1]
        assert get_list_args[0] == 'ReceiveMessage'
        assert get_list_args[1] == {
            'MaxNumberOfMessages': SQS.SQS_MAX_MESSAGES,
            'WaitTimeSeconds': self.channel.wait_time_seconds,
        }
        assert get_list_args[3] == \
            self.channel.sqs().get_queue_url(self.queue_name).url
        assert get_list_kwargs['parent'] == self.queue_name
        assert get_list_kwargs['protocol_params'] == {
            'json': {'AttributeNames': ['ApproximateReceiveCount']},
            'query': {'AttributeName.1': 'ApproximateReceiveCount'},
        }

    def test_fetch_message_attributes(self):
        self.connection.transport_options['fetch_message_attributes'] = ["Attribute1", "Attribute2"]
        async_sqs_conn = self.channel.asynsqs(self.queue_name)
        assert async_sqs_conn.fetch_message_attributes == ['Attribute1', 'Attribute2']

    def test_drain_events_with_empty_list(self):
        def mock_can_consume():
            return False
        self.channel.qos.can_consume = mock_can_consume
        with pytest.raises(Empty):
            self.channel.drain_events()

    def test_drain_events_with_prefetch_5(self):
        # Generate 20 messages
        message_count = 20
        prefetch_count = 5

        current_delivery_tag = [1]

        # Set the prefetch_count to 5
        self.channel.qos.prefetch_count = prefetch_count
        self.channel.connection._deliver = Mock(name='_deliver')

        def on_message_delivered(message, queue):
            current_delivery_tag[0] += 1
            self.channel.qos.append(message, current_delivery_tag[0])
        self.channel.connection._deliver.side_effect = on_message_delivered

        # Now, generate all the messages
        for i in range(message_count):
            self.producer.publish('message: %s' % i)

        # Now drain all the events
        for i in range(1000):
            try:
                self.channel.drain_events(timeout=0)
            except Empty:
                break
        else:
            assert False, 'disabled infinite loop'

        self.channel.qos._flush()
        assert len(self.channel.qos._delivered) == prefetch_count

        assert self.channel.connection._deliver.call_count == prefetch_count

    def test_drain_events_with_prefetch_none(self):
        # Generate 20 messages
        message_count = 20
        expected_receive_messages_count = 3

        current_delivery_tag = [1]

        # Set the prefetch_count to None
        self.channel.qos.prefetch_count = None
        self.channel.connection._deliver = Mock(name='_deliver')

        def on_message_delivered(message, queue):
            current_delivery_tag[0] += 1
            self.channel.qos.append(message, current_delivery_tag[0])
        self.channel.connection._deliver.side_effect = on_message_delivered

        # Now, generate all the messages
        for i in range(message_count):
            self.producer.publish('message: %s' % i)

        # Now drain all the events
        for i in range(1000):
            try:
                self.channel.drain_events(timeout=0)
            except Empty:
                break
        else:
            assert False, 'disabled infinite loop'

        assert self.channel.connection._deliver.call_count == message_count

        # How many times was the SQSConnectionMock receive_message method
        # called?
        assert (expected_receive_messages_count ==
                self.sqs_conn_mock._receive_messages_calls)

    def test_basic_ack(self, ):
        """Test that basic_ack calls the delete_message properly"""
        message = {
            'sqs_message': {
                'ReceiptHandle': '1'
            },
            'sqs_queue': 'testing_queue'
        }
        mock_messages = Mock()
        mock_messages.delivery_info = message
        self.channel.qos.append(mock_messages, 1)
        self.channel.sqs().delete_message = Mock()
        self.channel.basic_ack(1)
        self.sqs_conn_mock.delete_message.assert_called_with(
            QueueUrl=message['sqs_queue'],
            ReceiptHandle=message['sqs_message']['ReceiptHandle']
        )
        assert {1} == self.channel.qos._dirty

    @patch('kombu.transport.virtual.base.Channel.basic_ack')
    @patch('kombu.transport.virtual.base.Channel.basic_reject')
    def test_basic_ack_with_mocked_channel_methods(self, basic_reject_mock,
                                                   basic_ack_mock):
        """Test that basic_ack calls the delete_message properly"""
        message = {
            'sqs_message': {
                'ReceiptHandle': '1'
            },
            'sqs_queue': 'testing_queue'
        }
        mock_messages = Mock()
        mock_messages.delivery_info = message
        self.channel.qos.append(mock_messages, 1)
        self.channel.sqs().delete_message = Mock()
        self.channel.basic_ack(1)
        self.sqs_conn_mock.delete_message.assert_called_with(
            QueueUrl=message['sqs_queue'],
            ReceiptHandle=message['sqs_message']['ReceiptHandle']
        )
        basic_ack_mock.assert_called_with(1)
        assert not basic_reject_mock.called

    @patch('kombu.transport.virtual.base.Channel.basic_ack')
    @patch('kombu.transport.virtual.base.Channel.basic_reject')
    def test_basic_ack_without_sqs_message(self, basic_reject_mock,
                                           basic_ack_mock):
        """Test that basic_ack calls the delete_message properly"""
        message = {
            'sqs_queue': 'testing_queue'
        }
        mock_messages = Mock()
        mock_messages.delivery_info = message
        self.channel.qos.append(mock_messages, 1)
        self.channel.sqs().delete_message = Mock()
        self.channel.basic_ack(1)
        assert not self.sqs_conn_mock.delete_message.called
        basic_ack_mock.assert_called_with(1)
        assert not basic_reject_mock.called

    @patch('kombu.transport.virtual.base.Channel.basic_ack')
    @patch('kombu.transport.virtual.base.Channel.basic_reject')
    def test_basic_ack_invalid_receipt_handle(self, basic_reject_mock,
                                              basic_ack_mock):
        """Test that basic_ack calls the delete_message properly"""
        message = {
            'sqs_message': {
                'ReceiptHandle': '2'
            },
            'sqs_queue': 'testing_queue'
        }
        error_response = {
            'Error': {
                'Code': 'InvalidParameterValue',
                'Message': 'Value 2 for parameter ReceiptHandle is invalid.'
                           ' Reason: The receipt handle has expired.'
            }
        }
        operation_name = 'DeleteMessage'

        mock_messages = Mock()
        mock_messages.delivery_info = message
        self.channel.qos.append(mock_messages, 2)
        self.channel.sqs().delete_message = Mock()
        self.channel.sqs().delete_message.side_effect = ClientError(
            error_response=error_response,
            operation_name=operation_name
        )
        self.channel.basic_ack(2)
        self.sqs_conn_mock.delete_message.assert_called_with(
            QueueUrl=message['sqs_queue'],
            ReceiptHandle=message['sqs_message']['ReceiptHandle']
        )
        basic_reject_mock.assert_called_with(2)
        assert not basic_ack_mock.called

    @patch('kombu.transport.virtual.base.Channel.basic_ack')
    @patch('kombu.transport.virtual.base.Channel.basic_reject')
    def test_basic_ack_access_denied(self, basic_reject_mock, basic_ack_mock):
        """Test that basic_ack raises AccessDeniedQueueException when
           access is denied"""
        message = {
            'sqs_message': {
                'ReceiptHandle': '2'
            },
            'sqs_queue': 'testing_queue'
        }
        error_response = {
            'Error': {
                'Code': 'AccessDenied',
                'Message': """An error occurred (AccessDenied) when calling the
                              DeleteMessage operation."""
            }
        }
        operation_name = 'DeleteMessage'

        mock_messages = Mock()
        mock_messages.delivery_info = message
        self.channel.qos.append(mock_messages, 2)
        self.channel.sqs().delete_message = Mock()
        self.channel.sqs().delete_message.side_effect = ClientError(
            error_response=error_response,
            operation_name=operation_name
        )

        # Expecting the custom AccessDeniedQueueException to be raised
        with pytest.raises(SQS.AccessDeniedQueueException):
            self.channel.basic_ack(2)

        self.sqs_conn_mock.delete_message.assert_called_with(
            QueueUrl=message['sqs_queue'],
            ReceiptHandle=message['sqs_message']['ReceiptHandle']
        )
        assert not basic_reject_mock.called
        assert not basic_ack_mock.called

    def test_reject_when_no_predefined_queues(self):
        connection = Connection(transport=SQS.Transport, transport_options={})
        channel = connection.channel()

        mock_apply_backoff_policy = Mock()
        channel.qos.apply_backoff_policy = mock_apply_backoff_policy
        queue_name = "queue-1"

        exchange = Exchange('test_SQS', type='direct')
        queue = Queue(queue_name, exchange, queue_name)
        queue(channel).declare()

        message_mock = Mock()
        message_mock.delivery_info = {'routing_key': queue_name}
        channel.qos._delivered['test_message_id'] = message_mock
        channel.qos.reject('test_message_id')
        mock_apply_backoff_policy.assert_not_called()

    def test_predefined_queues_primes_queue_cache(self):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
        })
        channel = connection.channel()

        assert 'queue-1' in channel._queue_cache
        assert 'queue-2' in channel._queue_cache

    def test_predefined_queues_new_queue_raises_if_queue_not_exists(self):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
        })
        channel = connection.channel()

        with pytest.raises(SQS.UndefinedQueueException):
            channel._new_queue('queue-99')

    def test_predefined_queues_get_from_sqs(self):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
        })
        channel = connection.channel()

        def message_to_python(message, queue_name, queue):
            return message

        channel._message_to_python = Mock(side_effect=message_to_python)

        queue_name = "queue-1"

        exchange = Exchange('test_SQS', type='direct')
        p = messaging.Producer(channel, exchange, routing_key=queue_name)
        queue = Queue(queue_name, exchange, queue_name)
        queue(channel).declare()

        # Getting a single message
        p.publish('message')
        result = channel._get(queue_name)

        assert 'Body' in result.keys()

        # Getting many messages
        for i in range(3):
            p.publish(f'message: {i}')

        channel.connection._deliver = Mock(name='_deliver')
        channel._get_bulk(queue_name, max_if_unlimited=3)
        channel.connection._deliver.assert_called()

        assert len(channel.sqs(queue_name)._queues[queue_name].messages) == 0

    def test_predefined_queues_backoff_policy(self):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
        })
        channel = connection.channel()

        def apply_backoff_policy(
                queue_name, delivery_tag, retry_policy, backoff_tasks):
            return None

        mock_apply_policy = Mock(side_effect=apply_backoff_policy)
        channel.qos.apply_backoff_policy = mock_apply_policy
        queue_name = "queue-1"

        exchange = Exchange('test_SQS', type='direct')
        queue = Queue(queue_name, exchange, queue_name)
        queue(channel).declare()

        message_mock = Mock()
        message_mock.delivery_info = {'routing_key': queue_name}
        channel.qos._delivered['test_message_id'] = message_mock
        channel.qos.reject('test_message_id')
        mock_apply_policy.assert_called_once_with(
            'queue-1', 'test_message_id',
            {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640},
            ['svc.tasks.tasks.task1']
        )

    def test_predefined_queues_change_visibility_timeout(self):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
        })
        channel = connection.channel()

        def extract_task_name_and_number_of_retries(delivery_tag):
            return 'svc.tasks.tasks.task1', 2

        mock_extract_task_name_and_number_of_retries = Mock(
            side_effect=extract_task_name_and_number_of_retries)
        channel.qos.extract_task_name_and_number_of_retries = \
            mock_extract_task_name_and_number_of_retries

        queue_name = "queue-1"

        exchange = Exchange('test_SQS', type='direct')
        queue = Queue(queue_name, exchange, queue_name)
        queue(channel).declare()

        message_mock = Mock()
        message_mock.delivery_info = {'routing_key': queue_name}
        channel.qos._delivered['test_message_id'] = message_mock

        channel.sqs = Mock()
        sqs_queue_mock = Mock()
        channel.sqs.return_value = sqs_queue_mock
        channel.qos.reject('test_message_id')

        sqs_queue_mock.change_message_visibility.assert_called_once_with(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/xxx/queue-1',
            ReceiptHandle='test_message_id', VisibilityTimeout=20)

    def test_predefined_queues_put_to_fifo_queue(self):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
        })
        channel = connection.channel()

        queue_name = 'queue-3.fifo'

        exchange = Exchange('test_SQS', type='direct')
        p = messaging.Producer(channel, exchange, routing_key=queue_name)

        queue = Queue(queue_name, exchange, queue_name)
        queue(channel).declare()

        channel.sqs = Mock()
        sqs_queue_mock = Mock()
        channel.sqs.return_value = sqs_queue_mock
        p.publish('message')

        sqs_queue_mock.send_message.assert_called_once()
        assert 'MessageGroupId' in sqs_queue_mock.send_message.call_args[1]
        assert 'MessageDeduplicationId' in \
            sqs_queue_mock.send_message.call_args[1]

    def test_predefined_queues_put_to_queue(self):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
        })
        channel = connection.channel()

        queue_name = 'queue-2'

        exchange = Exchange('test_SQS', type='direct')
        p = messaging.Producer(channel, exchange, routing_key=queue_name)

        queue = Queue(queue_name, exchange, queue_name)
        queue(channel).declare()

        channel.sqs = Mock()
        sqs_queue_mock = Mock()
        channel.sqs.return_value = sqs_queue_mock
        p.publish('message', DelaySeconds=10)

        sqs_queue_mock.send_message.assert_called_once()

        assert 'DelaySeconds' in sqs_queue_mock.send_message.call_args[1]
        assert sqs_queue_mock.send_message.call_args[1]['DelaySeconds'] == 10

    @pytest.mark.parametrize('predefined_queues', (
        {
            'invalid-fifo-queue-name': {
                'url': 'https://sqs.us-east-1.amazonaws.com/xxx/queue.fifo',
                'access_key_id': 'a',
                'secret_access_key': 'b'
            }
        },
        {
            'standard-queue.fifo': {
                'url': 'https://sqs.us-east-1.amazonaws.com/xxx/queue',
                'access_key_id': 'a',
                'secret_access_key': 'b'
            }
        }
    ))
    def test_predefined_queues_invalid_configuration(self, predefined_queues):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': predefined_queues,
        })
        with pytest.raises(SQS.InvalidQueueException):
            connection.channel()

    def test_sts_new_session(self):
        # Arrange
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
            'sts_role_arn': 'test::arn'
        })
        channel = connection.channel()
        sqs = SQS_Channel_sqs.__get__(channel, SQS.Channel)
        queue_name = 'queue-1'

        mock_generate_sts_session_token = Mock()
        mock_new_sqs_client = Mock()
        channel.new_sqs_client = mock_new_sqs_client
        mock_generate_sts_session_token.side_effect = [
            {
                'Expiration': 123,
                'SessionToken': 123,
                'AccessKeyId': 123,
                'SecretAccessKey': 123
            }
        ]
        channel.generate_sts_session_token = mock_generate_sts_session_token

        # Act
        sqs(queue=queue_name)

        # Assert
        mock_generate_sts_session_token.assert_called_once()

    def test_sts_session_expired(self):
        # Arrange
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
            'sts_role_arn': 'test::arn'
        })
        channel = connection.channel()
        sqs = SQS_Channel_sqs.__get__(channel, SQS.Channel)
        channel.sts_expiration = datetime.utcnow() - timedelta(days=1)
        queue_name = 'queue-1'

        mock_generate_sts_session_token = Mock()
        mock_new_sqs_client = Mock()
        channel.new_sqs_client = mock_new_sqs_client
        mock_generate_sts_session_token.side_effect = [
            {
                'Expiration': 123,
                'SessionToken': 123,
                'AccessKeyId': 123,
                'SecretAccessKey': 123
            }
        ]
        channel.generate_sts_session_token = mock_generate_sts_session_token

        # Act
        sqs(queue=queue_name)

        # Assert
        mock_generate_sts_session_token.assert_called_once()

    def test_sts_session_not_expired(self):
        # Arrange
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
            'sts_role_arn': 'test::arn'
        })
        channel = connection.channel()
        channel.sts_expiration = datetime.utcnow() + timedelta(days=1)
        queue_name = 'queue-1'

        mock_generate_sts_session_token = Mock()
        mock_new_sqs_client = Mock()
        channel.new_sqs_client = mock_new_sqs_client
        channel._predefined_queue_clients = {queue_name: 'mock_client'}
        mock_generate_sts_session_token.side_effect = [
            {
                'Expiration': 123,
                'SessionToken': 123,
                'AccessKeyId': 123,
                'SecretAccessKey': 123
            }
        ]
        channel.generate_sts_session_token = mock_generate_sts_session_token

        # Act
        channel.sqs(queue=queue_name)

        # Assert
        mock_generate_sts_session_token.assert_not_called()

    def test_sts_session_with_multiple_predefined_queues(self):
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
            'sts_role_arn': 'test::arn'
        })
        channel = connection.channel()
        sqs = SQS_Channel_sqs.__get__(channel, SQS.Channel)

        mock_generate_sts_session_token = Mock()
        mock_new_sqs_client = Mock()
        channel.new_sqs_client = mock_new_sqs_client
        mock_generate_sts_session_token.return_value = {
            'Expiration': datetime.utcnow() + timedelta(days=1),
            'SessionToken': 123,
            'AccessKeyId': 123,
            'SecretAccessKey': 123
        }

        channel.generate_sts_session_token = mock_generate_sts_session_token

        # Act
        sqs(queue='queue-1')
        sqs(queue='queue-2')

        # Assert
        mock_generate_sts_session_token.assert_called()
        mock_new_sqs_client.assert_called()

    def test_message_attribute(self):
        message = 'my test message'
        self.producer.publish(message, message_attributes={
            'Attribute1': {'DataType': 'String',
                           'StringValue': 'STRING_VALUE'}
        }
        )
        output_message = self.queue(self.channel).get()
        assert message == output_message.payload
        # It's not propagated to the properties
        assert 'message_attributes' not in output_message.properties
