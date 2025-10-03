"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import random
import string
from datetime import datetime, timedelta, timezone
from queue import Empty
from unittest.mock import Mock, call, patch

import pytest

from kombu import Connection, Exchange, Queue, messaging
from kombu.exceptions import KombuError
from kombu.transport.SQS import (UndefinedExchangeException,
                                 UndefinedQueueException, _SnsFanout,
                                 _SnsSubscription)

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
    "queue-3.fifo": {
        "url": "https://sqs.us-east-1.amazonaws.com/xxx/queue-3.fifo",
        "access_key_id": "e",
        "secret_access_key": "f",
    },
}

example_predefined_exchanges = {
    "exchange-1": {
        "arn": "arn:aws:sns:us-east-1:xxx:exchange-1",
        "access_key_id": "a",
        "secret_access_key": "b",
    },
    "exchange-2.fifo": {
        "arn": "arn:aws:sns:us-east-1:xxx:exchange-2",
        "access_key_id": "a",
        "secret_access_key": "b",
    },
}


@pytest.fixture
def connection_fixture():
    return Connection(
        transport=SQS.Transport,
        transport_options={
            "predefined_queues": example_predefined_queues,
        },
    )


@pytest.fixture
def channel_fixture(connection_fixture) -> SQS.Channel:
    chan = connection_fixture.channel()
    chan.region = "some-aws-region"
    return chan


@pytest.fixture
def mock_sqs():
    with patch("kombu.transport.SQS.Channel.sqs") as mock:
        mock.name = "Sqs client mock"
        yield mock


@pytest.fixture
def mock_fanout():
    with patch("kombu.transport.SQS.Channel.fanout") as mock:
        yield mock


@pytest.fixture
def mock_new_sqs_client():
    with patch("kombu.transport.SQS.Channel.new_sqs_client") as mock:
        yield mock


@pytest.fixture
def sns_fanout(channel_fixture):
    inst = _SnsFanout(channel_fixture)

    # Clear previous class vars
    inst._predefined_clients = {}
    inst._topic_arn_cache = {}
    inst._exchange_topic_cache = {}

    return inst


@pytest.fixture
def sns_subscription(sns_fanout):
    inst = _SnsSubscription(sns_fanout)

    # Clear previous class vars
    inst._queue_arn_cache = {}
    inst._subscription_arn_cache = {}

    return inst


class SQSMessageMock:
    def __init__(self):
        """
        Imitate the SQS Message from boto3.
        """
        self.body = ""
        self.receipt_handle = "receipt_handle_xyz"


class QueueMock:
    """ Hold information about a queue. """

    def __init__(self, url, creation_attributes=None, tags=None):
        self.url = url
        # arguments of boto3.sqs.create_queue
        self.creation_attributes = creation_attributes
        self.tags = tags
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

    def create_queue(self, QueueName=None, Attributes=None, tags=None):
        q = self._queues[QueueName] = QueueMock(
            'https://sqs.us-east-1.amazonaws.com/xxx/' + QueueName,
            Attributes,
            tags,
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

    def receive_message(
        self,
        QueueUrl=None,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
        MessageAttributeNames=None,
        MessageSystemAttributeNames=None
    ):
        self._receive_messages_calls += 1
        for q in self._queues.values():
            if q.url == QueueUrl:
                msgs = q.messages[:MaxNumberOfMessages]
                q.messages = q.messages[MaxNumberOfMessages:]
                for msg in msgs:
                    msg['Attributes'] = {
                        "SenderId": "AIDAEXAMPLE123ABC",
                        "SentTimestamp": "1638368280000"
                    }
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

        # when the region is unspecified, and Boto3 also does not have a region set
        with patch("kombu.transport.SQS.boto3.Session") as boto_session_mock:
            boto_session_mock().region_name = None
            connection = Connection(transport=SQS.Transport)
            channel = connection.channel()
            assert channel.transport_options.get("region") is None
            # the default region is us-east-1
            assert channel.region == "us-east-1"

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
        # Reset transport options to avoid leaking state into other tests
        self.connection.transport_options.pop('sqs-creation-attributes', None)

    def test_new_queue_with_tags(self):
        self.connection.transport_options['queue_tags'] = {
            'Environment': 'test',
            'Team': 'backend',
        }
        queue_name = 'new_tagged_queue'
        self.channel._new_queue(queue_name)

        assert queue_name in self.sqs_conn_mock._queues.keys()
        queue = self.sqs_conn_mock._queues[queue_name]

        assert queue.tags is not None
        assert queue.tags['Environment'] == 'test'
        assert queue.tags['Team'] == 'backend'

        # For cleanup purposes, delete the queue and the queue file
        self.channel._delete(queue_name)
        # Reset transport options to avoid leaking state into other tests
        self.connection.transport_options.pop('queue_tags', None)

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

    def test_delete_with_no_exchange(
        self, mock_fanout, channel_fixture, connection_fixture
    ):
        # Arrange
        queue_name = "queue-1"
        channel_fixture.supports_fanout = True
        channel_fixture.predefined_queues = {}
        self.sqs_conn_mock._queues[queue_name] = QueueMock(
            url="https://sqs.us-east-1.amazonaws.com/xxx/queue-1"
        )
        channel_fixture._new_queue(queue_name)
        assert queue_name in channel_fixture._queue_cache

        # Act
        channel_fixture._delete(queue_name)

        # Assert
        assert mock_fanout.unsubscribe_queue.call_count == 0
        assert queue_name not in channel_fixture._queue_cache
        assert queue_name not in self.sqs_conn_mock._queues

    def test_delete_with_fanout_exchange(
        self, mock_fanout, channel_fixture, connection_fixture
    ):
        # Arrange
        queue_name = "queue-1"
        channel_fixture.supports_fanout = True
        channel_fixture.predefined_queues = {}
        self.sqs_conn_mock._queues[queue_name] = QueueMock(
            url="https://sqs.us-east-1.amazonaws.com/xxx/queue-1"
        )

        # Declare fanout exchange and queue
        exchange_name = "test_SQS_fanout"
        exchange = Exchange(exchange_name, type="fanout")
        queue = Queue("queue-1", exchange)
        queue(channel_fixture).declare()
        assert queue_name in channel_fixture._queue_cache

        # Act
        channel_fixture._delete(queue_name, exchange_name)

        # Assert
        assert mock_fanout.subscriptions.unsubscribe_queue.call_args_list == [
            call(queue_name="queue-1", exchange_name="test_SQS_fanout")
        ]
        assert queue_name not in channel_fixture._queue_cache
        assert queue_name not in self.sqs_conn_mock._queues

    def test_get_from_sqs(self):
        # Test getting a single message
        message = 'my test message'
        self.producer.publish(message)
        result = self.channel._get(self.queue_name)
        assert 'body' in result.keys()

        # Now test is getting many messages
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

    @pytest.mark.parametrize(
        "raw, expected",
        [
            # valid Base64 == decodes
            (b"VHJ1ZQ==", b"True"),
            (b"YWJjZA==", b"abcd"),
            # with surrounding whitespace/newline → still decodes
            (b"\nVHJ1ZQ==  ", b"True"),

            # “plain text” that happens to be length of 4 == no decode
            (b"True", b"True"),
            (b"abcd", b"abcd"),

            # wrong length == no decode
            (b"abc", b"abc"),
            (b"abcde", b"abcde"),

            # invalid chars == no decode
            (b"@@@@", b"@@@@"),

            # other
            (b'9', b'9'),
            (b'8.15', b'8.15'),
            (b'[1, 2, 3]', b'[1, 2, 3]'),
            (b'1234', b'1234'),

            # json/dict (encoded and raw)
            (
                b'{"id": "4cc7438e-afd4-4f8f-a2f3-f46567e7ca77","task": "celery.task.PingTask",'
                b'"args": [],"kwargs": {},"retries": 0,"eta": "2009-11-17T12:30:56.527191"}',
                b'{"id": "4cc7438e-afd4-4f8f-a2f3-f46567e7ca77","task": "celery.task.PingTask",'
                b'"args": [],"kwargs": {},"retries": 0,"eta": "2009-11-17T12:30:56.527191"}'
            ),
            (
                base64.b64encode(
                    b'{"id": "4cc7438e-afd4-4f8f-a2f3-f46567e7ca77","task": "celery.task.PingTask",'
                    b'"args": [],"kwargs": {},"retries": 0,"eta": "2009-11-17T12:30:56.527191"}'),
                b'{"id": "4cc7438e-afd4-4f8f-a2f3-f46567e7ca77","task": "celery.task.PingTask",'
                b'"args": [],"kwargs": {},"retries": 0,"eta": "2009-11-17T12:30:56.527191"}'
            )

        ],
    )
    def test_optional_b64_decode(self, raw, expected):
        result = self.channel._optional_b64_decode(raw)
        assert result == expected

    @pytest.mark.parametrize(
        "text, expected",
        [
            ('{"a":1}', {"a": 1}),
            ("[1,2,3]", {}),
            ("not json", {}),
            ('{"x":"y"}', {"x": "y"}),
            ("8", {}),
        ],
    )
    def test_prepare_json_payload(self, text, expected):
        result = self.channel._prepare_json_payload(text)
        assert result == expected

    @pytest.mark.parametrize(
        "queue_name, message, new_q_url",
        [
            ("q1", {"ReceiptHandle": "rh1"}, "new-url-1"),
            ("another", {"ReceiptHandle": "handle-2"}, "new-url-2"),
        ],
    )
    def test_delete_message(self, queue_name, message, new_q_url, monkeypatch):
        monkeypatch.setattr(self.channel, "_new_queue", lambda q: new_q_url)
        fake_sqs = Mock()
        monkeypatch.setattr(self.channel, "asynsqs", lambda queue: fake_sqs)

        self.channel._delete_message(queue_name, message)

        fake_sqs.delete_message.assert_called_once_with(
            new_q_url,
            message["ReceiptHandle"]
        )

    @pytest.mark.parametrize(
        "initial_payload, raw_text, message, q_url, expect_body",
        [
            # No 'properties'
            (
                {},
                "raw string",
                {"ReceiptHandle": "RH"},
                "http://queue.url",
                True,
            ),
            # Existing 'properties'
            (
                {"properties": {"delivery_info": {"foo": "bar"}}},
                "ignored",
                {"ReceiptHandle": "TAG"},
                "https://q.url",
                False,
            ),
        ],
    )
    def test_envelope_payload(self, initial_payload, raw_text, message, q_url, expect_body):
        payload = initial_payload.copy()
        result = self.channel._envelope_payload(payload, raw_text, message, q_url)

        if expect_body:
            assert result["body"] == raw_text
        else:
            assert "body" not in result

        di = result["properties"]["delivery_info"]
        assert di["sqs_message"] is message
        assert di["sqs_queue"] == q_url
        if not expect_body:
            assert di.get("foo") == "bar"

        assert result["properties"]["delivery_tag"] == message["ReceiptHandle"]

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

    @pytest.mark.parametrize('body', [
        '',
        'True',
        'False',
        '[]',
        '{}',
        '1',
        '1.0',
        'This is the body as a string'
    ])
    def test_messages_to_python_body_as_a_string(self, body):
        from kombu.asynchronous.aws.sqs.message import Message
        q_url = self.channel._new_queue(self.queue_name)
        msg_attributes = {
            'S3MessageBodyKey': '(the-test-bucket-name)the-test-key-body',
            'python_test_attr': 'python_test_attr_value'
        }
        attributes = {"SenderId": "AIDAEXAMPLE123ABC", "SentTimestamp": "1638368280000"}
        self.sqs_conn_mock.send_message(
            q_url,
            MessageBody=body,
            MessageAttributes=msg_attributes
        )

        received_messages = []
        for m in self.sqs_conn_mock.receive_message(
            QueueUrl=q_url,
            MaxNumberOfMessages=1
        )['Messages']:
            m['Body'] = Message(body=m['Body']).decode()
            received_messages.append(m)

        formatted_messages = self.channel._messages_to_python(
            received_messages,
            self.queue_name
        )

        for msg in formatted_messages:
            delivery_info = msg['properties']['delivery_info']

            assert msg['body'] == body
            assert delivery_info['sqs_message']['Body'] == body
            assert delivery_info['sqs_message']['Attributes'] == attributes
            assert delivery_info['sqs_message']['MessageAttributes'] == msg_attributes

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
        self.sqs_conn_mock.change_message_visibility.assert_called_once_with(
            QueueUrl='https://sqs.us-east-1.amazonaws.com/xxx/unittest',
            ReceiptHandle='test_message_id', VisibilityTimeout=10)

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
            'json': {'MessageSystemAttributeNames': ['ApproximateReceiveCount']},
            'query': {'MessageSystemAttributeName.1': 'ApproximateReceiveCount'},
        }

    @pytest.mark.parametrize('fetch_attributes,expected', [
        # as a list for backwards compatibility
        (
            None,
            {'message_system_attribute_names': ['ApproximateReceiveCount'], 'message_attribute_names': []}
        ),
        (
            'incorrect_value',
            {'message_system_attribute_names': ['ApproximateReceiveCount'], 'message_attribute_names': []}
        ),
        (
            [],
            {'message_system_attribute_names': ['ApproximateReceiveCount'], 'message_attribute_names': []}
        ),
        (
            ['ALL'],
            {'message_system_attribute_names': ['ALL'], 'message_attribute_names': []}
        ),
        (
            ['SenderId', 'SentTimestamp'],
            {
                'message_system_attribute_names': ['SenderId', 'ApproximateReceiveCount', 'SentTimestamp'],
                'message_attribute_names': []
            }
        ),
        # As a dict using only System Attributes
        (
            {'MessageSystemAttributeNames': ['All']},
            {
                'message_system_attribute_names': ['ALL'],
                'message_attribute_names': []
            }
        ),
        (
            {'MessageSystemAttributeNames': ['SenderId', 'SentTimestamp']},
            {
                'message_system_attribute_names': ['SenderId', 'ApproximateReceiveCount', 'SentTimestamp'],
                'message_attribute_names': []
            }
        ),
        (
            {'MessageSystemAttributeNames_BAD_KEY': ['That', 'This']},
            {
                'message_system_attribute_names': ['ApproximateReceiveCount'],
                'message_attribute_names': []
            }
        ),
        # As a dict using only Message Attributes
        (
            {'MessageAttributeNames': ['All']},
            {
                'message_system_attribute_names': ['ApproximateReceiveCount'],
                'message_attribute_names': ["ALL"]
            }
        ),
        (
            {'MessageAttributeNames': ['CustomProp', 'CustomProp2']},
            {
                'message_system_attribute_names': ['ApproximateReceiveCount'],
                'message_attribute_names': ['CustomProp', 'CustomProp2']
            }
        ),
        (
            {'MessageAttributeNames_BAD_KEY': ['That', 'This']},
            {
                'message_system_attribute_names': ['ApproximateReceiveCount'],
                'message_attribute_names': []
            }
        ),
        # all together now...
        (
            {
                'MessageSystemAttributeNames': ['SenderId', 'SentTimestamp'],
                'MessageAttributeNames': ['CustomProp', 'CustomProp2']},
            {
                'message_system_attribute_names': ['SenderId', 'SentTimestamp', 'ApproximateReceiveCount'],
                'message_attribute_names': ['CustomProp', 'CustomProp2']
            }
        ),
    ])
    @pytest.mark.usefixtures('hub')
    def test_fetch_message_attributes(self, fetch_attributes, expected):
        self.connection.transport_options['fetch_message_attributes'] = fetch_attributes  # type: ignore
        async_sqs_conn = self.channel.asynsqs(self.queue_name)
        assert async_sqs_conn.message_system_attribute_names == sorted(expected['message_system_attribute_names'])
        assert async_sqs_conn.message_attribute_names == expected['message_attribute_names']

    @pytest.mark.usefixtures('hub')
    def test_fetch_message_attributes_does_not_exist(self):
        self.connection.transport_options = {}
        async_sqs_conn = self.channel.asynsqs(self.queue_name)
        assert async_sqs_conn.message_system_attribute_names == ['ApproximateReceiveCount']
        assert async_sqs_conn.message_attribute_names == []

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

    def test_sts_new_session_with_buffer_time(self):
        # Arrange
        sts_token_timeout = 900
        sts_token_buffer_time = 60
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
            'sts_role_arn': 'test::arn',
            'sts_token_timeout': sts_token_timeout,
            'sts_token_buffer_time': sts_token_buffer_time,
        })
        channel = connection.channel()
        sqs = SQS_Channel_sqs.__get__(channel, SQS.Channel)
        queue_name = 'queue-1'

        mock_generate_sts_session_token = Mock()
        mock_new_sqs_client = Mock()
        channel.new_sqs_client = mock_new_sqs_client

        expiration_time = datetime.now(timezone.utc) + timedelta(seconds=sts_token_timeout)

        mock_generate_sts_session_token.side_effect = [
            {
                'Expiration': expiration_time,
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
        assert channel.sts_expiration == expiration_time - timedelta(seconds=sts_token_buffer_time)

    def test_sts_session_expired(self):
        # Arrange
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
            'sts_role_arn': 'test::arn'
        })
        channel = connection.channel()
        sqs = SQS_Channel_sqs.__get__(channel, SQS.Channel)
        channel.sts_expiration = datetime.now(timezone.utc) - timedelta(days=1)
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

    def test_sts_session_expired_with_buffer_time(self):
        # Arrange
        sts_token_timeout = 900
        sts_token_buffer_time = 60
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
            'sts_role_arn': 'test::arn',
            'sts_token_timeout': sts_token_timeout,
            'sts_token_buffer_time': sts_token_buffer_time,
        })
        channel = connection.channel()
        sqs = SQS_Channel_sqs.__get__(channel, SQS.Channel)
        channel.sts_expiration = datetime.now(timezone.utc) - timedelta(days=1)
        queue_name = 'queue-1'

        mock_generate_sts_session_token = Mock()
        mock_new_sqs_client = Mock()
        channel.new_sqs_client = mock_new_sqs_client

        expiration_time = datetime.now(timezone.utc) + timedelta(seconds=sts_token_timeout)

        mock_generate_sts_session_token.side_effect = [
            {
                'Expiration': expiration_time,
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
        assert channel.sts_expiration == expiration_time - timedelta(seconds=sts_token_buffer_time)

    def test_sts_session_not_expired(self):
        # Arrange
        connection = Connection(transport=SQS.Transport, transport_options={
            'predefined_queues': example_predefined_queues,
            'sts_role_arn': 'test::arn'
        })
        channel = connection.channel()
        channel.sts_expiration = datetime.now(timezone.utc) + timedelta(days=1)
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
            'Expiration': datetime.now(timezone.utc) + timedelta(days=1),
            'SessionToken': 123,
            'AccessKeyId': 123,
            'SecretAccessKey': 123
        }

        channel.generate_sts_session_token = mock_generate_sts_session_token

        # Act
        sqs(queue="queue-1")
        sqs(queue="queue-2")

        # Call queue a second time to check new STS token is not generated
        sqs(queue="queue-2")

        # Assert
        assert mock_generate_sts_session_token.call_count == 2
        assert mock_new_sqs_client.call_count == 2

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
        assert "message_attributes" not in output_message.properties

    def test_exchange_is_fanout_no_defined_queues(self, channel_fixture):
        # Act & assert
        assert channel_fixture._exchange_is_fanout("queue-1") is False

    def test_exchange_is_fanout_with_fanout_exchange(
        self, channel_fixture: SQS.Channel, mock_fanout
    ):
        # Arrange
        channel_fixture.supports_fanout = True

        # One fanout exchange and queue
        exchange = Exchange("test_SQS_fanout", type="fanout")
        queue = Queue("queue-1", exchange)
        queue(channel_fixture).declare()

        # One direct exchange and queue
        exchange = Exchange("test_SQS", type="direct")
        queue = Queue("queue-2", exchange)
        queue(channel_fixture).declare()

        # Act & Assert
        assert channel_fixture._exchange_is_fanout("test_SQS_fanout") is True
        assert channel_fixture._exchange_is_fanout("test_SQS") is False

    def test_get_exchange_for_queue_with_defined_queue(
        self, channel_fixture: SQS.Channel, mock_fanout
    ):
        # Arrange
        exchange = Exchange("test_SQS_fanout", type="fanout")
        queue = Queue("queue-1", exchange)
        queue(channel_fixture).declare()

        # Act
        result = channel_fixture._get_exchange_for_queue("queue-1")

        # Assert
        assert result == "test_SQS_fanout"

    def test_get_exchange_for_queue_with_queue_not_defined(
        self, channel_fixture: SQS.Channel, mock_fanout
    ):
        # Arrange
        exchange = Exchange("test_SQS_fanout", type="fanout")
        queue = Queue("queue-1", exchange)
        queue(channel_fixture).declare()

        # Act
        with pytest.raises(
            UndefinedQueueException, match="Queue 'queue-2' has not been defined."
        ):
            channel_fixture._get_exchange_for_queue("queue-2")

    def test_remove_stale_sns_subscriptions_no_defined_queues(
        self, mock_fanout, channel_fixture
    ):
        # Arrange
        mock_fanout.subscriptions.cleanup.return_value = "This should not be returned"

        # Act
        result = channel_fixture.remove_stale_sns_subscriptions("queue-1")

        # Assert
        assert result is None
        assert mock_fanout.subscriptions.cleanup.call_count == 0

    def test_remove_stale_sns_subscriptions_with_fanout_exchange(
        self, mock_fanout, channel_fixture: SQS.Channel
    ):
        # Arrange
        mock_fanout.subscriptions.cleanup.return_value = None
        channel_fixture.supports_fanout = True

        exchange = Exchange("test_SQS_fanout", type="fanout")
        queue = Queue("queue-1", exchange)
        queue(channel_fixture).declare()

        # Act
        result = channel_fixture.remove_stale_sns_subscriptions("test_SQS_fanout")

        # Assert
        assert result is None
        assert mock_fanout.subscriptions.cleanup.call_count == 1

    def test_subscribe_queue_to_fanout_exchange_if_required_with_fanout(
        self, mock_fanout, channel_fixture: SQS.Channel
    ):
        # Arrange
        mock_fanout.subscriptions.subscribe_queue.return_value = None
        channel_fixture.supports_fanout = True

        exchange = Exchange("test_SQS_fanout", type="fanout")
        queue = Queue("queue-1", exchange)
        queue(channel_fixture).declare()

        # Act
        result = channel_fixture._subscribe_queue_to_fanout_exchange_if_required(
            "queue-1"
        )

        # Assert
        assert result is None
        assert mock_fanout.subscriptions.subscribe_queue.call_args_list == [
            call(queue_name="queue-1", exchange_name="test_SQS_fanout")
        ]

    def test_subscribe_queue_to_fanout_exchange_if_required_without_fanout(
        self, mock_fanout, channel_fixture: SQS.Channel
    ):
        # Arrange
        mock_fanout.cleanup.return_value = None
        channel_fixture.supports_fanout = True

        exchange = Exchange("test_SQS_fanout", type="fanout")
        queue = Queue("queue-1", exchange)
        queue(channel_fixture).declare()

        exchange = Exchange("test_SQS", type="direct")
        queue = Queue("queue-2", exchange)
        queue(channel_fixture).declare()

        # Act
        result = channel_fixture._subscribe_queue_to_fanout_exchange_if_required(
            "queue-2"
        )

        # Assert
        assert result is None
        assert mock_fanout.subscribe_queue.call_count == 0

    def test_subscribe_queue_to_fanout_exchange_if_required_not_defined(
        self, mock_fanout, channel_fixture: SQS.Channel, caplog
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)
        mock_fanout.cleanup.return_value = None
        channel_fixture.supports_fanout = True

        exchange = Exchange("test_SQS_fanout", type="fanout")
        queue = Queue("queue-1", exchange)
        queue(channel_fixture).declare()

        # Act
        result = channel_fixture._subscribe_queue_to_fanout_exchange_if_required(
            "queue-2"
        )

        # Assert
        assert result is None
        assert mock_fanout.subscribe_queue.call_count == 0
        assert (
            "Not subscribing queue 'queue-2' to fanout exchange: Queue 'queue-2' has"
            " not been defined."
        ) in caplog.text

    @patch(
        "kombu.transport.SQS.uuid.uuid4",
        return_value="70c8cdfc-9bec-4d20-bbe1-3c155b794467",
    )
    def test_put_fanout_fifo_queue(
        self, _uuid_mock, mock_fanout, channel_fixture: SQS.Channel
    ):
        # Arrange
        message = {"key1": "This is a value", "key2": 123, "key3": True}

        # Act
        channel_fixture._put_fanout("queue-1.fifo", message, "")

        # Assert
        assert mock_fanout.publish.call_args_list == [
            call(
                exchange_name="queue-1.fifo",
                message='{"key1": "This is a value", "key2": 123, "key3": true}',
                message_attributes=None,
                request_params={
                    "MessageGroupId": "default",
                    "MessageDeduplicationId": "70c8cdfc-9bec-4d20-bbe1-3c155b794467",
                },
            )
        ]

    def test_put_fanout_fifo_queue_custom_msg_groups(
        self, mock_fanout, channel_fixture: SQS.Channel
    ):
        # Arrange
        message = {
            "key1": "This is a value",
            "key2": 123,
            "key3": True,
            "properties": {
                "MessageGroupId": "ThisIsNotDefault",
                "MessageDeduplicationId": "MyDedupId",
            },
        }

        # Act
        channel_fixture._put_fanout("queue-1.fifo", message, "")

        # Assert
        assert mock_fanout.publish.call_args_list == [
            call(
                exchange_name="queue-1.fifo",
                message=(
                    '{"key1": "This is a value", "key2": 123, "key3": true,'
                    ' "properties": {"MessageGroupId": "ThisIsNotDefault", '
                    '"MessageDeduplicationId": "MyDedupId"}}'
                ),
                message_attributes=None,
                request_params={
                    "MessageGroupId": "ThisIsNotDefault",
                    "MessageDeduplicationId": "MyDedupId",
                },
            )
        ]

    def test_put_fanout_non_fifo_queue(self, mock_fanout, channel_fixture: SQS.Channel):
        # Arrange
        message = {"key1": "This is a value", "key2": 123, "key3": True}

        # Act
        channel_fixture._put_fanout("queue-1", message, "")

        # Assert
        assert mock_fanout.publish.call_args_list == [
            call(
                exchange_name="queue-1",
                message='{"key1": "This is a value", "key2": 123, "key3": true}',
                message_attributes=None,
                request_params={},
            )
        ]

    def test_put_fanout_with_msg_attrs(self, mock_fanout, channel_fixture: SQS.Channel):
        # Arrange
        message = {
            "key1": "This is a value",
            "key2": 123,
            "key3": True,
            "properties": {
                "message_attributes": {"attr1": "my-attribute-value", "attr2": 123},
            },
        }

        # Act
        channel_fixture._put_fanout("queue-1", message, "")

        # Assert
        assert mock_fanout.publish.call_args_list == [
            call(
                exchange_name="queue-1",
                message=(
                    '{"key1": "This is a value", "key2": 123, "key3": true, '
                    '"properties": {"message_attributes": {"attr1": '
                    '"my-attribute-value", "attr2": 123}}}'
                ),
                message_attributes={"attr1": "my-attribute-value", "attr2": 123},
                request_params={},
            )
        ]

    @pytest.mark.parametrize(
        "sf_transport_value, expected_result",
        [(True, True), (False, False), (None, False)],
    )
    def test_supports_fanout(
        self, sf_transport_value, expected_result, channel_fixture
    ):
        # Arrange
        if sf_transport_value is not None:
            channel_fixture.transport_options["supports_fanout"] = sf_transport_value

        # Act & Assert
        assert channel_fixture.supports_fanout == expected_result

    def test_sqs_client_already_initialised(self, channel_fixture, mock_new_sqs_client):
        # Arrange
        sqs_client_mock = Mock(name="My SQS client")
        channel_fixture._sqs = sqs_client_mock

        # Act
        result = SQS_Channel_sqs.__get__(channel_fixture, SQS.Channel)()

        # Assert
        assert result is sqs_client_mock
        assert mock_new_sqs_client.call_count == 0

    def test_sqs_client_predefined_queue_not_defined(
        self, channel_fixture, mock_new_sqs_client
    ):
        # Arrange
        channel_fixture._sqs = None

        # Act
        with pytest.raises(
            UndefinedQueueException,
            match="Queue with name 'queue-4' must be defined in 'predefined_queues'.",
        ):
            SQS_Channel_sqs.__get__(channel_fixture, SQS.Channel)(queue="queue-4")

        # Assert
        assert mock_new_sqs_client.call_count == 0

    def test_sqs_client_predefined_queue_already_has_client(
        self, channel_fixture, mock_new_sqs_client
    ):
        # Arrange
        mock_client = Mock(name="My SQS client")
        channel_fixture._sqs = None
        channel_fixture._predefined_queue_clients["queue-1"] = mock_client

        # Act
        result = SQS_Channel_sqs.__get__(channel_fixture, SQS.Channel)(queue="queue-1")

        # Assert
        assert result == mock_client
        assert mock_new_sqs_client.call_count == 0

    def test_sqs_client_predefined_queue_does_not_have_client(
        self, channel_fixture, mock_new_sqs_client
    ):
        # Arrange
        queue_2_client = Mock(name="My new SQS client")
        queue_1_client = Mock(name="A different SQS client")
        channel_fixture._sqs = None
        channel_fixture._predefined_queue_clients = {"queue-1": queue_1_client}
        mock_new_sqs_client.return_value = queue_2_client

        # Act
        result = SQS_Channel_sqs.__get__(channel_fixture, SQS.Channel)(queue="queue-2")

        # Assert
        assert channel_fixture._predefined_queue_clients == {
            "queue-1": queue_1_client,
            "queue-2": queue_2_client,
        }
        assert result == queue_2_client
        assert mock_new_sqs_client.call_args_list == [
            call(region="some-aws-region", access_key_id="c", secret_access_key="d")
        ]

    def test_fanout_instance_already_initialised(self, channel_fixture):
        # Arrange
        sns_fanout_mock = Mock(name="SNS Fanout Class")
        channel_fixture._fanout = sns_fanout_mock

        # Act
        result = channel_fixture.fanout

        # Assert
        assert result is sns_fanout_mock

    def test_fanout_client_not_initialised(self, channel_fixture):
        with patch("kombu.transport.SQS._SnsFanout") as fan_mock:
            # Arrange
            channel_fixture._fanout = None

            # Act
            result = channel_fixture.fanout

            # Assert
            assert fan_mock.call_args_list == [call(channel_fixture)]
            assert result == fan_mock()

    @pytest.mark.parametrize("exchanges", [None, example_predefined_exchanges])
    def test_predefined_exchanges(self, exchanges, channel_fixture):
        # Arrange
        if exchanges is None:
            channel_fixture.transport_options.pop("predefined_exchanges", None)
        else:
            channel_fixture.transport_options["predefined_exchanges"] = exchanges

        # Act & Assert
        expected_result = exchanges if exchanges is not None else {}
        assert channel_fixture.predefined_exchanges == expected_result


class test_SnsFanout:
    @pytest.fixture
    def mock_sts_credentials(self):
        return {
            "AccessKeyId": "test_access_key",
            "SecretAccessKey": "test_secret_key",
            "SessionToken": "test_session_token",
            "Expiration": datetime.now(timezone.utc) + timedelta(hours=1),
        }

    @pytest.mark.parametrize("exchange_name", ["test_exchange"])
    def test_initialise_exchange_with_existing_topic(self, sns_fanout, exchange_name):
        # Arrange
        sns_fanout._topic_arn_cache[exchange_name] = "existing_arn"
        sns_fanout.subscriptions = Mock()

        # Act
        result = sns_fanout.initialise_exchange(exchange_name)

        # Assert
        assert result is None
        assert sns_fanout.subscriptions.cleanup.call_args_list == [call(exchange_name)]
        assert sns_fanout._topic_arn_cache[exchange_name] == "existing_arn"

    def test_initialise_exchange_with_predefined_exchanges(self, sns_fanout, caplog):
        # Arrange
        exchange_name = "test_exchange"

        sns_fanout.channel.predefined_exchanges = {"exchnage-1": {}}
        sns_fanout.subscriptions = Mock()
        caplog.set_level(logging.DEBUG)

        # Act
        result = sns_fanout.initialise_exchange(exchange_name)

        # Assert
        assert result is None
        assert sns_fanout.subscriptions.cleanup.call_args_list == [call(exchange_name)]
        assert (
            "'predefined_exchanges' has been specified, so SNS topics will not be created."
            in caplog.text
        )

    def test_initialise_exchange_create_new_topic(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"

        sns_fanout.channel.predefined_exchanges = False
        sns_fanout.subscriptions = Mock()
        sns_fanout._create_sns_topic = Mock(return_value="new_arn")

        # Act
        result = sns_fanout.initialise_exchange(exchange_name)

        # Assert
        assert result is None
        assert sns_fanout.subscriptions.cleanup.call_args_list == [call(exchange_name)]
        assert sns_fanout._create_sns_topic.call_args_list == [call(exchange_name)]
        assert sns_fanout._topic_arn_cache[exchange_name] == "new_arn"

    def test_get_topic_arn_create_new_topic(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"

        sns_fanout.channel.predefined_exchanges = {}
        sns_fanout._create_sns_topic = Mock(return_value="new_arn")

        # Act
        result = sns_fanout._get_topic_arn(exchange_name)

        # Assert
        assert result == "new_arn"
        assert sns_fanout._create_sns_topic.call_args_list == [call(exchange_name)]
        assert sns_fanout._topic_arn_cache[exchange_name] == "new_arn"

    def test_get_topic_arn_predefined_exchange_found(self, sns_fanout):
        # Arrange
        exchange_name = "exchange-1"

        sns_fanout.channel.predefined_exchanges = {
            "exchange-1": {"arn": "some-existing-arn"}
        }
        sns_fanout._create_sns_topic = Mock(return_value="new_arn")

        # Act
        result = sns_fanout._get_topic_arn(exchange_name)

        # Assert
        assert result == "some-existing-arn"
        assert sns_fanout._create_sns_topic.call_count == 0
        assert sns_fanout._topic_arn_cache[exchange_name] == "some-existing-arn"

    def test_get_topic_arn_predefined_exchange_not_found(self, sns_fanout):
        # Arrange
        exchange_name = "exchange-2"

        sns_fanout.channel.predefined_exchanges = {
            "exchange-1": {"arn": "some-existing-arn"}
        }
        sns_fanout._create_sns_topic = Mock(return_value="new_arn")

        # Act
        with pytest.raises(
            UndefinedExchangeException,
            match="Exchange with name 'exchange-2' must be defined in 'predefined_exchanges'.",
        ):
            sns_fanout._get_topic_arn(exchange_name)

        # Assert
        assert sns_fanout._create_sns_topic.call_count == 0
        assert sns_fanout._topic_arn_cache.get(exchange_name) is None

    def test_publish_successful(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        message = "test_message"

        sns_fanout._topic_arn_cache[exchange_name] = "existing_arn"
        mock_client = Mock()
        mock_client.publish.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        sns_fanout.get_client = Mock(return_value=mock_client)

        # Act
        sns_fanout.publish(exchange_name, message)

        # Assert
        assert sns_fanout.get_client.call_args_list == [call(exchange_name)]
        assert mock_client.publish.call_args_list == [
            call(TopicArn="existing_arn", Message="test_message")
        ]

    def test_publish_with_attributes_and_params(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        message = "test_message"
        message_attributes = {"attr1": "value1", "attr2": 123, "A boolean?": True}
        request_params = {"param1": "value1"}

        sns_fanout._topic_arn_cache[exchange_name] = "existing_arn"
        mock_client = Mock()
        mock_client.publish.return_value = {"ResponseMetadata": {"HTTPStatusCode": 200}}
        sns_fanout.get_client = Mock(return_value=mock_client)

        # Act
        sns_fanout.publish(exchange_name, message, message_attributes, request_params)

        # Assert
        assert sns_fanout.get_client.call_args_list == [((exchange_name,), {})]
        assert mock_client.publish.call_args_list == [
            call(
                TopicArn="existing_arn",
                Message="test_message",
                param1="value1",
                MessageAttributes={
                    "attr1": {"DataType": "String", "StringValue": "value1"},
                    "attr2": {"DataType": "String", "StringValue": "123"},
                    "A boolean?": {"DataType": "String", "StringValue": "True"},
                },
            )
        ]

    def test_publish_failure(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        message = "test_message"

        sns_fanout._topic_arn_cache[exchange_name] = "existing_arn"

        mock_client = Mock()
        mock_client.publish.return_value = {"ResponseMetadata": {"HTTPStatusCode": 400}}
        sns_fanout.get_client = Mock(return_value=mock_client)

        # Act and Assert
        with pytest.raises(
            UndefinedExchangeException,
            match="Unable to send message to topic 'existing_arn': status code was 400",
        ):
            sns_fanout.publish("test_exchange", message)

        assert sns_fanout.get_client.call_args_list == [call(exchange_name)]
        assert mock_client.publish.call_args_list == [
            call(TopicArn="existing_arn", Message="test_message")
        ]

    def test_create_sns_topic_success(self, sns_fanout, caplog):
        # Arrange
        caplog.set_level(logging.DEBUG)
        sns_fanout.get_client = Mock()
        mock_client = sns_fanout.get_client.return_value
        mock_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:my-new-topic",
        }

        # Act
        result = sns_fanout._create_sns_topic("my-new-topic")

        # Assert
        assert result == "arn:aws:sns:us-east-1:123456789012:my-new-topic"
        assert mock_client.create_topic.call_args_list == [
            call(
                Name="my-new-topic",
                Attributes={"FifoTopic": "False"},
                Tags=[
                    {"Key": "ManagedBy", "Value": "Celery/Kombu"},
                    {
                        "Key": "Description",
                        "Value": "This SNS topic is used by Kombu to enable Fanout support for AWS SQS.",
                    },
                ],
            )
        ]
        assert "Creating SNS topic 'my-new-topic'" in caplog.text
        assert (
            "Created SNS topic 'my-new-topic' with ARN 'arn:aws:sns:us-east-1:123456789012:my-new-topic'"
            in caplog.text
        )

    def test_create_sns_topic_failure(self, sns_fanout):
        # Arrange
        sns_fanout.get_client = Mock()
        mock_client = sns_fanout.get_client.return_value
        mock_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }

        # Act and Assert
        with pytest.raises(
            UndefinedExchangeException, match="Unable to create SNS topic"
        ):
            sns_fanout._create_sns_topic("test_exchange")

    def test_create_sns_topic_fifo(self, sns_fanout, caplog):
        # Arrange
        caplog.set_level(logging.DEBUG)
        sns_fanout.get_client = Mock()
        mock_client = sns_fanout.get_client.return_value
        mock_client.create_topic.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "TopicArn": "arn:aws:sns:us-east-1:123456789012:test_topic.fifo",
        }

        # Act
        result = sns_fanout._create_sns_topic("test_topic.fifo")

        # Assert
        assert result == "arn:aws:sns:us-east-1:123456789012:test_topic.fifo"
        assert mock_client.create_topic.call_args_list == [
            call(
                Name="test_topic.fifo",
                Attributes={"FifoTopic": "True"},
                Tags=[
                    {"Key": "ManagedBy", "Value": "Celery/Kombu"},
                    {
                        "Key": "Description",
                        "Value": "This SNS topic is used by Kombu to enable Fanout support for AWS SQS.",
                    },
                ],
            )
        ]
        assert "Creating SNS topic 'test_topic.fifo'" in caplog.text
        assert (
            "Created SNS topic 'test_topic.fifo' with ARN 'arn:aws:sns:us-east-1:123456789012:test_topic.fifo'"
            in caplog.text
        )

    def test_get_client_predefined_exchange(self, sns_fanout):
        # Arrange
        sns_fanout.channel.predefined_exchanges = {
            "test_exchange": {"region": "us-west-2"}
        }
        sns_fanout._create_boto_client = Mock()

        # Act
        result = sns_fanout.get_client("test_exchange")

        # Assert
        assert result == sns_fanout._create_boto_client.return_value
        assert sns_fanout._create_boto_client.call_args_list == [
            call(region="us-west-2", access_key_id=None, secret_access_key=None)
        ]

    def test_get_client_undefined_exchange(self, sns_fanout):
        # Arrange
        sns_fanout.channel.predefined_exchanges = {"exchange-1": {}}

        # Act & Assert
        with pytest.raises(
            UndefinedExchangeException,
            match="Exchange with name 'test_exchange' must be defined in 'predefined_exchanges'.",
        ):
            sns_fanout.get_client("test_exchange")

    def test_get_client_sts_session(self, sns_fanout):
        # Arrange
        sns_fanout.channel.predefined_exchanges = {
            "test_exchange": {
                "arn": "test_arn",
            }
        }
        sns_fanout.channel.connection.client.transport_options = {
            "sts_role_arn": "test_arn"
        }
        sns_fanout._handle_sts_session = Mock()

        # Act
        result = sns_fanout.get_client("test_exchange")

        # Assert
        assert result == sns_fanout._handle_sts_session.return_value
        assert sns_fanout._handle_sts_session.call_args_list == [
            call("test_exchange", {"arn": "test_arn"})
        ]

    def test_get_client_existing_predefined_client(self, sns_fanout):
        # Arrange
        sns_fanout.channel.predefined_exchanges = {
            "test_exchange": {
                "arn": "test_arn",
            }
        }
        client_mock = Mock()
        sns_fanout._predefined_clients = {"test_exchange": client_mock}

        # Act
        result = sns_fanout.get_client("test_exchange")

        # Assert
        assert result is client_mock

    def test_get_client_existing_client(self, sns_fanout):
        # Arrange
        sns_fanout._client = Mock()

        # Act
        result = sns_fanout.get_client()

        # Assert
        assert result == sns_fanout._client

    def test_get_client_new_client(self, sns_fanout):
        # Arrange
        sns_fanout._create_boto_client = Mock()
        sns_fanout.channel.conninfo.userid = "MyAccessKeyID"
        sns_fanout.channel.conninfo.password = "MySecretAccessKey"

        # Act
        result = sns_fanout.get_client()

        # Assert
        assert result == sns_fanout._create_boto_client.return_value
        assert (
            sns_fanout._create_boto_client.call_args_list
            == [
                call(
                    region="some-aws-region",
                    access_key_id="MyAccessKeyID",
                    secret_access_key="MySecretAccessKey",
                )
            ]
            != [
                call(
                    region="some-aws-region", access_key_id=None, secret_access_key=None
                )
            ]
        )

    def test_token_refresh_required_no_date(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        exchange_config = {"region": "us-west-2"}

        create_session_mock = Mock()
        sns_fanout._create_boto_client_with_sts_session = create_session_mock

        # Act
        result = sns_fanout._handle_sts_session(exchange_name, exchange_config)

        # Assert
        assert result == sns_fanout._create_boto_client_with_sts_session.return_value
        assert create_session_mock.call_args_list == [
            call("test_exchange", region="us-west-2")
        ]

    def test_token_refresh_required_expired_date(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        exchange_config = {"region": "us-west-2"}

        create_session_mock = Mock()
        sns_fanout._create_boto_client_with_sts_session = create_session_mock
        sns_fanout.sts_expiration = datetime.now(timezone.utc) - timedelta(minutes=1)

        client_mock = Mock()
        sns_fanout._predefined_clients = {"test_exchange": client_mock}

        # Act
        result = sns_fanout._handle_sts_session(exchange_name, exchange_config)

        # Assert
        assert result == sns_fanout._create_boto_client_with_sts_session.return_value
        assert create_session_mock.call_args_list == [
            call("test_exchange", region="us-west-2")
        ]

    def test_token_refresh_required_non_expired_date_without_client(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        exchange_config = {"region": "us-west-2"}

        create_session_mock = Mock()
        sns_fanout._create_boto_client_with_sts_session = create_session_mock
        sns_fanout.sts_expiration = datetime.now(timezone.utc) + timedelta(minutes=1)
        client_mock = Mock()

        sns_fanout._predefined_clients = {"another-exchange": client_mock}

        # Act
        result = sns_fanout._handle_sts_session(exchange_name, exchange_config)

        # Assert
        assert result == sns_fanout._create_boto_client_with_sts_session.return_value
        assert create_session_mock.call_args_list == [
            call("test_exchange", region="us-west-2")
        ]

    def test_token_refresh_required_non_expired_date_with_client(self, sns_fanout):
        # Arrange
        exchange_name = "test_exchange"
        exchange_config = {"region": "us-west-2"}

        create_session_mock = Mock()
        sns_fanout._create_boto_client_with_sts_session = create_session_mock
        sns_fanout.sts_expiration = datetime.now(timezone.utc) + timedelta(minutes=1)
        client_mock = Mock()

        sns_fanout._predefined_clients = {exchange_name: client_mock}

        # Act
        result = sns_fanout._handle_sts_session(exchange_name, exchange_config)

        # Assert
        assert create_session_mock.call_count == 0
        assert result is client_mock

    def test_create_boto_client_with_sts_session(
        self, sns_fanout, mock_sts_credentials
    ):
        # Arrange
        exchange_name = "test_exchange"
        region = "us-west-2"
        sns_fanout.channel.get_sts_credentials = Mock(return_value=mock_sts_credentials)

        boto_client_mock = Mock(name="My new boto client")
        sns_fanout.channel._new_boto_client = Mock(return_value=boto_client_mock)

        # Act
        result = sns_fanout._create_boto_client_with_sts_session(exchange_name, region)

        # Assert
        assert result is boto_client_mock

        # Check class vars have been updated
        assert sns_fanout.sts_expiration == mock_sts_credentials["Expiration"]
        assert sns_fanout._predefined_clients[exchange_name] == boto_client_mock

        # Check calls
        assert sns_fanout.channel.get_sts_credentials.call_args_list == [call()]
        assert sns_fanout.channel._new_boto_client.call_args_list == [
            call(
                service="sns",
                region="us-west-2",
                access_key_id="test_access_key",
                secret_access_key="test_secret_key",
                session_token="test_session_token",
            )
        ]


class test_SnsSubscription:
    @pytest.fixture
    def mock_get_queue_arn(self, sns_subscription):
        with patch.object(sns_subscription, "_get_queue_arn") as mock:
            yield mock

    @pytest.fixture
    def mock_get_topic_arn(self, sns_fanout):
        with patch.object(sns_fanout, "_get_topic_arn") as mock:
            yield mock

    @pytest.fixture
    def mock_get_client(self, sns_fanout):
        with patch.object(sns_fanout, "_get_client") as mock:
            yield mock

    @pytest.fixture
    def mock_set_permission_on_sqs_queue(self, sns_subscription):
        with patch.object(sns_subscription, "_set_permission_on_sqs_queue") as mock:
            yield mock

    @pytest.fixture
    def mock_subscribe_queue_to_sns_topic(self, sns_subscription):
        with patch.object(sns_subscription, "_subscribe_queue_to_sns_topic") as mock:
            yield mock

    def test_subscribe_queue_already_subscribed(
        self,
        sns_subscription,
    ):
        # Arrange
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        cached_subscription_arn = "arn:aws:sns:us-east-1:123456789012:test_topic:cached"
        sns_subscription._subscription_arn_cache[f"{exchange_name}:{queue_name}"] = (
            cached_subscription_arn
        )
        sns_fanout._get_client = Mock()

        # Act
        result = sns_subscription.subscribe_queue(queue_name, exchange_name)

        # Assert
        assert result == cached_subscription_arn
        assert sns_fanout._get_client.call_count == 0

    def test_subscribe_queue_success_queue_in_cache(
        self,
        sns_subscription,
        caplog,
        mock_get_topic_arn,
        mock_get_queue_arn,
        mock_set_permission_on_sqs_queue,
        mock_subscribe_queue_to_sns_topic,
    ):
        # Arrange
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:test_queue"
        topic_arn = "arn:aws:sns:us-east-1:123456789012:test_topic"
        subscription_arn = "arn:aws:sns:us-east-1:123456789012:test_topic:12345678-1234-1234-1234-123456789012"

        mock_get_queue_arn.return_value = queue_arn
        mock_get_topic_arn.return_value = topic_arn
        mock_subscribe_queue_to_sns_topic.return_value = subscription_arn

        # Act
        result = sns_subscription.subscribe_queue(queue_name, exchange_name)

        # Assert
        assert result == subscription_arn
        assert (
            sns_subscription._subscription_arn_cache[f"{exchange_name}:{queue_name}"]
            == subscription_arn
        )
        assert mock_get_queue_arn.call_args_list == [call("test_queue")]
        assert mock_get_topic_arn.call_args_list == [call(exchange_name)]
        assert mock_subscribe_queue_to_sns_topic.call_args_list == [
            call(topic_arn=topic_arn, queue_arn=queue_arn)
        ]
        assert mock_set_permission_on_sqs_queue.call_args_list == [
            call(topic_arn=topic_arn, queue_arn=queue_arn, queue_name=queue_name)
        ]

    def test_unsubscribe_queue_not_in_cache(
        self,
        sns_subscription,
    ):
        # Arrange
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        sns_subscription._subscription_arn_cache = {
            "another-exchange:another_queue": "123"
        }
        sns_subscription._unsubscribe_sns_subscription = Mock()

        # Act
        result = sns_subscription.unsubscribe_queue(queue_name, exchange_name)

        # Assert
        assert result is None
        assert sns_subscription._unsubscribe_sns_subscription.call_count == 0

    def test_unsubscribe_queue_in_cache(self, sns_subscription, caplog):
        # Arrange
        caplog.set_level(logging.DEBUG)
        queue_name = "test_queue"
        exchange_name = "test_exchange"
        subscription_arn = "arn:aws:sns:us-east-1:123456789012:test_topic:12345678-1234-1234-1234-123456789012"
        sns_subscription._subscription_arn_cache = {
            "test_exchange:test_queue": subscription_arn
        }
        sns_subscription._unsubscribe_sns_subscription = Mock()

        # Act
        result = sns_subscription.unsubscribe_queue(queue_name, exchange_name)

        # Assert
        assert result is None
        assert (
            f"Unsubscribed subscription '{subscription_arn}' for SQS queue '{queue_name}'"
            in caplog.text
        )
        assert sns_subscription._unsubscribe_sns_subscription.call_args_list == [
            call(subscription_arn)
        ]

    def test_cleanup_with_predefined_exchanges(
        self, sns_subscription, caplog, channel_fixture, sns_fanout
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        exchange_name = "exchange-1"

        channel_fixture.predefined_exchanges = {"exchange-1": {}}
        sns_fanout._get_topic_arn = Mock()

        # Act
        result = sns_subscription.cleanup(exchange_name)

        # Assert
        assert result is None
        assert (
            "'predefined_exchanges' has been specified, so stale SNS subscription"
            " cleanup will be skipped."
        ) in caplog.text
        assert sns_fanout._get_topic_arn.call_count == 0

    def test_cleanup_no_invalid_subscriptions(
        self, sns_subscription, caplog, channel_fixture, sns_fanout
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        exchange_name = "exchange-1"

        channel_fixture.predefined_exchanges = {}
        sns_fanout._get_topic_arn = Mock(return_value=topic_arn)
        sns_subscription._get_invalid_sns_subscriptions = Mock(return_value=[])
        sns_subscription._unsubscribe_sns_subscription = Mock()

        # Act
        result = sns_subscription.cleanup(exchange_name)

        # Assert
        assert result is None
        assert (
            f"Checking for stale SNS subscriptions for exchange '{exchange_name}'"
        ) in caplog.text
        assert sns_fanout._get_topic_arn.call_args_list == [call(exchange_name)]
        assert sns_subscription._unsubscribe_sns_subscription.call_count == 0

    def test_cleanup_with_invalid_subscriptions(
        self, sns_subscription, caplog, channel_fixture, sns_fanout
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        exchange_name = "exchange-1"

        channel_fixture.predefined_exchanges = {}
        sns_fanout._get_topic_arn = Mock(return_value=topic_arn)
        sns_subscription._get_invalid_sns_subscriptions = Mock(
            return_value=[
                "subscription-arn-1",
                "subscription-arn-2",
                "subscription-arn-3",
            ]
        )

        # Ensure that we carry on after hitting an exception
        sns_subscription._unsubscribe_sns_subscription = Mock(
            side_effect=[None, ConnectionError("A test exception"), None]
        )

        # Act
        result = sns_subscription.cleanup(exchange_name)

        # Assert
        assert result is None
        assert sns_fanout._get_topic_arn.call_args_list == [call(exchange_name)]
        assert sns_subscription._unsubscribe_sns_subscription.call_args_list == [
            call("subscription-arn-1"),
            call("subscription-arn-2"),
            call("subscription-arn-3"),
        ]

        # Check logs
        log_lines = [
            f"Removed stale subscription 'subscription-arn-1' for SNS topic '{topic_arn}'",
            f"Failed to remove stale subscription 'subscription-arn-2' for SNS topic"
            f" '{topic_arn}': A test exception",
            f"Removed stale subscription 'subscription-arn-3' for SNS topic '{topic_arn}'",
        ]
        for line in log_lines:
            assert line in caplog.text

    def test_set_permission_on_sqs_queue(
        self, sns_subscription, caplog, mock_sqs, channel_fixture
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        topic_arn = "arn:aws:sns:us-east-1:123456789012:my-topic"
        queue_name = "my-queue"
        queue_arn = "arn:aws:sqs:us-east-1:123456789012:my-queue"

        channel_fixture.predefined_queues = {}
        channel_fixture.sqs.return_value = mock_sqs()
        channel_fixture._queue_cache[queue_name] = (
            "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue"
        )

        exchange = Exchange("test_SQS", type="direct")
        queue = Queue(queue_name, exchange)
        queue(channel_fixture).declare()

        # Act
        sns_subscription._set_permission_on_sqs_queue(topic_arn, queue_name, queue_arn)

        # Assert
        expected_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "KombuManaged",
                    "Effect": "Allow",
                    "Principal": {"Service": "sns.amazonaws.com"},
                    "Action": "SQS:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {"ArnLike": {"aws:SourceArn": topic_arn}},
                }
            ],
        }

        assert mock_sqs().set_queue_attributes.call_args_list == [
            call(
                QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
                Attributes={"Policy": json.dumps(expected_policy)},
            )
        ]

        assert (
            "Set permissions on SNS topic 'arn:aws:sns:us-east-1:123456789012:my-topic'"
        ) in caplog.text

    def test_subscribe_queue_to_sns_topic_successful_subscription(
        self, sns_subscription, caplog, sns_fanout
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)

        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue"
        topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"
        subscription_arn = "arn:aws:sns:us-west-2:123456789012:my-topic:12345678-1234-1234-1234-123456789012"
        mock_client = Mock()
        mock_client.return_value.subscribe.return_value = {
            "SubscriptionArn": subscription_arn,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        sns_fanout.get_client = mock_client

        # Act
        result = sns_subscription._subscribe_queue_to_sns_topic(queue_arn, topic_arn)

        # Assert
        assert result == subscription_arn
        assert mock_client.return_value.subscribe.call_args_list == [
            call(
                TopicArn="arn:aws:sns:us-west-2:123456789012:my-topic",
                Protocol="sqs",
                Endpoint="arn:aws:sqs:us-west-2:123456789012:my-queue",
                Attributes={"RawMessageDelivery": "true"},
                ReturnSubscriptionArn=True,
            )
        ]
        assert (
            f"Subscribing queue '{queue_arn}' to SNS topic '{topic_arn}'" in caplog.text
        )
        assert (
            f"Create subscription '{subscription_arn}' for SQS queue '{queue_arn}' to SNS topic '{topic_arn}'"
            in caplog.text
        )

    def test_subscribe_queue_to_sns_topic_subscription_failure(
        self, sns_subscription, sns_fanout
    ):
        # Arrange
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue"
        topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"
        mock_client = Mock()
        mock_client.return_value.subscribe.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }
        sns_fanout.get_client = mock_client

        # Act and Assert
        with pytest.raises(
            Exception, match="Unable to subscribe queue: status code was 400"
        ):
            sns_subscription._subscribe_queue_to_sns_topic(queue_arn, topic_arn)

    def test_subscribe_queue_to_sns_topic_client_error(
        self, sns_subscription, sns_fanout
    ):
        # Arrange
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue"
        topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"

        mock_client = Mock()
        mock_client.return_value.subscribe.side_effect = ClientError(
            error_response={"Error": {"Code": "InvalidParameter"}},
            operation_name="Subscribe",
        )
        sns_fanout.get_client = mock_client

        # Act and Assert
        with pytest.raises(ClientError):
            sns_subscription._subscribe_queue_to_sns_topic(queue_arn, topic_arn)

    def test_unsubscribe_sns_subscription_success(self, sns_subscription, sns_fanout):
        # Arrange
        subscription_arn = (
            "arn:aws:sns:us-west-2:123456789012:my-topic:12345678-12:sub-id"
        )

        mock_client = Mock()
        mock_client.return_value.unsubscribe.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        sns_fanout.get_client = mock_client

        # Act
        result = sns_subscription._unsubscribe_sns_subscription(subscription_arn)

        # Assert
        assert result is None
        assert mock_client.return_value.unsubscribe.call_args_list == [
            call(SubscriptionArn=subscription_arn)
        ]

    def test_unsubscribe_sns_subscription_error(
        self, sns_subscription, sns_fanout, caplog
    ):
        # Arrange
        caplog.set_level(logging.DEBUG)
        subscription_arn = (
            "arn:aws:sns:us-west-2:123456789012:my-topic:12345678-12:sub-id"
        )

        mock_client = Mock()
        mock_client.return_value.unsubscribe.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 400}
        }
        sns_fanout.get_client = mock_client

        # Act
        result = sns_subscription._unsubscribe_sns_subscription(subscription_arn)

        # Assert
        assert result is None
        assert mock_client.return_value.unsubscribe.call_args_list == [
            call(SubscriptionArn=subscription_arn)
        ]
        assert (
            f"Unable to remove subscription '{subscription_arn}': status code was 400"
        ) in caplog.text

    def test_get_invalid_sns_subscriptions(self, sns_subscription, sns_fanout):
        # Arrange
        client_mock = Mock()
        sns_fanout.get_client = client_mock

        # Mock paginator
        mock_paginate = Mock()
        sns_fanout.get_client().get_paginator.return_value = mock_paginate
        mock_paginate.paginate.return_value = iter(
            [
                {
                    "Subscriptions": [
                        {"SubscriptionArn": "arn1"},
                        {"SubscriptionArn": "arn2"},
                    ]
                },
                {"Subscriptions": [{"SubscriptionArn": "arn3"}]},
            ]
        )

        # Mock filter
        sns_subscription._filter_sns_subscription_response = Mock(
            side_effect=[["arn3"], ["arn2"]]
        )

        sns_topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"

        # Act
        result = sns_subscription._get_invalid_sns_subscriptions(sns_topic_arn)

        # Assert
        assert result == ["arn3", "arn2"]
        assert mock_paginate.paginate.call_args_list == [call(TopicArn=sns_topic_arn)]
        assert sns_subscription._filter_sns_subscription_response.call_args_list == [
            call([{"SubscriptionArn": "arn1"}, {"SubscriptionArn": "arn2"}]),
            call([{"SubscriptionArn": "arn3"}]),
        ]

    def test_get_invalid_sns_subscriptions_empty(self, sns_subscription, sns_fanout):
        # Arrange
        client_mock = Mock()
        sns_fanout.get_client = client_mock

        # Mock paginator
        mock_paginate = Mock()
        sns_fanout.get_client().get_paginator.return_value = mock_paginate
        mock_paginate.paginate.return_value = iter(
            [
                {"Subscriptions": []},
                {"Subscriptions": []},
            ]
        )

        # Mock filter
        sns_subscription._filter_sns_subscription_response = Mock(return_value=[])

        # Act
        result = sns_subscription._get_invalid_sns_subscriptions(
            "arn:aws:sns:us-west-2:123456789012:my-topic"
        )

        # Assert
        assert result == []

    def test_get_invalid_sns_subscriptions_no_subscriptions_key(
        self, sns_subscription, sns_fanout
    ):
        # Arrange
        client_mock = Mock()
        sns_fanout.get_client = client_mock

        # Mock paginator
        mock_paginate = Mock()
        sns_fanout.get_client().get_paginator.return_value = mock_paginate
        mock_paginate.paginate.return_value = iter(
            [
                {},
                {"Subscriptions": [{"SubscriptionArn": "arn1"}]},
            ]
        )

        # Mock filter
        sns_subscription._filter_sns_subscription_response = Mock(return_value=[])

        sns_topic_arn = "arn:aws:sns:us-west-2:123456789012:my-topic"

        sns_subscription._filter_sns_subscription_response = Mock(
            side_effect=[[], ["arn1"]]
        )

        # Act
        result = sns_subscription._get_invalid_sns_subscriptions(sns_topic_arn)

        # Assert
        assert result == ["arn1"]
        assert sns_subscription._filter_sns_subscription_response.call_args_list == [
            call(None),
            call([{"SubscriptionArn": "arn1"}]),
        ]

    @pytest.mark.parametrize("value", [None, "", []])
    def test__filter_sns_subscription_response_nothing_provided(
        self, value, sns_subscription
    ):
        # Act & Assert
        assert sns_subscription._filter_sns_subscription_response(value) == []

    def test__filter_sns_subscription_response(self, sns_subscription, channel_fixture):
        # Arrange
        subscriptions = [
            {
                "Protocol": "SqS",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-1",
                "SubscriptionArn": "arn-1",
            },  # Test case-sensitivity
            {
                "Protocol": "sqs",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-2",
                "SubscriptionArn": "arn-2",
            },  # Test case-sensitivity
            {
                "Protocol": "Lambda",
                "Endpoint": "arn:aws:lambda:us-west-2:123456789012:function:my-lambda-function",
                "SubscriptionArn": "lambda-arn-1",
            },  # This should be filtered out
            {
                "Protocol": "sqs",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-3",
                "SubscriptionArn": "arn-3",
            },
            {
                "Protocol": "sqs",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-4",
                "SubscriptionArn": "arn-4",
            },
            {
                "Protocol": "sqs",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-5",
                "SubscriptionArn": "arn-5",
            },
            {
                "Protocol": "SQS",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-6",
                "SubscriptionArn": "arn-6",
            },
        ]
        sqs_mock = Mock()
        channel_fixture.sqs = sqs_mock

        # Setup errors on queues 2,4 and 5
        sqs_mock.return_value.get_queue_url.side_effect = [
            None,  # queue-1
            ClientError(
                error_response={"Error": {"Code": "QueueDoesNotExist"}},
                operation_name="GetQueueUrl",
            ),  # queue-2
            None,  # queue-3
            ClientError(
                error_response={"Error": {"Code": "NonExistentQueue"}},
                operation_name="GetQueueUrl",
            ),  # queue-4
            ClientError(
                error_response={"Error": {"Code": "NonExistentQueue"}},
                operation_name="GetQueueUrl",
            ),  # queue-5
            None,  # queue-6
        ]

        # Act
        result = sns_subscription._filter_sns_subscription_response(subscriptions)

        # Assert
        assert result == ["arn-2", "arn-4", "arn-5"]
        assert sqs_mock.return_value.get_queue_url.call_args_list == [
            call(QueueName="//sqs.us-west-2.amazonaws.com/123456789012/my-queue-1"),
            call(QueueName="//sqs.us-west-2.amazonaws.com/123456789012/my-queue-2"),
            call(QueueName="//sqs.us-west-2.amazonaws.com/123456789012/my-queue-3"),
            call(QueueName="//sqs.us-west-2.amazonaws.com/123456789012/my-queue-4"),
            call(QueueName="//sqs.us-west-2.amazonaws.com/123456789012/my-queue-5"),
            call(QueueName="//sqs.us-west-2.amazonaws.com/123456789012/my-queue-6"),
        ]

    @pytest.mark.parametrize(
        "exc_type", [ClientError, ValueError, Exception, IndexError, KeyError]
    )
    def test__filter_sns_subscription_response_exceptions(
        self, exc_type, sns_subscription, channel_fixture
    ):
        # Arrange
        subscriptions = [
            {
                "Protocol": "SqS",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-1",
                "SubscriptionArn": "arn-1",
            },  # Test case-sensitivity
            {
                "Protocol": "sqs",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-2",
                "SubscriptionArn": "arn-2",
            },  # Test case-sensitivity
            {
                "Protocol": "Lambda",
                "Endpoint": "arn:aws:lambda:us-west-2:123456789012:function:my-lambda-function",
                "SubscriptionArn": "lambda-arn-1",
            },  # This should be filtered out
            {
                "Protocol": "sqs",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-3",
                "SubscriptionArn": "arn-3",
            },
            {
                "Protocol": "sqs",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-4",
                "SubscriptionArn": "arn-4",
            },
            {
                "Protocol": "sqs",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-5",
                "SubscriptionArn": "arn-5",
            },
            {
                "Protocol": "SQS",
                "Endpoint": "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-6",
                "SubscriptionArn": "arn-6",
            },
        ]
        sqs_mock = Mock()
        channel_fixture.sqs = sqs_mock

        # Build exception
        if exc_type == ClientError:
            exc = ClientError(
                error_response={"Error": {"Code": "ThisIsATest"}},
                operation_name="GetQueueUrl",
            )
        else:
            exc = exc_type("This is a test exception")

        sqs_mock.return_value.get_queue_url.side_effect = [
            None,  # queue-1
            exc,  # queue-2
            None,  # queue-3
            ClientError(
                error_response={"Error": {"Code": "NonExistentQueue"}},
                operation_name="GetQueueUrl",
            ),  # queue-4
            ClientError(
                error_response={"Error": {"Code": "NonExistentQueue"}},
                operation_name="GetQueueUrl",
            ),  # queue-5
            None,  # queue-6
        ]

        # Act & Assert
        with pytest.raises(exc_type):
            sns_subscription._filter_sns_subscription_response(subscriptions)

    def test_get_queue_arn_in_cache(self, sns_subscription, sns_fanout):
        # Arrange
        sns_subscription._queue_arn_cache = {
            "my_queue": "arn:aws:sqs:us-west-2:123456789012:my-queue",
            "my-queue-2": "arn:aws:sqs:us-west-2:123456789012:my-queue-2",
            "my-queue-3": "arn:aws:sqs:us-west-2:123456789012:my-queue-3",
        }

        chan_mock = Mock()
        sns_fanout.channel = chan_mock

        # Act
        result = sns_subscription._get_queue_arn("my-queue-2")

        # Assert
        assert result == "arn:aws:sqs:us-west-2:123456789012:my-queue-2"
        assert chan_mock._resolve_queue_url.call_count == 0

    def test_get_queue_arn_lookup_success(self, sns_subscription, sns_fanout):
        # Arrange
        sns_subscription._queue_arn_cache = {
            "my_queue": "arn:aws:sqs:us-west-2:123456789012:my-queue",
            "my-queue-2": "arn:aws:sqs:us-west-2:123456789012:my-queue-2",
            "my-queue-3": "arn:aws:sqs:us-west-2:123456789012:my-queue-3",
        }
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue-4"
        queue_url = "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-4"

        chan_mock = Mock()
        sns_fanout.channel = chan_mock
        chan_mock._resolve_queue_url.return_value = queue_url
        chan_mock.sqs.return_value.get_queue_attributes.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200},
            "Attributes": {"QueueArn": queue_arn},
        }
        assert "my-queue-4" not in sns_subscription._queue_arn_cache

        # Act
        result = sns_subscription._get_queue_arn("my-queue-4")

        # Assert
        assert result == queue_arn
        assert chan_mock._resolve_queue_url.call_args_list == [call("my-queue-4")]
        assert chan_mock.sqs.return_value.get_queue_attributes.call_args_list == [
            call(QueueUrl=queue_url, AttributeNames=["QueueArn"])
        ]
        assert sns_subscription._queue_arn_cache["my-queue-4"] == queue_arn

    def test_get_queue_arn_lookup_failure(self, sns_subscription, sns_fanout):
        # Arrange
        sns_subscription._queue_arn_cache = {
            "my_queue": "arn:aws:sqs:us-west-2:123456789012:my-queue",
            "my-queue-2": "arn:aws:sqs:us-west-2:123456789012:my-queue-2",
            "my-queue-3": "arn:aws:sqs:us-west-2:123456789012:my-queue-3",
        }
        queue_arn = "arn:aws:sqs:us-west-2:123456789012:my-queue-4"
        queue_url = "https://sqs.us-west-2.amazonaws.com/123456789012/my-queue-4"

        chan_mock = Mock()
        sns_fanout.channel = chan_mock
        chan_mock._resolve_queue_url.return_value = queue_url
        chan_mock.sqs.return_value.get_queue_attributes.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 500},
            "Attributes": {"QueueArn": queue_arn},
        }
        assert "my-queue-4" not in sns_subscription._queue_arn_cache

        # Act & assert
        with pytest.raises(
            KombuError,
            match="Unable to get ARN for SQS queue 'my-queue-4': status code was '500'",
        ):
            sns_subscription._get_queue_arn("my-queue-4")
