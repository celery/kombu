"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""

from __future__ import absolute_import, unicode_literals

import pytest
import random
import string

from case import Mock, skip

from kombu import messaging
from kombu import Connection, Exchange, Queue

from kombu.five import Empty
from kombu.transport import SQS

SQS_Channel_sqs = SQS.Channel.sqs


class SQSMessageMock(object):
    def __init__(self):
        """
        Imitate the SQS Message from boto3.
        """
        self.body = ""
        self.receipt_handle = "receipt_handle_xyz"


class QueueMock(object):
    """ Hold information about a queue. """

    def __init__(self, url):
        self.url = url
        self.attributes = {'ApproximateNumberOfMessages': '0'}

        self.messages = []

    def __repr__(self):
        return 'QueueMock: {} {} messages'.format(self.url, len(self.messages))


class SQSClientMock(object):

    def __init__(self):
        """
        Imitate the SQS Client from boto3.
        """
        self._receive_messages_calls = 0
        # _queues doesn't exist on the real client, here for testing.
        self._queues = {}
        for n in range(1):
            name = 'q_{}'.format(n)
            url = 'sqs://q_{}'.format(n)
            self.create_queue(QueueName=name)

        url = self.create_queue(QueueName='unittest_queue')['QueueUrl']
        self.send_message(QueueUrl=url, MessageBody='hello')

    def _get_q(self, url):
        """ Helper method to quickly get a queue. """
        for q in self._queues.values():
            if q.url == url:
                return q
        raise Exception("Queue url {} not found".format(url))

    def create_queue(self, QueueName=None, Attributes=None):
        q = self._queues[QueueName] = QueueMock('sqs://' + QueueName)
        return {'QueueUrl': q.url}

    def list_queues(self, QueueNamePrefix=None):
        """ Return a list of queue urls """
        urls = (val.url for key, val in self._queues.items()
                if key.startswith(QueueNamePrefix))
        return {'QueueUrls': urls}

    def get_queue_url(self, QueueName=None):
        return self._queues[QueueName]

    def send_message(self, QueueUrl=None, MessageBody=None):
        for q in self._queues.values():
            if q.url == QueueUrl:
                handle = ''.join(random.choice(string.ascii_lowercase) for
                                 x in range(10))
                q.messages.append({'Body': MessageBody,
                                   'ReceiptHandle': handle})
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


@skip.unless_module('boto3')
class test_Channel:

    def handleMessageCallback(self, message):
        self.callback_message = message

    def setup(self):
        """Mock the back-end SQS classes"""
        # Sanity check... if SQS is None, then it did not import and we
        # cannot execute our tests.
        SQS.Channel._queue_cache.clear()

        # Common variables used in the unit tests
        self.queue_name = 'unittest'

        # Mock the sqs() method that returns an SQSConnection object and
        # instead return an SQSConnectionMock() object.
        self.sqs_conn_mock = SQSClientMock()

        def mock_sqs():
            return self.sqs_conn_mock

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

    def test_init(self):
        """kombu.SQS.Channel instantiates correctly with mocked queues"""
        assert self.queue_name in self.channel._queue_cache

    def test_endpoint_url(self):
        url = 'sqs://@localhost:5493'
        self.connection = Connection(hostname=url, transport=SQS.Transport)
        self.channel = self.connection.channel()
        self.channel._sqs = None
        expected_endpoint_url = 'http://localhost:5493'
        assert self.channel.endpoint_url == expected_endpoint_url
        boto3_sqs = SQS_Channel_sqs.__get__(self.channel, SQS.Channel)
        assert boto3_sqs._endpoint.host == expected_endpoint_url

    def test_none_hostname_persists(self):
        conn = Connection(hostname=None, transport=SQS.Transport)
        assert conn.hostname == conn.clone().hostname

    def test_new_queue(self):
        queue_name = 'new_unittest_queue'
        self.channel._new_queue(queue_name)
        assert queue_name in self.sqs_conn_mock._queues.keys()
        # For cleanup purposes, delete the queue and the queue file
        self.channel._delete(queue_name)

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

    def test_get_from_sqs(self):
        # Test getting a single message
        message = 'my test message'
        self.producer.publish(message)
        result = self.channel._get(self.queue_name)
        assert 'body' in result.keys()

        # Now test getting many messages
        for i in range(3):
            message = 'message: {0}'.format(i)
            self.producer.publish(message)

        self.channel._get_bulk(self.queue_name, max_if_unlimited=3)
        assert len(self.sqs_conn_mock._queues[self.queue_name].messages) == 0

    def test_get_with_empty_list(self):
        with pytest.raises(Empty):
            self.channel._get(self.queue_name)

    def test_get_bulk_raises_empty(self):
        with pytest.raises(Empty):
            self.channel._get_bulk(self.queue_name)

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
            self.channel.qos.append(Mock(name='message{0}'.format(i)), i)

        # Now, do the get again, the number of messages returned should be 1.
        self.channel.connection._deliver.reset_mock()
        self.channel._get_bulk(self.queue_name)
        self.channel.connection._deliver.assert_called_once()

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
