"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""

from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock, skip

from kombu import messaging
from kombu import Connection, Exchange, Queue

from kombu.async.aws.ext import exception
from kombu.five import Empty
from kombu.transport import SQS


class SQSMessageMock(object):
    def __init__(self):
        """
        Imitate the SQS Message from boto3.
        """
        self.body = ""
        self.receipt_handle = "receipt_handle_xyz"


class SQSQueueMock(object):
    """
    Imitate the SQS Queue in boto3.  It has two attributes, url and
    "attributes".
    """

    def __init__(self, url):
        self.url = url
        self.attributes = {'ApproximateNumberOfMessages': '0'}

        self.messages = []
        self._receive_messages_calls = 0

    def send_message(self, MessageBody=None, DelaySeconds=0, MessageAttributes=None):
        msg = SQSMessageMock()
        msg.body = MessageBody
        self.messages.append(msg)
        self.attributes['ApproximateNumberOfMessages'] = len(self.messages)

    def receive_messages(self, MaxNumberOfMessages=10):
        self._receive_messages_calls += 1
        returning = self.messages[:MaxNumberOfMessages]
        self.messages = self.messages[MaxNumberOfMessages:]
        self.attributes['ApproximateNumberOfMessages'] = len(self.messages)
        return returning

    def purge(self):
        empty, self.messages[:] = not self.messages, []
        self.attributes['ApproximateNumberOfMessages'] = len(self.messages)
        return not empty



class SQSConnectionMock(object):

    def __init__(self):
        """
        Imitate the SQS Resource in boto3.
        """
        self.queues = self.Queues()
        self.queues.queues = {
            'q_%s' % n: SQSQueueMock('q_%s' % n) for n in range(1500)
        }
        q = SQSQueueMock('unittest_queue')
        q.send_message(MessageBody='hello')
        self.queues.queues['unittest_queue'] = q

    class Queues:
        """
        Imitate the queues attribute on the SQS Resource in boto3.  It has
        a filter() method and is an iterator rather than a list.
        """
        def __init__(self):
            self.queues = []

        def filter(self, QueueNamePrefix=None):
            """ Return an iterable of queues. """
            return (val for key, val in self.queues.items()
                    if key.startswith(QueueNamePrefix))

    def create_queue(self, QueueName=None, Attributes=None):
        q = self.queues.queues[QueueName] = SQSQueueMock(QueueName)
        return q

    def get_queue_by_name(self, QueueName=None):
        return self.queues.queues.get(QueueName)


@skip.unless_module('boto')
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
        self.sqs_conn_mock = SQSConnectionMock()

        def mock_sqs():
            return self.sqs_conn_mock
        SQS.Channel.sqs = mock_sqs()

        # Set up a task exchange for passing tasks through the queue
        self.exchange = Exchange('test_SQS', type='direct')
        self.queue = Queue(self.queue_name, self.exchange, self.queue_name)

        # Mock up a test SQS Queue with the SQSQueueMock class (and always
        # make sure its a clean empty queue)
        self.sqs_queue_mock = SQSQueueMock(self.queue_name)

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

    # TODO: I dropped the old behavior of changing the error message when
    # bad auth is provided.  Is this stuff important?
    # def test_auth_fail(self):
    #     normal_func = SQS.Channel.sqs.list_queues
    #
    #     def get_all_queues_fail_403(prefix=''):
    #         # mock auth error
    #         raise exception.SQSError(403, None, None)
    #
    #     def get_all_queues_fail_not_403(prefix=''):
    #         # mock non-auth error
    #         raise exception.SQSError(500, None, None)
    #
    #     try:
    #         SQS.Channel.sqs.access_key = '1234'
    #         SQS.Channel.sqs.get_all_queues = get_all_queues_fail_403
    #         with pytest.raises(RuntimeError) as excinfo:
    #             self.channel = self.connection.channel()
    #         assert 'access_key=1234' in str(excinfo.value)
    #         SQS.Channel.sqs.get_all_queues = get_all_queues_fail_not_403
    #         with pytest.raises(exception.SQSError):
    #             self.channel = self.connection.channel()
    #     finally:
    #         SQS.Channel.sqs.get_all_queues = normal_func

    def test_new_queue(self):
        queue_name = 'new_unittest_queue'
        self.channel._new_queue(queue_name)
        assert queue_name in self.sqs_conn_mock.queues.queues
        # For cleanup purposes, delete the queue and the queue file
        self.channel._delete(queue_name)

    def test_dont_create_duplicate_new_queue(self):
        # All queue names start with "q", except "unittest_queue".
        # which is definitely out of cache when get_all_queues returns the
        # first 1000 queues sorted by name.
        queue_name = 'unittest_queue'
        self.channel._new_queue(queue_name)
        assert queue_name in self.sqs_conn_mock.queues.queues
        queue = self.sqs_conn_mock.queues.queues[queue_name]
        assert 1 == int(queue.attributes['ApproximateNumberOfMessages'])
        assert 'hello' == queue.messages[0].body

    def test_delete(self):
        queue_name = 'new_unittest_queue'
        self.channel._new_queue(queue_name)
        self.channel._delete(queue_name)
        assert queue_name not in self.channel._queue_cache

    def test_get_from_sqs(self):
        # Test getting a single message
        message = 'my test message'
        self.producer.publish(message)
        q = self.channel._new_queue(self.queue_name)
        results = q.receive_messages()
        assert len(results) == 1

        # Now test getting many messages
        for i in range(3):
            message = 'message: {0}'.format(i)
            self.producer.publish(message)

        results = q.receive_messages(MaxNumberOfMessages=3)
        assert len(results) == 3

    def test_get_with_empty_list(self):
        with pytest.raises(Empty):
            self.channel._get(self.queue_name)

    def test_get_bulk_raises_empty(self):
        with pytest.raises(Empty):
            self.channel._get_bulk(self.queue_name)

    def test_messages_to_python(self):
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

        q = self.channel._new_queue(self.queue_name)
        # Get the messages now
        kombu_messages = q.receive_messages(MaxNumberOfMessages=kombu_message_count)
        json_messages = q.receive_messages(MaxNumberOfMessages=json_message_count)

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

        # How many times was the SQSConnectionMock get_message method called?
        assert (expected_receive_messages_count ==
                self.channel._queue_cache[self.queue_name]._receive_messages_calls)
