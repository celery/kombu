"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""

from __future__ import absolute_import

try:
    import boto
except ImportError:  # pragma: no cover
    boto = None  # noqa

from kombu import five
from kombu import messaging
from kombu import Connection, Exchange, Queue
from kombu.tests.case import Case, SkipTest

from kombu.transport import SQS


class SQSQueueMock(object):

    def __init__(self, name):
        self.name = name
        self.messages = []
        self._get_message_calls = 0

    def clear(self, page_size=10, vtimeout=10):
        empty, self.messages[:] = not self.messages, []
        return not empty

    def count(self, page_size=10, vtimeout=10):
        return len(self.messages)
    count_slow = count

    def delete(self):
        self.messages[:] = []
        return True

    def delete_message(self, message):
        try:
            self.messages.remove(message)
        except ValueError:
            return False
        return True

    def get_messages(self, num_messages=1, visibility_timeout=None,
                     attributes=None, *args, **kwargs):
        self._get_message_calls += 1
        return self.messages[:num_messages]

    def read(self, visibility_timeout=None):
        return self.messages.pop(0)

    def write(self, message):
        self.messages.append(message)
        return True


class SQSConnectionMock(object):

    def __init__(self):
        self.queues = {}

    def get_queue(self, queue):
        return self.queues.get(queue)

    def get_all_queues(self, prefix=""):
        return self.queues.values()

    def delete_queue(self, queue, force_deletion=False):
        q = self.get_queue(queue)
        if q:
            if q.count():
                return False
            q.clear()
            self.queues.pop(queue, None)

    def delete_message(self, queue, message):
        return queue.delete_message(message)

    def create_queue(self, name, *args, **kwargs):
        q = self.queues[name] = SQSQueueMock(name)
        return q


class test_Channel(Case):

    def handleMessageCallback(self, message):
        self.callback_message = message

    def setup(self):
        """Mock the back-end SQS classes"""
        # Sanity check... if SQS is None, then it did not import and we
        # cannot execute our tests.
        if boto is None:
            raise SkipTest('boto is not installed')

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

    def test_init(self):
        """kombu.SQS.Channel instantiates correctly with mocked queues"""
        self.assertIn(self.queue_name, self.channel._queue_cache)

    def test_new_queue(self):
        queue_name = 'new_unittest_queue'
        self.channel._new_queue(queue_name)
        self.assertIn(queue_name, self.sqs_conn_mock.queues)
        # For cleanup purposes, delete the queue and the queue file
        self.channel._delete(queue_name)

    def test_delete(self):
        queue_name = 'new_unittest_queue'
        self.channel._new_queue(queue_name)
        self.channel._delete(queue_name)
        self.assertNotIn(queue_name, self.channel._queue_cache)

    def test_get_from_sqs(self):
        # Test getting a single message
        message = 'my test message'
        self.producer.publish(message)
        q = self.channel._new_queue(self.queue_name)
        results = q.get_messages()
        self.assertEquals(len(results), 1)

        # Now test getting many messages
        for i in range(3):
            message = 'message: {0}'.format(i)
            self.producer.publish(message)

        results = q.get_messages(num_messages=3)
        self.assertEquals(len(results), 3)

    def test_get_with_empty_list(self):
        with self.assertRaises(five.Empty):
            self.channel._get(self.queue_name)

    def test_get_bulk_raises_empty(self):
        with self.assertRaises(five.Empty):
            self.channel._get_bulk(self.queue_name)

    def test_messages_to_python(self):
        message_count = 3
        # Create several test messages and publish them
        for i in range(message_count):
            message = 'message: %s' % i
            self.producer.publish(message)

        q = self.channel._new_queue(self.queue_name)
        # Get the messages now
        messages = q.get_messages(num_messages=message_count)

        # Now convert them to payloads
        payloads = self.channel._messages_to_python(
            messages, self.queue_name,
        )

        # We got the same number of payloads back, right?
        self.assertEquals(len(payloads), message_count)

        # Make sure they're payload-style objects
        for p in payloads:
            self.assertTrue('properties' in p)

    def test_put_and_get(self):
        message = 'my test message'
        self.producer.publish(message)
        results = self.queue(self.channel).get().payload
        self.assertEquals(message, results)

    def test_put_and_get_bulk(self):
        # With QoS.prefetch_count = 0
        message = 'my test message'
        self.producer.publish(message)
        results = self.channel._get_bulk(self.queue_name)
        self.assertEquals(1, len(results))

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
        results = self.channel._get_bulk(self.queue_name)
        self.assertEquals(5, len(results))
        for i, r in enumerate(results):
            self.channel.qos.append(r, i)

        # Now, do the get again, the number of messages returned should be 1.
        results = self.channel._get_bulk(self.queue_name)
        self.assertEquals(len(results), 1)

    def test_drain_events_with_empty_list(self):
        def mock_can_consume():
            return False
        self.channel.qos.can_consume = mock_can_consume
        with self.assertRaises(five.Empty):
            self.channel.drain_events()

    def test_drain_events_with_prefetch_5(self):
        # Generate 20 messages
        message_count = 20
        expected_get_message_count = 4

        # Set the prefetch_count to 5
        self.channel.qos.prefetch_count = 5

        # Now, generate all the messages
        for i in range(message_count):
            self.producer.publish('message: %s' % i)

        # Now drain all the events
        for i in range(message_count):
            self.channel.drain_events()

        # How many times was the SQSConnectionMock get_message method called?
        self.assertEquals(
            expected_get_message_count,
            self.channel._queue_cache[self.queue_name]._get_message_calls)

    def test_drain_events_with_prefetch_none(self):
        # Generate 20 messages
        message_count = 20
        expected_get_message_count = 2

        # Set the prefetch_count to None
        self.channel.qos.prefetch_count = None

        # Now, generate all the messages
        for i in range(message_count):
            self.producer.publish('message: %s' % i)

        # Now drain all the events
        for i in range(message_count):
            self.channel.drain_events()

        # How many times was the SQSConnectionMock get_message method called?
        self.assertEquals(
            expected_get_message_count,
            self.channel._queue_cache[self.queue_name]._get_message_calls)
