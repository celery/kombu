"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""

from __future__ import absolute_import

import os
import pickle
import sys

from kombu import Connection
from kombu import messaging
from kombu import five
from kombu.tests.case import Case, SkipTest
import kombu

try:
    from kombu.transport import SQS
except ImportError:
    # Boto must not be installed if the SQS transport fails to import,
    # so we skip all unit tests. Set SQS to None here, and it will be
    # checked during the setUp() phase later.
    SQS = None


class SQSQueueMock:
    name = None

    def __init__(self, filename, create=False):
        try:
            open(filename + ".sqs")
        except IOError:
            if create:
                open(filename + ".sqs", 'w')
            else:
                raise SyntaxError("Queue %s does not exist" % filename)
        self.name = filename
        self._get_message_calls = 0

    def clear(self, page_size=10, vtimeout=10):
        try:
            open(self.name + '.sqs', 'w').close()
        except EOFError:
            return False
        return True

    def count(self, page_size=10, vtimeout=10):
        try:
            prev_data = pickle.load(open(self.name + '.sqs', 'r'))
        except EOFError:
            return 0
        return len(prev_data)

    def count_slow(self, page_size=10, vtimeout=10):
        return self.count(page_size=page_size, vtimeout=vtimeout)

    def delete(self):
        try:
            os.remove(self.name + '.sqs')
        except OSError:
            # What happens here?
            return False
        return True

    def delete_message(self, message):
        prev_data = pickle.load(open(self.name + '.sqs', 'r'))
        for data in prev_data:
            if data.get_body() == message.get_body():
                try:
                    prev_data.remove(data)
                    break
                except ValueError:
                    return False
        try:
            pickle.dump(prev_data, open(self.name + '.sqs', 'w'))
        except IOError:
            return False

        return True

    def get_messages(self, num_messages=1, visibility_timeout=None,
                     attributes=None, *args, **kwargs):
        self._get_message_calls += 1
        messages = []
        try:
            prev_data = pickle.load(open(self.name + '.sqs', 'r'))
        except EOFError:
            prev_data = []
        i = 0
        while i < num_messages and len(prev_data) > 0:
            try:
                messages.append(prev_data[i])
            except IndexError:
                pass
            i += 1
        return messages

    def read(self, visibility_timeout=None):
        prev_data = pickle.load(open(self.name + '.sqs', 'r'))
        try:
            return prev_data.pop(0)
        except IndexError:
            # Is this the real action?
            return None

    def write(self, message):
        # Should do some error checking

        # read in all the data in the queue first
        try:
            prev_data = pickle.load(open(self.name + '.sqs', 'r'))
        except EOFError:
            prev_data = []

        prev_data.append(message)

        try:
            pickle.dump(prev_data, open(self.name + '.sqs', 'w'))
        except IOError:
            return False

        return True


class SQSConnectionMock:
    def get_queue(self, queue):
        try:
            open(queue + ".sqs")
        except IOError:
            return None
        try:
            return SQSQueueMock(queue)
        except SyntaxError:
            return None

    def get_all_queues(self, prefix=""):
        queue_list = []
        files = os.listdir(".")
        for f in files:
            if f[-4:] == '.sqs':
                if prefix != "":
                    if f[0:len(prefix)] == prefix:
                        try:
                            # Try to make the queue. If there's
                            # something wrong, just move on.
                            q = SQSQueueMock(f)
                        except SyntaxError:
                            continue
                        queue_list.append(q)
                else:
                    try:
                        # Try to make the queue. If there's something
                        # wrong, just move on.
                        q = SQSQueueMock(f[:-4])
                    except SyntaxError:
                        continue
                    queue_list.append(q)
        return queue_list

    def delete_queue(self, queue, force_deletion=False):
        q = self.get_queue(queue)
        if q.count() != 0:
            # Can only delete empty queues
            return False
        return q.delete()

    def delete_message(self, queue, message):
        return queue.delete_message(message)

    def create_queue(self, name, *args, **kwargs):
        a = SQSQueueMock(name, create=True)
        return a


class test_Channel(Case):
    def removeMockedQueueFile(self, queue):
        """Simple method to remove SQSQueueMock files"""
        try:
            os.remove('%s.sqs' % queue)
        except OSError:
            pass

    def handleMessageCallback(self, message):
        self.callback_message = message

    def setUp(self):
        """Mock the back-end SQS classes"""
        # Sanity check... if SQS is None, then it did not import and we
        # cannot execute our tests.
        if SQS is None:
            raise SkipTest('Boto is not installed')

        # Boto imports in Python 3.3, but then does not execute. Cannot
        # run tests in Python 3.3
        if sys.version_info[0:2] >= (3,0):
            raise SkipTest('Boto does not support Python 3.3')

        # Common variables used in the unit tests
        self.queue_name = 'unittest'

        # Mock the sqs() method that returns an SQSConnection object and
        # instead return an SQSConnectionMock() object.
        self.sqs_conn_mock = SQSConnectionMock()

        def mock_sqs():
            return self.sqs_conn_mock
        SQS.Channel.sqs = mock_sqs()

        # Set up a task exchange for passing tasks through the queue
        self.exchange = kombu.Exchange('test_SQS', type='direct')
        self.queue = kombu.Queue(self.queue_name,
                                 self.exchange,
                                 self.queue_name)

        # Mock up a test SQS Queue with the SQSQueueMock class (and always
        # make sure its a clean empty queue)
        self.removeMockedQueueFile(self.queue_name)
        self.sqs_queue_mock = SQSQueueMock(self.queue_name, create=True)

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
                                   no_ack=True,
                                   callback=self.handleMessageCallback,
                                   consumer_tag='unittest')

    def tearDown(self):
        """Clean up after ourselves"""
        self.removeMockedQueueFile(self.queue_name)

    def test_init(self):
        """kombu.SQS.Channel instantiates correctly with mocked queues"""
        self.assertIn(self.queue_name, self.channel._queue_cache)

    def test_new_queue(self):
        queue_name = "new_unittest_queue"
        self.channel._new_queue(queue_name)
        self.assertTrue(os.path.exists('%s.sqs' % queue_name))
        # For cleanup purposes, delete the queue and the queue file
        self.channel._delete(queue_name)
        self.removeMockedQueueFile(queue_name)

    def test_delete(self):
        queue_name = "new_unittest_queue"
        self.channel._new_queue(queue_name)
        self.channel._delete(queue_name)
        self.removeMockedQueueFile(queue_name)
        self.assertNotIn(queue_name, self.channel._queue_cache)

    def test_get_from_sqs(self):
        # Test getting a single message
        message = "my test message"
        self.producer.publish(message)
        results = self.channel._get_from_sqs(self.queue_name)
        self.assertEquals(len(results), 1)

        # Now test getting many messages
        for i in xrange(3):
            message = "message: %s" % i
            self.producer.publish(message)

        results = self.channel._get_from_sqs(self.queue_name, count=3)
        self.assertEquals(len(results), 3)

    def test_get_with_empty_list(self):
        self.assertRaises(five.Empty, self.channel._get, self.queue_name)

    def test_get_bulk_raises_empty(self):
        self.assertRaises(five.Empty, self.channel._get_bulk, self.queue_name)

    def test_messages_to_payloads(self):
        message_count = 3
        # Create several test messages and publish them
        for i in xrange(message_count):
            message = "message: %s" % i
            self.producer.publish(message)

        # Get the messages now
        messages = self.channel._get_from_sqs(self.queue_name,
                                              count=message_count)

        # Now convert them to payloads
        payloads = self.channel._messages_to_payloads(messages,
                                                      self.queue_name)

        # We got the same number of payloads back, right?
        self.assertEquals(len(payloads), message_count)

        # Make sure they're payload-style objects
        for p in payloads:
            self.assertTrue('properties' in p)

    def test_put_and_get(self):
        message = "my test message"
        self.producer.publish(message)
        results = self.queue(self.channel).get().payload
        self.assertEquals(message, results)

    def test_puts_and_gets(self):
        for i in xrange(3):
            message = "message: %s" % i
            self.producer.publish(message)

        for i in xrange(3):
            self.assertEquals("message: %s" % i,
                              self.queue(self.channel).get().payload)

    def test_put_and_get_bulk(self):
        # With QoS.prefetch_count = 0
        message = "my test message"
        self.producer.publish(message)
        results = self.channel._get_bulk(self.queue_name)
        self.assertEquals(1, len(results))

    def test_puts_and_get_bulk(self):
        # Generate 8 messages
        message_count = 8

        # Set the prefetch_count to 5
        self.channel.qos.prefetch_count = 5

        # Now, generate all the messages
        for i in xrange(message_count):
            message = "message: %s" % i
            self.producer.publish(message)

        # Count how many messages are retrieved the first time. Should
        # be 5 (message_count).
        results = self.channel._get_bulk(self.queue_name)
        self.assertEquals(5, len(results))

        # Now, do the get again, the number of messages returned should be 3.
        results = self.channel._get_bulk(self.queue_name)
        self.assertEquals(3, len(results))

    def test_drain_events_with_empty_list(self):
        def mock_can_consume():
            return False
        self.channel.qos.can_consume = mock_can_consume
        self.assertRaises(five.Empty, self.channel.drain_events)

    def test_drain_events_with_prefetch_5(self):
        # Generate 20 messages
        message_count = 20
        expected_get_message_count = 4

        # Set the prefetch_count to 5
        self.channel.qos.prefetch_count = 5

        # Now, generate all the messages
        for i in xrange(message_count):
            self.producer.publish("message: %s" % i)

        # Now drain all the events
        for i in xrange(message_count):
            self.channel.drain_events()

        # How many times was the SQSConnectionMock get_message method called?
        self.assertEquals(
            expected_get_message_count,
            self.channel._queue_cache[self.queue_name]._get_message_calls)

    def test_drain_events_with_prefetch_none(self):
        # Generate 20 messages
        message_count = 20
        expected_get_message_count = 20

        # Set the prefetch_count to None
        self.channel.qos.prefetch_count = None

        # Now, generate all the messages
        for i in xrange(message_count):
            self.producer.publish("message: %s" % i)

        # Now drain all the events
        for i in xrange(message_count):
            self.channel.drain_events()

        # How many times was the SQSConnectionMock get_message method called?
        self.assertEquals(
            expected_get_message_count,
            self.channel._queue_cache[self.queue_name]._get_message_calls)
