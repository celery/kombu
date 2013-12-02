"""Testing module for the kombu.transport.SQS package.

NOTE: The SQSQueueMock and SQSConnectionMock classes originally come from
http://github.com/pcsforeducation/sqs-mock-python. They have been patched
slightly.
"""

from __future__ import absolute_import

import os
import pickle

from kombu import Connection
from kombu.tests.case import Case
from kombu.transport import SQS


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
        return count(page_size=10, vtimeout=10)

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
    def get_messages(self, num_messages=1, visibility_timeout=None, attributes=None, *args, **kwargs):
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
            queue_file = open(queue + ".sqs")
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
                            # Try to make the queue. If there's something wrong, just move on.
                            q = SQSQueueMock(f)
                        except SyntaxError:
                            continue
                        queue_list.append(q)
                else:
                    try:
                        # Try to make the queue. If there's something wrong, just move on.
                        q = SQSQueueMock(f[:-4])
                    except SyntaxError:
                        print 'err', f
                        continue
                    queue_list.append(q)
        return queue_list
    def delete_queue(self, queue, force_deletion=False):
        q = self.get_queue(queue)
        #print 'type', type(q)
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
        # Common variables used in the unit tests
        self.queue_name = 'unittest'

        # Mock the sqs() method that returns an SQSConnection object and
        # instead return an SQSConnectionMock() object.
        self.sqs_conn_mock = SQSConnectionMock()
        def mock_sqs():
            return self.sqs_conn_mock
        SQS.Channel.sqs = mock_sqs()

        # Mock up a test SQS Queue with the SQSQueueMock class (and always
        # make sure its a clean empty queue)
        self.removeMockedQueueFile(self.queue_name)
        self.sqs_queue_mock = SQSQueueMock(self.queue_name, create=True)

        # Now, create our Connection object with the SQS Transport and store
        # the connection/channel objects as references for use in these tests.
        self.connection = Connection(transport=SQS.Transport)
        self.channel = self.connection.channel()

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
        result = self.channel._new_queue(queue_name)
        self.assertTrue(os.path.exists('%s.sqs' % queue_name))
        # For cleanup purposes, delete the queue and the queue file
        self.channel._delete(queue_name)
        self.removeMockedQueueFile(queue_name)

    def test_delete(self):
        queue_name = 'newunittestqueue'
        self.channel._new_queue(queue_name)
        self.channel._delete(queue_name)
        self.removeMockedQueueFile(queue_name)
        self.assertNotIn(queue_name, self.channel._queue_cache)

    def test_put_and_get(self):
        message = "my test message"
        self.channel._put(self.queue_name, message)
        returned_message = self.channel._get(self.queue_name)
        self.assertEquals(message, returned_message)

    def test_puts_and_gets(self):
        message1 = "my test message1"
        message2 = "my test message2"
        message3 = "my test message3"
        self.channel._put(self.queue_name, message1)
        self.channel._put(self.queue_name, message2)
        self.channel._put(self.queue_name, message3)
        self.assertEquals(message1, self.channel._get(self.queue_name))
        self.assertEquals(message2, self.channel._get(self.queue_name))
        self.assertEquals(message3, self.channel._get(self.queue_name))
