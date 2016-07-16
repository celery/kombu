from __future__ import absolute_import, unicode_literals

from kombu import Consumer, Producer, Exchange, Queue
from kombu.five import range
from kombu.utils.compat import nested

from funtests import transport

from kombu.tests.case import skip


@skip.unless_module('pymongo')
class test_mongodb(transport.TransportCase):
    transport = 'mongodb'
    prefix = 'mongodb'
    event_loop_max = 100

    def after_connect(self, connection):
        connection.channel().client  # evaluate connection.
        self.c = self.connection   # shortcut

    def test_fanout(self, name='test_mongodb_fanout'):
        if not self.verify_alive():
            return
        c = self.connection
        self.e = Exchange(name, type='fanout')
        self.q = Queue(name, exchange=self.e, routing_key=name)
        self.q2 = Queue(name + '2', exchange=self.e, routing_key=name + '2')

        channel = c.default_channel
        producer = Producer(channel, self.e)
        consumer1 = Consumer(channel, self.q)
        consumer2 = Consumer(channel, self.q2)
        self.q2(channel).declare()

        for i in range(10):
            producer.publish({'foo': i}, routing_key=name)
        for i in range(10):
            producer.publish({'foo': i}, routing_key=name + '2')

        _received1 = []
        _received2 = []

        def callback1(message_data, message):
            _received1.append(message)
            message.ack()

        def callback2(message_data, message):
            _received2.append(message)
            message.ack()

        consumer1.register_callback(callback1)
        consumer2.register_callback(callback2)

        with nested(consumer1, consumer2):

            while 1:
                if len(_received1) + len(_received2) == 20:
                    break
                c.drain_events(timeout=60)
        self.assertEqual(len(_received1) + len(_received2), 20)

        # queue.delete
        for i in range(10):
            producer.publish({'foo': i}, routing_key=name)
        self.assertTrue(self.q(channel).get())
        self.q(channel).delete()
        self.q(channel).declare()
        self.assertIsNone(self.q(channel).get())

        # queue.purge
        for i in range(10):
            producer.publish({'foo': i}, routing_key=name + '2')
        self.assertTrue(self.q2(channel).get())
        self.q2(channel).purge()
        self.assertIsNone(self.q2(channel).get())
