from __future__ import absolute_import
from __future__ import with_statement

import socket

from ..connection import BrokerConnection
from ..entity import Exchange, Queue
from ..messaging import Consumer, Producer

from .utils import TestCase

import sys


class test_MongoDBTransport(TestCase):

    def setUp(self):
        self.c = BrokerConnection(transport="mongodb")
        self.e = Exchange("test_transport_mongodb", type="fanout")
        self.q = Queue("test_transport_mongodb",
                       exchange=self.e,
                       routing_key="test_transport_mongodb")
        self.q2 = Queue("test_transport_memory2",
                        exchange=self.e,
                        routing_key="test_transport_mongodb2")
                        
    def test_fanout(self):
        return
        fanexch = Exchange("test_transport_mongodb_fanout", type="fanout")
        channel = self.c.channel()
        producer = Producer(channel, fanexch)
        _received = []
        
        self.assertEqual(len(channel._queue_cursors), 0)
        badMsg = {"please":"noFails"}
        producer.publish(badMsg)
        
        def callback(message_data, message):
            print "_callback"
            _received.append(message)
            message.ack()
            
        goodMsg = {"something":"important"}
        fanqueue = Queue("test_transport_mongodb_fanq", exchange=fanexch, channel=channel)        
        
        consumer = Consumer(channel, fanqueue)
        consumer.register_callback(callback)
        consumer.register_callback(lambda p,x: sys.stdout("_derp"))
        
        self.assertIn("test_transport_mongodb_fanq", channel._fanout_queues)
        
        producer.publish(goodMsg)
        consumer.consume()
        channel.drain_events()
        
        self.assertIn(goodMsg, _received)
        self.assertNotIn(badMsg, _received)
        
        fanqueue.delete()
        self.assertEqual(len(channel._queue_cursors), 0)
        

    def test_produce_consume(self):
        channel = self.c.channel()
        producer = Producer(channel, self.e)
        consumer1 = Consumer(channel, self.q)
        consumer2 = Consumer(channel, self.q2)
        self.q2(channel).declare()

        for i in range(10):
            producer.publish({"foo": i}, routing_key="test_transport_mongodb")
        for i in range(10):
            producer.publish({"foo": i}, routing_key="test_transport_mongodb2")

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

        consumer1.consume()
        consumer2.consume()

        while 1:
            if len(_received1) + len(_received2) == 20:
                break
            self.c.drain_events()

        self.assertEqual(len(_received1) + len(_received2), 20)

        # queue.delete
        for i in range(10):
            producer.publish({"foo": i}, routing_key="test_transport_mongodb")
        self.assertTrue(self.q(channel).get())
        self.q(channel).delete()
        self.q(channel).declare()
        self.assertIsNone(self.q(channel).get())

        # queue.purge
        for i in range(10):
            producer.publish({"foo": i}, routing_key="test_transport_mongodb2")
        self.assertTrue(self.q2(channel).get())
        self.q2(channel).purge()
        self.assertIsNone(self.q2(channel).get())
