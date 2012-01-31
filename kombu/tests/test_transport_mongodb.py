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
