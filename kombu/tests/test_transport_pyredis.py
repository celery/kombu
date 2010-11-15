import socket
import types
import unittest2 as unittest

from Queue import Empty, Queue as _Queue

from kombu.connection import BrokerConnection
from kombu.entity import Exchange, Queue
from kombu.messaging import Consumer, Producer

from kombu.transport import pyredis

from kombu.tests.utils import module_exists


class ResponseError(Exception):
    pass


class Client(object):

    def __init__(self, db=None, port=None, **kwargs):
        self.port = port
        self.db = db
        self._called = []
        self.queues = {}
        self.sets = {}
        self._connection = None
        self.bgsave_raises_ResponseError = False

    def bgsave(self):
        self._called.append("BGSAVE")
        if self.bgsave_raises_ResponseError:
            raise ResponseError()

    def delete(self, key):
        self.queues.pop(key, None)

    def sadd(self, key, member):
        if key not in self.sets:
            self.sets[key] = set()
        self.sets[key].add(member)

    def smembers(self, key):
        return self.sets.get(key, set())

    def llen(self, key):
        return self.queues[key].qsize()

    def lpush(self, key, value):
        self.queues[key].put_nowait(value)

    def brpop(self, keys, timeout=None):
        key = keys[0]
        try:
            item = self.queues[key].get(timeout=timeout)
        except Empty:
            pass
        else:
            return key, item

    def rpop(self, key):
        try:
            return self.queues[key].get_nowait()
        except KeyError:
            pass

    def __contains__(self, k):
        return k in self._called

    def _new_queue(self, key):
        self.queues[key] = _Queue()

    class _sconnection(object):
        disconnected = False

        def disconnect(self):
            self.disconnected = True

    @property
    def connection(self):
        if self._connection is None:
            self._connection = self._sconnection()
        return self._connection


class Channel(pyredis.Channel):

    def _get_client(self):
        return Client

    def _get_response_error(self):
        return ResponseError

    def _new_queue(self, queue, **kwargs):
        self.client._new_queue(queue)


class Transport(pyredis.Transport):
    Channel = Channel

    def _get_errors(self):
        return ((), ())


class test_Redis(unittest.TestCase):

    def setUp(self):
        self.connection = BrokerConnection(transport=Transport)
        self.exchange = Exchange("test_Redis", type="direct")
        self.queue = Queue("test_Redis", self.exchange, "test_Redis")

    def tearDown(self):
        self.connection.close()

    def test_publish__get(self):
        channel = self.connection.channel()
        producer = Producer(channel, self.exchange, routing_key="test_Redis")
        self.queue(channel).declare()

        producer.publish({"hello": "world"})

        self.assertDictEqual(self.queue(channel).get().payload,
                             {"hello": "world"})
        self.assertIsNone(self.queue(channel).get())
        self.assertIsNone(self.queue(channel).get())
        self.assertIsNone(self.queue(channel).get())

    def test_publish__consume(self):
        connection = BrokerConnection(transport=Transport)
        channel = connection.channel()
        producer = Producer(channel, self.exchange, routing_key="test_Redis")
        consumer = Consumer(channel, self.queue)

        producer.publish({"hello2": "world2"})
        _received = []

        def callback(message_data, message):
            _received.append(message_data)
            message.ack()

        consumer.register_callback(callback)
        consumer.consume()

        self.assertTrue(channel._poller._can_start())
        try:
            connection.drain_events(timeout=1)
            self.assertTrue(_received)
            self.assertFalse(channel._poller._can_start())
            self.assertRaises(socket.timeout,
                              connection.drain_events, timeout=0.01)
        finally:
            channel.close()

    def test_purge(self):
        channel = self.connection.channel()
        producer = Producer(channel, self.exchange, routing_key="test_Redis")
        self.queue(channel).declare()

        for i in range(10):
            producer.publish({"hello": "world-%s" % (i, )})

        self.assertEqual(channel._size("test_Redis"), 10)
        self.assertEqual(self.queue(channel).purge(), 10)
        channel.close()

    def test_db_values(self):
        c1 = BrokerConnection(virtual_host=1,
                              transport=Transport).channel()
        self.assertEqual(c1.client.db, 1)

        c2 = BrokerConnection(virtual_host="1",
                              transport=Transport).channel()
        self.assertEqual(c2.client.db, 1)

        c3 = BrokerConnection(virtual_host="/1",
                              transport=Transport).channel()
        self.assertEqual(c3.client.db, 1)

        c4 = BrokerConnection(virtual_host="/foo",
                              transport=Transport).channel()
        self.assertRaises(ValueError, getattr, c4, "client")

    def test_db_port(self):
        c1 = BrokerConnection(port=None, transport=Transport).channel()
        self.assertEqual(c1.client.port, Transport.default_port)
        c1.close()

        c2 = BrokerConnection(port=9999, transport=Transport).channel()
        self.assertEqual(c2.client.port, 9999)
        c2.close()

    def test_close_poller_not_active(self):
        c = BrokerConnection(transport=Transport).channel()
        c.client.connection
        c.close()
        self.assertFalse(c._poller.isAlive())
        self.assertTrue("BGSAVE" in c.client)

    def test_close_ResponseError(self):
        c = BrokerConnection(transport=Transport).channel()
        c.client.bgsave_raises_ResponseError = True
        c.close()

    def test_close_disconnects(self):
        c = BrokerConnection(transport=Transport).channel()
        conn = c.client.connection
        c.close()
        self.assertTrue(conn.disconnected)

    def test_get__Empty(self):
        channel = self.connection.channel()
        self.assertRaises(Empty, channel._get, "does-not-exist")
        channel.close()

    def test_get_client(self):

        redis, exceptions = _redis_modules()

        @module_exists(redis, exceptions)
        def _do_test():
            conn = BrokerConnection(transport="redis")
            chan = conn.channel()
            self.assertTrue(chan.Client)
            self.assertTrue(chan.ResponseError)
            self.assertTrue(conn.transport.connection_errors)
            self.assertTrue(conn.transport.channel_errors)

        _do_test()


def _redis_modules():

    class ConnectionError(Exception):
        pass

    class AuthenticationError(Exception):
        pass

    class InvalidData(Exception):
        pass

    class InvalidResponse(Exception):
        pass

    class ResponseError(Exception):
        pass

    exceptions = types.ModuleType("redis.exceptions")
    exceptions.ConnectionError = ConnectionError
    exceptions.AuthenticationError = AuthenticationError
    exceptions.InvalidData = InvalidData
    exceptions.InvalidResponse = InvalidResponse
    exceptions.ResponseError = ResponseError

    class Redis(object):
        pass

    redis = types.ModuleType("redis")
    redis.exceptions = exceptions
    redis.Redis = Redis

    return redis, exceptions
