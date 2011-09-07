from __future__ import absolute_import

import socket
import types

from itertools import count
from Queue import Empty, Queue as _Queue

from ..connection import BrokerConnection
from ..entity import Exchange, Queue
from ..messaging import Consumer, Producer
from ..utils import eventio  # patch poll

from .utils import unittest
from .utils import module_exists


class _poll(eventio._select):

    def poll(self, timeout):
        events = []
        for fd in self._rfd:
            if fd.data:
                events.append((fd.fileno(), eventio.POLL_READ))
        return events


eventio.poll = _poll
from ..transport import redis  # must import after poller patch


class ResponseError(Exception):
    pass


class Client(object):
    queues = {}
    sets = {}
    shard_hint = None

    def __init__(self, db=None, port=None, **kwargs):
        self.port = port
        self.db = db
        self._called = []
        self._connection = None
        self.bgsave_raises_ResponseError = False
        self.connection = self._sconnection(self)

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

    def parse_response(self, connection, type, **options):
        cmd, queues = self.connection._sock.data.pop()
        assert cmd == type
        self.connection._sock.data = []
        if type == "BRPOP":
            item = self.brpop(queues, 0.001)
            if item:
                return item
            raise Empty()

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

    def pipeline(self):
        return Pipeline(self)

    def encode(self, value):
        return str(value)

    def _new_queue(self, key):
        self.queues[key] = _Queue()

    class _sconnection(object):
        disconnected = False

        class _socket(object):
            blocking = True
            next_fileno = count(30).next

            def __init__(self, *args):
                self._fileno = self.next_fileno()
                self.data = []

            def fileno(self):
                return self._fileno

            def setblocking(self, blocking):
                self.blocking = blocking

        def __init__(self, client):
            self.client = client
            self._sock = self._socket()

        def disconnect(self):
            self.disconnected = True

        def send_command(self, cmd, *args):
            self._sock.data.append((cmd, args))

    def info(self):
        return {"foo": 1}

    def pubsub(self, *args, **kwargs):
        connection = self.connection

        class ConnectionPool(object):

            def get_connection(self, *args, **kwargs):
                return connection
        self.connection_pool = ConnectionPool()

        return self


class Pipeline(object):

    def __init__(self, client):
        self.client = client
        self.stack = []

    def __getattr__(self, key):
        if key not in self.__dict__:

            def _add(*args, **kwargs):
                self.stack.append((getattr(self.client, key), args, kwargs))
                return self

            return _add
        return self.__dict__[key]

    def execute(self):
        stack = list(self.stack)
        self.stack[:] = []
        return [fun(*args, **kwargs) for fun, args, kwargs in stack]


class Channel(redis.Channel):

    def _get_client(self):
        return Client

    def _get_response_error(self):
        return ResponseError

    def _new_queue(self, queue, **kwargs):
        self.client._new_queue(queue)


class Transport(redis.Transport):
    Channel = Channel

    def _get_errors(self):
        return ((KeyError, ), (IndexError, ))


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

        self.assertIn(channel, channel.connection.cycle._channels)
        try:
            connection.drain_events(timeout=1)
            self.assertTrue(_received)
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

        self.assertRaises(BrokerConnection(virtual_host="/foo",
                              transport=Transport).channel)

    def test_db_port(self):
        c1 = BrokerConnection(port=None, transport=Transport).channel()
        self.assertEqual(c1.client.port, Transport.default_port)
        c1.close()

        c2 = BrokerConnection(port=9999, transport=Transport).channel()
        self.assertEqual(c2.client.port, 9999)
        c2.close()

    def test_close_poller_not_active(self):
        c = BrokerConnection(transport=Transport).channel()
        cycle = c.connection.cycle
        c.client.connection
        c.close()
        self.assertNotIn(c, cycle._channels)

    def test_close_ResponseError(self):
        c = BrokerConnection(transport=Transport).channel()
        c.client.bgsave_raises_ResponseError = True
        c.close()

    def test_close_disconnects(self):
        c = BrokerConnection(transport=Transport).channel()
        conn1 = c.client.connection
        conn2 = c.subclient.connection
        c.close()
        self.assertTrue(conn1.disconnected)
        self.assertTrue(conn2.disconnected)

    def test_get__Empty(self):
        channel = self.connection.channel()
        self.assertRaises(Empty, channel._get, "does-not-exist")
        channel.close()

    def test_get_client(self):

        myredis, exceptions = _redis_modules()

        @module_exists(myredis, exceptions)
        def _do_test():
            conn = BrokerConnection(transport=Transport)
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

    myredis = types.ModuleType("redis")
    myredis.exceptions = exceptions
    myredis.Redis = Redis

    return myredis, exceptions
