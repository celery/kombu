from __future__ import annotations

import socket
from itertools import count
from queue import Empty
from unittest.mock import Mock, patch

import pytest
from kombu import Connection, Producer, Exchange, Queue, Consumer
from kombu.transport import rediscluster

pytest.importorskip('redis')

from t.unit.transport.test_redis import Client, _poll, test_MultiChannelPoller


class ClusterNode:
    def __init__(self, host=None, port=None):
        self.redis_connection = ClusterClient(host=host, port=port)
        self.host = host
        self.port = port
        self.name = f'{self.host}:{self.port}'


class NodesManager:

    def __init__(self, host=None, port=None, **kwargs):
        self.host = host
        self.port = port

    def get_node_from_slot(self, *args, **kwargs):
        return ClusterNode(host=self.host, port=self.port)


class ClusterConnection:
    class _socket:
        blocking = True
        filenos = count(30)

        def __init__(self, *args):
            self._fileno = next(self.filenos)
            self.data = []

        def fileno(self):
            return self._fileno

    def __init__(self, host=None, port=None):
        self._sock = self._socket()
        self.host = host
        self.port = port

    def read_response(self, type, **kwargs):
        cmd, queues = self._sock.data.pop()
        queues = list(queues)
        assert cmd == type
        self._sock.data = []
        if type == 'BRPOP':
            for queue in queues:
                try:
                    item = Client.queues[queue].get_nowait()
                    if item:
                        return queue, item
                except Empty:
                    pass
                raise Empty()

    def send_command(self, cmd, *args):
        self._sock.data.append((cmd, args))

    def disconnect(self):
        pass


class ClusterConnectionPool:
    def __init__(self, **kwargs):
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')

    def get_connection(self, key):
        return ClusterConnection()


class ClusterClient(Client):
    read_from_replicas = True
    DEFAULT_PORT = 6379
    DEFAULT_HOST = 'localhost'

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, **kwargs):
        self.nodes_manager = NodesManager(**kwargs)
        self.connection_pool = ClusterConnectionPool(**kwargs)
        self.lock_id = None

    def keyslot(self, key):
        return 0

    def set(self, name, lock_id, **kwargs):
        self.lock_id = lock_id
        return True

    def get(self, name):
        return self.lock_id

    def zrevrangebyscore(self, *args, **kwargs):
        return []


class Channel(rediscluster.Channel):

    def _get_client(self):
        return ClusterClient

    def _new_queue(self, queue, **kwargs):
        self.client._new_queue(queue)


class Transport(rediscluster.Transport):
    Channel = Channel
    connection_errors = (KeyError,)
    channel_errors = (IndexError,)


class test_Channel:

    def setup_method(self):
        self.connection = self.create_connection()
        self.channel = self.connection.default_channel

    def create_connection(self, **kwargs):
        kwargs.setdefault('transport_options', {'fanout_patterns': True})
        return Connection(transport=Transport, **kwargs)

    def _get_one_delivery_tag(self, n='test_uniq_tag'):
        with self.create_connection() as conn1:
            chan = conn1.default_channel
            chan.exchange_declare(n)
            chan.queue_declare(n)
            chan.queue_bind(n, n, n)
            msg = chan.prepare_message('quick brown fox')
            chan.basic_publish(msg, n, n)
            payload = chan._get(n)
            assert payload
            pymsg = chan.message_to_python(payload)
            return pymsg.delivery_tag

    def test_delivery_tag_is_uuid(self):
        seen = set()
        for i in range(100):
            tag = self._get_one_delivery_tag()
            assert tag not in seen
            seen.add(tag)
            with pytest.raises(ValueError):
                int(tag)
            assert len(tag) == 36

    def test_ssl_argument__dict(self):
        with patch('kombu.transport.rediscluster.Channel._create_client'):
            ssl_params = {
                'ssl_cert_reqs': 2,
                'ssl_ca_certs': '/foo/ca.pem',
                'ssl_certfile': '/foo/cert.crt',
                'ssl_keyfile': '/foo/pkey.key'
            }
            with Connection('rediscluster://', ssl=ssl_params) as conn:
                params = conn.default_channel._connparams()
                assert params['ssl_cert_reqs'] == ssl_params['ssl_cert_reqs']
                assert params['ssl_ca_certs'] == ssl_params['ssl_ca_certs']
                assert params['ssl_certfile'] == ssl_params['ssl_certfile']
                assert params['ssl_keyfile'] == ssl_params['ssl_keyfile']
                assert params.get('ssl') is None


class test_RedisCluster:

    def test_publish__consume(self):
        connection = Connection(transport=Transport)
        connection.transport.cycle.poller = _poll()
        channel = connection.channel()
        exchange = Exchange('test_Redis', type='direct')
        queue = Queue('test_Redis', exchange, 'test_Redis')
        producer = Producer(channel, exchange, routing_key='test_Redis')
        consumer = Consumer(channel, queues=[queue])

        producer.publish({'hello': 'world'})
        _received = []

        def callback(message_data, message):
            _received.append(message_data)
            message.ack()

        consumer.register_callback(callback)
        consumer.consume()

        assert channel in channel.connection.cycle._channels
        try:
            connection.drain_events(timeout=1)
            assert _received
            with pytest.raises(socket.timeout):
                connection.drain_events(timeout=0.01)
        finally:
            channel.close()

    def test_close_in_poll(self):
        conn = Connection(transport=Transport)
        channel = conn.channel()

        conn.transport.cycle._chan_to_sock = {(channel, None, ClusterConnection(), 'BRPOP'): 6}
        for (channel, client, c, type) in conn.transport.cycle._chan_to_sock:
            c._sock.data = [('BRPOP', ('test_Redis',))]
        channel._in_poll = True
        channel.close()
        for (cha, client, c, type) in conn.transport.cycle._chan_to_sock:
            if cha == channel:
                assert c._sock.data == []


class test_ClusterMultiChannelPoller(test_MultiChannelPoller):
    def setup_method(self):
        self.Poller = rediscluster.MultiChannelPoller


class test_Mutex:

    def test_mutex(self):
        mock_uuid = rediscluster.uuid = Mock()
        mock_uuid.return_value = 'mock_uuid'
        client = Mock(name='client')

        # Won
        client.set.return_value = True
        client.get.return_value = b'mock_uuid'
        with rediscluster.Mutex(client, 'foo1', 100):
            held = True
        assert held
        client.delete.assert_called_with('foo1')
        client.get.assert_called_with('foo1')
        client.reset_mock()

        # Did not win
        client.set.return_value = None
        client.get.return_value = b'mock_uuid'
        with pytest.raises(rediscluster.MutexHeld):
            with rediscluster.Mutex(client, 'foo1', 100):
                held = True
            assert not held
