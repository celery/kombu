from __future__ import annotations

import base64
import copy
import socket
from collections import defaultdict
from itertools import count
from queue import Empty
from queue import Queue as _Queue
from unittest.mock import ANY, Mock, call, patch

import pytest
from redis.exceptions import MovedError

from kombu import Connection, Consumer, Exchange, Producer, Queue
from kombu.exceptions import VersionMismatch
from kombu.transport import redis as _redis
from kombu.transport import rediscluster as redis
from kombu.transport import virtual
from kombu.utils import eventio
from kombu.utils.json import dumps


class _poll(eventio._select):

    def register(self, fd, flags):
        if flags & eventio.READ:
            self._rfd.add(fd)

    def poll(self, timeout):
        events = []
        for fd in self._rfd:
            if fd.data:
                events.append((fd.fileno(), eventio.READ))
        return events


_redis.poll = _poll

pytest.importorskip('redis')


class RedisCommandBase:
    data = defaultdict(dict)
    sets = defaultdict(set)
    hashes = defaultdict(dict)
    queues = {}

    def __init__(self, *args, **kwargs):
        self.connection_pool = RedisConnectionPool()

    def sadd(self, key, value):
        if key not in self.data:
            self.data[key] = set()
        self.data[key].add(value)

    def smembers(self, key):
        return self.data.get(key)

    def hset(self, key, k, v):
        self.hashes[key][k] = v

    def hget(self, key, k):
        return self.hashes[key].get(k)

    def hdel(self, key, k):
        self.hashes[key].pop(k, None)

    def ping(self):
        return True

    def zrevrangebyscore(self):
        return []

    def llen(self, key):
        try:
            return self.queues[key].qsize()
        except KeyError:
            return 0

    def lpush(self, key, value):
        self.queues[key].put_nowait(value)

    def set(self, key, value, **kwargs):
        self.data[key] = value
        return True

    def get(self, key):
        return self.data[key]

    def delete(self, key):
        self.data.pop(key, None)

    def rpop(self, key):
        try:
            return self.queues[key].get_nowait()
        except (KeyError, Empty):
            pass

    def brpop(self, keys, timeout=None):
        for key in keys:
            try:
                item = self.queues[key].get_nowait()
            except Empty:
                pass
            else:
                return key, item

    def zadd(self, key, *args):
        (mapping,) = args
        for item in mapping:
            self.sets[key].add(item)

    def zrem(self, key, *args):
        self.sets.pop(key, None)

    def srem(self, key, *args):
        self.sets.pop(key, None)


class RedisPipelineBase:
    def __init__(self, client):
        self.client = client
        self.command_stack = []

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        pass

    def __getattr__(self, key):
        if key not in self.__dict__:
            def _add(*args, **kwargs):
                self.command_stack.append((getattr(self.client, key), args, kwargs))
                return self

            return _add
        return self.__dict__[key]

    def watch(self, key):
        pass

    def multi(self):
        pass

    def execute(self):
        stack = list(self.command_stack)
        self.command_stack[:] = []
        return [fun(*args, **kwargs) for fun, args, kwargs in stack]


class RedisConnection:
    class _socket:
        filenos = count(30)

        def __init__(self, *args):
            self._fileno = next(self.filenos)
            self.data = []

        def fileno(self):
            return self._fileno

    def __init__(self, host="localhost", port=6379):
        self._sock = self._socket()
        self.host = host
        self.port = port

    def send_command(self, cmd, *args, **kwargs):
        self._sock.data.append((cmd, args))

    def read_response(self, *args, **kwargs):
        cmd, queues = self._sock.data.pop()
        queues = list(queues)
        self._sock.data = []
        if cmd == 'BRPOP':
            queues.pop()
            item = None
            for key in queues:
                try:
                    res = RedisCommandBase.queues[key].get_nowait()
                except Empty:
                    pass
                else:
                    item = key, res
            if item:
                return item
            raise Empty()


class RedisConnectionPool:
    def __init__(self, *args, **kargs):
        self._available_connections = []
        self._in_use_connections = set()

    def get_connection(self, command):
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.make_connection()
        self._in_use_connections.add(connection)
        return connection

    def make_connection(self):
        return RedisConnection()


class ClusterPipeline(RedisPipelineBase):
    pass


class RedisPipeline(RedisPipelineBase):
    pass


class Redis(RedisCommandBase):

    def pipeline(self):
        return RedisPipeline(self)


class RedisCluster(RedisCommandBase):
    db_0 = {}

    def __init__(self, *args, **kwargs):
        self.nodes_manager = NodesManager()

    def disconnect_connection_pools(self):
        pass

    def close(self):
        pass

    def pipeline(self):
        return ClusterPipeline(self)

    def keyslot(self, key):
        return 0


DEFAULT_PORT = 6379
DEFAULT_HOST = 'localhost'


class NodesManager:
    def __init__(self):
        self.nodes_cache = {0: ClusterNode()}

    def get_node_from_slot(self, slot, **kwargs):
        return self.nodes_cache.get(slot)


class ClusterNode:
    def __init__(self, host="localhost", port=6379):
        self.host = host
        self.port = port
        self.name = f'{self.host}:{self.port}'
        self.redis_connection = Redis()


class Channel(redis.Channel):

    def _get_client(self):
        return RedisCluster

    def _new_queue(self, queue, **kwargs):
        for pri in self.priority_steps:
            self.client.queues[self._q_for_pri(queue, pri)] = _Queue()


class Transport(redis.Transport):
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

    def test_init(self):
        hash_tag = None
        channel = self.create_connection().channel()
        assert channel.priority_steps == [0]
        assert channel._registered is True
        assert channel.global_keyprefix == ''

        hash_tag = '{tag}'
        channel = self.create_connection(transport_options={'hash_tag': hash_tag}).channel()
        assert channel.priority_steps == [0, 3, 6, 9]
        assert channel._registered is True
        assert channel.global_keyprefix == '{tag}'
        assert channel.unacked_mutex_key == 'unacked_mutex'
        assert channel.unacked_index_key == 'unacked_index'
        assert channel.unacked_key == 'unacked'
        assert channel.keyprefix_fanout == '/{db}.'
        assert channel.keyprefix_queue == '_kombu.binding.%s'

        global_keyprefix = 'foo'
        channel = self.create_connection(transport_options={'global_keyprefix': global_keyprefix}).channel()
        assert channel.priority_steps == [0]
        assert channel._registered is True
        assert channel.global_keyprefix == global_keyprefix

    def test_after_fork(self):
        channel = self.create_connection().channel()
        channel._after_fork()
        assert channel._client is None

    def test_sep_transport_option(self):
        with Connection(transport=Transport, transport_options={
            'sep': ':',
        }) as conn:
            key = conn.default_channel.keyprefix_queue % 'celery'
            conn.default_channel.client.sadd(key, 'celery::celery')

            assert conn.default_channel.sep == ':'
            assert conn.default_channel.get_table('celery') == [
                ('celery', '', 'celery'),
            ]

    def test_ack_emulation_transport_option(self):
        conn = Connection(transport=Transport, transport_options={
            'ack_emulation': False,
        })

        chan = conn.channel()
        assert not chan.ack_emulation
        assert chan.QoS == virtual.QoS

    def test_do_restore_message(self):
        client = Mock(name='client')
        pl1 = {'body': 'BODY'}
        spl1 = dumps(pl1)
        lookup = self.channel._lookup = Mock(name='_lookup')
        lookup.return_value = {'george', 'elaine'}
        self.channel._do_restore_message(
            pl1, 'ex', 'rkey', client,
        )
        client.rpush.assert_has_calls([
            call('george', spl1), call('elaine', spl1),
        ], any_order=True)

        client = Mock(name='client')
        pl2 = {'body': 'BODY2', 'headers': {'x-funny': 1}}
        headers_after = dict(pl2['headers'], redelivered=True)
        spl2 = dumps(dict(pl2, headers=headers_after))
        self.channel._do_restore_message(
            pl2, 'ex', 'rkey', client,
        )
        client.rpush.assert_any_call('george', spl2)
        client.rpush.assert_any_call('elaine', spl2)

        client.rpush.side_effect = KeyError()
        with patch('kombu.transport.rediscluster.crit') as crit:
            self.channel._do_restore_message(
                pl2, 'ex', 'rkey', client,
            )
            crit.assert_called()

    def test_do_restore_message_celery(self):
        payload = {
            "body": base64.b64encode(dumps([
                [],
                {},
                {
                    "callbacks": None,
                    "errbacks": None,
                    "chain": None,
                    "chord": None,
                },
            ]).encode()).decode(),
            "content-encoding": "utf-8",
            "content-type": "application/json",
            "headers": {
                "lang": "py",
                "task": "common.tasks.test_task",
                "id": "980ad2bf-104c-4ce0-8643-67d1947173f6",
                "shadow": None,
                "eta": None,
                "expires": None,
                "group": None,
                "group_index": None,
                "retries": 0,
                "timelimit": [None, None],
                "root_id": "980ad2bf-104c-4ce0-8643-67d1947173f6",
                "parent_id": None,
                "argsrepr": "()",
                "kwargsrepr": "{}",
                "origin": "gen3437@Desktop",
                "ignore_result": False,
            },
            "properties": {
                "correlation_id": "980ad2bf-104c-4ce0-8643-67d1947173f6",
                "reply_to": "512f2489-ca40-3585-bc10-9b801a981782",
                "delivery_mode": 2,
                "delivery_info": {
                    "exchange": "",
                    "routing_key": "celery",
                },
                "priority": 3,
                "body_encoding": "base64",
                "delivery_tag": "badb725e-9c3e-45be-b0a4-07e44630519f",
            },
        }
        result_payload = copy.deepcopy(payload)
        result_payload['headers']['redelivered'] = True
        result_payload['properties']['delivery_info']['redelivered'] = True
        queue = 'celery'

        client = Mock(name='client')
        lookup = self.channel._lookup = Mock(name='_lookup')
        lookup.return_value = [queue]

        self.channel._do_restore_message(
            payload, 'exchange', 'routing_key', client,
        )

        client.rpush.assert_called_with(self.channel._q_for_pri(queue, 3),
                                        dumps(result_payload))

    def test_restore_messages(self):
        message = Mock(name='message')
        message.delivery_tag = mock_tag = 'tag'
        channel = self.create_connection().channel()
        _do_restore_message = channel._do_restore_message = Mock()
        channel.client.hset('unacked', mock_tag, message)
        with patch('kombu.transport.rediscluster.loads') as loads:
            loads.return_value = 'M', 'EX', 'RK'
            channel._restore(message)
        _do_restore_message.assert_called_with('M', 'EX', 'RK', ANY, False)

    def test_qos_restore_visible(self):
        channel = self.create_connection().channel()
        client = channel.client
        zrevrangebyscore = client.zrevrangebyscore = Mock()
        zrevrangebyscore.return_value = [
            (1, 10),
            (2, 20),
            (3, 30),
        ]
        qos = redis.QoS(channel)
        restore = qos.restore_by_tag = Mock(name='restore_by_tag')
        qos._vrestore_count = 1
        qos.restore_visible()
        zrevrangebyscore.assert_not_called()
        assert qos._vrestore_count == 2

        qos._vrestore_count = 0
        qos.restore_visible()
        restore.assert_has_calls([
            call(1, client), call(2, client), call(3, client),
        ])
        assert qos._vrestore_count == 1

        qos._vrestore_count = 0
        restore.reset_mock()
        zrevrangebyscore.return_value = []
        qos.restore_visible()
        restore.assert_not_called()
        assert qos._vrestore_count == 1

        qos._vrestore_count = 0
        set = client.set = Mock()
        set.side_effect = redis.MutexHeld()
        qos.restore_visible()

    def test_basic_consume_when_fanout_queue(self):
        self.channel.exchange_declare(exchange='txconfan', type='fanout')
        self.channel.queue_declare(queue='txconfanq')
        self.channel.queue_bind(queue='txconfanq', exchange='txconfan')

        assert 'txconfanq' in self.channel._fanout_queues
        self.channel.basic_consume('txconfanq', False, None, 1)
        assert 'txconfanq' in self.channel.active_fanout_queues
        assert self.channel._fanout_to_queue.get('txconfan') == 'txconfanq'

    @patch('redis.cluster.RedisCluster.execute_command')
    @patch('redis.cluster.NodesManager.initialize')
    def test_get_prefixed_client(self, mock_initialize, mock_execute_command):
        self.channel.global_keyprefix = "test_"
        PrefixedRedis = redis.Channel._get_client(self.channel)
        assert isinstance(PrefixedRedis(startup_nodes=[ClusterNode()]), redis.PrefixedStrictRedis)

    @patch("redis.cluster.RedisCluster.execute_command")
    @patch("redis.cluster.NodesManager.initialize")
    def test_global_keyprefix(self, mock_initialize, mock_execute_command):
        with Connection(transport=Transport) as conn:
            client = redis.PrefixedStrictRedis(global_keyprefix='foo_', startup_nodes=[ClusterNode()])

            channel = conn.channel()
            channel._create_client = Mock()
            channel._create_client.return_value = client

            body = {'hello': 'world'}
            channel._put_fanout('exchange', body, '')
            mock_execute_command.assert_called_with(
                'PUBLISH',
                'foo_/{db}.exchange',
                dumps(body)
            )

    @patch("redis.cluster.RedisCluster.execute_command")
    @patch("redis.cluster.NodesManager.initialize")
    def test_global_keyprefix_queue_bind(self, mock_initialize, mock_execute_command):
        with Connection(transport=Transport) as conn:
            client = redis.PrefixedStrictRedis(global_keyprefix='foo_', startup_nodes=[ClusterNode()])

            channel = conn.channel()
            channel._create_client = Mock()
            channel._create_client.return_value = client

            channel._queue_bind('default', '', None, 'queue')
            mock_execute_command.assert_called_with(
                'SADD',
                'foo__kombu.binding.default',
                '\x06\x16\x06\x16queue'
            )

    @patch("redis.cluster.RedisCluster.execute_command")
    @patch('redis.cluster.ClusterPubSub.execute_command')
    @patch('redis.cluster.NodesManager.initialize')
    def test_global_keyprefix_pubsub(self, mock_initialize, mock_pubsub, mock_execute_command):
        with Connection(transport=Transport) as conn:
            client = redis.PrefixedStrictRedis(global_keyprefix='foo_', startup_nodes=[ClusterNode()])

            channel = conn.channel()
            channel.global_keyprefix = 'foo_'
            channel._create_client = Mock()
            channel._create_client.return_value = client
            channel.subclient.connection = Mock()
            channel._fanout_queues.update(a=('a', ''))
            channel.active_fanout_queues.add('a')

            channel._subscribe()
            mock_pubsub.assert_called_with(
                'PSUBSCRIBE',
                'foo_/{db}.a',
            )

    @patch("redis.cluster.RedisCluster.execute_command")
    @patch('redis.cluster.NodesManager.initialize')
    def test_get_client(self, mock_initialize, mock_execute_command):
        import redis as R
        KombuRedis = redis.Channel._get_client(self.channel)
        assert isinstance(KombuRedis(startup_nodes=[ClusterNode()]), R.cluster.RedisCluster)

        Rv = getattr(R, 'VERSION', None)
        try:
            R.VERSION = (2, 4, 0)
            with pytest.raises(VersionMismatch):
                redis.Channel._get_client(self.channel)
        finally:
            if Rv is not None:
                R.VERSION = Rv


class test_Redis:

    def setup_method(self):
        self.connection = Connection(transport=Transport)
        self.exchange = Exchange('test_Redis', type='direct')
        self.queue = Queue('test_Redis', self.exchange, 'test_Redis')

    def teardown_method(self):
        self.connection.close()

    def test_publish_get(self):
        channel = self.connection.channel()
        producer = Producer(channel, self.exchange, routing_key='test_Redis')
        self.queue(channel).declare()

        producer.publish({'hello': 'world'})

        assert self.queue(channel).get().payload == {'hello': 'world'}
        assert self.queue(channel).get() is None
        assert self.queue(channel).get() is None
        assert self.queue(channel).get() is None

    def test_publish_consume(self):
        redis.poll = _poll
        connection = Connection(transport=Transport)
        channel = connection.channel()
        producer = Producer(channel, self.exchange, routing_key='test_Redis')
        consumer = Consumer(channel, queues=[self.queue])

        producer.publish({'hello2': 'world2'})
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

    def test_purge(self):
        channel = self.connection.channel()
        producer = Producer(channel, self.exchange, routing_key='test_Redis')
        self.queue(channel).declare()

        for i in range(10):
            producer.publish({'hello': f'world-{i}'})

        assert channel._size('test_Redis') == 10
        assert self.queue(channel).purge() == 10
        channel.close()

    def test_db_values(self):
        Connection(virtual_host=1,
                   transport=Transport).channel()

        Connection(virtual_host='1',
                   transport=Transport).channel()

        Connection(virtual_host='/1',
                   transport=Transport).channel()

        with pytest.raises(Exception):
            Connection('redis:///foo').channel()

    def test_db_port(self):
        c1 = Connection(port=None, transport=Transport).channel()
        c1.close()

        c2 = Connection(port=9999, transport=Transport).channel()
        c2.close()

    def test_close_poller_not_active(self):
        c = Connection(transport=Transport).channel()
        cycle = c.connection.cycle
        c.close()
        assert c not in cycle._channels

    def test_close_ResponseError(self):
        c = Connection(transport=Transport).channel()
        c.client.bgsave_raises_ResponseError = True
        c.close()

    def test_close_in_poll(self):
        c = Connection(transport=Transport).channel()
        conn = RedisConnection()
        conn._sock.data = [('BRPOP', ('test_Redis',))]
        c._in_poll_connections.add(conn)
        c._in_poll = True
        c.close()
        assert conn._sock.data == []

    def test_get__Empty(self):
        channel = self.connection.channel()
        with pytest.raises(Empty):
            channel._get('does-not-exist')
        channel.close()


class test_MultiChannelPoller:

    def setup_method(self):
        self.Poller = redis.MultiChannelPoller
        self.connection = Connection(transport=redis.Transport)

    def test_init(self):
        p = self.Poller()
        assert p._chan_active_queues_to_conn == {}

    def test_on_poll_start(self):
        p = self.Poller()
        p._channels = []
        p.on_poll_start()
        p._register_BRPOP = Mock(name='_register_BRPOP')
        p._register_LISTEN = Mock(name='_register_LISTEN')

        chan1 = Mock(name='chan1')
        p._channels = [chan1]
        chan1.active_queues = []
        chan1.active_fanout_queues = []
        p.on_poll_start()

        chan1.active_queues = ['q1']
        chan1.active_fanout_queues = ['q2']
        chan1.qos.can_consume.return_value = False

        p.on_poll_start()
        p._register_LISTEN.assert_called_with(chan1)
        p._register_BRPOP.assert_not_called()

        chan1.qos.can_consume.return_value = True
        p._register_LISTEN.reset_mock()
        p.on_poll_start()

        p._register_BRPOP.assert_called_with(chan1)
        p._register_LISTEN.assert_called_with(chan1)

    def test_on_poll_init(self):
        p = self.Poller()
        chan1 = Mock(name='chan1')
        p._channels = []
        poller = Mock(name='poller')
        p.on_poll_init(poller)
        assert p.poller is poller

        p._channels = [chan1]
        p.on_poll_init(poller)
        chan1.qos.restore_visible.assert_called_with(
            num=chan1.unacked_restore_limit,
        )

    def test_handle_event(self):
        p = self.Poller()
        chan = Mock(name='chan')
        conn = Mock(name='conn')
        p._fd_to_chan[13] = chan, conn, 'BRPOP'
        chan.handlers = {'BRPOP': Mock(name='BRPOP')}

        chan.qos.can_consume.return_value = False
        p.handle_event(13, redis.READ)
        chan.handlers['BRPOP'].assert_not_called()

        chan.qos.can_consume.return_value = True
        p.handle_event(13, redis.READ)
        chan.handlers['BRPOP'].assert_called_with(conn=conn)

        p.handle_event(13, redis.ERR)
        chan._poll_error.assert_called_with(conn, 'BRPOP')

        p.handle_event(13, ~(redis.READ | redis.ERR))

    def test_fds(self):
        p = self.Poller()
        p._fd_to_chan = {1: 2}
        assert p.fds == p._fd_to_chan

    def test_close_unregisters_fds(self):
        p = self.Poller()
        poller = p.poller = Mock()
        p._chan_to_sock.update({1: 1, 2: 2, 3: 3})

        p.close()

        assert poller.unregister.call_count == 3
        u_args = poller.unregister.call_args_list

        assert sorted(u_args) == [
            ((1,), {}),
            ((2,), {}),
            ((3,), {}),
        ]

    def test_close_when_unregister_raises_KeyError(self):
        p = self.Poller()
        p.poller = Mock()
        p._chan_to_sock.update({1: 1})
        p.poller.unregister.side_effect = KeyError(1)
        p.close()

    def test_close_resets_state(self):
        p = self.Poller()
        p.poller = Mock()
        p._channels = Mock()
        p._fd_to_chan = Mock()
        p._chan_to_sock = Mock()
        p._chan_active_queues_to_conn = Mock()

        p._chan_to_sock.itervalues.return_value = []
        p._chan_to_sock.values.return_value = []

        p.close()
        p._channels.clear.assert_called_with()
        p._fd_to_chan.clear.assert_called_with()
        p._chan_to_sock.clear.assert_called_with()
        p._chan_active_queues_to_conn.clear.assert_called_with()

    def test_register_when_registered_reregisters(self):
        p = self.Poller()
        p.poller = Mock()
        channel, client, conn, type = Mock(), Mock(), Mock(), Mock()
        sock = conn._sock = Mock()
        sock.fileno.return_value = 10

        p._chan_to_sock = {(channel, client, conn, type): 6}
        p._register(channel, client, conn, type)
        p.poller.unregister.assert_called_with(6)
        assert p._fd_to_chan[10] == (channel, conn, type)
        assert p._chan_to_sock[(channel, client, conn, type)] == sock
        p.poller.register.assert_called_with(sock, p.eventflags)

        conn._sock = None

        def after_connected():
            conn._sock = Mock()

        conn.connect.side_effect = after_connected
        p._register(channel, client, conn, type)
        conn.connect.assert_called_with()

    def test_get_conns_for_channel(self):
        p = self.Poller()
        channel = Mock()
        channel.active_queues = ['queue']
        p._chan_active_queues_to_conn = {}
        conns = p._get_conns_for_channel(channel)
        assert p._chan_active_queues_to_conn[(channel, 'queue')] == conns.pop()

    def test_register_BRPOP(self):
        p = self.Poller()
        conn = Mock()
        conn._sock = None
        get_conns_for_channel = p._get_conns_for_channel = Mock()
        get_conns_for_channel.return_value = [conn]

        channel = Mock()
        channel.active_queues = []
        p._register = Mock()

        channel._in_poll = False
        p._register_BRPOP(channel)
        assert channel._brpop_start.call_count == 1
        assert p._register.call_count == 1

        conn._sock = Mock()
        p._chan_to_sock[(channel, channel.client, conn, 'BRPOP')] = True
        channel._in_poll = True
        p._register_BRPOP(channel)
        assert channel._brpop_start.call_count == 1
        assert p._register.call_count == 1

    def test_register_LISTEN(self):
        p = self.Poller()
        conn = Mock()
        conn._sock = None
        get_conns_for_channel = p.get_conns_for_channel = Mock()
        get_conns_for_channel.return_value = [conn]

        channel = Mock()
        conn._sock = None
        channel._in_listen = False
        p._register = Mock()

        p._register_LISTEN(channel)
        p._register.assert_called_with(channel, channel.subclient, channel.subclient.connection, 'LISTEN')
        assert p._register.call_count == 1
        assert channel._subscribe.call_count == 1

        channel._in_listen = True
        p._chan_to_sock[(channel, channel.subclient, channel.subclient.connection, 'LISTEN')] = 3
        channel.subclient.connection._sock = Mock()
        p._register_LISTEN(channel)
        assert p._register.call_count == 1
        assert channel._subscribe.call_count == 1

    def test_on_readable(self):
        p = self.Poller()
        channel, conn, _brpop_read, _receive = Mock(), Mock(), Mock(), Mock()
        channel.handlers = {'BRPOP': _brpop_read, 'LISTEN': _receive}
        p._fd_to_chan = {0: (channel, conn, 'BRPOP')}
        p.on_readable(0)
        _brpop_read.assert_called_with(conn=conn)
        p._fd_to_chan = {0: (channel, conn, 'LISTEN')}
        p.on_readable(0)
        _receive.assert_called_with(conn=conn)

    def test_on_readable_when_moved(self):
        p = self.Poller()
        channel, conn, _brpop_read, _receive = Mock(), Mock(), Mock(), Mock()
        channel.handlers = {'BRPOP': _brpop_read, 'LISTEN': _receive}
        sock = conn._sock = Mock()
        sock.fileno.return_value = 0
        _get_conns_for_channel = p._get_conns_for_channel = Mock()
        _get_conns_for_channel.return_value = [conn]

        p._register_BRPOP(channel)
        assert p._fd_to_chan == {0: (channel, conn, 'BRPOP')}
        assert p._chan_to_sock == {(channel, channel.client, conn, 'BRPOP'): sock}

        _brpop_read.side_effect = MovedError('1 0.0.0.0:0')
        poller_unregister = p.poller.unregister = Mock()
        with pytest.raises(Empty):
            p.on_readable(0)
        assert p._fd_to_chan == {}
        assert p._chan_to_sock == {}
        poller_unregister.assert_called_with(sock)

    def create_get(self, events=None, queues=None, fanouts=None):
        _pr = [] if events is None else events
        _aq = [] if queues is None else queues
        _af = [] if fanouts is None else fanouts
        p = self.Poller()
        p.poller = Mock()
        p.poller.poll.return_value = _pr

        p._register_BRPOP = Mock()
        p._register_LISTEN = Mock()

        channel = Mock()
        p._channels = [channel]
        channel.active_queues = _aq
        channel.active_fanout_queues = _af

        return p, channel

    def test_get_no_actions(self):
        p, channel = self.create_get()

        with pytest.raises(redis.Empty):
            p.get(Mock())

    def test_qos_reject(self):
        p, channel = self.create_get()
        qos = redis.QoS(channel)
        qos._remove_from_indices = Mock(name='_remove_from_indices')
        qos.reject(1234)
        qos._remove_from_indices.assert_called_with(1234)

    def test_qos_requeue(self):
        p, channel = self.create_get()
        qos = redis.QoS(channel)
        qos.restore_by_tag = Mock(name='restore_by_tag')
        qos.reject(1234, True)
        qos.restore_by_tag.assert_called_with(1234, leftmost=True)

    def test_get_brpop_qos_allow(self):
        p, channel = self.create_get(queues=['a_queue'])
        channel.qos.can_consume.return_value = True

        with pytest.raises(redis.Empty):
            p.get(Mock())

        p._register_BRPOP.assert_called_with(channel)

    def test_get_brpop_qos_disallow(self):
        p, channel = self.create_get(queues=['a_queue'])
        channel.qos.can_consume.return_value = False

        with pytest.raises(redis.Empty):
            p.get(Mock())

        p._register_BRPOP.assert_not_called()

    def test_get_listen(self):
        p, channel = self.create_get(fanouts=['f_queue'])

        with pytest.raises(redis.Empty):
            p.get(Mock())

        p._register_LISTEN.assert_called_with(channel)

    def test_get_receives_ERR(self):
        p, channel = self.create_get(events=[(1, eventio.ERR)])
        conn = Mock()
        p._fd_to_chan[1] = (channel, conn, 'BRPOP')

        with pytest.raises(redis.Empty):
            p.get(Mock())

        channel._poll_error.assert_called_with(conn, 'BRPOP')

    def test_get_receives_multiple(self):
        p, channel = self.create_get(events=[(1, eventio.ERR),
                                             (1, eventio.ERR)])
        conn = Mock()
        p._fd_to_chan[1] = (channel, conn, 'BRPOP')

        with pytest.raises(redis.Empty):
            p.get(Mock())

        channel._poll_error.assert_called_with(conn, 'BRPOP')


class test_Mutex:

    def test_mutex(self):
        import redis as _redis_py
        client = _redis_py.client.Redis()
        set = client.set = Mock()

        # Won
        set.return_value = True
        held = False
        with redis.Mutex(client, 'foo1', 100):
            held = True
        assert held

        # Did not win
        set.return_value = False
        held = False
        with pytest.raises(redis.MutexHeld):
            with redis.Mutex(client, 'foo1', 100):
                held = True
            assert not held


class test_GlobalKeyPrefixMixin:
    global_keyprefix = "prefix_"
    hash_tag = "{tag}"
    mixin = redis.GlobalKeyPrefixMixin()
    mixin.global_keyprefix = global_keyprefix
    mixin.hash_tag = hash_tag

    def test_prefix_simple_args(self):
        for command in self.mixin.PREFIXED_SIMPLE_COMMANDS:
            prefixed_args = self.mixin._prefix_args([command, "fake_key"])
            assert prefixed_args == [
                command,
                f"{self.global_keyprefix}fake_key"
            ]

    def test_prefix_delete_args(self):
        prefixed_args = self.mixin._prefix_args([
            "DEL",
            "fake_key",
            "fake_key2",
            "fake_key3"
        ])

        assert prefixed_args == [
            "DEL",
            f"{self.global_keyprefix}fake_key",
            f"{self.global_keyprefix}fake_key2",
            f"{self.global_keyprefix}fake_key3",
        ]

    def test_prefix_brpop_args(self):
        prefixed_args = self.mixin._prefix_args([
            "BRPOP",
            "fake_key",
            "fake_key2",
            "not_prefixed"
        ])

        assert prefixed_args == [
            "BRPOP",
            f"{self.global_keyprefix}fake_key",
            f"{self.global_keyprefix}fake_key2",
            "not_prefixed",
        ]

    def test_prefix_evalsha_args(self):
        prefixed_args = self.mixin._prefix_args([
            "EVALSHA",
            "not_prefixed",
            "not_prefixed",
            "fake_key",
            "not_prefixed",
        ])

        assert prefixed_args == [
            "EVALSHA",
            "not_prefixed",
            "not_prefixed",
            f"{self.global_keyprefix}fake_key",
            "not_prefixed",
        ]
