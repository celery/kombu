"""Redis cluster transport module for Kombu.

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: Yes (If hash_tag is set)
* Supports TTL: No

Connection String
=================
Connection string has the following format:

.. code-block::

    rediscluster://[USER:PASSWORD@]REDIS_CLUSTER_ADDRESS[:PORT][/VIRTUALHOST]

Transport Options
=================
* ``sep``
* ``ack_emulation``: (bool) If set to True transport will
  simulate Acknowledge of AMQP protocol.
* ``unacked_key``
* ``unacked_index_key``
* ``unacked_mutex_key``
* ``unacked_mutex_expire``
* ``visibility_timeout``
* ``unacked_restore_limit``
* ``fanout_prefix``
* ``fanout_patterns``
* ``global_keyprefix``: (str) The global key prefix to be prepended to all keys
* ``hash_tag``: (str) Prefix keys with it,
                      effective at the same time as global_keyprefix.({hash_tag}{global_keyprefix}key)
  used by Kombu
* ``socket_timeout``
* ``socket_connect_timeout``
* ``socket_keepalive``
* ``socket_keepalive_options``
* ``queue_order_strategy``
* ``max_connections``
* ``health_check_interval``
* ``retry_on_timeout``
* ``priority_steps``
"""

from __future__ import annotations

import functools
from contextlib import contextmanager
from queue import Empty
from time import time

from redis.exceptions import (AskError, MovedError, RedisClusterException,
                              TryAgainError)

from kombu.exceptions import VersionMismatch
from kombu.log import get_logger
from kombu.transport import virtual
from kombu.transport.virtual.base import Channel as VirtualBaseChannel
from kombu.utils import uuid
from kombu.utils.compat import register_after_fork
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property

from ..utils.scheduling import cycle_by_name
from .redis import Channel as RedisChannel
from .redis import GlobalKeyPrefixMixin as RedisGlobalKeyPrefixMixin
from .redis import MultiChannelPoller as RedisMultiChannelPoller
from .redis import MutexHeld
from .redis import QoS as RedisQoS
from .redis import Transport as RedisTransport
from .redis import _after_fork_cleanup_channel

try:
    import redis
except ImportError:
    redis = None

logger = get_logger('kombu.transport.rediscluster')
crit, warning = logger.critical, logger.warning


@contextmanager
def Mutex(client, name, expire):
    # The internal implementation of lock uses uuid as the key, so it cannot be used in cluster mode.
    # Use setnx instead
    lock_id = uuid().encode('utf-8')
    acquired = client.set(name, lock_id, ex=expire, nx=True)
    try:
        if acquired:
            yield
        else:
            raise MutexHeld()
    finally:
        if acquired:
            with client.pipeline() as pipe:
                try:
                    pipe.watch(name)
                    if client.get(name) == lock_id:
                        pipe.multi()
                        pipe.delete(name)
                        pipe.execute()
                        return
                    pipe.unwatch()
                except redis.exceptions.WatchError:
                    pass


class GlobalKeyPrefixMixin(RedisGlobalKeyPrefixMixin):

    def pipeline(self, transaction=False, shard_hint=None):
        if shard_hint:
            raise RedisClusterException("shard_hint is deprecated in cluster mode")
        if transaction:
            raise RedisClusterException("transaction is deprecated in cluster mode")

        return PrefixedRedisPipeline(
            nodes_manager=self.nodes_manager,
            commands_parser=self.commands_parser,
            startup_nodes=self.nodes_manager.startup_nodes,
            result_callbacks=self.result_callbacks,
            cluster_response_callbacks=self.cluster_response_callbacks,
            cluster_error_retry_attempts=self.cluster_error_retry_attempts,
            read_from_replicas=self.read_from_replicas,
            reinitialize_steps=self.reinitialize_steps,
            lock=self._lock,
            global_keyprefix=self.global_keyprefix,
        )


class PrefixedStrictRedis(GlobalKeyPrefixMixin, redis.RedisCluster):

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        redis.RedisCluster.__init__(self, *args, **kwargs)

    def pubsub(self, **kwargs):
        return PrefixedRedisPubSub(
            self,
            global_keyprefix=self.global_keyprefix,
            **kwargs,
        )

    def keyslot(self, key):
        return super().keyslot(f'{self.global_keyprefix}{key}')


class PrefixedRedisPipeline(GlobalKeyPrefixMixin, redis.cluster.ClusterPipeline):

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        redis.cluster.ClusterPipeline.__init__(self, *args, **kwargs)


class PrefixedRedisPubSub(redis.cluster.ClusterPubSub):
    PUBSUB_COMMANDS = (
        "SUBSCRIBE",
        "UNSUBSCRIBE",
        "PSUBSCRIBE",
        "PUNSUBSCRIBE",
    )

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        super().__init__(*args, **kwargs)

    def _prefix_args(self, args):
        args = list(args)
        command = args.pop(0)

        if command in self.PUBSUB_COMMANDS:
            args = [
                self.global_keyprefix + str(arg)
                for arg in args
            ]

        return [command, *args]

    def parse_response(self, *args, **kwargs):
        ret = super().parse_response(*args, **kwargs)
        if ret is None:
            return ret
        if not isinstance(ret, list):
            return ret

        message_type, *channels, message = ret
        return [
            message_type,
            *[channel[len(self.global_keyprefix):] for channel in channels],
            message,
        ]

    def execute_command(self, *args, **kwargs):
        return super().execute_command(*self._prefix_args(args), **kwargs)


class QoS(RedisQoS):

    def restore_visible(self, start=0, num=10, interval=10):
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return
        with self.channel.conn_or_acquire() as client:
            ceil = time() - self.visibility_timeout
            try:
                node = client.nodes_manager.get_node_from_slot(client.keyslot(self.unacked_mutex_key))
                with Mutex(node.redis_connection, self.unacked_mutex_key,
                           self.unacked_mutex_expire):
                    visible = client.zrevrangebyscore(
                        self.unacked_index_key, ceil, 0,
                        start=num and start, num=num, withscores=True)
                    for tag, score in visible or []:
                        self.restore_by_tag(tag, client)
            except MutexHeld:
                pass

    def restore_by_tag(self, tag, client=None, leftmost=False):

        def restore_transaction(pipe):
            p = pipe.hget(self.channel.global_keyprefix + self.unacked_key, tag)
            pipe.multi()
            self._remove_from_indices(tag, pipe, key_prefix=self.channel.global_keyprefix)
            if p:
                M, EX, RK = loads(bytes_to_str(p))
                self.channel._do_restore_message(M, EX, RK, pipe, leftmost, key_prefix=self.channel.global_keyprefix)

        with self.channel.conn_or_acquire(client) as client:
            if self.channel.hash_tag:
                # Redis doesn't support transaction, if keys are located on different slots/nodes.
                # We must ensure all keys related to transaction are stored on a single slot.
                # We can use hash tag to do that.
                # Then we can take the node holding the slot as a single Redis instance,
                # and run transaction on that node.
                node = client.nodes_manager.get_node_from_slot(client.keyslot(self.unacked_key))
                # Because node.redis_connection(redis.client.Redis) is not override-able,
                # global_prefix cannot take effect.
                node.redis_connection.transaction(restore_transaction,
                                                  self.channel.global_keyprefix + self.unacked_key)
            else:
                # Without transactions, problems may occur
                p = client.hget(self.unacked_key, tag)
                with client.pipeline() as pipe:
                    self._remove_from_indices(tag, pipe)
                    if p:
                        M, EX, RK = loads(bytes_to_str(p))
                        self.channel._do_restore_message(M, EX, RK, pipe, leftmost)
                    pipe.execute()

    def _remove_from_indices(self, delivery_tag, pipe=None, key_prefix=''):
        with self.pipe_or_acquire(pipe) as pipe:
            return pipe.zrem(key_prefix + self.unacked_index_key, delivery_tag) \
                .hdel(key_prefix + self.unacked_key, delivery_tag)


class MultiChannelPoller(RedisMultiChannelPoller):

    def __init__(self):
        super().__init__()
        # channel1
        #     |-> redis.connection1(fd1 <-> node1)
        #     |-> redis.connection2(fd2 <-> node2)
        # channel2
        #     |-> redis.connection3(fd3 <-> node1)
        #     |-> redis.connection4(fd4 <-> node3)

        # (channel, queue) -> conn
        self._chan_active_queues_to_conn = {}

    def close(self):
        for fd in self._chan_to_sock.values():
            try:
                self.poller.unregister(fd)
            except (KeyError, ValueError):
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()
        self._chan_active_queues_to_conn.clear()

    def _register(self, channel, client, conn, type):
        if (channel, client, conn, type) in self._chan_to_sock:
            self._unregister(channel, client, conn, type)
        if conn._sock is None:
            # We closed the connection when exception occurred during `_brpop_read`
            conn.connect()

        sock = conn._sock
        self._fd_to_chan[sock.fileno()] = (channel, conn, type)
        self._chan_to_sock[(channel, client, conn, type)] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, conn, type):
        self.poller.unregister(self._chan_to_sock[(channel, client, conn, type)])

    def _get_conns_for_channel(self, channel):
        conns = set()
        for queue in channel.active_queues:
            if (channel, queue) not in self._chan_active_queues_to_conn:
                slot = channel.client.keyslot(queue)
                node = channel.client.nodes_manager.get_node_from_slot(slot, read_from_replicas=False)
                # Different queues use different connections
                conn = node.redis_connection.connection_pool.get_connection("_")
                self._chan_active_queues_to_conn[(channel, queue)] = conn
            conns.add(self._chan_active_queues_to_conn[(channel, queue)])
        return conns

    def _register_BRPOP(self, channel):
        conns = self._get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.client, conn, 'BRPOP')
            if conn._sock is None or ident not in self._chan_to_sock:
                channel._in_poll = False
                self._register(*ident)
        if not channel._in_poll:
            channel._brpop_start()

    def _register_LISTEN(self, channel):
        conn = channel.subclient.connection
        ident = (channel, channel.subclient, conn, 'LISTEN')
        if conn._sock is None or ident not in self._chan_to_sock:
            channel._in_listen = False
            self._register(*ident)
        if not channel._in_listen:
            channel._subscribe()

    def on_readable(self, fileno):
        chan, conn, type = self._fd_to_chan[fileno]
        if chan.qos.can_consume():
            try:
                chan.handlers[type](**{'conn': conn})
            except MovedError:
                # When a key is moved, the connection previously used to access the key
                # needs to be replaced with the new connection after the move.
                # The connection will be rebuilt in the next loop.
                self._unregister_connection(conn, fileno=fileno)
                raise Empty()

    def _unregister_connection(self, redis_connection, fileno=None):
        if not fileno and redis_connection._sock:
            fileno = redis_connection._sock.fileno()

        self._fd_to_chan.pop(fileno, None)
        for channel, client, conn, type in list(self._chan_to_sock.keys()):
            if conn == redis_connection:
                del self._chan_to_sock[(channel, client, conn, type)]

        for channel, queue in list(self._chan_active_queues_to_conn.keys()):
            if self._chan_active_queues_to_conn[(channel, queue)] == redis_connection:
                del self._chan_active_queues_to_conn[(channel, queue)]
        try:
            self.poller.unregister(redis_connection._sock)
        except (KeyError, ValueError):
            pass

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno), self
        elif event & ERR:
            chan, conn, type = self._fd_to_chan[fileno]
            chan._poll_error(conn, type)


class Channel(RedisChannel):
    QoS = QoS

    _client = None
    _in_poll = False
    _in_poll_connections = set()
    _in_listen = False

    hash_tag = ''
    unacked_key = 'unacked'
    unacked_index_key = 'unacked_index'
    unacked_mutex_key = 'unacked_mutex'
    global_keyprefix = ''

    from_transport_options = (
            RedisChannel.from_transport_options +
            ('hash_tag',)
    )

    def __init__(self, connection, *args, **kwargs):
        VirtualBaseChannel.__init__(self, connection, *args, **kwargs)
        if not self.ack_emulation:
            self.QoS = virtual.QoS
        self._registered = False
        self._queue_cycle = cycle_by_name(self.queue_order_strategy)()
        self.ResponseError = self._get_response_error()
        self.active_fanout_queues = set()
        self.auto_delete_queues = set()
        self._fanout_to_queue = {}
        self.handlers = {'BRPOP': self._brpop_read, 'LISTEN': self._receive}

        if self.fanout_prefix:
            if isinstance(self.fanout_prefix, str):
                self.keyprefix_fanout = self.fanout_prefix
        else:
            self.keyprefix_fanout = ''

        self.connection.cycle.add(self)
        self._registered = True
        self.connection_errors = self.connection.connection_errors

        if register_after_fork is not None:
            register_after_fork(self, _after_fork_cleanup_channel)

        if not self.hash_tag:
            self.priority_steps = [0]
        else:
            self.global_keyprefix = f'{self.hash_tag}{self.global_keyprefix}'

        self.Client = self._get_client()

    def _after_fork(self):
        self._disconnect_pools()

    def _disconnect_pools(self):
        client = self._client
        if client is not None:
            client.disconnect_connection_pools()
            client.close()
        self._client = None

    def _on_connection_disconnect(self, connection):
        if self._in_poll is not None and connection in self._in_poll_connections:
            self._in_poll = None
            self._in_poll_connections.discard(connection)
        if self._in_listen is connection:
            self._in_listen = None
        if self.connection and self.connection.cycle:
            self.connection.cycle._on_connection_disconnect(connection)

    def _restore(self, message, leftmost=False):
        if not self.ack_emulation:
            return super()._restore(message)
        tag = message.delivery_tag

        def restore_transaction(pipe):
            P = pipe.hget(self.global_keyprefix + self.unacked_key, tag)
            pipe.multi()
            pipe.hdel(self.global_keyprefix + self.unacked_key, tag)
            if P:
                M, EX, RK = loads(bytes_to_str(P))
                self._do_restore_message(M, EX, RK, pipe, leftmost, key_prefix=self.global_keyprefix)

        with self.conn_or_acquire() as client:
            if self.hash_tag:
                node = client.nodes_manager.get_node_from_slot(client.keyslot(self.unacked_key))
                node.redis_connection.transaction(restore_transaction, self.global_keyprefix + self.unacked_key)
            else:
                # Without transactions, problems may occur
                P = client.hget(self.unacked_key, tag)
                with client.pipeline() as pipe:
                    pipe.hdel(self.unacked_key, tag)
                    if P:
                        M, EX, RK = loads(bytes_to_str(P))
                        self._do_restore_message(M, EX, RK, pipe, leftmost)
                    pipe.execute()

    def _brpop_start(self, timeout=1):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return
        pri_queues = [self._q_for_pri(queue, pri) for pri in self.priority_steps
                      for queue in queues]
        self._in_poll = True

        node_to_keys = {}
        for key in pri_queues:
            node = self.client.nodes_manager.get_node_from_slot(self.client.keyslot(key))
            node_to_keys.setdefault(f'{node.host}:{node.port}', []).append(key)

        for chan, client, conn, cmd in self.connection.cycle._chan_to_sock:
            expected = (self, self.client, 'BRPOP')
            keys = node_to_keys.get(f'{conn.host}:{conn.port}')

            if keys and (chan, client, cmd) == expected:
                command_args = ['BRPOP', *keys, timeout]
                if self.global_keyprefix:
                    command_args = self.client._prefix_args(command_args)
                conn.send_command(*command_args)
                self._in_poll_connections.add(conn)

    def _brpop_read(self, **options):
        conn = options.pop('conn', None)
        try:
            try:
                dest__item = conn.read_response('BRPOP', **options)
                if dest__item:
                    key, value = dest__item
                    key = key[len(self.global_keyprefix):]
                    dest__item = key, value
            except self.connection_errors:
                if conn is not None:
                    conn.disconnect()
                # Remove the failed node from the startup nodes before we try
                # to reinitialize the cluster
                target_node = self.client.nodes_manager.startup_nodes.pop(f'{conn.host}:{conn.port}', None)
                # Reset the cluster node's connection
                target_node.redis_connection = None
                self.client.nodes_manager.initialize()
                raise
            except MovedError:
                # poller need to remove conn
                self.client.nodes_manager.initialize()
                raise
            except (TryAgainError, AskError):
                raise Empty()

            if dest__item:
                dest, item = dest__item
                dest = bytes_to_str(dest).rsplit(self.sep, 1)[0]
                self._queue_cycle.rotate(dest)
                self.connection._deliver(loads(bytes_to_str(item)), dest)
                return True
            else:
                raise Empty()
        finally:
            self._in_poll_connections.discard(conn)
            # To avoid inconsistencies between _in_poll and _in_poll_connections in abnormal situations,
            # _in_poll is set to None after any connection being read.
            self._in_poll = None

    def _receive(self, **kwargs):
        super()._receive()

    def _poll_error(self, conn, type, **options):
        if type == 'LISTEN':
            self.subclient.parse_response()
        else:
            conn.read_response(type)

    def close(self):
        self._closing = True
        if self._in_poll or len(self._in_poll_connections) != 0:
            try:
                for conn in self._in_poll_connections.copy():
                    self._brpop_read(**{'conn': conn})
            except Empty:
                pass
        if not self.closed:
            self.connection.cycle.discard(self)
            client = self.__dict__.get('client')
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)
            self._disconnect_pools()
            self._close_clients()
        VirtualBaseChannel.close(self)

    def _close_clients(self):
        for attr in 'client', 'subclient':
            try:
                client = self.__dict__[attr]
                if attr == 'client':
                    client.disconnect_connection_pools()
                    client.close()
                if attr == 'subclient':
                    connection, client.connection = client.connection, None
                    # The Redis server will automatically detect the disconnection of the client connection
                    # and clean up all subscription states of the client.
                    connection.disconnect()
            except (KeyError, AttributeError, self.ResponseError):
                pass

    def _connparams(self, asynchronous=False):
        conn_params = super()._connparams(asynchronous=asynchronous)
        # connection_class and db is not supported in redis.client.Redis
        # connection_pool_class is only effective when the url parameter is not empty
        conn_params.pop('db', None)
        conn_params.pop('connection_class', None)
        connection_cls = redis.Connection
        if self.connection.client.ssl:
            connection_cls = redis.SSLConnection

        if asynchronous:
            channel = self

            class ManagedConnection(connection_cls):
                def disconnect(self, *args):
                    super().disconnect(*args)
                    if channel._registered:
                        channel._on_connection_disconnect(self)

            class ManagedConnectionPool(redis.ConnectionPool):
                def __init__(self, *args, **kwargs):
                    kwargs['connection_class'] = ManagedConnection
                    super().__init__(*args, **kwargs)

            conn_params['connection_pool_class'] = ManagedConnectionPool

        conn_params['url'] = f'redis://{conn_params["host"]}:{conn_params["port"]}'
        return conn_params

    def _create_client(self, asynchronous=False):
        if self._client is None:
            conn_params = self._connparams(asynchronous=asynchronous)
            self._client = self.Client(**conn_params)
        return self._client

    def _get_client(self):
        if redis.VERSION < (4, 1, 0):
            raise VersionMismatch(
                'Redis cluster transport requires redis-py versions 4.1.0 or later. '
                'You have {0.__version__}'.format(redis))

        if self.global_keyprefix:
            return functools.partial(
                PrefixedStrictRedis,
                global_keyprefix=self.global_keyprefix,
            )

        return redis.cluster.RedisCluster

    @cached_property
    def subclient(self):
        client = self._create_client(asynchronous=True)
        pubsub_client = client.pubsub()
        # Init connection and connection pool in client.
        # `ClusterPubSub` initializes the connection and connection pool
        # when `execute_command` is called for the first time
        pubsub_client.ping()
        return pubsub_client

    def _do_restore_message(self, payload, exchange, routing_key,
                            pipe, leftmost=False, key_prefix=''):
        try:
            try:
                payload['headers']['redelivered'] = True
                payload['properties']['delivery_info']['redelivered'] = True
            except KeyError:
                pass
            for queue in self._lookup(exchange, routing_key):
                pri = self._get_message_priority(payload, reverse=False)

                (pipe.lpush if leftmost else pipe.rpush)(
                    key_prefix + self._q_for_pri(queue, pri), dumps(payload),
                )
        except Exception:
            crit('Could not restore message: %r', payload, exc_info=True)


class Transport(RedisTransport):
    Channel = Channel

    driver_type = 'rediscluster'
    driver_name = 'rediscluster'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cycle = MultiChannelPoller()
