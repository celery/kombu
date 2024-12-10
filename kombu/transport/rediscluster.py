from __future__ import annotations

from collections import namedtuple
from contextlib import contextmanager
from queue import Empty
from time import time

from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ
from kombu.utils.json import loads
from kombu.utils.uuid import uuid

from . import virtual
from .redis import (
    Channel as RedisChannel,
    MultiChannelPoller,
    MutexHeld,
    QoS as RedisQoS,
    Transport as RedisTransport,
)
from ..exceptions import VersionMismatch, GlobalPrefixNotSpport
from ..log import get_logger

try:
    import redis
    from redis.cluster import ClusterPubSub
except ImportError:
    redis = None

logger = get_logger('kombu.transport.rediscluster')
crit, warning = logger.critical, logger.warning


@contextmanager
def Mutex(client, name, expire):
    lock_id = uuid().encode('utf-8')
    acquired = client.set(name, lock_id, ex=expire, nx=True)
    try:
        if acquired:
            yield
        else:
            raise MutexHeld()
    finally:
        if acquired:
            if client.get(name) == lock_id:
                client.delete(name)


class QoS(RedisQoS):

    def restore_visible(self, start=0, num=10, interval=10):
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return
        with self.channel.conn_or_acquire() as client:
            ceil = time() - self.visibility_timeout
            try:
                with Mutex(client, self.unacked_mutex_key,
                           self.unacked_mutex_expire):
                    visible = client.zrevrangebyscore(
                        self.unacked_index_key, ceil, 0,
                        start=num and start, num=num, withscores=True)
                    for tag, score in visible or []:
                        self.restore_by_tag(tag, client)
            except MutexHeld:
                pass

    def restore_by_tag(self, tag, client=None, leftmost=False):
        with self.channel.conn_or_acquire(client) as client:
            # Transaction support is disabled in redis cluster.
            # Use pipelines to avoid extra network round-trips, not to ensure atomicity.
            p = client.hget(self.unacked_key, tag)
            with self.pipe_or_acquire() as pipe:
                self._remove_from_indices(tag, pipe)
                if p:
                    M, EX, RK = loads(bytes_to_str(p))  # json is unicode
                    self.channel._do_restore_message(M, EX, RK, pipe, leftmost)
                pipe.execute()


class ClusterMultiChannelPoller(MultiChannelPoller):

    def _register(self, channel, client, conn, type):
        if (channel, client, conn, type) in self._chan_to_sock:
            self._unregister(channel, client, conn, type)
        if conn._sock is None:  # not connected yet.
            conn.connect()
        sock = conn._sock
        self._fd_to_chan[sock.fileno()] = (channel, conn, type)
        self._chan_to_sock[(channel, client, conn, type)] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, conn, type):
        self.poller.unregister(self._chan_to_sock[(channel, client, conn, type)])

    def _register_BRPOP(self, channel):
        """Enable BRPOP mode for channel."""
        conns = self.get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.client, conn, 'BRPOP')

            if (conn._sock is None or ident not in self._chan_to_sock):
                channel._in_poll = False
                self._register(*ident)

        if not channel._in_poll:  # send BRPOP
            channel._brpop_start()

    def _register_LISTEN(self, channel):
        conns = self.get_conns_for_channel(channel)

        for conn in conns:
            ident = (channel, channel.subclient, conn, 'LISTEN')
            if (conn._sock is None or ident not in self._chan_to_sock):
                channel._in_listen = False
                self._register(*ident)

        if not channel._in_listen:
            channel._subscribe()  # send SUBSCRIBE

    def get_conns_for_channel(self, channel):
        if self._chan_to_sock:
            return [conn for _, _, conn, _ in self._chan_to_sock]

        conns = []
        for key in channel.active_queues:
            slot = channel.client.keyslot(key)
            node = channel.client.nodes_manager.get_node_from_slot(
                slot, channel.client.read_from_replicas
            )
            conns.append(node.redis_connection.connection_pool.get_connection("_"))
        return conns

    def on_readable(self, fileno):
        try:
            chan, conn, type = self._fd_to_chan[fileno]
        except KeyError:
            return

        if chan.qos.can_consume():
            chan.handlers[type](**{'conn': conn})

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno), self
        elif event & ERR:
            chan, conn, type = self._fd_to_chan[fileno]
            chan._poll_error(conn, type)


class Channel(RedisChannel):
    QoS = QoS

    min_priority = 0
    max_priority = 0
    priority_steps = [min_priority]

    def _brpop_start(self, timeout=1):
        queues = self._queue_cycle.consume(len(self.active_queues))
        if not queues:
            return
        self._in_poll = True

        node_to_keys = {}
        for key in queues:
            slot = self.client.keyslot(key)
            node = self.client.nodes_manager.get_node_from_slot(
                slot, self.client.read_from_replicas
            )
            node_to_keys.setdefault(f'{node.host}:{node.port}', []).append(key)

        for chan, client, conn, cmd in self.connection.cycle._chan_to_sock:
            expected = (self, self.client, 'BRPOP')
            keys = node_to_keys.get(f'{conn.host}:{conn.port}')

            if keys and (chan, client, cmd) == expected:
                for key in keys:
                    command_args = ['BRPOP', key, timeout]
                    conn.send_command(*command_args)

    def _brpop_read(self, **options):
        try:
            dest__item = None
            conn = options.pop('conn', None)
            if conn:
                try:
                    dest__item = conn.read_response('BRPOP', **options)
                except self.connection_errors:
                    conn.disconnect()
                    raise
            if dest__item:
                dest, item = dest__item
                dest = bytes_to_str(dest).rsplit(self.sep, 1)[0]
                self._queue_cycle.rotate(dest)
                self.connection._deliver(loads(bytes_to_str(item)), dest)
                return True
            else:
                raise Empty()
        finally:
            self._in_poll = None

    def _create_client(self, asynchronous=False):
        params = self._connparams(asynchronous=asynchronous)
        return self.Client(**params)

    def _connparams(self, asynchronous=False):
        conninfo = self.connection.client
        connparams = {
            'host': conninfo.hostname or '127.0.0.1',
            'port': conninfo.port or self.connection.default_port,
            'username': conninfo.userid,
            'password': conninfo.password,
            'max_connections': self.max_connections,
            'socket_timeout': self.socket_timeout,
            'socket_connect_timeout': self.socket_connect_timeout,
            'socket_keepalive': self.socket_keepalive,
            'socket_keepalive_options': self.socket_keepalive_options,
            'health_check_interval': self.health_check_interval,
            'retry_on_timeout': self.retry_on_timeout,
        }
        return connparams

    def _get_client(self):
        if redis.VERSION < (4, 1, 0):
            raise VersionMismatch(
                'Redis cluster transport requires redis-py versions 4.0.0 or later. '
                'You have {0.__version__}'.format(redis))

        if self.global_keyprefix:
            raise GlobalPrefixNotSpport('Redis cluster transport do not support global_keyprefix')

        return redis.RedisCluster

    def close(self):
        self._closing = True
        if self._in_poll:
            try:
                for channel, conn in [(channel, conn) for channel, _, conn, _ in self.connection.cycle._chan_to_sock]:
                    if channel == self:
                        self._brpop_read(**{'conn': conn})
            except Empty:
                pass
        if not self.closed:
            self.connection.cycle.discard(self)
            client = self.__dict__.get('client')  # only if property cached
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)
            self._disconnect_pools()
            self._close_clients()
        super().close()


class Transport(RedisTransport):
    Channel = Channel

    driver_type = 'redis-cluster'
    driver_name = 'redis-cluster'

    implements = virtual.Transport.implements.extend(
        asynchronous=True, exchange_type=frozenset(['direct'])
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cycle = ClusterMultiChannelPoller()
