"""Redis cluster transport module for Kombu.

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: Yes
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
import numbers
import os
from bisect import bisect
from contextlib import contextmanager
from queue import Empty
from time import time

from kombu.utils import uuid
from vine import promise

from kombu.exceptions import VersionMismatch
from kombu.log import get_logger
from kombu.utils.compat import register_after_fork
from kombu.utils.encoding import bytes_to_str
from kombu.utils.eventio import ERR, READ, poll
from kombu.utils.json import dumps, loads
from kombu.utils.objects import cached_property
from kombu.utils.scheduling import cycle_by_name

from . import virtual
from .redis import (
    Channel as RedisChannel,
    MultiChannelPoller as RedisMultiChannelPoller,
    MutexHeld,
    QoS as RedisQoS,
    Transport as RedisTransport,
    _after_fork_cleanup_channel,
    DEFAULT_HEALTH_CHECK_INTERVAL,
)

try:
    import redis
except ImportError:
    redis = None

logger = get_logger('kombu.transport.rediscluster')
crit, warning = logger.critical, logger.warning

PRIORITY_STEPS = [0, 3, 6, 9]

HASH_TAG = os.getenv('CELERY_REDIS_CLUSTER_HASH_TAG', '')


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


class GlobalKeyPrefixMixin:
    """Mixin to provide common logic for global key prefixing.

    Overriding all the methods used by Kombu with the same key prefixing logic
    would be cumbersome and inefficient. Hence, we override the command
    execution logic that is called by all commands.
    """

    PREFIXED_SIMPLE_COMMANDS = [
        "HDEL",
        "HGET",
        "HLEN",
        "HSET",
        "LLEN",
        "LPUSH",
        "PUBLISH",
        "RPUSH",
        "RPOP",
        "SADD",
        "SREM",
        "SET",
        "SMEMBERS",
        "ZADD",
        "ZREM",
        "ZREVRANGEBYSCORE",
    ]

    PREFIXED_COMPLEX_COMMANDS = {
        "DEL": {"args_start": 0, "args_end": None},
        "BRPOP": {"args_start": 0, "args_end": -1},
        "EVALSHA": {"args_start": 2, "args_end": 3},
        "WATCH": {"args_start": 0, "args_end": None},
    }

    def _prefix_args(self, args):
        args = list(args)
        command = args.pop(0)

        if command in self.PREFIXED_SIMPLE_COMMANDS:
            args[0] = self.global_keyprefix + str(args[0])
        elif command in self.PREFIXED_COMPLEX_COMMANDS:
            args_start = self.PREFIXED_COMPLEX_COMMANDS[command]["args_start"]
            args_end = self.PREFIXED_COMPLEX_COMMANDS[command]["args_end"]

            pre_args = args[:args_start] if args_start > 0 else []
            post_args = []

            if args_end is not None:
                post_args = args[args_end:]

            args = pre_args + [
                self.global_keyprefix + str(arg)
                for arg in args[args_start:args_end]
            ] + post_args

        return [command, *args]

    def parse_response(self, connection, command_name, **options):
        """Parse a response from the Redis server.

        Method wraps ``redis.parse_response()`` to remove prefixes of keys
        returned by redis command.
        """
        ret = super().parse_response(connection, command_name, **options)
        if command_name == 'BRPOP' and ret:
            key, value = ret
            key = key[len(self.global_keyprefix):]
            return key, value
        return ret

    def execute_command(self, *args, **kwargs):
        return super().execute_command(*self._prefix_args(args), **kwargs)

    def pipeline(self, transaction=True, shard_hint=None):
        return PrefixedRedisPipeline(
            self.connection_pool,
            self.response_callbacks,
            transaction,
            shard_hint,
            global_keyprefix=self.global_keyprefix,
        )


class PrefixedStrictRedis(GlobalKeyPrefixMixin, redis.Redis):
    """Returns a ``StrictRedis`` client that prefixes the keys it uses."""

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        redis.Redis.__init__(self, *args, **kwargs)

    def pubsub(self, **kwargs):
        return PrefixedRedisPubSub(
            self.connection_pool,
            global_keyprefix=self.global_keyprefix,
            **kwargs,
        )


class PrefixedRedisPipeline(GlobalKeyPrefixMixin, redis.client.Pipeline):
    """Custom Redis pipeline that takes global_keyprefix into consideration.

    As the ``PrefixedStrictRedis`` client uses the `global_keyprefix` to prefix
    the keys it uses, the pipeline called by the client must be able to prefix
    the keys as well.
    """

    def __init__(self, *args, **kwargs):
        self.global_keyprefix = kwargs.pop('global_keyprefix', '')
        redis.client.Pipeline.__init__(self, *args, **kwargs)


class PrefixedRedisPubSub(redis.client.PubSub):
    """Redis pubsub client that takes global_keyprefix into consideration."""

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
        """Parse a response from the Redis server.

        Method wraps ``PubSub.parse_response()`` to remove prefixes of keys
        returned by redis command.
        """
        ret = super().parse_response(*args, **kwargs)
        if ret is None:
            return ret

        # response formats
        # SUBSCRIBE and UNSUBSCRIBE
        #  -> [message type, channel, message]
        # PSUBSCRIBE and PUNSUBSCRIBE
        #  -> [message type, pattern, channel, message]
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

        def restore_transaction(pipe):
            p = pipe.hget(self.unacked_key, tag)
            pipe.multi()
            self._remove_from_indices(tag, pipe)
            if p:
                M, EX, RK = loads(bytes_to_str(p))  # json is unicode
                self.channel._do_restore_message(M, EX, RK, pipe, leftmost)

        with self.channel.conn_or_acquire(client) as client:
            if HASH_TAG:
                # Redis doesn't support transaction, if keys are located on different slots/nodes.
                # We must ensure all keys related to your transaction are stored on a single slot. We can use hash tag to do that.
                # Then we can take the node holding the slot as a single Redis instance, and run transaction on that node.
                slot = client.keyslot(self.unacked_key)
                node = client.nodes_manager.get_node_from_slot(slot)
                node.redis_connection.transaction(restore_transaction, self.unacked_key)
            else:
                p = client.hget(self.unacked_key, tag)
                with client.pipeline() as pipe:
                    self._remove_from_indices(tag, pipe)
                    if p:
                        M, EX, RK = loads(bytes_to_str(p))  # json is unicode
                        self.channel._do_restore_message(M, EX, RK, pipe, leftmost)
                    pipe.execute()

    @cached_property
    def unacked_key(self):
        return self.channel.unacked_key

    @cached_property
    def unacked_index_key(self):
        return self.channel.unacked_index_key

    @cached_property
    def unacked_mutex_key(self):
        return self.channel.unacked_mutex_key

    @cached_property
    def unacked_mutex_expire(self):
        return self.channel.unacked_mutex_expire

    @cached_property
    def visibility_timeout(self):
        return self.channel.visibility_timeout


class MultiChannelPoller(RedisMultiChannelPoller):

    def __init__(self):
        self._channels = set()
        # self._fd_to_chan[sock.fileno()] = (channel, conn, type)
        self._fd_to_chan = {}
        # self._chan_to_sock[(channel, client, conn, type)] = sock
        self._chan_to_sock = {}

        self.poller = poll()
        self.after_read = set()
        # channel1
        #     |-> redis.connection1 -> fd1(queue1)
        #     |-> redis.connection2 -> fd2(queue2)
        # channel2
        #     |-> redis.connection3 -> fd3(queue3)
        #     |-> redis.connection4 -> fd4(queue4)
        # todo: when to remove? how to make it right
        self._chan_active_queues_to_conn = {}  # {(chan, queue): conn}

    def close(self):
        for fd in self._chan_to_sock.values():
            try:
                self.poller.unregister(fd)
            except (KeyError, ValueError):
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()

    def _on_connection_disconnect(self, connection):
        try:
            self.poller.unregister(connection._sock)
        except (AttributeError, TypeError):
            pass

    def _register(self, channel, client, conn, type):
        if (channel, client, conn, type) in self._chan_to_sock:
            self._unregister(channel, client, conn, type)
        if conn._sock is None:
            conn.connect()

        sock = conn._sock
        if sock.fileno() in self._fd_to_chan:
            raise Exception(
                f'self._fd_to_chan exist {sock.fileno()} is {self._fd_to_chan[sock.fileno()]}, new is {(channel, conn, type)}')
        if (channel, client, conn, type) in self._chan_to_sock:
            raise Exception(
                f'self._chan_to_sock exist {(channel, client, conn, type)} is {self._chan_to_sock[(channel, client, conn, type)]}, new is {sock}')
        self._fd_to_chan[sock.fileno()] = (channel, conn, type)
        self._chan_to_sock[(channel, client, conn, type)] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, conn, type):
        self.poller.unregister(self._chan_to_sock[(channel, client, conn, type)])

    def get_conns_for_channel(self, channel):
        # Queues in channel should own different redis.Connection instance although they in are in same node
        conns = set()
        for queue in channel.active_queues:
            if (channel, queue) not in self._chan_active_queues_to_conn:
                slot = channel.client.keyslot(queue)
                node = channel.client.nodes_manager.get_node_from_slot(slot)
                conn = node.redis_connection.connection_pool.get_connection("_")
                self._chan_active_queues_to_conn[(channel, queue)] = conn
            conns.add(self._chan_active_queues_to_conn[(channel, queue)])
        return conns

    def _register_BRPOP(self, channel: Channel):
        conns = self.get_conns_for_channel(channel)

        # todo: use conn for brpop, use conn for subclient
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

    def on_poll_start(self):
        for channel in self._channels:
            print(
                f'kombu.transport.rediscluster.MultiChannelPoller.on_poll_start start {channel} {channel.active_queues}')
            if channel.active_queues:  # BRPOP mode?
                if channel.qos.can_consume():
                    self._register_BRPOP(channel)
            if channel.active_fanout_queues:  # LISTEN mode?
                self._register_LISTEN(channel)

    def on_poll_init(self, poller):
        self.poller = poller
        for channel in self._channels:
            return channel.qos.restore_visible(
                num=channel.unacked_restore_limit,
            )

    def maybe_restore_messages(self):
        for channel in self._channels:
            if channel.active_queues:
                # only need to do this once, as they are not local to channel.
                return channel.qos.restore_visible(
                    num=channel.unacked_restore_limit,
                )

    def maybe_check_subclient_health(self):
        for channel in self._channels:
            # only if subclient property is cached
            client = channel.__dict__.get('subclient')
            if client is not None \
                and callable(getattr(client, 'check_health', None)):
                client.check_health()

    def on_readable(self, fileno):
        chan, conn, type = self._fd_to_chan[fileno]
        print(f'kombu.transport.rediscluster.MultiChannelPoller.on_readable {fileno} {type} {self._chan_to_sock}')
        if chan.qos.can_consume():
            chan.handlers[type](**{'conn': conn})

    def handle_event(self, fileno, event):
        print(f'kombu.transport.rediscluster.MultiChannelPoller.handle_event {fileno} {event}')
        if event & READ:
            return self.on_readable(fileno), self
        elif event & ERR:
            chan, type = self._fd_to_chan[fileno]
            chan._poll_error(type)

    def get(self, callback, timeout=None):
        self._in_protected_read = True
        try:
            for channel in self._channels:
                if channel.active_queues:
                    if channel.qos.can_consume():
                        self._register_BRPOP(channel)
                if channel.active_fanout_queues:
                    self._register_LISTEN(channel)

            events = self.poller.poll(timeout)
            if events:
                for fileno, event in events:
                    ret = self.handle_event(fileno, event)
                    if ret:
                        return
            # - no new data, so try to restore messages.
            # - reset active redis commands.
            self.maybe_restore_messages()
            raise Empty()
        finally:
            self._in_protected_read = False
            while self.after_read:
                try:
                    fun = self.after_read.pop()
                except KeyError:
                    break
                else:
                    fun()

    @property
    def fds(self):
        return self._fd_to_chan


class Channel(RedisChannel):
    QoS = QoS
    _client = None
    _in_poll = False
    _in_poll_connections = set()
    _in_listen = False

    # todo: comment more
    keyprefix_queue = HASH_TAG + '_kombu.binding.%s'
    keyprefix_fanout = HASH_TAG + '/{db}.'
    unacked_key = HASH_TAG + 'unacked'
    unacked_index_key = HASH_TAG + 'unacked_index'
    unacked_mutex_key = HASH_TAG + 'unacked_mutex'
    # todo:check if support
    global_keyprefix = ''

    def __init__(self, connection, *args, **kwargs):
        super().__init__(connection, *args, **kwargs)
        self.global_keyprefix = HASH_TAG + self.global_keyprefix

    def _after_fork(self):
        self._disconnect_pools()

    def _disconnect_pools(self):
        pass

    def _on_connection_disconnect(self, connection):
        if self._in_poll is connection:
            self._in_poll = None
        if self._in_listen is connection:
            self._in_listen = None
        if self.connection and self.connection.cycle:
            self.connection.cycle._on_connection_disconnect(connection)

    def _do_restore_message(self, payload, exchange, routing_key,
                            pipe, leftmost=False):
        try:
            try:
                payload['headers']['redelivered'] = True
                payload['properties']['delivery_info']['redelivered'] = True
            except KeyError:
                pass
            for queue in self._lookup(exchange, routing_key):
                pri = self._get_message_priority(payload, reverse=False)

                (pipe.lpush if leftmost else pipe.rpush)(
                    self._q_for_pri(queue, pri), dumps(payload),
                )
        except Exception:
            crit('Could not restore message: %r', payload, exc_info=True)

    def _restore(self, message, leftmost=False):
        if not self.ack_emulation:
            return super()._restore(message)
        tag = message.delivery_tag

        def restore_transaction(pipe):
            P = pipe.hget(self.unacked_key, tag)
            pipe.multi()
            pipe.hdel(self.unacked_key, tag)
            if P:
                M, EX, RK = loads(bytes_to_str(P))
                self._do_restore_message(M, EX, RK, pipe, leftmost)

        with self.conn_or_acquire() as client:
            if HASH_TAG:
                client.transaction(restore_transaction, self.unacked_key)
            else:
                P = client.hget(self.unacked_key, tag)
                with client.pipeline() as pipe:
                    pipe.hdel(self.unacked_key, tag)
                    if P:
                        M, EX, RK = loads(bytes_to_str(P))
                        self._do_restore_message(M, EX, RK, pipe, leftmost)
                    pipe.execute()

    def _restore_at_beginning(self, message):
        return self._restore(message, leftmost=True)

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            exchange, _ = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
            self._fanout_to_queue[exchange] = queue
        ret = super().basic_consume(queue, *args, **kwargs)
        self._update_queue_cycle()
        return ret

    def basic_cancel(self, consumer_tag):
        # If we are busy reading messages we may experience
        # a race condition where a message is consumed after
        # canceling, so we must delay this operation until reading
        # is complete (Issue celery/celery#1773).
        connection = self.connection
        if connection:
            if connection.cycle._in_protected_read:
                return connection.cycle.after_read.add(
                    promise(self._basic_cancel, (consumer_tag,)),
                )
            return self._basic_cancel(consumer_tag)

    def _subscribe(self):
        keys = [self._get_subscribe_topic(queue)
                for queue in self.active_fanout_queues]
        if not keys:
            return
        c = self.subclient
        try:
            # todo: auto connect and init connection?
            c.psubscribe(keys)
        finally:
            self._in_listen = c.connection

    def _unsubscribe_from(self, queue):
        topic = self._get_subscribe_topic(queue)
        c = self.subclient
        if c.connection and c.connection._sock:
            c.unsubscribe([topic])

    def _handle_message(self, client, r):
        if bytes_to_str(r[0]) == 'unsubscribe' and r[2] == 0:
            client.subscribed = False
            return

        if bytes_to_str(r[0]) == 'pmessage':
            type, pattern, channel, data = r[0], r[1], r[2], r[3]
        else:
            type, pattern, channel, data = r[0], None, r[1], r[2]
        return {
            'type': type,
            'pattern': pattern,
            'channel': channel,
            'data': data,
        }

    def _receive(self, **kwargs):
        conn = kwargs.pop('conn', None)
        c = self.subclient
        print(f'kombu.transport.rediscluster.Channel._receive start {conn} {c.connection}')
        ret = []
        try:
            ret.append(self._receive_one(c))
        except Empty:
            pass
        while c.connection is not None and c.connection.can_read(timeout=0):
            ret.append(self._receive_one(c))
        return any(ret)

    def _receive_one(self, client, conn=None, timeout=1):
        print(f'kombu.transport.rediscluster.Channel._receive_one start {client} {conn}')
        response = None
        try:
            response = client.parse_response()
            print(f'kombu.transport.rediscluster.Channel._receive_one {response}')
        except self.connection_errors:
            self._in_listen = None
            raise
        if isinstance(response, (list, tuple)):
            payload = self._handle_message(client, response)
            if bytes_to_str(payload['type']).endswith('message'):
                channel = bytes_to_str(payload['channel'])
                if payload['data']:
                    if channel[0] == '/':
                        _, _, channel = channel.partition('.')
                    try:
                        message = loads(bytes_to_str(payload['data']))
                    except (TypeError, ValueError):
                        warning('Cannot process event on channel %r: %s',
                                channel, repr(payload)[:4096], exc_info=1)
                        raise Empty()
                    exchange = channel.split('/', 1)[0]
                    self.connection._deliver(
                        message, self._fanout_to_queue[exchange])
                    return True

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
                command_args = ['BRPOP', *keys, timeout]
                if self.global_keyprefix:
                    command_args = self.client._prefix_args(command_args)
                conn.send_command(*command_args)

    def _brpop_read(self, **options):
        try:
            conn = options.pop('conn', None)
            try:
                dest__item = conn.read_response('BRPOP', **options)
            except self.connection_errors:
                # if there's a ConnectionError, disconnect so the next
                # iteration will reconnect automatically.
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

    def _poll_error(self, type, **options):
        if type == 'LISTEN':
            self.subclient.parse_response()
        else:
            self.client.parse_response(self.client.connection, type)

    def _get(self, queue):
        with self.conn_or_acquire() as client:
            for pri in self.priority_steps:
                item = client.rpop(self._q_for_pri(queue, pri))
                if item:
                    return loads(bytes_to_str(item))
            raise Empty()

    def _size(self, queue):
        with self.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                for pri in self.priority_steps:
                    pipe = pipe.llen(self._q_for_pri(queue, pri))
                sizes = pipe.execute()
                return sum(size for size in sizes
                           if isinstance(size, numbers.Integral))

    def _q_for_pri(self, queue, pri):
        pri = self.priority(pri)
        if pri:
            return f"{queue}{self.sep}{pri}"
        return queue

    def priority(self, n):
        steps = self.priority_steps
        return steps[bisect(steps, n) - 1]

    def _put(self, queue, message, **kwargs):
        """Deliver message."""
        pri = self._get_message_priority(message, reverse=False)

        with self.conn_or_acquire() as client:
            client.lpush(self._q_for_pri(queue, pri), dumps(message))

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Deliver fanout message."""
        with self.conn_or_acquire() as client:
            client.publish(
                self._get_publish_topic(exchange, routing_key),
                dumps(message),
            )

    def _new_queue(self, queue, auto_delete=False, **kwargs):
        if auto_delete:
            self.auto_delete_queues.add(queue)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == 'fanout':
            # Mark exchange as fanout.
            self._fanout_queues[queue] = (
                exchange, routing_key.replace('#', '*'),
            )
        with self.conn_or_acquire() as client:
            client.sadd(self.keyprefix_queue % (exchange,),
                        self.sep.join([routing_key or '',
                                       pattern or '',
                                       queue or '']))

    def _delete(self, queue, exchange, routing_key, pattern, *args, **kwargs):
        self.auto_delete_queues.discard(queue)
        with self.conn_or_acquire(client=kwargs.get('client')) as client:
            client.srem(self.keyprefix_queue % (exchange,),
                        self.sep.join([routing_key or '',
                                       pattern or '',
                                       queue or '']))
            with client.pipeline() as pipe:
                for pri in self.priority_steps:
                    pipe = pipe.delete(self._q_for_pri(queue, pri))
                pipe.execute()

    def _has_queue(self, queue, **kwargs):
        with self.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                for pri in self.priority_steps:
                    pipe = pipe.exists(self._q_for_pri(queue, pri))
                return any(pipe.execute())

    def get_table(self, exchange):
        key = self.keyprefix_queue % exchange
        with self.conn_or_acquire() as client:
            values = client.smembers(key)
            if not values:
                # table does not exists since all queues bound to the exchange
                # were deleted. We need just return empty list.
                return []
            return [tuple(bytes_to_str(val).split(self.sep)) for val in values]

    def _purge(self, queue):
        with self.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                for pri in self.priority_steps:
                    priq = self._q_for_pri(queue, pri)
                    pipe = pipe.llen(priq).delete(priq)
                sizes = pipe.execute()
                return sum(sizes[::2])

    def close(self):
        self._closing = True
        if self._in_poll:
            try:
                # todo close what?
                for channel, conn in [(channel, conn) for channel, _, conn, _ in self.connection.cycle._chan_to_sock]:
                    if channel == self:
                        self._brpop_read(**{'conn': conn})
            except Empty:
                pass
        if not self.closed:
            # remove from channel poller.
            self.connection.cycle.discard(self)

            # delete fanout bindings
            client = self.__dict__.get('client')  # only if property cached
            if client is not None:
                for queue in self._fanout_queues:
                    if queue in self.auto_delete_queues:
                        self.queue_delete(queue, client=client)
            self._disconnect_pools()
            self._close_clients()
        super().close()

    def _close_clients(self):
        # Close connections
        for attr in 'client', 'subclient':
            try:
                client = self.__dict__[attr]
                connection, client.connection = client.connection, None
                connection.disconnect()
            except (KeyError, AttributeError, self.ResponseError):
                pass

    def _connparams(self, asynchronous=False):
        conn_params = super()._connparams(asynchronous=asynchronous)
        conn_params.pop('db', None)
        conn_params.pop('connection_class', None)
        return conn_params

    def _create_client(self, asynchronous=False):
        if self._client is None:
            conn_params = self._connparams(asynchronous=asynchronous)
            self._client = self.Client(**conn_params)
        return self._client

    def _get_pool(self, asynchronous=False):
        params = self._connparams(asynchronous=asynchronous)
        return redis.ConnectionPool(**params)

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

    @contextmanager
    def conn_or_acquire(self, client=None):
        if client:
            yield client
        else:
            yield self._create_client()

    @property
    def pool(self):
        if self._pool is None:
            self._pool = self._get_pool()
        return self._pool

    @property
    def async_pool(self):
        if self._async_pool is None:
            self._async_pool = self._get_pool(asynchronous=True)
        return self._async_pool

    @cached_property
    def client(self):
        return self._create_client(asynchronous=True)

    @cached_property
    def subclient(self):
        client = self._create_client(asynchronous=True)
        pubsub_client = client.pubsub()
        pubsub_client.ping()
        return pubsub_client

    def _update_queue_cycle(self):
        self._queue_cycle.update(self.active_queues)

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

    @property
    def active_queues(self):
        """Set of queues being consumed from (excluding fanout queues)."""
        return {queue for queue in self._active_queues
                if queue not in self.active_fanout_queues}


class Transport(RedisTransport):
    Channel = Channel

    polling_interval = None  # disable sleep between unsuccessful polls.
    driver_type = 'rediscluster'
    driver_name = 'rediscluster'

    implements = virtual.Transport.implements.extend(
        asynchronous=True,
        exchange_type=frozenset(['direct', 'topic', 'fanout'])
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cycle = MultiChannelPoller()

    def register_with_event_loop(self, connection, loop):
        cycle = self.cycle
        cycle.on_poll_init(loop.poller)
        cycle_poll_start = cycle.on_poll_start
        add_reader = loop.add_reader
        on_readable = self.on_readable

        def _on_disconnect(connection):
            if connection._sock:
                loop.remove(connection._sock)

            # must have started polling or this will break reconnection
            if cycle.fds:
                # stop polling in the event loop
                try:
                    loop.on_tick.remove(on_poll_start)
                except KeyError:
                    pass

        cycle._on_connection_disconnect = _on_disconnect

        def on_poll_start():
            cycle_poll_start()
            [add_reader(fd, on_readable, fd) for fd in cycle.fds]

        loop.on_tick.add(on_poll_start)
        loop.call_repeatedly(10, cycle.maybe_restore_messages)
        health_check_interval = connection.client.transport_options.get(
            'health_check_interval',
            DEFAULT_HEALTH_CHECK_INTERVAL
        )
        loop.call_repeatedly(
            health_check_interval,
            cycle.maybe_check_subclient_health
        )

    def on_readable(self, fileno):
        self.cycle.on_readable(fileno)
