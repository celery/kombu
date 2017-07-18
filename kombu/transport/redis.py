"""Redis transport."""
from __future__ import absolute_import, unicode_literals

import numbers
import socket

from bisect import bisect
from collections import namedtuple
from contextlib import contextmanager
from time import time

from vine import promise

from kombu.exceptions import InconsistencyError, VersionMismatch
from kombu.five import Empty, values, string_t
from kombu.log import get_logger
from kombu.utils.compat import register_after_fork
from kombu.utils.eventio import poll, READ, ERR
from kombu.utils.encoding import bytes_to_str
from kombu.utils.json import loads, dumps
from kombu.utils.objects import cached_property
from kombu.utils.scheduling import cycle_by_name
from kombu.utils.url import _parse_url
from kombu.utils.uuid import uuid

from . import virtual

try:
    import redis
except ImportError:  # pragma: no cover
    redis = None     # noqa

try:
    from redis import sentinel
except ImportError:  # pragma: no cover
    sentinel = None  # noqa


logger = get_logger('kombu.transport.redis')
crit, warn = logger.critical, logger.warn

DEFAULT_PORT = 6379
DEFAULT_DB = 0

PRIORITY_STEPS = [0, 3, 6, 9]

error_classes_t = namedtuple('error_classes_t', (
    'connection_errors', 'channel_errors',
))

NO_ROUTE_ERROR = """
Cannot route message for exchange {0!r}: Table empty or key no longer exists.
Probably the key ({1!r}) has been removed from the Redis database.
"""

# This implementation may seem overly complex, but I assure you there is
# a good reason for doing it this way.
#
# Consuming from several connections enables us to emulate channels,
# which means we can have different service guarantees for individual
# channels.
#
# So we need to consume messages from multiple connections simultaneously,
# and using epoll means we don't have to do so using multiple threads.
#
# Also it means we can easily use PUBLISH/SUBSCRIBE to do fanout
# exchanges (broadcast), as an alternative to pushing messages to fanout-bound
# queues manually.


def get_redis_error_classes():
    """Return tuple of redis error classes."""
    from redis import exceptions
    # This exception suddenly changed name between redis-py versions
    if hasattr(exceptions, 'InvalidData'):
        DataError = exceptions.InvalidData
    else:
        DataError = exceptions.DataError
    return error_classes_t(
        (virtual.Transport.connection_errors + (
            InconsistencyError,
            socket.error,
            IOError,
            OSError,
            exceptions.ConnectionError,
            exceptions.AuthenticationError,
            exceptions.TimeoutError)),
        (virtual.Transport.channel_errors + (
            DataError,
            exceptions.InvalidResponse,
            exceptions.ResponseError)),
    )


def get_redis_ConnectionError():
    """Return the redis ConnectionError exception class."""
    from redis import exceptions
    return exceptions.ConnectionError


class MutexHeld(Exception):
    """Raised when another party holds the lock."""


@contextmanager
def Mutex(client, name, expire):
    """The Redis lock implementation (probably shaky)."""
    lock_id = uuid()
    i_won = client.setnx(name, lock_id)
    try:
        if i_won:
            client.expire(name, expire)
            yield
        else:
            if not client.ttl(name):
                client.expire(name, expire)
            raise MutexHeld()
    finally:
        if i_won:
            try:
                with client.pipeline(True) as pipe:
                    pipe.watch(name)
                    if pipe.get(name) == lock_id:
                        pipe.multi()
                        pipe.delete(name)
                        pipe.execute()
                    pipe.unwatch()
            except redis.WatchError:
                pass


def _after_fork_cleanup_channel(channel):
    channel._after_fork()


class QoS(virtual.QoS):
    """Redis Ack Emulation."""

    restore_at_shutdown = True

    def __init__(self, *args, **kwargs):
        super(QoS, self).__init__(*args, **kwargs)
        self._vrestore_count = 0

    def append(self, message, delivery_tag):
        delivery = message.delivery_info
        EX, RK = delivery['exchange'], delivery['routing_key']
        with self.pipe_or_acquire() as pipe:
            pipe.zadd(self.unacked_index_key, time(), delivery_tag) \
                .hset(self.unacked_key, delivery_tag,
                      dumps([message._raw, EX, RK])) \
                .execute()
            super(QoS, self).append(message, delivery_tag)

    def restore_unacked(self, client=None):
        with self.channel.conn_or_acquire(client) as client:
            for tag in self._delivered:
                self.restore_by_tag(tag, client=client)
        self._delivered.clear()

    def ack(self, delivery_tag):
        self._remove_from_indices(delivery_tag).execute()
        super(QoS, self).ack(delivery_tag)

    def reject(self, delivery_tag, requeue=False):
        if requeue:
            self.restore_by_tag(delivery_tag, leftmost=True)
        self.ack(delivery_tag)

    @contextmanager
    def pipe_or_acquire(self, pipe=None, client=None):
        if pipe:
            yield pipe
        else:
            with self.channel.conn_or_acquire(client) as client:
                yield client.pipeline()

    def _remove_from_indices(self, delivery_tag, pipe=None):
        with self.pipe_or_acquire(pipe) as pipe:
            return pipe.zrem(self.unacked_index_key, delivery_tag) \
                       .hdel(self.unacked_key, delivery_tag)

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
            with client.pipeline() as pipe:
                p, _, _ = self._remove_from_indices(
                    tag, pipe.hget(self.unacked_key, tag)).execute()
            if p:
                M, EX, RK = loads(bytes_to_str(p))  # json is unicode
                self.channel._do_restore_message(M, EX, RK, client, leftmost)

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


class MultiChannelPoller(object):
    """Async I/O poller for Redis transport."""

    eventflags = READ | ERR

    #: Set by :meth:`get` while reading from the socket.
    _in_protected_read = False

    #: Set of one-shot callbacks to call after reading from socket.
    after_read = None

    def __init__(self):
        # active channels
        self._channels = set()
        # file descriptor -> channel map.
        self._fd_to_chan = {}
        # channel -> socket map
        self._chan_to_sock = {}
        # poll implementation (epoll/kqueue/select)
        self.poller = poll()
        # one-shot callbacks called after reading from socket.
        self.after_read = set()

    def close(self):
        for fd in values(self._chan_to_sock):
            try:
                self.poller.unregister(fd)
            except (KeyError, ValueError):
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()

    def add(self, channel):
        self._channels.add(channel)

    def discard(self, channel):
        self._channels.discard(channel)

    def _on_connection_disconnect(self, connection):
        try:
            self.poller.unregister(connection._sock)
        except (AttributeError, TypeError):
            pass

    def _register(self, channel, client, type):
        if (channel, client, type) in self._chan_to_sock:
            self._unregister(channel, client, type)
        if client.connection._sock is None:   # not connected yet.
            client.connection.connect()
        sock = client.connection._sock
        self._fd_to_chan[sock.fileno()] = (channel, type)
        self._chan_to_sock[(channel, client, type)] = sock
        self.poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, type):
        self.poller.unregister(self._chan_to_sock[(channel, client, type)])

    def _client_registered(self, channel, client, cmd):
        if getattr(client, 'connection', None) is None:
            client.connection = client.connection_pool.get_connection('_')
        return (client.connection._sock is not None and
                (channel, client, cmd) in self._chan_to_sock)

    def _register_BRPOP(self, channel):
        """Enable BRPOP mode for channel."""
        ident = channel, channel.client, 'BRPOP'
        if not self._client_registered(channel, channel.client, 'BRPOP'):
            channel._in_poll = False
            self._register(*ident)
        if not channel._in_poll:  # send BRPOP
            channel._brpop_start()

    def _register_LISTEN(self, channel):
        """Enable LISTEN mode for channel."""
        if not self._client_registered(channel, channel.subclient, 'LISTEN'):
            channel._in_listen = False
            self._register(channel, channel.subclient, 'LISTEN')
        if not channel._in_listen:
            channel._subscribe()  # send SUBSCRIBE

    def on_poll_start(self):
        for channel in self._channels:
            if channel.active_queues:           # BRPOP mode?
                if channel.qos.can_consume():
                    self._register_BRPOP(channel)
            if channel.active_fanout_queues:    # LISTEN mode?
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

    def on_readable(self, fileno):
        chan, type = self._fd_to_chan[fileno]
        if chan.qos.can_consume():
            chan.handlers[type]()

    def handle_event(self, fileno, event):
        if event & READ:
            return self.on_readable(fileno), self
        elif event & ERR:
            chan, type = self._fd_to_chan[fileno]
            chan._poll_error(type)

    def get(self, callback, timeout=None):
        self._in_protected_read = True
        try:
            for channel in self._channels:
                if channel.active_queues:           # BRPOP mode?
                    if channel.qos.can_consume():
                        self._register_BRPOP(channel)
                if channel.active_fanout_queues:    # LISTEN mode?
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


class Channel(virtual.Channel):
    """Redis Channel."""

    QoS = QoS

    _client = None
    _subclient = None
    _closing = False
    supports_fanout = True
    keyprefix_queue = '_kombu.binding.%s'
    keyprefix_fanout = '/{db}.'
    sep = '\x06\x16'
    _in_poll = False
    _in_listen = False
    _fanout_queues = {}
    ack_emulation = True
    unacked_key = 'unacked'
    unacked_index_key = 'unacked_index'
    unacked_mutex_key = 'unacked_mutex'
    unacked_mutex_expire = 300  # 5 minutes
    unacked_restore_limit = None
    visibility_timeout = 3600   # 1 hour
    priority_steps = PRIORITY_STEPS
    socket_timeout = None
    socket_connect_timeout = None
    socket_keepalive = None
    socket_keepalive_options = None
    max_connections = 10
    #: Transport option to disable fanout keyprefix.
    #: Can also be string, in which case it changes the default
    #: prefix ('/{db}.') into to something else.  The prefix must
    #: include a leading slash and a trailing dot.
    #:
    #: Enabled by default since Kombu 4.x.
    #: Disable for backwards compatibility with Kombu 3.x.
    fanout_prefix = True

    #: If enabled the fanout exchange will support patterns in routing
    #: and binding keys (like a topic exchange but using PUB/SUB).
    #:
    #: Enabled by default since Kombu 4.x.
    #: Disable for backwards compatibility with Kombu 3.x.
    fanout_patterns = True

    #: Order in which we consume from queues.
    #:
    #: Can be either string alias, or a cycle strategy class
    #:
    #: - ``round_robin``
    #:   (:class:`~kombu.utils.scheduling.round_robin_cycle`).
    #:
    #:    Make sure each queue has an equal opportunity to be consumed from.
    #:
    #: - ``sorted``
    #:   (:class:`~kombu.utils.scheduling.sorted_cycle`).
    #:
    #:    Consume from queues in alphabetical order.
    #:    If the first queue in the sorted list always contains messages,
    #:    then the rest of the queues will never be consumed from.
    #:
    #: - ``priority``
    #:   (:class:`~kombu.utils.scheduling.priority_cycle`).
    #:
    #:    Consume from queues in original order, so that if the first
    #:    queue always contains messages, the rest of the queues
    #:    in the list will never be consumed from.
    #:
    #: The default is to consume from queues in round robin.
    queue_order_strategy = 'round_robin'

    _async_pool = None
    _pool = None

    from_transport_options = (
        virtual.Channel.from_transport_options +
        ('ack_emulation',
         'unacked_key',
         'unacked_index_key',
         'unacked_mutex_key',
         'unacked_mutex_expire',
         'visibility_timeout',
         'unacked_restore_limit',
         'fanout_prefix',
         'fanout_patterns',
         'socket_timeout',
         'socket_connect_timeout',
         'socket_keepalive',
         'socket_keepalive_options',
         'queue_order_strategy',
         'max_connections',
         'priority_steps')  # <-- do not add comma here!
    )

    connection_class = redis.Connection if redis else None

    def __init__(self, *args, **kwargs):
        super_ = super(Channel, self)
        super_.__init__(*args, **kwargs)

        if not self.ack_emulation:  # disable visibility timeout
            self.QoS = virtual.QoS

        self._queue_cycle = cycle_by_name(self.queue_order_strategy)()
        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()
        self.active_fanout_queues = set()
        self.auto_delete_queues = set()
        self._fanout_to_queue = {}
        self.handlers = {'BRPOP': self._brpop_read, 'LISTEN': self._receive}

        if self.fanout_prefix:
            if isinstance(self.fanout_prefix, string_t):
                self.keyprefix_fanout = self.fanout_prefix
        else:
            # previous versions did not set a fanout, so cannot enable
            # by default.
            self.keyprefix_fanout = ''

        # Evaluate connection.
        try:
            self.client.ping()
        except Exception:
            self._disconnect_pools()
            raise

        self.connection.cycle.add(self)  # add to channel poller.
        # copy errors, in case channel closed but threads still
        # are still waiting for data.
        self.connection_errors = self.connection.connection_errors

        if register_after_fork is not None:
            register_after_fork(self, _after_fork_cleanup_channel)

    def _after_fork(self):
        self._disconnect_pools()

    def _disconnect_pools(self):
        pool = self._pool
        async_pool = self._async_pool

        self._async_pool = self._pool = None

        if pool is not None:
            pool.disconnect()

        if async_pool is not None:
            async_pool.disconnect()

    def _on_connection_disconnect(self, connection):
        if self._in_poll is connection:
            self._in_poll = None
        if self._in_listen is connection:
            self._in_listen = None
        if self.connection and self.connection.cycle:
            self.connection.cycle._on_connection_disconnect(connection)

    def _do_restore_message(self, payload, exchange, routing_key,
                            client=None, leftmost=False):
        with self.conn_or_acquire(client) as client:
            try:
                try:
                    payload['headers']['redelivered'] = True
                except KeyError:
                    pass
                for queue in self._lookup(exchange, routing_key):
                    (client.lpush if leftmost else client.rpush)(
                        queue, dumps(payload),
                    )
            except Exception:
                crit('Could not restore message: %r', payload, exc_info=True)

    def _restore(self, message, leftmost=False):
        if not self.ack_emulation:
            return super(Channel, self)._restore(message)
        tag = message.delivery_tag
        with self.conn_or_acquire() as client:
            with client.pipeline() as pipe:
                P, _ = pipe.hget(self.unacked_key, tag) \
                           .hdel(self.unacked_key, tag) \
                           .execute()
            if P:
                M, EX, RK = loads(bytes_to_str(P))  # json is unicode
                self._do_restore_message(M, EX, RK, client, leftmost)

    def _restore_at_beginning(self, message):
        return self._restore(message, leftmost=True)

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            exchange, _ = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
            self._fanout_to_queue[exchange] = queue
        ret = super(Channel, self).basic_consume(queue, *args, **kwargs)

        # Update fair cycle between queues.
        #
        # We cycle between queues fairly to make sure that
        # each queue is equally likely to be consumed from,
        # so that a very busy queue will not block others.
        #
        # This works by using Redis's `BRPOP` command and
        # by rotating the most recently used queue to the
        # and of the list.  See Kombu github issue #166 for
        # more discussion of this method.
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

    def _basic_cancel(self, consumer_tag):
        try:
            queue = self._tag_to_queue[consumer_tag]
        except KeyError:
            return
        try:
            self.active_fanout_queues.remove(queue)
        except KeyError:
            pass
        else:
            self._unsubscribe_from(queue)
        try:
            exchange, _ = self._fanout_queues[queue]
            self._fanout_to_queue.pop(exchange)
        except KeyError:
            pass
        ret = super(Channel, self).basic_cancel(consumer_tag)
        self._update_queue_cycle()
        return ret

    def _get_publish_topic(self, exchange, routing_key):
        if routing_key and self.fanout_patterns:
            return ''.join([self.keyprefix_fanout, exchange, '/', routing_key])
        return ''.join([self.keyprefix_fanout, exchange])

    def _get_subscribe_topic(self, queue):
        exchange, routing_key = self._fanout_queues[queue]
        return self._get_publish_topic(exchange, routing_key)

    def _subscribe(self):
        keys = [self._get_subscribe_topic(queue)
                for queue in self.active_fanout_queues]
        if not keys:
            return
        c = self.subclient
        if c.connection._sock is None:
            c.connection.connect()
        self._in_listen = c.connection
        c.psubscribe(keys)

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

    def _receive(self):
        c = self.subclient
        ret = []
        try:
            ret.append(self._receive_one(c))
        except Empty:
            pass
        if c.connection is not None:
            while c.connection.can_read(timeout=0):
                ret.append(self._receive_one(c))
        return any(ret)

    def _receive_one(self, c):
        response = None
        try:
            response = c.parse_response()
        except self.connection_errors:
            self._in_listen = None
            raise
        if response is not None:
            payload = self._handle_message(c, response)
            if bytes_to_str(payload['type']).endswith('message'):
                channel = bytes_to_str(payload['channel'])
                if payload['data']:
                    if channel[0] == '/':
                        _, _, channel = channel.partition('.')
                    try:
                        message = loads(bytes_to_str(payload['data']))
                    except (TypeError, ValueError):
                        warn('Cannot process event on channel %r: %s',
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
        keys = [self._q_for_pri(queue, pri) for pri in self.priority_steps
                for queue in queues] + [timeout or 0]
        self._in_poll = self.client.connection
        self.client.connection.send_command('BRPOP', *keys)

    def _brpop_read(self, **options):
        try:
            try:
                dest__item = self.client.parse_response(self.client.connection,
                                                        'BRPOP',
                                                        **options)
            except self.connection_errors:
                # if there's a ConnectionError, disconnect so the next
                # iteration will reconnect automatically.
                self.client.connection.disconnect()
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
        return '%s%s%s' % ((queue, self.sep, pri) if pri else (queue, '', ''))

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
                raise InconsistencyError(NO_ROUTE_ERROR.format(exchange, key))
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
        super(Channel, self).close()

    def _close_clients(self):
        # Close connections
        for attr in 'client', 'subclient':
            try:
                client = self.__dict__[attr]
                connection, client.connection = client.connection, None
                connection.disconnect()
            except (KeyError, AttributeError, self.ResponseError):
                pass

    def _prepare_virtual_host(self, vhost):
        if not isinstance(vhost, numbers.Integral):
            if not vhost or vhost == '/':
                vhost = DEFAULT_DB
            elif vhost.startswith('/'):
                vhost = vhost[1:]
            try:
                vhost = int(vhost)
            except ValueError:
                raise ValueError(
                    'Database is int between 0 and limit - 1, not {0}'.format(
                        vhost,
                    ))
        return vhost

    def _filter_tcp_connparams(self, socket_keepalive=None,
                               socket_keepalive_options=None, **params):
        return params

    def _connparams(self, async=False):
        conninfo = self.connection.client
        connparams = {
            'host': conninfo.hostname or '127.0.0.1',
            'port': conninfo.port or self.connection.default_port,
            'virtual_host': conninfo.virtual_host,
            'password': conninfo.password,
            'max_connections': self.max_connections,
            'socket_timeout': self.socket_timeout,
            'socket_connect_timeout': self.socket_connect_timeout,
            'socket_keepalive': self.socket_keepalive,
            'socket_keepalive_options': self.socket_keepalive_options,
        }
        if conninfo.ssl:
            # Connection(ssl={}) must be a dict containing the keys:
            # 'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'
            try:
                connparams.update(conninfo.ssl)
                connparams['connection_class'] = redis.SSLConnection
            except TypeError:
                pass
        host = connparams['host']
        if '://' in host:
            scheme, _, _, _, password, path, query = _parse_url(host)
            if scheme == 'socket':
                connparams = self._filter_tcp_connparams(**connparams)
                connparams.update({
                    'connection_class': redis.UnixDomainSocketConnection,
                    'path': '/' + path}, **query)

                connparams.pop('socket_connect_timeout', None)
                connparams.pop('socket_keepalive', None)
                connparams.pop('socket_keepalive_options', None)
            connparams['password'] = password

            connparams.pop('host', None)
            connparams.pop('port', None)
        connparams['db'] = self._prepare_virtual_host(
            connparams.pop('virtual_host', None))

        channel = self
        connection_cls = (
            connparams.get('connection_class') or
            self.connection_class
        )

        if async:
            class Connection(connection_cls):
                def disconnect(self):
                    super(Connection, self).disconnect()
                    channel._on_connection_disconnect(self)
            connection_cls = Connection

        connparams['connection_class'] = connection_cls

        return connparams

    def _create_client(self, async=False):
        if async:
            return self.Client(connection_pool=self.async_pool)
        return self.Client(connection_pool=self.pool)

    def _get_pool(self, async=False):
        params = self._connparams(async=async)
        self.keyprefix_fanout = self.keyprefix_fanout.format(db=params['db'])
        return redis.ConnectionPool(**params)

    def _get_client(self):
        if redis.VERSION < (2, 10, 0):
            raise VersionMismatch(
                'Redis transport requires redis-py versions 2.10.0 or later. '
                'You have {0.__version__}'.format(redis))
        return redis.StrictRedis

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
            self._async_pool = self._get_pool(async=True)
        return self._async_pool

    @cached_property
    def client(self):
        """Client used to publish messages, BRPOP etc."""
        return self._create_client(async=True)

    @cached_property
    def subclient(self):
        """Pub/Sub connection used to consume fanout queues."""
        client = self._create_client(async=True)
        return client.pubsub()

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


class Transport(virtual.Transport):
    """Redis Transport."""

    Channel = Channel

    polling_interval = None  # disable sleep between unsuccessful polls.
    default_port = DEFAULT_PORT
    driver_type = 'redis'
    driver_name = 'redis'

    implements = virtual.Transport.implements.extend(
        async=True,
        exchange_type=frozenset(['direct', 'topic', 'fanout'])
    )

    def __init__(self, *args, **kwargs):
        if redis is None:
            raise ImportError('Missing redis library (pip install redis)')
        super(Transport, self).__init__(*args, **kwargs)

        # Get redis-py exceptions.
        self.connection_errors, self.channel_errors = self._get_errors()
        # All channels share the same poller.
        self.cycle = MultiChannelPoller()

    def driver_version(self):
        return redis.__version__

    def register_with_event_loop(self, connection, loop):
        cycle = self.cycle
        cycle.on_poll_init(loop.poller)
        cycle_poll_start = cycle.on_poll_start
        add_reader = loop.add_reader
        on_readable = self.on_readable

        def _on_disconnect(connection):
            if connection._sock:
                loop.remove(connection._sock)
        cycle._on_connection_disconnect = _on_disconnect

        def on_poll_start():
            cycle_poll_start()
            [add_reader(fd, on_readable, fd) for fd in cycle.fds]
        loop.on_tick.add(on_poll_start)
        loop.call_repeatedly(10, cycle.maybe_restore_messages)

    def on_readable(self, fileno):
        """Handle AIO event for one of our file descriptors."""
        self.cycle.on_readable(fileno)

    def _get_errors(self):
        """Utility to import redis-py's exceptions at runtime."""
        return get_redis_error_classes()


class SentinelChannel(Channel):
    """Channel with explicit Redis Sentinel knowledge.

    Broker url is supposed to look like:

    sentinel://0.0.0.0:26379;sentinel://0.0.0.0:26380/...

    where each sentinel is separated by a `;`.  Multiple sentinels are handled
    by :class:`kombu.Connection` constructor, and placed in the alternative
    list of servers to connect to in case of connection failure.

    Other arguments for the sentinel should come from the transport options
    (see :method:`Celery.connection` which is in charge of creating the
    `Connection` object).

    You must provide at least one option in Transport options:
     * `master_name` - name of the redis group to poll
    """

    from_transport_options = Channel.from_transport_options + (
        'master_name',
        'min_other_sentinels',
        'sentinel_kwargs')

    connection_class = sentinel.SentinelManagedConnection if sentinel else None

    def _sentinel_managed_pool(self, async=False):
        connparams = self._connparams(async)

        additional_params = connparams.copy()

        additional_params.pop('host', None)
        additional_params.pop('port', None)

        sentinel_inst = sentinel.Sentinel(
            [(connparams['host'], connparams['port'])],
            min_other_sentinels=getattr(self, 'min_other_sentinels', 0),
            sentinel_kwargs=getattr(self, 'sentinel_kwargs', {}),
            **additional_params)

        master_name = getattr(self, 'master_name', None)

        return sentinel_inst.master_for(
            master_name,
            self.Client,
        ).connection_pool

    def _get_pool(self, async=False):
        return self._sentinel_managed_pool(async)


class SentinelTransport(Transport):
    """Redis Sentinel Transport."""

    default_port = 26379
    Channel = SentinelChannel
