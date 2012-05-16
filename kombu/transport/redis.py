"""
kombu.transport.redis
=====================

Redis transport.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

from bisect import bisect
from itertools import cycle, islice
from time import time
from Queue import Empty

from anyjson import loads, dumps

from kombu.exceptions import (
    InconsistencyError,
    StdChannelError,
    VersionMismatch,
)
from kombu.log import get_logger
from kombu.utils import eventio, cached_property, uuid
from kombu.utils.encoding import str_t

from . import virtual

logger = get_logger("kombu.transport.redis")

DEFAULT_PORT = 6379
DEFAULT_DB = 0

PRIORITY_STEPS = [0, 3, 6, 9]

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


class DummyLock(object):

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass


class QoS(virtual.QoS):
    restore_at_shutdown = True

    def __init__(self, *args, **kwargs):
        super(QoS, self).__init__(*args, **kwargs)
        self.mutex = DummyLock()  # XXX let's see how this works out
        self._vrestore_count = 0

    def append(self, message, delivery_tag):
        with self.mutex:
            delivery = message.delivery_info
            EX, RK = delivery["exchange"], delivery["routing_key"]
            self.client.pipeline() \
                   .zadd(self.unacked_index_key, delivery_tag, time()) \
                   .hset(self.unacked_key, delivery_tag,
                       dumps([message._raw, EX, RK])) \
                   .execute()
            super(QoS, self).append(message, delivery_tag)

    def restore_unacked(self):
        for tag in self._delivered.iterkeys():
            self.restore_by_tag(tag)
        self._delivered.clear()

    def ack(self, delivery_tag):
        with self.mutex:
            self._remove_from_indices(delivery_tag).execute()
            super(QoS, self).ack(delivery_tag)
    reject = ack

    def _remove_from_indices(self, delivery_tag, pipe=None):
        return (pipe or self.client.pipeline()) \
                .zrem(self.unacked_index_key, delivery_tag) \
                .hdel(self.unacked_key, delivery_tag)

    def restore_visible(self, start=0, num=10, interval=10):
        self._vrestore_count += 1
        if (self._vrestore_count - 1) % interval:
            return
        with self.mutex:
            ceil = time() - self.visibility_timeout
            visible = self.client.zrevrangebyscore(
                    self.unacked_index_key, ceil, 0,
                    start=start, num=num, withscores=True)
            for tag, score in visible or []:
                self.restore_by_tag(tag)

    def restore_by_tag(self, tag):
        p, _, _ = self._remove_from_indices(tag,
                        self.client.pipeline().hget(self.unacked_key, tag)) \
                            .execute()
        if p:
            M, EX, RK = loads(p)
            self.channel._do_restore_message(M, EX, RK)

    @cached_property
    def client(self):
        return self.channel._avail_client

    @cached_property
    def unacked_key(self):
        return self.channel.unacked_key

    @cached_property
    def unacked_index_key(self):
        return self.channel.unacked_index_key

    @cached_property
    def visibility_timeout(self):
        return self.channel.visibility_timeout


class MultiChannelPoller(object):
    eventflags = eventio.POLL_READ | eventio.POLL_ERR

    def __init__(self):
        # active channels
        self._channels = set()
        # file descriptor -> channel map.
        self._fd_to_chan = {}
        # channel -> socket map
        self._chan_to_sock = {}
        # poll implementation (epoll/kqueue/select)
        self.poller = eventio.poll()

    def close(self):
        for fd in self._chan_to_sock.itervalues():
            try:
                self.poller.unregister(fd)
            except KeyError:
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()
        self.poller = None

    def add(self, channel):
        self._channels.add(channel)

    def discard(self, channel):
        self._channels.discard(channel)

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

    def _register_BRPOP(self, channel):
        """enable BRPOP mode for channel."""
        ident = channel, channel.client, "BRPOP"
        if channel.client.connection._sock is None or \
                ident not in self._chan_to_sock:
            channel._in_poll = False
            self._register(*ident)

        if not channel._in_poll:  # send BRPOP
            channel._brpop_start()

    def _register_LISTEN(self, channel):
        """enable LISTEN mode for channel."""
        if channel.subclient.connection._sock is None:
            channel._in_listen = False
            self._register(channel, channel.subclient, "LISTEN")
        if not channel._in_listen:
            channel._subscribe()  # send SUBSCRIBE

    def on_poll_start(self):
        for channel in self._channels:
            if channel.active_queues:           # BRPOP mode?
                if channel.qos.can_consume():
                    self._register_BRPOP(channel)
            if channel.active_fanout_queues:    # LISTEN mode?
                self._register_LISTEN(channel)

    def handle_event(self, fileno, event):
        if event & eventio.POLL_READ:
            chan, type = self._fd_to_chan[fileno]
            if chan.qos.can_consume():
                return chan.handlers[type](), self
        elif event & eventio.POLL_ERR:
            chan, type = self._fd_to_chan[fileno]
            chan._poll_error(type)

    def get(self, timeout=None):
        for channel in self._channels:
            if channel.active_queues:           # BRPOP mode?
                if channel.qos.can_consume():
                    self._register_BRPOP(channel)
            if channel.active_fanout_queues:    # LISTEN mode?
                self._register_LISTEN(channel)

        events = self.poller.poll(timeout)
        for fileno, event in events or []:
            ret = self.handle_event(fileno, event)
            if ret:
                return ret

        # - no new data, so try to restore messages.
        # - reset active redis commands.
        for channel in self._channels:
            if channel.active_queues:
                channel.qos.restore_visible()
        raise Empty()

    @property
    def fds(self):
        return self._fd_to_chan


class Channel(virtual.Channel):
    QoS = QoS

    _client = None
    _subclient = None
    supports_fanout = True
    keyprefix_queue = "_kombu.binding.%s"
    sep = '\x06\x16'
    _in_poll = False
    _in_listen = False
    _fanout_queues = {}
    unacked_key = "unacked"
    unacked_index_key = "unacked_index"
    visibility_timeout = 18000  # 5 hours
    priority_steps = PRIORITY_STEPS

    from_transport_options = (virtual.Channel.from_transport_options
                            + ("unacked_key",
                               "unacked_index_key",
                               "visibility_timeout",
                               "priority_steps"))

    def __init__(self, *args, **kwargs):
        super_ = super(Channel, self)
        super_.__init__(*args, **kwargs)

        self._queue_cycle = cycle([])
        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()
        self.active_fanout_queues = set()
        self.auto_delete_queues = set()
        self._fanout_to_queue = {}
        self.handlers = {"BRPOP": self._brpop_read, "LISTEN": self._receive}

        # Evaluate connection.
        self.client.info()

        self.connection.cycle.add(self)  # add to channel poller.
        # copy errors, in case channel closed but threads still
        # are still waiting for data.
        self.connection_errors = self.connection.connection_errors

    def _do_restore_message(self, payload, exchange, routing_key):
        try:
            try:
                payload["headers"]["redelivered"] = True
            except KeyError:
                pass
            for queue in self._lookup(exchange, routing_key):
                self._avail_client.lpush(queue, dumps(payload))
        except Exception:
            logger.critical("Could not restore message: %r", payload,
                    exc_info=True)

    def _restore(self, message, payload=None):
        tag = message.delivery_tag
        P, _ = self.client.pipeline() \
                            .hget(self.unacked_key, tag) \
                            .hdel(self.unacked_key, tag) \
                         .execute()
        if P:
            M, EX, RK = loads(P)
            self._do_restore_message(M, EX, RK)

    def _next_delivery_tag(self):
        return uuid()

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            exchange = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
            self._fanout_to_queue[exchange] = queue
        ret = super(Channel, self).basic_consume(queue, *args, **kwargs)
        self._update_cycle()
        return ret

    def basic_cancel(self, consumer_tag):
        try:
            queue = self._tag_to_queue[consumer_tag]
        except KeyError:
            return
        try:
            self.active_fanout_queues.discard(queue)
            self._fanout_to_queue.pop(self._fanout_queues[queue])
        except KeyError:
            pass
        ret = super(Channel, self).basic_cancel(consumer_tag)
        self._update_cycle()
        return ret

    def _subscribe(self):
        keys = [self._fanout_queues[queue]
                    for queue in self.active_fanout_queues]
        if not keys:
            return
        c = self.subclient
        if c.connection._sock is None:
            c.connection.connect()
        self.subclient.subscribe(keys)
        self._in_listen = True

    def _handle_message(self, client, r):
        if r[0] == "unsubscribe" and r[2] == 0:
            client.subscribed = False
        elif r[0] == "pmessage":
            return {"type":    r[0], "pattern": r[1],
                    "channel": r[2], "data":    r[3]}
        else:
            return {"type":    r[0], "pattern": None,
                    "channel": r[1], "data":    r[2]}

    def _receive(self):
        c = self.subclient
        response = None
        try:
            response = c.parse_response()
        except self.connection_errors:
            self._in_listen = False
        if response is not None:
            payload = self._handle_message(c, response)
            if payload["type"] == "message":
                return (loads(payload["data"]),
                        self._fanout_to_queue[payload["channel"]])
        raise Empty()

    def _brpop_start(self, timeout=1):
        queues = self._consume_cycle()
        if not queues:
            return
        keys = [self._q_for_pri(queue, pri) for pri in PRIORITY_STEPS
                        for queue in queues] + [timeout or 0]
        self.client.connection.send_command("BRPOP", *keys)
        self._in_poll = True

    def _brpop_read(self, **options):
        try:
            try:
                dest__item = self.client.parse_response(self.client.connection,
                                                        "BRPOP",
                                                        **options)
            except self.connection_errors, exc:
                # if there's a ConnectionError, disconnect so the next
                # iteration will reconnect automatically.
                self.client.connection.disconnect()
                raise Empty()
            if dest__item:
                dest, item = dest__item
                dest = dest.rsplit(self.sep, 1)[0]
                return loads(item), dest
            else:
                raise Empty()
        finally:
            self._in_poll = False

    def _poll_error(self, type, **options):
        try:
            self.client.parse_response(type)
        except self.connection_errors:
            pass

    def _get(self, queue):
        for pri in PRIORITY_STEPS:
            item = self._avail_client.rpop(self._q_for_pri(queue, pri))
            if item:
                return loads(item)
        raise Empty()

    def _size(self, queue):
        cmds = self._avail_client.pipeline()
        for pri in PRIORITY_STEPS:
            cmds = cmds.llen(self._q_for_pri(queue, pri))
        sizes = cmds.execute()
        return sum(size for size in sizes if isinstance(size, int))

    def _q_for_pri(self, queue, pri):
        pri = self.priority(pri)
        return '%s%s%s' % ((queue, self.sep, pri) if pri else (queue, '', ''))

    def priority(self, n):
        steps = self.priority_steps
        return steps[bisect(steps, n) - 1]

    def _put(self, queue, message, **kwargs):
        """Deliver message."""
        try:
            pri = max(min(int(
                message["properties"]["delivery_info"]["priority"]), 9), 0)
        except (TypeError, ValueError, KeyError):
            pri = 0
        self._avail_client.lpush(self._q_for_pri(queue, pri), dumps(message))

    def _put_fanout(self, exchange, message, **kwargs):
        """Deliver fanout message."""
        self._avail_client.publish(exchange, dumps(message))

    def _new_queue(self, queue, auto_delete=False, **kwargs):
        if auto_delete:
            self.auto_delete_queues.add(queue)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == "fanout":
            # Mark exchange as fanout.
            self._fanout_queues[queue] = exchange
        self._avail_client.sadd(self.keyprefix_queue % (exchange, ),
                            self.sep.join([routing_key or "",
                                           pattern or "",
                                           queue or ""]))

    def _delete(self, queue, exchange, routing_key, pattern, *args):
        self.auto_delete_queues.discard(queue)
        self._avail_client.srem(self.keyprefix_queue % (exchange, ),
                                self.sep.join([routing_key or "",
                                               pattern or "",
                                               queue or ""]))
        cmds = self._avail_client.pipeline()
        for pri in PRIORITY_STEPS:
            cmds = cmds.delete(self._q_for_pri(queue, pri))
        cmds.execute()

    def _has_queue(self, queue, **kwargs):
        cmds = self._avail_client.pipeline()
        for pri in PRIORITY_STEPS:
            cmds = cmds.exists(self._q_for_pri(queue, pri))
        return any(cmds.execute())

    def get_table(self, exchange):
        key = self.keyprefix_queue % exchange
        values = self.client.smembers(key)
        if not values:
            raise InconsistencyError(
                    "Queue list empty or key does not exist: %r" % (
                        self.keyprefix_queue % exchange))
        return [tuple(val.split(self.sep)) for val in values]

    def _purge(self, queue):
        cmds = self.pipeline()
        for pri in PRIORITY_STEPS:
            priq = self._q_for_pri(queue, pri)
            cmds = cmds.llen(priq).delete(priq)
        sizes = cmds.execute()
        return sum(sizes[::2])

    def close(self):
        if not self.closed:
            # remove from channel poller.
            self.connection.cycle.discard(self)

            # delete fanout bindings
            for queue in self._fanout_queues.iterkeys():
                if queue in self.auto_delete_queues:
                    self.queue_delete(queue)

            # Close connections
            for attr in "client", "subclient":
                try:
                    delattr(self, attr)
                except (AttributeError, self.ResponseError):
                    pass
        super(Channel, self).close()

    def _create_client(self):
        conninfo = self.connection.client
        database = conninfo.virtual_host
        if not isinstance(database, int):
            if not database or database == "/":
                database = DEFAULT_DB
            elif database.startswith("/"):
                database = database[1:]
            try:
                database = int(database)
            except ValueError:
                raise ValueError(
                    "Database name must be int between 0 and limit - 1")

        return self.Client(host=conninfo.hostname or "127.0.0.1",
                           port=conninfo.port or DEFAULT_PORT,
                           db=database,
                           password=conninfo.password)

    def _get_client(self):
        import redis

        version = getattr(redis, "__version__", (0, 0, 0))
        version = tuple(map(int, version.split(".")))
        if version < (2, 4, 4):
            raise VersionMismatch(
                "Redis transport requires redis-py versions 2.4.4 or later. "
                "You have %r" % (".".join(map(str_t, version)), ))

        # KombuRedis maintains a connection attribute on it's instance and
        # uses that when executing commands
        class KombuRedis(redis.Redis):  # pragma: no cover

            def __init__(self, *args, **kwargs):
                super(KombuRedis, self).__init__(*args, **kwargs)
                self.connection = self.connection_pool.get_connection('_')

            def execute_command(self, *args, **options):
                conn = self.connection
                command_name = args[0]
                try:
                    conn.send_command(*args)
                    return self.parse_response(conn, command_name, **options)
                except redis.ConnectionError:
                    conn.disconnect()
                    conn.send_command(*args)
                    return self.parse_response(conn, command_name, **options)

        return KombuRedis

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

    def pipeline(self):
        return self._avail_client.pipeline()

    @property
    def _avail_client(self):
        if self._in_poll:
            return self._create_client()
        return self.client

    @cached_property
    def client(self):
        return self._create_client()

    @client.deleter     # noqa
    def client(self, client):
        client.connection.disconnect()

    @cached_property
    def subclient(self):
        client = self._create_client()
        pubsub = client.pubsub()
        pool = pubsub.connection_pool
        pubsub.connection = pool.get_connection("pubsub", pubsub.shard_hint)
        return pubsub

    @subclient.deleter  # noqa
    def subclient(self, client):
        client.connection.disconnect()

    def _update_cycle(self):
        self._queue_cycle = cycle(self.active_queues)

    def _consume_cycle(self):
        active = len(self.active_queues)
        return list(islice(self._queue_cycle, 0, active + 1))[:active]

    @property
    def active_queues(self):
        return set(queue for queue in self._active_queues
                            if queue not in self.active_fanout_queues)


class Transport(virtual.Transport):
    Channel = Channel

    polling_interval = None  # disable sleep between unsuccessful polls.
    default_port = DEFAULT_PORT

    def __init__(self, *args, **kwargs):
        super(Transport, self).__init__(*args, **kwargs)
        self.connection_errors, self.channel_errors = self._get_errors()
        self.cycle = MultiChannelPoller()

    def on_poll_init(self, poller):
        self.cycle.poller = poller

    def on_poll_start(self):
        cycle = self.cycle
        cycle.on_poll_start()
        return dict((fd, self.handle_event) for fd in cycle.fds)

    def handle_event(self, fileno, event):
        ret = self.cycle.handle_event(fileno, event)
        if ret:
            item, channel = ret
            message, queue = item
            if not queue or queue not in self._callbacks:
                raise KeyError(
                    "Received message for queue '%s' without consumers: %s" % (
                        queue, message))
            self._callbacks[queue](message)

    def _get_errors(self):
        from redis import exceptions
        # This exception suddenly changed name between redis-py versions
        if hasattr(exceptions, "InvalidData"):
            DataError = exceptions.InvalidData
        else:
            DataError = exceptions.DataError
        return ((exceptions.ConnectionError,
                 exceptions.AuthenticationError),
                (exceptions.ConnectionError,
                 DataError,
                 exceptions.InvalidResponse,
                 exceptions.ResponseError,
                 StdChannelError))
