"""
kombu.transport.redis
=====================

Redis transport.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from Queue import Empty

from anyjson import loads, dumps

from ..exceptions import VersionMismatch
from ..utils import eventio, cached_property
from ..utils.encoding import str_t

from . import virtual

DEFAULT_PORT = 6379
DEFAULT_DB = 0


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
        self._poller = eventio.poll()

    def close(self):
        for fd in self._chan_to_sock.itervalues():
            try:
                self._poller.unregister(fd)
            except KeyError:
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self._chan_to_sock.clear()
        self._poller = None

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
        self._poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, type):
        self._poller.unregister(self._chan_to_sock[(channel, client, type)])

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

    def get(self, timeout=None):
        for channel in self._channels:
            if channel.active_queues:           # BRPOP mode?
                if channel.qos.can_consume():
                    self._register_BRPOP(channel)
            if channel.active_fanout_queues:    # LISTEN mode?
                self._register_LISTEN(channel)

        events = self._poller.poll(timeout)
        for fileno, event in events:
            if event & eventio.POLL_READ:
                chan, type = self._fd_to_chan[fileno]
                if chan.qos.can_consume():
                    return chan.handlers[type](), self
            elif event & eventio.POLL_ERR:
                chan, type = self._fd_to_chan[fileno]
                chan._poll_error(type)
                break
        raise Empty()


class Channel(virtual.Channel):
    _client = None
    _subclient = None
    supports_fanout = True
    keyprefix_queue = "_kombu.binding.%s"
    sep = '\x06\x16'
    _in_poll = False
    _in_listen = False
    _fanout_queues = {}

    def __init__(self, *args, **kwargs):
        super_ = super(Channel, self)
        super_.__init__(*args, **kwargs)

        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()
        self.active_fanout_queues = set()
        self._fanout_to_queue = {}
        self.handlers = {"BRPOP": self._brpop_read, "LISTEN": self._receive}

        # Evaluate connection.
        self.client.info()

        self.connection.cycle.add(self)  # add to channel poller.
        # copy errors, in case channel closed but threads still
        # are still waiting for data.
        self.connection_errors = self.connection.connection_errors

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            exchange = self._fanout_queues[queue]
            self.active_fanout_queues.add(queue)
            self._fanout_to_queue[exchange] = queue
        return super(Channel, self).basic_consume(queue, *args, **kwargs)

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
        return super(Channel, self).basic_cancel(consumer_tag)

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
        queues = self.active_queues
        if not queues:
            return
        keys = list(queues) + [timeout or 0]
        self.client.connection.send_command("BRPOP", *keys)
        self._in_poll = True

    def _brpop_read(self, **options):
        try:
            try:
                dest__item = self.client.parse_response(self.client.connection,
                                                        "BRPOP",
                                                        **options)
            except self.connection_errors:
                # if there's a ConnectionError, disconnect so the next
                # iteration will reconnect automatically.
                self.client.connection.disconnect()
                raise Empty()
            if dest__item:
                dest, item = dest__item
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
        """basic.get

        .. note::

            Implies ``no_ack=True``

        """
        item = self._avail_client.rpop(queue)
        if item:
            return loads(item)
        raise Empty()

    def _size(self, queue):
        return self._avail_client.llen(queue)

    def _put(self, queue, message, **kwargs):
        """Deliver message."""
        self._avail_client.lpush(queue, dumps(message))

    def _put_fanout(self, exchange, message, **kwargs):
        """Deliver fanout message."""
        self._avail_client.publish(exchange, dumps(message))

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == "fanout":
            # Mark exchange as fanout.
            self._fanout_queues[queue] = exchange
        self._avail_client.sadd(self.keyprefix_queue % (exchange, ),
                            self.sep.join([routing_key or "",
                                           pattern or "",
                                           queue or ""]))

    def _delete(self, queue, exchange, routing_key, pattern, *args):
        self._avail_client.srem(self.keyprefix_queue % (exchange, ),
                                self.sep.join([routing_key or "",
                                               pattern or "",
                                               queue or ""]))
        self._avail_client.delete(queue)

    def _has_queue(self, queue, **kwargs):
        return self._avail_client.exists(queue)

    def get_table(self, exchange):
        return [tuple(val.split(self.sep))
                    for val in self._avail_client.smembers(
                            self.keyprefix_queue % exchange)]

    def _purge(self, queue):
        size, _ = self._avail_client.pipeline().llen(queue) \
                                        .delete(queue).execute()
        return size

    def close(self):
        if not self.closed:
            # remove from channel poller.
            self.connection.cycle.discard(self)

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

        return self.Client(host=conninfo.hostname,
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
                 exceptions.ResponseError))
