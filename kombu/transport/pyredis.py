"""
kombu.transport.pyredis
=======================

Redis transport.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import socket

from itertools import imap
from Queue import Empty

from anyjson import serialize, deserialize

from kombu.transport import virtual
from kombu.utils import eventio

DEFAULT_PORT = 6379
DEFAULT_DB = 0


class MultiChannelPoller(object):
    eventflags = eventio.POLL_READ | eventio.POLL_ERR

    def __init__(self):
        self._channels = set()
        self._fd_to_chan = {}
        self._chan_to_sock = {}
        self._poller = eventio.poll()

    def add(self, channel):
        if channel not in self._channels:
            self._channels.add(channel)

    def _register(self, channel, client, type):
        if (channel, client, type) in self._chan_to_sock:
            self._unregister(channel, client, type)
        if client.connection._sock is None:
            client.connection.connect(client)
        sock = client.connection._sock
        sock.setblocking(0)
        self._fd_to_chan[sock.fileno()] = (channel, type)
        self._chan_to_sock[(channel, client, type)] = sock
        self._poller.register(sock, self.eventflags)

    def _unregister(self, channel, client, type):
        self._poller.unregister(self._chan_to_sock[(channel, client, type)])

    def _register_BRPOP(self, channel):
        ident = channel, channel.client, "BRPOP"
        if channel.client.connection._sock is None or \
                ident not in self._chan_to_sock:
            channel._in_poll = False
            self._register(*ident)
        # start BRPOP command
        if not channel._in_poll:
            channel._brpop_start()

    def _register_LISTEN(self, channel):
        if channel.subclient.connection._sock is None:
            channel._in_listen = False
            self._register(channel, channel.subclient, "LISTEN")
        if not channel._in_listen:
            channel._subscribe()

    def get(self, timeout=None):
        for channel in self._channels:
            if channel.active_queues:
                self._register_BRPOP(channel)
            if channel.active_fanout_queues:
                self._register_LISTEN(channel)

        events = self._poller.poll(timeout and timeout * 1000 or None)
        for fileno, event in events:
            if event & eventio.POLL_READ:
                chan, type = self._fd_to_chan[fileno]
                return chan.handlers[type](), self
            elif event & eventio.POLL_HUP:
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

    def __repr__(self):
        return "<Channel: %x BRPOP:%r SUB:%r>" % (id(self),
                                                  self.active_queues,
                                                  self.active_fanout_queues)

    def __init__(self, *args, **kwargs):
        super_ = super(Channel, self)
        super_.__init__(*args, **kwargs)

        self.Client = self._get_client()
        self.connection.cycle.add(self)
        self.active_fanout_queues = set()
        self._fanout_to_queue = {}
        self.ResponseError = self._get_response_error()

        self.handlers = {"BRPOP": self._brpop_read,
                         "LISTEN": self._receive}

    def _get_client(self):
        from redis import Redis
        return Redis

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

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
            c.connection.connect(c)
            c.connection._sock.setblocking(0)
        self.subclient.subscribe(keys)
        self._in_listen = True

    def _receive(self):
        c = self.subclient
        response = None
        try:
            response = c.parse_response("LISTEN")
        except self.connection.connection_errors:
            self._in_listen = False
        if response is not None:
            payload = c._handle_message(response)
            if payload["type"] == "message":
                return (deserialize(payload["data"]),
                        self._fanout_to_queue[payload["channel"]])
        raise Empty()

    def _brpop_start(self, timeout=0):
        queues = self.active_queues
        if not queues:
            return
        keys = list(queues) + [timeout or 0]
        name, cmd = self._encode_command("BRPOP", *keys)
        self.client.connection.send(cmd, self)
        self._in_poll = True

    def _encode_command(self, *args):
        encode = self.client.encode
        cmds = "".join('$%s\r\n%s\r\n' % (len(enc_value), enc_value)
                for enc_value in imap(encode, args))
        return args[0], '*%s\r\n%s' % (len(args), cmds)

    def _brpop_read(self, **options):
        try:
            try:
                dest__item = self.client.parse_response("BRPOP", **options)
            except self.connection.connection_errors:
                raise Empty()
            dest, item = dest__item
            return deserialize(item), dest
        finally:
            self._in_poll = False

    def _poll_error(self, type, **options):
        try:
            self.client.parse_response(type)
        except self.connection.connection_errors:
            pass

    def _get(self, queue):
        item = self.client.rpop(queue)
        if item:
            return deserialize(item)
        raise Empty()

    def _size(self, queue):
        return self.client.llen(queue)

    def _get_many(self, queues, timeout=None):
        dest__item = self.client.brpop(queues, timeout=timeout)
        if dest__item:
            dest, item = dest__item
            return deserialize(item), dest
        raise Empty()

    def _put(self, queue, message, **kwargs):
        self.client.lpush(queue, serialize(message))

    def _put_fanout(self, exchange, message, **kwargs):
        self.client.publish(exchange, serialize(message))

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == "fanout":
            self._fanout_queues[queue] = exchange
        self.client.sadd(self.keyprefix_queue % (exchange, ),
                          self.sep.join([routing_key or "",
                                        pattern or "",
                                        queue or ""]))

    def get_table(self, exchange):
        members = self.client.smembers(self.keyprefix_queue % (exchange, ))
        return [tuple(val.split(self.sep)) for val in members]


    def _purge(self, queue):
        size = self.client.llen(queue)
        self.client.delete(queue)
        return size

    def close(self):
        if self._client is not None:
            try:
                self._client.connection.disconnect()
            except (AttributeError, self.ResponseError):
                pass
        super(Channel, self).close()

    def _open(self):
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

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client

    @property
    def subclient(self):
        if self._subclient is None:
            self._subclient = self._open()
        return self._subclient

    @property
    def active_queues(self):
        return set(queue for queue in self._active_queues
                            if queue not in self.active_fanout_queues)


class Transport(virtual.Transport):
    Channel = Channel

    interval = 1
    default_port = DEFAULT_PORT
    default_cycle = MultiChannelPoller()

    def __init__(self, *args, **kwargs):
        super(Transport, self).__init__(*args, **kwargs)
        self.connection_errors, self.channel_errors = self._get_errors()
        self.cycle = self.default_cycle

    def close_connection(self, connection):
        self.cycle.close()
        super(Transport, self).close_connection(connection)

    def _get_errors(self):
        from redis import exceptions
        return ((exceptions.ConnectionError,
                 exceptions.AuthenticationError),
                (exceptions.ConnectionError,
                 exceptions.InvalidData,
                 exceptions.InvalidResponse,
                 exceptions.ResponseError))
