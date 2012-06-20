from __future__ import absolute_import

import errno
import os
import socket

from anyjson import loads, dumps
from Queue import Empty

import zmq

from kombu.exceptions import StdChannelError
from kombu.utils import cached_property
from kombu.utils.eventio import poll, READ

from . import virtual

DEFAULT_PORT = 5555
DEFAULT_HWM = 1000
DEFAULT_INCR = 2


class MultiChannelPoller(object):
    eventflags = READ

    def __init__(self):
        # active channels
        self._channels = set()
        # file descriptor -> channel map
        self._fd_to_chan = {}
        # poll implementation (epoll/kqueue/select)
        self.poller = poll()

    def close(self):
        for fd in self._fd_to_chan:
            try:
                self.poller.unregister(fd)
            except KeyError:
                pass
        self._channels.clear()
        self._fd_to_chan.clear()
        self.poller = None

    def add(self, channel):
        self._channels.add(channel)

    def discard(self, channel):
        self._channels.discard(channel)
        self._fd_to_chan.clear()

    def _register(self, channel, conn):
        self._fd_to_chan[conn.fd] = channel
        self.poller.register(conn.fd, self.eventflags)

    def on_poll_start(self):
        for channel in self._channels:
            if channel.active_queues:
                self._register(channel, channel.client.connection)
            if channel.active_fanout_queues:
                self._register(channel, channel.fanout_client.connection)

    def handle_event(self, fileno, event):
        chan = self._fd_to_chan[fileno]
        return (chan.drain_events(), chan)

    def get(self, timeout=None):
        self.on_poll_start()

        events = self.poller.poll(timeout)
        for fileno, event in events or []:
            return self.handle_event(fileno, event)

        raise Empty()

    @property
    def fds(self):
        return self._fd_to_chan


class Client(object):
    SUPPORTED_SCHEMES = frozenset(['tcp', 'inproc', 'ipc'])

    def __init__(self, uri='tcp://127.0.0.1', port=DEFAULT_PORT, hwm=DEFAULT_HWM,
            swap_size=None, enable_sink=True, fanout=False, context=None):
        try:
            scheme, parts = uri.split('://')
        except ValueError:
            scheme = 'tcp'
            parts = uri
        endpoints = parts.split(';')

        if scheme not in self.SUPPORTED_SCHEMES:
            raise NotImplementedError('Unsupported scheme (%s). Supported schemes are: %s' %
                (scheme, ', '.join(self.SUPPORTED_SCHEMES)))

        # Use the passed in context or grab the global instance
        self.context = context or zmq.Context.instance()

        # Choose zmq socket type based on transport options
        if not fanout:
            if enable_sink:
                vent_socket_type = zmq.DEALER
            else:
                vent_socket_type = zmq.PUSH
        else:
            vent_socket_type = zmq.PUB

        # Create zmq_socket and set options
        self.vent = self.context.socket(vent_socket_type)
        self.vent.setsockopt(zmq.HWM, hwm)
        if swap_size:
            self.vent.setsockopt(zmq.SWAP, swap_size)

        self.enable_sink = enable_sink
        self.fanout = fanout
        self.scheme = scheme
        self.endpoints = endpoints
        self.port = port

        # Tell zmq_socket to attempt to connect to other sockets
        if scheme == 'tcp':
            for endpoint in endpoints:
                if ':' not in endpoint:
                    endpoint += ':' + str(DEFAULT_PORT)

                host, port = endpoint.split(':')

                endpoint = ''.join([scheme, '://', host, ':', str(int(port) + fanout)])

                self.connect(endpoint)
        elif scheme == 'inproc':
            self.connect(endpoints[0 + fanout])

    def connect(self, endpoint):
        self.vent.connect(endpoint)

    def subscribe(self, queue):
        self.sink.setsockopt(zmq.SUBSCRIBE, queue)

    def unsubscribe(self, queue):
        self.sink.setsockopt(zmq.UNSUBSCRIBE, queue)

    def get(self, timeout=None):
        if timeout:
            self.sink.poll(timeout=timeout)

        try:
            return self.sink.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.ZMQError, e:
            if e.errno == zmq.EAGAIN:
                raise socket.error(errno.EAGAIN, e.strerror)
            else:
                raise

    def put(self, queue, message, **kwargs):
        return self.vent.send_multipart([queue, message])

    def close(self):
        if not self.vent.closed:
            self.vent.close()

        try:
            sink = self.__dict__.pop('sink')
        except KeyError:
            pass
        else:
            if not sink.closed:
                sink.close()

    @property
    def connection(self):
        return self.sink

    @cached_property
    def sink(self):
        sink = self.vent

        if self.enable_sink:
            if self.fanout:
                sink = self.context.socket(zmq.SUB)

            if self.scheme == 'tcp':
                sink.bind('tcp://*:%s' % str(self.port + self.fanout))
            elif self.scheme == 'inproc':
                sink.bind('inproc://%s' % self.endpoints[0 + self.fanout])

        return sink


class Channel(virtual.Channel):
    Client = Client

    hwm = DEFAULT_HWM
    swap_size = None
    enable_sink = True
    port_incr = DEFAULT_INCR

    supports_fanout = True
    from_transport_options = (virtual.Channel.from_transport_options
                            + ('hwm',
                               'swap_size',
                               'enable_sink',
                               'port_incr'))

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)

        self.active_fanout_queues = set()
        self._fanout_queues = {}

        self.connection.cycle.add(self)
        self.connection_errors = self.connection.connection_errors

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == 'fanout':
            self._fanout_queues[queue] = exchange

    def _purge(self, queue):
        return 0

    def _delete(self, queue, exchange, *meta):
        if self.typeof(exchange).type == 'fanout':
            self._fanout_queues.pop(queue, None)

    def basic_consume(self, queue, *args, **kwargs):
        if queue in self._fanout_queues:
            self.fanout_client.subscribe(self._fanout_queues[queue])
            self.active_fanout_queues.add(queue)
        return super(Channel, self).basic_consume(queue, *args, **kwargs)

    def basic_cancel(self, consumer_tag):
        try:
            queue = self._tag_to_queue[consumer_tag]
        except KeyError:
            return
        if queue in self.active_fanout_queues:
            self.fanout_client.unsubscribe(queue)
            self.active_fanout_queues.discard(queue)
        return super(Channel, self).basic_cancel(consumer_tag)

    def _get(self, queue, timeout=None):
        try:
            if queue in self.active_queues:
                q, msg = self.client.get(timeout)
            elif queue in self.active_fanout_queues:
                q, msg = self.fanout_client.get(timeout)
        except socket.error, exc:
            if exc.errno == errno.EAGAIN and timeout != 0:
                raise Empty()
            else:
                raise
        else:
            return loads(msg)

    def _put(self, queue, message, **kwargs):
        self.client.put(queue, dumps(message), **kwargs)

    def _put_fanout(self, exchange, message, **kwargs):
        self.fanout_client.put(exchange, dumps(message))

    def _poll(self, cycle, timeout=None):
        return cycle.get(timeout=timeout)

    def close(self):
        if not self.closed:
            self.connection.cycle.discard(self)

        super(Channel, self).close()

        for attr in 'client', 'fanout_client':
            try:
                self.__dict__.pop(attr).close()
            except KeyError:
                pass

    def _create_client(self, fanout=False):
        conninfo = self.connection.client
        return self.Client(uri=conninfo.hostname or 'tcp://127.0.0.1',
                           port=(conninfo.port or DEFAULT_PORT) + ((self.channel_id - 1) * self.port_incr),
                           hwm=self.hwm,
                           swap_size=self.swap_size,
                           enable_sink=self.enable_sink,
                           fanout=fanout,
                           context=self.connection.context)

    @cached_property
    def client(self):
        return self._create_client()

    @cached_property
    def fanout_client(self):
        return self._create_client(fanout=True)

    @property
    def active_queues(self):
        return set(self._active_queues) - self.active_fanout_queues


class Transport(virtual.Transport):
    Channel = Channel

    default_port = DEFAULT_PORT
    driver_type = 'zeromq'
    driver_name = 'zmq'

    connection_errors = (zmq.ZMQError,)
    channel_errors = (zmq.ZMQError, StdChannelError,)

    polling_interval = None
    nb_keep_draining = True

    def __init__(self, *args, **kwargs):
        super(Transport, self).__init__(*args, **kwargs)

        self.cycle = MultiChannelPoller()

    def driver_version(self):
        return zmq.__version__

    def on_poll_init(self, poller):
        self.cycle.poller = poller

    def on_poll_start(self):
        cycle = self.cycle
        cycle.on_poll_start()
        return dict((fd, self.handle_event) for fd in cycle.fds)

    def handle_event(self, fileno, event):
        evt = self.cycle.handle_event(fileno, event)
        self._handle_event(evt)

    def drain_events(self, connection, timeout=None):
        more_to_read = False
        for channel in connection.channels:
            try:
                evt = channel.cycle.get(timeout=timeout)
            except socket.error, e:
                if e.errno == errno.EAGAIN:
                    continue
                raise
            else:
                connection._handle_event((evt, channel))
                more_to_read = True
        if not more_to_read:
            raise socket.error(errno.EAGAIN, os.strerror(errno.EAGAIN))

    def _handle_event(self, evt):
        item, channel = evt
        message, queue = item
        if not queue or queue not in self._callbacks:
            raise KeyError(
                "Received message for queue '%s' without consumers: %s" % (
                    queue, message))
        self._callbacks[queue](message)

    def establish_connection(self):
        self.context.closed
        return super(Transport, self).establish_connection()

    def close_connection(self, connection):
        super(Transport, self).close_connection(connection)
        try:
            connection.__dict__.pop('context').term()
        except KeyError:
            pass

    @cached_property
    def context(self):
        return zmq.Context(1)
