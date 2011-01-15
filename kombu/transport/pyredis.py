"""
kombu.transport.pyredis
=======================

Redis transport.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import select

from threading import Condition, Event, Lock, Thread
from itertools import imap
from Queue import Empty, Queue as _Queue

from anyjson import serialize, deserialize

from kombu.transport import virtual
from kombu.utils.finalize import Finalize

DEFAULT_PORT = 6379
DEFAULT_DB = 0

POLL_READ = 0x001
POLL_ERR = 0x008 | 0x010 | 0x2000


class _kqueue(object):

    def __init__(self):
        self._kqueue = select.kqueue()
        self._active = {}

    def register(self, fd, events):
        self._control(fd, events, select.KQ_EV_ADD)
        self._active[fd] = events

    def unregister(self, fd):
        events = self._active.pop(fd)
        self._control(fd, events, select.KQ_EV_DELETE)

    def _control(self, fd, events, flags):
        self._kqueue.control([select.kevent(fd, filter=select.KQ_FILTER_READ,
                                                flags=flags)], 0)

    def poll(self, timeout):
        kevents = self._kqueue.control(None, 1000, timeout)
        events = {}
        for kevent in kevents:
            fd = kevent.ident
            flags = 0
            if kevent.filter == select.KQ_FILTER_READ:
                events[fd] = events.get(fd, 0) | POLL_READ
            if kevent.filter == select.KQ_EV_ERROR:
                events[fd] = events.get(fd, 0) | POLL_ERR
        return events.items()


class _select(object):

    def __init__(self):
        self.read_fds = set()
        self.error_fds = set()
        self.fd_sets = (self.read_fds, self.error_fds)

    def register(self, fd, events):
        if events & POLL_READ:
            self.read_fds.add(fd)
        if events & POLL_ERR:
            self.error_fds.add(fd)
            self.read_fds.add(fd)

    def unregister(self, fd):
        self.read_fds.discard(fd)
        self.error_fds.discard(fd)

    def poll(self, timeout):
        read, _write, error = select.select(self.read_fds, [],
                                             self.error_fds, timeout)
        events = {}
        for fd in read:
            fd = fd.fileno()
            events[fd] = events.get(fd, 0) | POLL_READ
        for fd in error:
            fd = fd.fileno()
            events[fd] = events.get(fd, 0) | POLL_ERR
        return events.items()


if hasattr(select, "epoll"):
    # Py2.6+ Linux
    _poll = select.epoll
elif hasattr(select, "kqueue"):
    # Py2.6+ on BSD / Darwin
    _poll = _kqueue
else:
    _poll = _select


class ChannelPoller(Thread):

    # Method used to drain_events
    drain_events = None

    #: Queue to put inbound message onto.
    inbound = None

    #: Condition primitive used to notify poll requests.
    poll_request = None

    #: Event set when the thread should shut down.
    shutdown = None

    def __init__(self, drain_events):
        self.inbound = _Queue()
        self.mutex = Lock()
        self.poll_request = Condition(self.mutex)
        self.shutdown = Event()
        self.stopped = Event()
        self._on_collect = Finalize(self, self.close)
        Thread.__init__(self)
        self.setDaemon(False)
        self.started = False

    def poll(self):
        # start thread on demand.
        self.ensure_started()

        # notify the thread that a poll request has been initiated.
        self.poll_request.acquire()
        try:
            self.poll_request.notify()
        finally:
            self.poll_request.release()

        # the thread should start to poll now, so just wait
        # for it to put a message onto the inbound queue.
        return self.inbound.get(timeout=0.3)

    def _can_start(self):
        return not (self.started or
                    self.isAlive() or
                    self.shutdown.isSet() or
                    self.stopped.isSet())

    def ensure_started(self):
        if self._can_start():
            self.started = True
            self.start()

    def close(self):
        self.shutdown.set()
        if self.isAlive():
            self.join()

    def run(self):  # pragma: no cover
        inbound = self.inbound
        shutdown = self.shutdown
        drain_events = self.drain_events
        poll_request = self.poll_request

        while 1:
            if shutdown.isSet():
                break

            try:
                item = drain_events(timeout=1)
            except Empty:
                pass
            else:
                inbound.put_nowait(item)

            if shutdown.isSet():
                break

            # Wait for next poll request
            #
            # Timeout needs to be short here, otherwise it will block
            # shutdown.  This means that polling will continue even when
            # there are no actual calls to `poll`.  However, this doesn't
            # cause any problems for our current use (especially with
            # the active QoS manager).
            poll_request.acquire()
            try:
                poll_request.wait(1)
            finally:
                poll_request.release()

        self.stopped.set()


class pollChannelPoller(ChannelPoller):
    eventflags = POLL_READ | POLL_ERR

    def __init__(self):
        self._channels = set()
        self._fd_to_chan = {}
        self._chan_to_sock = {}
        self._poller = _poll()
        super(pollChannelPoller, self).__init__(self.drain_events)

    def add(self, channel):
        if channel not in self._channels:
            self._channels.add(channel)

    def _register(self, channel):
        if channel in self._chan_to_sock:
            self._unregister(channel)
        if channel.client.connection._sock is None:
            channel.client.connection.connect(channel.client)
        sock = channel.client.connection._sock
        sock.setblocking(0)
        self._fd_to_chan[sock.fileno()] = channel
        self._chan_to_sock[channel] = sock
        self._poller.register(sock, self.eventflags)

    def _unregister(self, channel):
        self._poller.unregister(self._chan_to_sock[channel])

    def drain_events(self, timeout=None):
        fdmap = self._fd_to_chan
        for chan in self._channels:
            # connection lost
            if chan.client.connection._sock is None:
                chan._in_poll = False
                self._register(chan)
            # start brpop command
            if not chan._in_poll:
                chan._brpop_start(chan._active_queues)
            assert chan.client.connection._sock

        for fileno, event in self._poller.poll(timeout * 1000):
            if event & (select.POLLIN | select.POLLPRI):
                chan = fdmap[fileno]
                return chan._brpop_read()
            elif event & (select.POLLHUP | select.POLLERR):
                chan = fdmap[fileno]
                return chan._brpop_read_error()
        raise Empty()

class Channel(virtual.Channel):
    _client = None
    supports_fanout = True
    keyprefix_fanout = "_kombu.fanout.%s"
    keyprefix_queue = "_kombu.binding.%s"
    sep = '\x06\x16'
    _in_poll = False
    _poller = pollChannelPoller()

    def __init__(self, *args, **kwargs):
        super_ = super(Channel, self)
        super_.__init__(*args, **kwargs)

        #self._poller = ChannelPoller(super_.drain_events)
        self.Client = self._get_client()
        self._poller.add(self)
        self.ResponseError = self._get_response_error()

    def _get_client(self):
        from redis import Redis
        return Redis

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

    def drain_events(self, timeout=None):
        return self._poller.poll()

    def _brpop_start(self, keys, timeout=0):
        self._in_poll = True
        timeout = timeout or 0
        if isinstance(keys, basestring):
            keys = [keys, timeout]
        else:
            keys = list(keys) + [timeout]
        name, cmd = self._encode_command("BRPOP", *keys)
        self.client.connection.send(cmd, self)

    def _encode_command(self, *args):
        encode = self.client.encode
        cmds = "".join('$%s\r\n%s\r\n' % (len(enc_value), enc_value)
                for enc_value in imap(encode, args))
        return args[0], '*%s\r\n%s' % (len(args), cmds)

    def _brpop_read(self, **options):
        try:
            dest__item = self.client.parse_response("BRPOP", **options)
            dest, item = dest__item
            return deserialize(item), dest
        finally:
            self._in_poll = False

    def _brpop_read_error(self, **options):
        self.client.parse_response("BRPOP")

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        self.client.sadd(self.keyprefix_queue % (exchange, ),
                         self.sep.join([routing_key or "",
                                        pattern or "",
                                        queue or ""]))

    def get_table(self, exchange):
        members = self.client.smembers(self.keyprefix_queue % (exchange, ))
        return [tuple(val.split(self.sep)) for val in members]

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

    def _purge(self, queue):
        size = self.client.llen(queue)
        self.client.delete(queue)
        return size

    def close(self):
        self._poller.close()
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


class Transport(virtual.Transport):
    Channel = Channel

    interval = 1
    default_port = DEFAULT_PORT

    def __init__(self, *args, **kwargs):
        self.connection_errors, self.channel_errors = self._get_errors()
        super(Transport, self).__init__(*args, **kwargs)

    def _get_errors(self):
        from redis import exceptions
        return ((exceptions.ConnectionError,
                 exceptions.AuthenticationError),
                (exceptions.ConnectionError,
                 exceptions.InvalidData,
                 exceptions.InvalidResponse,
                 exceptions.ResponseError))
