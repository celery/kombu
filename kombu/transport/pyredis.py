"""
kombu.transport.pyredis
=======================

Redis transport.

:copyright: (c) 2009 - 2010 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from threading import Condition, Event, Lock, Thread
from Queue import Empty, Queue as _Queue

from anyjson import serialize, deserialize

from kombu.transport import virtual

DEFAULT_PORT = 6379
DEFAULT_DB = 0


class ChannelPoller(Thread):

    #: The method used to poll for new messages.
    drain_events = None

    #: Queue to put inbound messages onto.
    inbound = None

    #: Condition primitive used to notify poll requests.
    poll_request = None

    #: Event set when the thread should shut down.
    shutdown = None

    def __init__(self, drain_events):
        self.drain_events = drain_events
        self.inbound = _Queue()
        self.mutex = Lock()
        self.poll_request = Condition(self.mutex)
        self.shutdown = Event()
        self.stopped = Event()
        Thread.__init__(self)
        self.setDaemon(False)
        self.started = False

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


class Channel(virtual.Channel):
    _client = None
    supports_fanout = True
    keyprefix_fanout = "_kombu.fanout.%s"
    keyprefix_queue = "_kombu.binding.%s"
    sep = '\x06\x16'

    def __init__(self, *args, **kwargs):
        super_ = super(Channel, self)
        super_.__init__(*args, **kwargs)

        self._poller = ChannelPoller(super_.drain_events)
        self.Client = self._get_client()
        self.ResponseError = self._get_response_error()

    def _get_client(self):
        from redis import Redis
        return Redis

    def _get_response_error(self):
        from redis import exceptions
        return exceptions.ResponseError

    def drain_events(self, timeout=None):
        return self._poller.poll()

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
        super(Channel, self).close()
        try:
            self.client.bgsave()
        except self.ResponseError:
            pass

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
