import socket
import threading

from collections import deque
from copy import copy
from functools import wraps

from kombu import exceptions
from kombu.backends import get_backend_cls
from kombu.utils import retry_over_time, OrderedDict





class BrokerConnection(object):
    port = None
    virtual_host = "/"
    connect_timeout = 5

    _closed = None
    _connection = None
    _backend = None

    def __init__(self, hostname="localhost", userid="guest",
            password="guest", virtual_host="/", port=None, insist=False,
            ssl=False, backend_cls=None, connect_timeout=5, pool=None):
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = insist
        self.connect_timeout = connect_timeout or self.connect_timeout
        self.ssl = ssl
        self.backend_cls = backend_cls
        self.pool = pool

    def connect(self):
        """Establish a connection to the AMQP server."""
        self._closed = False
        return self.connection

    def channel(self):
        """Request a new AMQP channel."""
        return self.backend.create_channel(self.connection)

    def drain_events(self, **kwargs):
        return self.backend.drain_events(self.connection, **kwargs)

    def close(self):
        """Close the currently open connection."""
        try:
            if self._connection:
                self.backend.close_connection(self._connection)
                self._connection = None
        except socket.error:
            pass
        self._closed = True

    def ensure_connection(self, errback=None, max_retries=None,
            interval_start=2, interval_step=2, interval_max=30):
        retry_over_time(self.connect, self.connection_errors, (), {},
                        errback, max_retries,
                        interval_start, interval_step, interval_max)
        return self

    def ensure(self, fun, errback=None, max_retries=None,
            interval_start=2, interval_step=2, interval_max=30):

        @wraps(fun)
        def _insured(*args, **kwargs):
            for retries in count(0):
                if max_retries and retries >= max_retries:
                    raise exceptions.EnsureExhausted()
                try:
                    return fun(*args, **kwargs)
                except self.connection_errors + self.channel_errors, exc:
                    errback and errback(exc, 0)
                    self.close()
                    self.ensure_connection(errback,
                                           max_retries - retries,
                                           interval_start,
                                           interval_step,
                                           interval_max)
        _insured.func_name = _insured.__name__ = "%s(insured)" % fun.__name__
        return _insured

    def acquire(self):
        return self

    def release(self):
        if self.pool:
            self.pool.release(self)
        else:
            self.close()

    def create_backend(self):
        return self.get_backend_cls()(client=self)

    def clone(self, **kwargs):
        return self.__class__(**dict(self.info(), **kwargs))

    def get_backend_cls(self):
        """Get the currently used backend class."""
        backend_cls = self.backend_cls
        if not backend_cls or isinstance(backend_cls, basestring):
            backend_cls = get_backend_cls(backend_cls)
        return backend_cls

    def info(self):
        return OrderedDict((("hostname", self.hostname),
                            ("userid", self.userid),
                            ("password", self.password),
                            ("virtual_host", self.virtual_host),
                            ("port", self.port),
                            ("insist", self.insist),
                            ("ssl", self.ssl),
                            ("backend_cls", self.backend_cls),
                            ("connect_timeout", self.connect_timeout),
                            ("pool", self.pool)))

    def _establish_connection(self):
        return self.backend.establish_connection()

    def __repr__(self):
        info = self.info()
        return "<BrokerConnection: %s>" % (
                    ", ".join("%s=%r" % (item, info[item])
                                for item in info.keys()[:8]))

    def __copy__(self):
        return self.clone()

    def __reduce__(self):
        return (self.__class__, tuple(self.info().values()), None)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.release()

    @property
    def connection(self):
        if self._closed:
            return
        if not self._connection:
            self._connection = self._establish_connection()
            self._closed = False
        return self._connection

    @property
    def host(self):
        """The host as a hostname/port pair separated by colon."""
        return ":".join([self.hostname, str(self.port)])

    @property
    def backend(self):
        if self._backend is None:
            self._backend = self.create_backend()
        return self._backend

    @property
    def connection_errors(self):
        return self.backend.connection_errors

    @property
    def channel_errors(self):
        return self.backend.channel_errors



class BrokerConnectionPool(object):
    _t = None

    def __init__(self, initial, min=2, max=10, ensure=False, preconnect=False):
        self.initial = initial
        self.min = min
        self.max = max
        self.preconnect = preconnect
        self._t = threading.local()
        self._t.connections = deque()
        self._t.dirty = set()

        self.grow(self.min)
        if self.preconnect:
            for connection in self._connections:
                if self.ensure:
                    connection.ensure_connection()
                else:
                    connection.connect()


    def grow(self, n=1):
        for _ in xrange(n):
            if self.total >= self.max:
                raise exceptions.PoolLimitExceeded(
                        "Can't add more connections to pool.")
            connection = self.initial.clone(pool=self)
            self._connections.append(connection)

    def acquire(self):
        try:
            connection = self._connections.popleft()
        except IndexError:
            raise exceptions.PoolExhausted("All connections acquired")
        self._dirty.add(connection)
        return connection

    def release(self, connection):
        try:
            self._dirty.remove(connection)
        except KeyError:
            pass
        self._connections.append(connection)

    def replace(self, connection):
        try:
            self._dirty.remove(connection)
            self._connections.remove(connection)
        except (KeyError, ValueError):
            pass
        self.grow(1)

    def ensure(self, fun, errback=None, max_retries=None,
            interval_start=2, interval_step=2, interval_max=30):

        @wraps(fun)
        def _insured(*args, **kwargs):
            conn = self.acquire()
            try:
                return conn.ensure(fun, errback, max_retries,
                                   interval_start,
                                   interval_step,
                                   interval_max)(*args, **kwargs)
            finally:
                conn.release()

        return insured

    def __repr__(self):
        info = self.initial.info()
        return "<BrokerConnectionPool(%s): %s>" % (
                    self.max,
                    ", ".join("%s=%r" % (item, info[item])
                                for item in info.keys()[:8]))

    @property
    def active(self):
        return len(self._dirty)

    @property
    def total(self):
        return self.active + len(self._connections)

    @property
    def _dirty(self):
        return self._t.dirty

    @property
    def _connections(self):
        return self._t.connections
