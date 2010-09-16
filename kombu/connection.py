import socket
import threading

from collections import deque
from copy import copy
from itertools import count
from time import time

from kombu import exceptions
from kombu.transport import get_transport_cls
from kombu.simple import SimpleQueue
from kombu.utils import retry_over_time, OrderedDict
from kombu.utils.functional import wraps


class BrokerConnection(object):
    """A connection to the broker.

    :keyword hostname: Hostname/address of the server to connect to.
      Default is ``"localhost"``.

    :keyword userid: Username. Default is ``"guest"``.

    :keyword password: Password. Default is ``"guest"``.

    :keyword virtual_host: Virtual host. Default is ``"/"``.

    :keyword port: Port of the server. Default is transport specific.

    :keyword insist: Insist on connecting to a server.
      In a configuration with multiple load-sharing servers, the insist
      option tells the server that the client is insisting on a connection
      to the specified server.  Default is ``False``.

    :keyword ssl: Use ssl to connect to the server. Default is ``False``.

    :keyword transport: Transport class to use. Can be a class,
         or a string specifying the path to the class. (e.g.
         ``kombu.transport.pyamqplib.Transport``), or one of the aliases:
         ``amqplib``, ``pika``, ``redis``, ``memory``.

    :keyword connect_timeout: Timeout in seconds for connecting to the
      server. May not be suported by the specified transport.


    **Usage**

    Creating a connection::

        >>> conn = BrokerConnection("rabbit.example.com")

    The connection is established lazily when needed. If you need the
    connection to be established, then do so expliclty using :meth:`connect`::

        >>> conn.connect()

    Remember to always close the connection::

        >>> conn.release()

    """
    port = None
    virtual_host = "/"
    connect_timeout = 5

    _closed = None
    _connection = None
    _transport = None

    def __init__(self, hostname="localhost", userid="guest",
            password="guest", virtual_host="/", port=None, insist=False,
            ssl=False, transport=None, connect_timeout=5, pool=None,
            backend_cls=None):
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = insist
        self.connect_timeout = connect_timeout or self.connect_timeout
        self.ssl = ssl
        # backend_cls argument will be removed shortly.
        self.transport_cls = transport or backend_cls
        self.pool = pool

    def connect(self):
        """Establish connection to server immediately."""
        self._closed = False
        return self.connection

    def channel(self):
        """Request a new channel."""
        return self.transport.create_channel(self.connection)

    def drain_events(self, **kwargs):
        """Wait for a single event from the server.

        :keyword timeout: Timeout in seconds before we give up.
            Raises :exc:`socket.timeout` if the timeout is execeded.

        Usually used from an event loop.

        """
        return self.transport.drain_events(self.connection, **kwargs)

    def close(self):
        """Close the connection (if open)."""
        try:
            if self._connection:
                self.transport.close_connection(self._connection)
                self._connection = None
        except socket.error:
            pass
        self._closed = True

    def ensure_connection(self, errback=None, max_retries=None,
            interval_start=2, interval_step=2, interval_max=30):
        """Ensure we have a connection to the server.

        If not retry establishing the connection with the settings
        specified.

        :keyword errback: Optional callback called each time the connection
          can't be established. Arguments provided are the exception
          raised and the interval that will be slept ``(exc, interval)``.

        :keyword max_retries: Maximum number of times to retry.
          If this limit is exceeded the connection error will be re-raised.

        :keyword interval_start: The number of seconds we start sleeping for.
        :keyword interval_step: How many seconds added to the interval
          for each retry.
        :keyword interval_max: Maximum number of seconds to sleep between
          each retry.

        """
        retry_over_time(self.connect, self.connection_errors, (), {},
                        errback, max_retries,
                        interval_start, interval_step, interval_max)
        return self

    def ensure(self, fun, errback=None, max_retries=None,
            interval_start=1, interval_step=1, interval_max=1):
        """Ensure operation completes, regardless of any channel/connection
        errors occuring.

        Will retry by establishing the connection, and reapplying
        the function.

        :param fun: Method to apply.

        :keyword errback: Optional callback called each time the connection
          can't be established. Arguments provided are the exception
          raised and the interval that will be slept ``(exc, interval)``.

        :keyword max_retries: Maximum number of times to retry.
          If this limit is exceeded the connection error will be re-raised.

        :keyword interval_start: The number of seconds we start sleeping for.
        :keyword interval_step: How many seconds added to the interval
          for each retry.
        :keyword interval_max: Maximum number of seconds to sleep between
          each retry.

        **Example**

        This is an example ensuring a publish operation::

            >>> def errback(exc, interval):
            ...     print("Couldn't publish message: %r. Retry in %ds" % (
            ...             exc, interval))
            >>> publish = conn.ensure(producer.publish,
            ...                       errback=errback, max_retries=3)
            >>> publish(message, routing_key)

        """

        max_retries = max_retries or 0

        @wraps(fun)
        def _insured(*args, **kwargs):
            for ret in count(0):
                if max_retries and retries >= max_retries:
                    raise exceptions.EnsureExhausted()
                try:
                    return fun(*args, **kwargs)
                except self.connection_errors + self.channel_errors, exc:
                    errback and errback(exc, 0)
                    self.connection.connection = None
                    self.close()
                    self.ensure_connection(errback,
                                           max(max_retries - retries, 1),
                                           interval_start,
                                           interval_step,
                                           interval_max)

        _insured.func_name = _insured.__name__ = "%s(insured)" % fun.__name__
        return _insured

    def acquire(self):
        """Acquire connection.

        Only here for API compatibility with :class:`BrokerConnectionPool`.

        """
        return self

    def release(self):
        """Close the connection, or if the connection is managed by a pool
        the connection will be released to the pool so it can be reused.

        **NOTE:** You must never perform operations on a connection
        that has been released.

        """
        if self.pool:
            self.pool.release(self)
        else:
            self.close()

    def create_transport(self):
        return self.get_transport_cls()(client=self)
    create_backend = create_transport # FIXME

    def clone(self, **kwargs):
        """Create a copy of the connection with the same connection
        settings."""
        return self.__class__(**dict(self.info(), **kwargs))

    def get_transport_cls(self):
        """Get the currently used transport class."""
        transport_cls = self.transport_cls
        if not transport_cls or isinstance(transport_cls, basestring):
            transport_cls = get_transport_cls(transport_cls)
        return transport_cls

    def info(self):
        """Get connection info."""
        return OrderedDict((("hostname", self.hostname),
                            ("userid", self.userid),
                            ("password", self.password),
                            ("virtual_host", self.virtual_host),
                            ("port", self.port),
                            ("insist", self.insist),
                            ("ssl", self.ssl),
                            ("transport_cls", self.transport_cls),
                            ("connect_timeout", self.connect_timeout),
                            ("pool", self.pool)))

    def SimpleQueue(self, name, no_ack=False, queue_opts=None,
            exchange_opts=None, channel=None):
        """Create new :class:`~kombu.simple.SimpleQueue`, using a channel
        from this connection.

        If ``name`` is a string, a queue and exchange will be automatically
        created using that name as the name of the queue and exchange,
        also it will be used as the default routing key.

        :param name: Name of the queue/or a :class:`~kombu.entity.Queue`.
        :keyword no_ack: Disable acknowledgements. Default is false.
        :keyword queue_opts: Additional keyword arguments passed to the
          constructor of the automatically created
          :class:`~kombu.entity.Queue`.
        :keyword exchange_opts: Additional keyword arguments passed to the
          constructor of the automatically created
          :class:`~kombu.entity.Exchange`.
        :keyword channel: Channel to use. If not specified a new channel
           from the current connection will be used. Remember to call
           :meth:`~kombu.simple.SimpleQueue.close` when done with the
           object.

        """
        channel_autoclose = False
        if channel is None:
            channel = self.channel()
            channel_autoclose = True
        return SimpleQueue(channel, name, no_ack, queue_opts, exchange_opts,
                           channel_autoclose=channel_autoclose)

    def SimpleBuffer(self, name, no_ack=False, queue_opts=None,
            exchange_opts=None, channel=None):
        """Create new :class:`~kombu.simple.SimpleQueue` using a channel
        from this connection.

        Same as :meth:`SimpleQueue`, but configured with buffering
        semantics. The resulting queue and exchange will not be durable, also
        auto delete is enabled. Messages will be transient (not persistent),
        and acknowledgements are disabled (``no_ack``).

        """
        channel_autoclose = False
        if channel is None:
            channel = self.channel()
            channel_autoclose = True
        return SimpleBuffer(channel, name, no_ack, queue_opts, exchange_opts,
                            channel_autoclose=channel_autoclose)

    def _establish_connection(self):
        return self.transport.establish_connection()

    def __repr__(self):
        """``x.__repr__() <==> repr(x)``"""
        info = self.info()
        return "<BrokerConnection: %s>" % (
                    ", ".join("%s=%r" % (item, info[item])
                                for item in info.keys()[:8]))
    def __copy__(self):
        """``x.__copy__() <==> copy(x)``"""
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
    def transport(self):
        if self._transport is None:
            self._transport = self.create_transport()
        return self._transport

    @property
    def connection_errors(self):
        """List of exceptions that may be raised by the connection."""
        return self.transport.connection_errors

    @property
    def channel_errors(self):
        """List of exceptions that may be raised by the channel."""
        return self.transport.channel_errors


class BrokerConnectionPool(object):
    """Pool of connections.

    :param initial: Initial :class:`BrokerConnection` to take connection
      parameters from.

    :keyword max: Maximum number of connections in the pool.
      Default is 10.

    :keyword ensure: When ``preconnect`` on, ensure we're able to establish
      a connection. Default is ``False``.

    :keyword preconnect: Number of connections at
      instantiation. Default is to only establish connections when needed.

    """

    def __init__(self, initial, max=10, ensure=False, preconnect=0):
        self.initial = initial
        self.max = max
        self.preconnect = preconnect
        self._connections = deque()
        self._dirty = set()
        self.mutex = threading.Lock()
        self.not_empty = threading.Condition(self.mutex)

        self.grow(self.preconnect, connect=True)
        self.grow(self.max - self.preconnect)

    def acquire(self, block=True, timeout=None):
        """Acquire connection.

        :raises kombu.exceptions.PoolExhausted: If there are no
          available connections to be acquired.

        """
        self.not_empty.acquire()
        time_start = time()
        try:
            while 1:
                try:
                    connection = self._connections.popleft()
                    self._dirty.add(connection)
                    return connection
                except IndexError:
                    if not block:
                        raise exceptions.PoolExhausted(
                                "All connections acquired")
                    if timeout:
                        elapsed = time() - time_start
                        remaining = timeout - elapsed
                        if elapsed > timeout:
                            raise exceptions.TimeoutError(
                                "Timed out while acquiring connection.")
                        self.not_empty.wait(remaining)
        finally:
            self.not_empty.release()

    def release(self, connection):
        """Release connection so it can be used by others.

        **NOTE:** You must never perform operations on a connection
        that has been released.

        """
        self.mutex.acquire()
        try:
            try:
                self._dirty.remove(connection)
            except KeyError:
                pass
            self._connections.append(connection)
            self.not_empty.notify()
        finally:
            self.mutex.release()

    def replace(self, connection):
        """Clone and replace connection with a new one.

        This is useful if the connection is broken.

        """
        connection.close()
        self.mutex.acquire()
        try:
            try:
                self._dirty.remove(connection)
                self._connections.remove(connection)
            except (KeyError, ValueError):
                pass
        finally:
            self.mutex.release()
        self.grow(1)

    def ensure(self, fun, errback=None, max_retries=None,
            interval_start=2, interval_step=2, interval_max=30):
        """See :meth:`BrokerConnection.ensure`."""

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

    def grow(self, n, connect=False):
        """Add ``n`` more connections to the pool.

        :keyword connect: Establish connections imemediately.
          By default connections are only established when needed.

        :raises kombu.exceptions.PoolLimitExceeded: If there are already
          more than :attr:`max` number of connections in the pool.

        """
        self.mutex.acquire()
        try:
            for _ in xrange(n):
                if self.total >= self.max:
                    raise exceptions.PoolLimitExceeded(
                            "Can't add more connections to the pool.")
                connection = self.initial.clone(pool=self)
                connect and self._establish_connection(connection)
                self._connections.append(connection)
                self.not_empty.notify()
        finally:
            self.mutex.release()

    def close(self):
        """Close all connections."""
        while self._connections:
            self._connections.popleft().close()
        while self._dirty:
            self._dirty.pop().close()

    def _establish_connection(self, connection):
        if self.ensure:
            return connection.ensure_connection()
        return connection.connect()

    def __repr__(self):
        """``x.__repr__() <==> repr(x)``"""
        info = self.initial.info()
        return "<BrokerConnectionPool(%s): %s>" % (
                    self.max,
                    ", ".join("%s=%r" % (item, info[item])
                                for item in info.keys()[:8]))

    @property
    def active(self):
        """Number of acquired connections."""
        return len(self._dirty)

    @property
    def total(self):
        """Current total number of connections"""
        return self.active + len(self._connections)
