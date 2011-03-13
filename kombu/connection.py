"""
kombu.connection
================

Broker connection and pools.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import socket

from copy import copy
from itertools import count
from Queue import Empty, Queue as _Queue

from kombu import exceptions
from kombu.transport import get_transport_cls
from kombu.utils import retry_over_time
from kombu.utils.compat import OrderedDict
from kombu.utils.functional import wraps


#: Connection info -> URI
URI_FORMAT = """\
%(transport)s://%(userid)s@%(hostname)s%(port)s%(virtual_host)s\
"""


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
    :backend_extra_args: A dict of additional connection arguments to pass to
    alternate kombu channel implementations (useful for things like SQLAlchemy
    engine arguments)

    **Usage**

    Creating a connection::

        >>> conn = BrokerConnection("rabbit.example.com")

    The connection is established lazily when needed. If you need the
    connection to be established, then force it to do so using
    :meth:`connect`::

        >>> conn.connect()

    Remember to always close the connection::

        >>> conn.release()

    """
    URI_FORMAT = URI_FORMAT

    port = None
    virtual_host = "/"
    connect_timeout = 5

    _closed = None
    _connection = None
    _transport = None

    def __init__(self, hostname="localhost", userid="guest",
            password="guest", virtual_host="/", port=None, insist=False,
            ssl=False, transport=None, connect_timeout=5, backend_cls=None, backend_extra_args={}):
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
        self.backend_extra_args = backend_extra_args

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

    def _close(self):
        if self._connection:
            try:
                self.transport.close_connection(self._connection)
            except self.transport.connection_errors + (AttributeError,
                                                       socket.error):
                pass
            self._connection = None
        self._closed = True

    def release(self):
        """Close the connection (if open)."""
        self._close()
    close = release

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

    def ensure(self, obj, fun, errback=None, max_retries=None,
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
            >>> publish = conn.ensure(producer, producer.publish,
            ...                       errback=errback, max_retries=3)
            >>> publish(message, routing_key)

        """

        max_retries = max_retries or 0

        @wraps(fun)
        def _insured(*args, **kwargs):
            got_connection = 0
            for retries in count(0):
                try:
                    return fun(*args, **kwargs)
                except self.connection_errors + self.channel_errors, exc:
                    if got_connection or \
                            max_retries and retries > max_retries:
                        raise
                    errback and errback(exc, 0)
                    self._connection = None
                    self.close()
                    remaining_retries = max_retries and \
                                            max(max_retries - retries, 1)
                    self.ensure_connection(errback,
                                           remaining_retries,
                                           interval_start,
                                           interval_step,
                                           interval_max)
                    obj.revive(self.channel())
                    got_connection += 1

        _insured.func_name = _insured.__name__ = "%s(insured)" % fun.__name__
        return _insured

    def create_transport(self):
        return self.get_transport_cls()(client=self)
    create_backend = create_transport   # FIXME

    def get_transport_cls(self):
        """Get the currently used transport class."""
        transport_cls = self.transport_cls
        if not transport_cls or isinstance(transport_cls, basestring):
            transport_cls = get_transport_cls(transport_cls)
        return transport_cls

    def clone(self, **kwargs):
        """Create a copy of the connection with the same connection
        settings."""
        return self.__class__(**dict(self.info(), **kwargs))

    def info(self):
        """Get connection info."""
        transport_cls = self.transport_cls or "amqplib"
        port = self.port or self.transport.default_port
        return OrderedDict((("hostname", self.hostname),
                            ("userid", self.userid),
                            ("password", self.password),
                            ("virtual_host", self.virtual_host),
                            ("port", port),
                            ("insist", self.insist),
                            ("ssl", self.ssl),
                            ("transport", transport_cls),
                            ("connect_timeout", self.connect_timeout)))

    def as_uri(self):
        fields = self.info()
        port = fields["port"]
        if port:
            fields["port"] = ":%s" % (port, )
        vhost = fields["virtual_host"]
        if not vhost.startswith('/'):
            fields["virtual_host"] = '/' + vhost
        return self.URI_FORMAT % fields

    def Pool(self, limit=None, preload=None):
        """Pool of connections.

        See :class:`ConnectionPool`.

        :keyword limit: Maximum number of active connections.
          Default is no limit.
        :keyword preload: Number of connections to preload
          when the pool is created.  Default is 0.

        *Example usage*::

            >>> pool = connection.Pool(2)
            >>> c1 = pool.acquire()
            >>> c2 = pool.acquire()
            >>> c3 = pool.acquire()
            Traceback (most recent call last):
              File "<stdin>", line 1, in <module>
              File "kombu/connection.py", line 354, in acquire
              raise ConnectionLimitExceeded(self.limit)
                kombu.exceptions.ConnectionLimitExceeded: 2
            >>> c1.release()
            >>> c3 = pool.acquire()

        """
        return ConnectionPool(self, limit, preload)

    def ChannelPool(self, limit=None, preload=None):
        """Pool of channels.

        See :class:`ChannelPool`.

        :keyword limit: Maximum number of active channels.
          Default is no limit.
        :keyword preload: Number of channels to preload
          when the pool is created.  Default is 0.

        *Example usage*::

            >>> pool = connection.ChannelPool(2)
            >>> c1 = pool.acquire()
            >>> c2 = pool.acquire()
            >>> c3 = pool.acquire()
            Traceback (most recent call last):
              File "<stdin>", line 1, in <module>
              File "kombu/connection.py", line 354, in acquire
              raise ChannelLimitExceeded(self.limit)
                kombu.connection.ChannelLimitExceeded: 2
            >>> c1.release()
            >>> c3 = pool.acquire()

        """
        return ChannelPool(self, limit, preload)

    def SimpleQueue(self, name, no_ack=None, queue_opts=None,
            exchange_opts=None, channel=None, **kwargs):
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
        from kombu.simple import SimpleQueue

        channel_autoclose = False
        if channel is None:
            channel = self.channel()
            channel_autoclose = True
        return SimpleQueue(channel, name, no_ack, queue_opts, exchange_opts,
                           channel_autoclose=channel_autoclose, **kwargs)

    def SimpleBuffer(self, name, no_ack=None, queue_opts=None,
            exchange_opts=None, channel=None, **kwargs):
        """Create new :class:`~kombu.simple.SimpleQueue` using a channel
        from this connection.

        Same as :meth:`SimpleQueue`, but configured with buffering
        semantics. The resulting queue and exchange will not be durable, also
        auto delete is enabled. Messages will be transient (not persistent),
        and acknowledgements are disabled (``no_ack``).

        """
        from kombu.simple import SimpleBuffer

        channel_autoclose = False
        if channel is None:
            channel = self.channel()
            channel_autoclose = True
        return SimpleBuffer(channel, name, no_ack, queue_opts, exchange_opts,
                            channel_autoclose=channel_autoclose, **kwargs)

    def _establish_connection(self):
        return self.transport.establish_connection()

    def __repr__(self):
        """``x.__repr__() <==> repr(x)``"""
        return "<BrokerConnection: %s>" % self.as_uri()

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
        """The underlying connection object.

        .. warning::
            This instance is transport specific, so do not
            depend on the interface of this object.

        """
        if self._closed:
            return
        if not self._connection or not \
                self.transport.verify_connection(self._connection):
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


class Resource(object):

    def __init__(self, limit=None, preload=None):
        self.limit = limit
        self.preload = preload or 0

        self._resource = _Queue()
        self._dirty = set()
        self.setup()

    def setup(self):
        raise NotImplementedError("subclass responsibilty")

    def acquire(self, block=False, timeout=None):
        """Acquire resource.

        :keyword block: If the limit is exceeded,
          block until there is an available item.
        :keyword timeout: Timeout to wait
          if ``block`` is true. Default is :const:`None` (forever).

        :raises LimitExceeded: if block is false
          and the limit has been exceeded.

        """
        while True:
            try:
                resource = self._resource.get(block=block, timeout=timeout)
            except Empty:
                if self.limit and len(self._dirty) >= self.limit:
                    raise self.LimitExceeded(self.limit)
                # All taken, put new on the queue and
                # try get again, this way the first in line
                # will get the resource.
                self._resource.put_nowait(self.new())
            else:
                resource = self.prepare(resource)
                self._dirty.add(resource)

                @wraps(self.release)
                def _release():
                    self.release(resource)
                resource.release = _release

                return resource

    def prepare(self, resource):
        return resource

    def close_resource(self, resource):
        resource.close()

    def release(self, resource):
        """Release resource so it can be used by another thread.

        The caller is responsible for discarding the object,
        and to never use the resource again.  A new resource must
        be acquired if so needed.

        """
        self._dirty.discard(resource)
        self._resource.put_nowait(resource)

    def force_close_all(self):
        """Closes and removes all resources in the pool (also those in use).

        Can be used to close resources from parent processes
        after fork (e.g. sockets/connections).

        """
        dirty = self._dirty
        resource = self._resource
        while 1:
            try:
                dres = dirty.pop()
            except KeyError:
                break
            self.close_resource(dres)

        resource.mutex.acquire()
        try:
            while 1:
                try:
                    res = resource.queue.pop()
                except IndexError:
                    break
                self.close_resource(res)
        finally:
            resource.mutex.release()


class PoolChannelContext(object):

    def __init__(self, pool, block=False):
        self.pool = pool
        self.block = block

    def __enter__(self):
        self.conn = self.pool.acquire(block=self.block)
        self.chan = self.conn.channel()
        return self.conn, self.chan

    def __exit__(self, *exc_info):
        self.chan.close()
        self.conn.release()


class ConnectionPool(Resource):
    LimitExceeded = exceptions.ConnectionLimitExceeded

    def __init__(self, connection, limit=None, preload=None):
        self.connection = connection
        super(ConnectionPool, self).__init__(limit=limit,
                                             preload=preload)

    def new(self):
        return copy(self.connection)

    def acquire_channel(self, block=False):
        return PoolChannelContext(self, block)

    def setup(self):
        if self.limit:
            for i in xrange(self.limit):
                conn = self.new()
                if i < self.preload:
                    conn.connect()
                self._resource.put_nowait(conn)

    def prepare(self, resource):
        resource.connect()
        return resource


class ChannelPool(Resource):
    LimitExceeded = exceptions.ChannelLimitExceeded

    def __init__(self, connection, limit=None, preload=None):
        self.connection = connection
        super(ChannelPool, self).__init__(limit=limit,
                                          preload=preload)

    def new(self):
        return self.connection.channel

    def setup(self):
        channel = self.new()
        for i in xrange(self.limit):
            self._resource.put_nowait(
                    i < self.preload and channel() or channel)

    def prepare(self, channel):
        if callable(channel):
            channel = channel()

        return channel
