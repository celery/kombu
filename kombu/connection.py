"""
kombu.connection
================

Broker connection and pools.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import os
import sys
import socket

from contextlib import contextmanager
from copy import copy
from functools import partial, wraps
from itertools import count
from urllib import quote
from Queue import Empty
# jython breaks on relative import for .exceptions for some reason
# (Issue #112)
from kombu import exceptions
from .transport import get_transport_cls
from .utils import cached_property, retry_over_time
from .utils.compat import OrderedDict, LifoQueue as _LifoQueue
from .utils.url import parse_url

_LOG_CONNECTION = os.environ.get("KOMBU_LOG_CONNECTION", False)
_LOG_CHANNEL = os.environ.get("KOMBU_LOG_CHANNEL", False)

__all__ = ["parse_url", "BrokerConnection", "Resource",
           "ConnectionPool", "ChannelPool"]


class BrokerConnection(object):
    """A connection to the broker.

    :param URL:  Connection URL.

    :keyword hostname: Default host name/address if not provided in the URL.
    :keyword userid: Default user name if not provided in the URL.
    :keyword password: Default password if not provided in the URL.
    :keyword virtual_host: Default virtual host if not provided in the URL.
    :keyword port: Default port if not provided in the URL.
    :keyword ssl: Use SSL to connect to the server. Default is ``False``.
      May not be supported by the specified transport.
    :keyword transport: Default transport if not specified in the URL.
    :keyword connect_timeout: Timeout in seconds for connecting to the
      server. May not be supported by the specified transport.
    :keyword transport_options: A dict of additional connection arguments to
      pass to alternate kombu channel implementations.  Consult the transport
      documentation for available options.
    :keyword insist: *Deprecated*

    .. note::

        The connection is established lazily when needed. If you need the
        connection to be established, then force it to do so using
        :meth:`connect`::

            >>> conn.connect()

        Remember to always close the connection::

            >>> conn.release()

    """
    port = None
    virtual_host = "/"
    connect_timeout = 5

    _closed = None
    _connection = None
    _default_channel = None
    _transport = None
    _logger = None
    uri_passthrough = set(["sqla", "sqlalchemy"])
    uri_prefix = None

    #: The cache of declared entities is per connection,
    #: in case the server loses data.
    declared_entities = None

    def __init__(self, hostname="localhost", userid=None,
            password=None, virtual_host=None, port=None, insist=False,
            ssl=False, transport=None, connect_timeout=5,
            transport_options=None, login_method=None, uri_prefix=None,
            **kwargs):
        # have to spell the args out, just to get nice docstrings :(
        params = {"hostname": hostname, "userid": userid,
                  "password": password, "virtual_host": virtual_host,
                  "port": port, "insist": insist, "ssl": ssl,
                  "transport": transport, "connect_timeout": connect_timeout,
                  "login_method": login_method}
        if hostname and "://" in hostname \
                and transport not in self.uri_passthrough:
            if '+' in hostname[:hostname.index("://")]:
                # e.g. sqla+mysql://root:masterkey@localhost/
                params["transport"], params["hostname"] = hostname.split('+')
                self.uri_prefix = params["transport"]
            else:
                params.update(parse_url(hostname))
        self._init_params(**params)

        # backend_cls argument will be removed shortly.
        self.transport_cls = self.transport_cls or kwargs.get("backend_cls")

        if transport_options is None:
            transport_options = {}
        self.transport_options = transport_options

        if _LOG_CONNECTION:  # pragma: no cover
            from .log import get_logger
            self._logger = get_logger("kombu.connection")

        if uri_prefix:
            self.uri_prefix = uri_prefix

        self.declared_entities = set()

    def _init_params(self, hostname, userid, password, virtual_host, port,
            insist, ssl, transport, connect_timeout, login_method):
        self.hostname = hostname
        self.userid = userid
        self.password = password
        self.login_method = login_method
        self.virtual_host = virtual_host or self.virtual_host
        self.port = port or self.port
        self.insist = insist
        self.connect_timeout = connect_timeout
        self.ssl = ssl
        self.transport_cls = transport

    def _debug(self, msg, ident="[Kombu connection:0x%(id)x] ", **kwargs):
        if self._logger:  # pragma: no cover
            self._logger.debug((ident + unicode(msg)) % {"id": id(self)},
                               **kwargs)

    def connect(self):
        """Establish connection to server immediately."""
        self._closed = False
        return self.connection

    def channel(self):
        """Request a new channel."""
        self._debug("create channel")
        chan = self.transport.create_channel(self.connection)
        if _LOG_CHANNEL:  # pragma: no cover
            from .utils.debug import Logwrapped
            return Logwrapped(chan, "kombu.channel",
                    "[Kombu channel:%(channel_id)s] ")
        return chan

    def drain_events(self, **kwargs):
        """Wait for a single event from the server.

        :keyword timeout: Timeout in seconds before we give up.
            Raises :exc:`socket.timeout` if the timeout is exceeded.

        Usually used from an event loop.

        """
        return self.transport.drain_events(self.connection, **kwargs)

    def maybe_close_channel(self, channel):
        try:
            channel.close()
        except (self.connection_errors + self.channel_errors):
            pass

    def _do_close_self(self):
        # Closes only the connection and channel(s) not transport.
        self.declared_entities.clear()
        if self._default_channel:
            self.maybe_close_channel(self._default_channel)
        if self._connection:
            try:
                self.transport.close_connection(self._connection)
            except self.connection_errors + (AttributeError, socket.error):
                pass
            self._connection = None

    def _close(self):
        self._do_close_self()
        if self._transport:
            self._transport.client = None
            self._transport = None
        self._debug("closed")
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

    def revive(self, new_channel):
        if self._default_channel:
            self.maybe_close_channel(self._default_channel)
            self._default_channel = None

    def ensure(self, obj, fun, errback=None, max_retries=None,
            interval_start=1, interval_step=1, interval_max=1, on_revive=None):
        """Ensure operation completes, regardless of any channel/connection
        errors occurring.

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

        @wraps(fun)
        def _ensured(*args, **kwargs):
            got_connection = 0
            for retries in count(0):  # for infinity
                try:
                    return fun(*args, **kwargs)
                except self.connection_errors + self.channel_errors, exc:
                    self._debug("ensure got exception: %r" % (exc, ),
                                exc_info=sys.exc_info())
                    if got_connection:
                        raise
                    if max_retries is not None and retries > max_retries:
                        raise
                    errback and errback(exc, 0)
                    self._connection = None
                    self._do_close_self()
                    remaining_retries = None
                    if max_retries is not None:
                        remaining_retries = max(max_retries - retries, 1)
                    self.ensure_connection(errback,
                                           remaining_retries,
                                           interval_start,
                                           interval_step,
                                           interval_max)
                    new_channel = self.channel()
                    self.revive(new_channel)
                    obj.revive(new_channel)
                    if on_revive:
                        on_revive(new_channel)
                    got_connection += 1

        _ensured.func_name = _ensured.__name__ = "%s(ensured)" % fun.__name__
        return _ensured

    def autoretry(self, fun, channel=None, **ensure_options):
        """Decorator for functions supporting a ``channel`` keyword argument.

        The resulting callable will retry calling the function if
        it raises connection or channel related errors.
        The return value will be a tuple of ``(retval, last_created_channel)``.

        If a ``channel`` is not provided, then one will be automatically
        acquired (remember to close it afterwards).

        See :meth:`ensure` for the full list of supported keyword arguments.

        Example usage::

            channel = connection.channel()
            try:
                ret, channel = connection.autoretry(publish_messages, channel)
            finally:
                channel.close()
        """
        channels = [channel]
        create_channel = self.channel

        class Revival(object):
            __name__ = fun.__name__
            __module__ = fun.__module__
            __doc__ = fun.__doc__

            def revive(self, channel):
                channels[0] = channel

            def __call__(self, *args, **kwargs):
                if channels[0] is None:
                    self.revive(create_channel())
                kwargs["channel"] = channels[0]
                return fun(*args, **kwargs), channels[0]

        revive = Revival()
        return self.ensure(revive, revive, **ensure_options)

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
        transport_cls = self.transport_cls or "amqp"
        transport_cls = {"amqplib": "amqp"}.get(transport_cls, transport_cls)
        defaults = self.transport.default_connection_params
        hostname = self.hostname
        if self.uri_prefix:
            hostname = "%s+%s" % (self.uri_prefix, hostname)
        info = OrderedDict((("hostname", hostname),
                            ("userid", self.userid),
                            ("password", self.password),
                            ("virtual_host", self.virtual_host),
                            ("port", self.port),
                            ("insist", self.insist),
                            ("ssl", self.ssl),
                            ("transport", transport_cls),
                            ("connect_timeout", self.connect_timeout),
                            ("transport_options", self.transport_options),
                            ("login_method", self.login_method),
                            ("uri_prefix", self.uri_prefix)))
        for key, value in defaults.iteritems():
            if info[key] is None:
                info[key] = value
        return info

    def __eqhash__(self):
        return hash("|".join(map(str, self.info().itervalues())))

    def as_uri(self, include_password=False):
        if self.transport_cls in self.uri_passthrough:
            return self.transport_cls + '+' + self.hostname
        quoteS = partial(quote, safe="")   # strict quote
        fields = self.info()
        port = fields["port"]
        userid = fields["userid"]
        password = fields["password"]
        transport = fields["transport"]
        url = "%s://" % transport
        if userid:
            url += quoteS(userid)
            if include_password and password:
                url += ':' + quoteS(password)
            url += '@'
        url += quoteS(fields["hostname"])

        # If the transport equals 'mongodb' the
        # hostname contains a full mongodb connection
        # URI. Let pymongo retreive the port from there.
        if port and transport != "mongodb":
            url += ':' + str(port)

        url += '/' + quote(fields["virtual_host"])
        if self.uri_prefix:
            return "%s+%s" % (self.uri_prefix, url)
        return url

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

    def Producer(self, channel=None, *args, **kwargs):
        from .messaging import Producer
        if channel is None:
            channel = self   # use default channel support.
        return Producer(channel, *args, **kwargs)

    def Consumer(self, queues=None, channel=None, *args, **kwargs):
        from .messaging import Consumer
        if channel is None:
            channel = self  # use default channel support.
        return Consumer(channel, queues, *args, **kwargs)

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
        from .simple import SimpleQueue

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
        from .simple import SimpleBuffer

        channel_autoclose = False
        if channel is None:
            channel = self.channel()
            channel_autoclose = True
        return SimpleBuffer(channel, name, no_ack, queue_opts, exchange_opts,
                            channel_autoclose=channel_autoclose, **kwargs)

    def _establish_connection(self):
        self._debug("establishing connection...")
        conn = self.transport.establish_connection()
        self._debug("connection established: %r" % (conn, ))
        return conn

    def __repr__(self):
        """``x.__repr__() <==> repr(x)``"""
        return "<BrokerConnection: %s at 0x%x>" % (self.as_uri(), id(self))

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
    def connected(self):
        """Returns true if the connection has been established."""
        return (not self._closed and
                self._connection is not None and
                self.transport.verify_connection(self._connection))

    @property
    def connection(self):
        """The underlying connection object.

        .. warning::
            This instance is transport specific, so do not
            depend on the interface of this object.

        """
        if not self._closed:
            if not self.connected:
                self.declared_entities.clear()
                self._default_channel = None
                self._connection = self._establish_connection()
                self._closed = False
            return self._connection

    @property
    def default_channel(self):
        # make sure we're still connected, and if not refresh.
        self.connection
        if self._default_channel is None:
            self._default_channel = self.channel()
        return self._default_channel

    @property
    def host(self):
        """The host as a host name/port pair separated by colon."""
        return ":".join([self.hostname, str(self.port)])

    @property
    def transport(self):
        if self._transport is None:
            self._transport = self.create_transport()
        return self._transport

    @cached_property
    def manager(self):
        return self.transport.manager

    def get_manager(self, *args, **kwargs):
        return self.transport.get_manager(*args, **kwargs)

    @property
    def connection_errors(self):
        """List of exceptions that may be raised by the connection."""
        return self.transport.connection_errors

    @property
    def channel_errors(self):
        """List of exceptions that may be raised by the channel."""
        return self.transport.channel_errors
Connection = BrokerConnection


class Resource(object):
    LimitExceeded = exceptions.LimitExceeded

    def __init__(self, limit=None, preload=None):
        self.limit = limit
        self.preload = preload or 0

        self._resource = _LifoQueue()
        self._dirty = set()
        self.setup()

    def setup(self):
        raise NotImplementedError("subclass responsibility")

    def _add_when_empty(self):
        if self.limit and len(self._dirty) >= self.limit:
            raise self.LimitExceeded(self.limit)
        # All taken, put new on the queue and
        # try get again, this way the first in line
        # will get the resource.
        self._resource.put_nowait(self.new())

    def acquire(self, block=False, timeout=None):
        """Acquire resource.

        :keyword block: If the limit is exceeded,
          block until there is an available item.
        :keyword timeout: Timeout to wait
          if ``block`` is true. Default is :const:`None` (forever).

        :raises LimitExceeded: if block is false
          and the limit has been exceeded.

        """
        if self.limit:
            while 1:
                try:
                    R = self._resource.get(block=block, timeout=timeout)
                except Empty:
                    self._add_when_empty()
                else:
                    R = self.prepare(R)
                    self._dirty.add(R)
                    break
        else:
            R = self.prepare(self.new())

        @wraps(self.release)
        def _release():
            self.release(R)
        R.release = _release

        return R

    def prepare(self, resource):
        return resource

    def close_resource(self, resource):
        resource.close()

    def release_resource(self, resource):
        pass

    def replace(self, resource):
        """Replace resource with a new instance.  This can be used in case
        of defective resources."""
        if self.limit:
            self._dirty.discard(resource)
        self.close_resource(resource)

    def release(self, resource):
        """Release resource so it can be used by another thread.

        The caller is responsible for discarding the object,
        and to never use the resource again.  A new resource must
        be acquired if so needed.

        """
        if self.limit:
            self._dirty.discard(resource)
            self._resource.put_nowait(resource)
            self.release_resource(resource)
        else:
            self.close_resource(resource)

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
            try:
                self.close_resource(dres)
            except AttributeError:  # Issue #78
                pass

        mutex = getattr(resource, "mutex", None)
        if mutex:
            mutex.acquire()
        try:
            while 1:
                try:
                    res = resource.queue.pop()
                except IndexError:
                    break
                try:
                    self.close_resource(res)
                except AttributeError:
                    pass  # Issue #78
        finally:
            if mutex:
                mutex.release()

    if os.environ.get("KOMBU_DEBUG_POOL"):  # pragma: no cover
        _orig_acquire = acquire
        _orig_release = release

        _next_resource_id = 0

        def acquire(self, *args, **kwargs):  # noqa
            import traceback
            id = self._next_resource_id = self._next_resource_id + 1
            print("+%s ACQUIRE %s" % (id, self.__class__.__name__, ))
            r = self._orig_acquire(*args, **kwargs)
            r._resource_id = id
            print("-%s ACQUIRE %s" % (id, self.__class__.__name__, ))
            if not hasattr(r, "acquired_by"):
                r.acquired_by = []
            r.acquired_by.append(traceback.format_stack())
            return r

        def release(self, resource):  # noqa
            id = resource._resource_id
            print("+%s RELEASE %s" % (id, self.__class__.__name__, ))
            r = self._orig_release(resource)
            print("-%s RELEASE %s" % (id, self.__class__.__name__, ))
            self._next_resource_id -= 1
            return r


class ConnectionPool(Resource):
    LimitExceeded = exceptions.ConnectionLimitExceeded

    def __init__(self, connection, limit=None, preload=None):
        self.connection = connection
        super(ConnectionPool, self).__init__(limit=limit,
                                             preload=preload)

    def new(self):
        return copy(self.connection)

    def release_resource(self, resource):
        resource._debug("released")

    def close_resource(self, resource):
        resource._close()

    @contextmanager
    def acquire_channel(self, block=False):
        with self.acquire(block=block) as connection:
            yield connection, connection.default_channel

    def setup(self):
        if self.limit:
            for i in xrange(self.limit):
                if i < self.preload:
                    conn = self.new()
                    conn.connect()
                else:
                    conn = self.new
                self._resource.put_nowait(conn)

    def prepare(self, resource):
        if callable(resource):
            resource = resource()
        resource._debug("acquired")
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
        if self.limit:
            for i in xrange(self.limit):
                self._resource.put_nowait(
                    i < self.preload and channel() or channel)

    def prepare(self, channel):
        if callable(channel):
            channel = channel()

        return channel
