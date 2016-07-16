"""Base transport interface."""
from __future__ import absolute_import, unicode_literals

import errno
import socket

from amqp.exceptions import RecoverableConnectionError

from kombu.exceptions import ChannelError, ConnectionError
from kombu.message import Message
from kombu.utils.objects import cached_property

__all__ = ['Message', 'StdChannel', 'Management', 'Transport']


def _LeftBlank(obj, method):
    return NotImplementedError(
        'Transport {0.__module__}.{0.__name__} does not implement {1}'.format(
            obj.__class__, method))


class StdChannel(object):
    no_ack_consumers = None

    def Consumer(self, *args, **kwargs):
        from kombu.messaging import Consumer
        return Consumer(self, *args, **kwargs)

    def Producer(self, *args, **kwargs):
        from kombu.messaging import Producer
        return Producer(self, *args, **kwargs)

    def get_bindings(self):
        raise _LeftBlank(self, 'get_bindings')

    def after_reply_message_received(self, queue):
        """reply queue semantics: can be used to delete the queue
           after transient reply message received."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class Management(object):

    def __init__(self, transport):
        self.transport = transport

    def get_bindings(self):
        raise _LeftBlank(self, 'get_bindings')


class Implements(dict):

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        self[key] = value

    def extend(self, **kwargs):
        return self.__class__(self, **kwargs)


default_transport_capabilities = Implements(
    async=False,
    exchange_type=frozenset(['direct', 'topic', 'fanout', 'headers']),
    heartbeats=False,
)


class Transport(object):
    """Base class for transports."""
    Management = Management

    #: The :class:`~kombu.Connection` owning this instance.
    client = None

    #: Set to True if :class:`~kombu.Connection` should pass the URL
    #: unmodified.
    can_parse_url = False

    #: Default port used when no port has been specified.
    default_port = None

    #: Tuple of errors that can happen due to connection failure.
    connection_errors = (ConnectionError,)

    #: Tuple of errors that can happen due to channel/method failure.
    channel_errors = (ChannelError,)

    #: Type of driver, can be used to separate transports
    #: using the AMQP protocol (driver_type: 'amqp'),
    #: Redis (driver_type: 'redis'), etc...
    driver_type = 'N/A'

    #: Name of driver library (e.g. 'py-amqp', 'redis').
    driver_name = 'N/A'

    __reader = None

    implements = default_transport_capabilities.extend()

    def __init__(self, client, **kwargs):
        self.client = client

    def establish_connection(self):
        raise _LeftBlank(self, 'establish_connection')

    def close_connection(self, connection):
        raise _LeftBlank(self, 'close_connection')

    def create_channel(self, connection):
        raise _LeftBlank(self, 'create_channel')

    def close_channel(self, connection):
        raise _LeftBlank(self, 'close_channel')

    def drain_events(self, connection, **kwargs):
        raise _LeftBlank(self, 'drain_events')

    def heartbeat_check(self, connection, rate=2):
        pass

    def driver_version(self):
        return 'N/A'

    def get_heartbeat_interval(self, connection):
        return 0

    def register_with_event_loop(self, connection, loop):
        pass

    def unregister_from_event_loop(self, connection, loop):
        pass

    def verify_connection(self, connection):
        return True

    def _make_reader(self, connection, timeout=socket.timeout,
                     error=socket.error, _unavail=(errno.EAGAIN, errno.EINTR)):
        drain_events = connection.drain_events

        def _read(loop):
            if not connection.connected:
                raise RecoverableConnectionError('Socket was disconnected')
            try:
                drain_events(timeout=0)
            except timeout:
                return
            except error as exc:
                if exc.errno in _unavail:
                    return
                raise
            loop.call_soon(_read, loop)

        return _read

    def qos_semantics_matches_spec(self, connection):
        return True

    def on_readable(self, connection, loop):
        reader = self.__reader
        if reader is None:
            reader = self.__reader = self._make_reader(connection)
        reader(loop)

    @property
    def default_connection_params(self):
        return {}

    def get_manager(self, *args, **kwargs):
        return self.Management(self)

    @cached_property
    def manager(self):
        return self.get_manager()

    @property
    def supports_heartbeats(self):
        return self.implements.heartbeats

    @property
    def supports_ev(self):
        return self.implements.async
