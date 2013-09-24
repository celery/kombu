"""
kombu.transport.base
====================

Base transport interface.

"""
from __future__ import absolute_import

from kombu.message import Message
from kombu.utils import cached_property

ACKNOWLEDGED_STATES = frozenset(['ACK', 'REJECTED', 'REQUEUED'])


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


class Transport(object):
    """Base class for transports."""
    Management = Management

    #: The :class:`~kombu.Connection` owning this instance.
    client = None

    #: Default port used when no port has been specified.
    default_port = None

    #: Tuple of errors that can happen due to connection failure.
    connection_errors = ()

    #: Tuple of errors that can happen due to channel/method failure.
    channel_errors = ()

    #: For non-blocking use, an eventloop should keep
    #: draining events as long as ``connection.more_to_read`` is True.
    nb_keep_draining = False

    #: Type of driver, can be used to separate transports
    #: using the AMQP protocol (driver_type: 'amqp'),
    #: Redis (driver_type: 'redis'), etc...
    driver_type = 'N/A'

    #: Name of driver library (e.g. 'py-amqp', 'redis', 'beanstalkc').
    driver_name = 'N/A'

    #: Whether this transports support heartbeats,
    #: and that the :meth:`heartbeat_check` method has any effect.
    supports_heartbeats = False

    #: Set to true if the transport supports the AIO interface.
    supports_ev = False

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

    def eventmap(self, connection):
        """Map of fd -> event handler for event based use.
        Unconvenient to use, and limited transport support."""
        return {}

    def on_poll_init(self, poller):
        pass

    def on_poll_start(self):
        raise _LeftBlank(self, 'on_poll_start')

    def on_poll_empty(self):
        pass

    def verify_connection(self, connection):
        return True

    @property
    def default_connection_params(self):
        return {}

    def get_manager(self, *args, **kwargs):
        return self.Management(self)

    @cached_property
    def manager(self):
        return self.get_manager()
