"""Base transport interface."""
import amqp.types
import errno
import socket
from typing import Any, Callable, ChannelT, Dict, Mapping, Sequence, Tuple
from amqp.exceptions import RecoverableConnectionError
from kombu.exceptions import ChannelError, ConnectionError
from kombu.message import Message
from kombu.types import ClientT, ConnectionT, ConsumerT, ProducerT, TransportT
from kombu.utils.functional import dictfilter
from kombu.utils.objects import cached_property
from kombu.utils.time import maybe_s_to_ms

__all__ = ['Message', 'StdChannel', 'Management', 'Transport']

RABBITMQ_QUEUE_ARGUMENTS: Mapping[str, Tuple[str, Callable]] = {
    'expires': ('x-expires', maybe_s_to_ms),
    'message_ttl': ('x-message-ttl', maybe_s_to_ms),
    'max_length': ('x-max-length', int),
    'max_length_bytes': ('x-max-length-bytes', int),
    'max_priority': ('x-max-priority', int),
}


def to_rabbitmq_queue_arguments(arguments: Mapping, **options) -> Dict:
    """Convert queue arguments to RabbitMQ queue arguments.

    This is the implementation for Channel.prepare_queue_arguments
    for AMQP-based transports.  It's used by both the pyamqp and librabbitmq
    transports.

    Arguments:
        arguments (Mapping):
            User-supplied arguments (``Queue.queue_arguments``).

    Keyword Arguments:
        expires (float): Queue expiry time in seconds.
            This will be converted to ``x-expires`` in int milliseconds.
        message_ttl (float): Message TTL in seconds.
            This will be converted to ``x-message-ttl`` in int milliseconds.
        max_length (int): Max queue length (in number of messages).
            This will be converted to ``x-max-length`` int.
        max_length_bytes (int): Max queue size in bytes.
            This will be converted to ``x-max-length-bytes`` int.
        max_priority (int): Max priority steps for queue.
            This will be converted to ``x-max-priority`` int.

    Returns:
        Dict: RabbitMQ compatible queue arguments.
    """
    prepared = dictfilter(dict(
        _to_rabbitmq_queue_argument(key, value)
        for key, value in options.items()
    ))
    return dict(arguments, **prepared) if prepared else arguments


def _to_rabbitmq_queue_argument(key: str, value: Any) -> Tuple[str, Any]:
    opt, typ = RABBITMQ_QUEUE_ARGUMENTS[key]
    return opt, typ(value) if value is not None else value


def _LeftBlank(obj: Any, method: str) -> Exception:
    return NotImplementedError(
        'Transport {0.__module__}.{0.__name__} does not implement {1}'.format(
            obj.__class__, method))


class StdChannel(amqp.types.ChannelT):
    """Standard channel base class."""

    no_ack_consumers = None

    def Consumer(self, *args, **kwargs) -> ConsumerT:
        from kombu.messaging import Consumer
        return Consumer(self, *args, **kwargs)

    def Producer(self, *args, **kwargs) -> ProducerT:
        from kombu.messaging import Producer
        return Producer(self, *args, **kwargs)

    async def get_bindings(self) -> Sequence[Mapping]:
        raise _LeftBlank(self, 'get_bindings')

    async def after_reply_message_received(self, queue: str) -> None:
        """Callback called after RPC reply received.

        Notes:
           Reply queue semantics: can be used to delete the queue
           after transient reply message received.
        """
        ...

    def prepare_queue_arguments(self, arguments: Mapping, **kwargs) -> Mapping:
        return arguments

    def __enter__(self) -> ChannelT:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()


class Management:
    """AMQP Management API (incomplete)."""

    def __init__(self, transport: TransportT):
        self.transport = transport

    async def get_bindings(self) -> Sequence[Mapping]:
        raise _LeftBlank(self, 'get_bindings')


class Implements(dict):
    """Helper class used to define transport features."""

    def __getattr__(self, key: str) -> bool:
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key: str, value: bool) -> None:
        self[key] = value

    def extend(self, **kwargs) -> 'Implements':
        return self.__class__(self, **kwargs)


default_transport_capabilities = Implements(
    async=False,
    exchange_type=frozenset(['direct', 'topic', 'fanout', 'headers']),
    heartbeats=False,
)


class Transport(amqp.types.ConnectionT):
    """Base class for transports."""

    Management: type = Management

    #: The :class:`~kombu.Connection` owning this instance.
    client: ClientT = None

    #: Set to True if :class:`~kombu.Connection` should pass the URL
    #: unmodified.
    can_parse_url: bool = False

    #: Default port used when no port has been specified.
    default_port: int = None

    #: Tuple of errors that can happen due to connection failure.
    connection_errors: Tuple[type, ...] = (ConnectionError,)

    #: Tuple of errors that can happen due to channel/method failure.
    channel_errors: Tuple[type, ...] = (ChannelError,)

    #: Type of driver, can be used to separate transports
    #: using the AMQP protocol (driver_type: 'amqp'),
    #: Redis (driver_type: 'redis'), etc...
    driver_type: str = 'N/A'

    #: Name of driver library (e.g. 'py-amqp', 'redis').
    driver_name: str = 'N/A'

    __reader = None

    implements = default_transport_capabilities.extend()

    def __init__(self, client: ClientT, **kwargs) -> None:
        self.client = client

    async def establish_connection(self) -> ConnectionT:
        raise _LeftBlank(self, 'establish_connection')

    async def close_connection(self, connection: ConnectionT) -> None:
        raise _LeftBlank(self, 'close_connection')

    def create_channel(self, connection: ConnectionT) -> ChannelT:
        raise _LeftBlank(self, 'create_channel')

    async def close_channel(self, connection: ConnectionT) -> None:
        raise _LeftBlank(self, 'close_channel')

    async def drain_events(self, connection: ConnectionT, **kwargs) -> None:
        raise _LeftBlank(self, 'drain_events')

    async def heartbeat_check(self, connection: ConnectionT,
                              rate: int = 2) -> None:
        ...

    def driver_version(self) -> str:
        return 'N/A'

    def get_heartbeat_interval(self, connection: ConnectionT) -> float:
        return 0.0

    def register_with_event_loop(self, connection, loop):
        ...

    def unregister_from_event_loop(self, connection, loop):
        ...

    def verify_connection(self, connection: ConnectionT) -> bool:
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

    def qos_semantics_matches_spec(self, connection: ConnectionT) -> bool:
        return True

    def on_readable(self, connection, loop):
        reader = self.__reader
        if reader is None:
            reader = self.__reader = self._make_reader(connection)
        reader(loop)

    @property
    def default_connection_params(self) -> Mapping:
        return {}

    def get_manager(self, *args, **kwargs) -> Management:
        return self.Management(self)

    @cached_property
    def manager(self) -> Management:
        return self.get_manager()

    @property
    def supports_heartbeats(self) -> bool:
        return self.implements.heartbeats

    @property
    def supports_ev(self) -> bool:
        return self.implements.async
