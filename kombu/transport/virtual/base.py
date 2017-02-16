"""Virtual transport implementation.

Emulates the AMQ API for non-AMQ transports.
"""
import abc
import base64
import socket
import sys
import warnings

from array import array
from asyncio import sleep
from collections import OrderedDict, defaultdict
from itertools import count
from multiprocessing.util import Finalize
from queue import Empty
from time import monotonic
from typing import (
    Any, AnyStr, Callable, IO, Iterable, List, Mapping, MutableMapping,
    NamedTuple, Optional, Set, Sequence, Tuple,
)

from amqp.protocol import queue_declare_ok_t
from amqp.types import ChannelT, ConnectionT

from kombu.exceptions import ResourceError, ChannelError
from kombu.log import get_logger
from kombu.types import ClientT, MessageT, TransportT
from kombu.utils.encoding import str_to_bytes, bytes_to_str
from kombu.utils.div import emergency_dump_state
from kombu.utils.scheduling import FairCycle
from kombu.utils.uuid import uuid

from kombu.transport import base

from .exchange import STANDARD_EXCHANGE_TYPES, ExchangeType

ARRAY_TYPE_H = 'H' if sys.version_info[0] == 3 else b'H'

UNDELIVERABLE_FMT = """\
Message could not be delivered: No queues bound to exchange {exchange!r} \
using binding key {routing_key!r}.
"""

NOT_EQUIVALENT_FMT = """\
Cannot redeclare exchange {0!r} in vhost {1!r} with \
different type, durable, autodelete or arguments value.\
"""

W_NO_CONSUMERS = """\
Requeuing undeliverable message for queue %r: No consumers.\
"""

RESTORING_FMT = 'Restoring {0!r} unacknowledged message(s)'
RESTORE_PANIC_FMT = 'UNABLE TO RESTORE {0} MESSAGES: {1}'

logger = get_logger(__name__)


class binding_key_t(NamedTuple):
    """Key format used for queue argument lookups in BrokerState.bindings."""

    queue: str
    exchange: str
    routing_key: str


class queue_binding_t(NamedTuple):
    """BrokerState.queue_bindings generates tuples in this format."""

    exchange: str
    routing_key: str
    arguments: Mapping


class Codec(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def encode(self, s: AnyStr) -> str:
        ...

    @abc.abstractmethod
    def decode(self, s: AnyStr) -> str:
        ...


class Base64(Codec):
    """Base64 codec."""

    def encode(self, s: AnyStr) -> str:
        return bytes_to_str(base64.b64encode(str_to_bytes(s)))

    def decode(self, s: AnyStr) -> str:
        return base64.b64decode(str_to_bytes(s))


class NotEquivalentError(Exception):
    """Entity declaration is not equivalent to the previous declaration."""


class UndeliverableWarning(UserWarning):
    """The message could not be delivered to a queue."""


class BrokerState(object):
    """Broker state holds exchanges, queues and bindings."""

    #: Mapping of exchange name to
    #: :class:`kombu.transport.virtual.exchange.ExchangeType`
    exchanges: Mapping[str, ExchangeType] = None

    #: This is the actual bindings registry, used to store bindings and to
    #: test 'in' relationships in constant time.  It has the following
    #: structure::
    #:
    #:     {
    #:         (queue, exchange, routing_key): arguments,
    #:         # ...,
    #:     }
    bindings: Mapping[binding_key_t, Mapping] = None

    #: The queue index is used to access directly (constant time)
    #: all the bindings of a certain queue.  It has the following structure::
    #:     {
    #:         queue: {
    #:             (queue, exchange, routing_key),
    #:             # ...,
    #:         },
    #:         # ...,
    #:     }
    queue_index: Mapping[str, binding_key_t] = None

    def __init__(self, exchanges: Mapping[str, ExchangeType] = None) -> None:
        self.exchanges = {} if exchanges is None else exchanges
        self.bindings = {}
        self.queue_index = defaultdict(set)

    def clear(self) -> None:
        self.exchanges.clear()
        self.bindings.clear()
        self.queue_index.clear()

    def has_binding(self, queue: str, exchange: str, routing_key: str) -> bool:
        return (queue, exchange, routing_key) in self.bindings

    def binding_declare(self,
                        queue: str,
                        exchange: str,
                        routing_key: str,
                        arguments: Mapping) -> None:
        key = binding_key_t(queue, exchange, routing_key)
        self.bindings.setdefault(key, arguments)
        self.queue_index[queue].add(key)

    def binding_delete(
            self, queue: str, exchange: str, routing_key: str) -> None:
        key = binding_key_t(queue, exchange, routing_key)
        try:
            del self.bindings[key]
        except KeyError:
            pass
        else:
            self.queue_index[queue].remove(key)

    def queue_bindings_delete(self, queue: str) -> None:
        try:
            bindings = self.queue_index.pop(queue)
        except KeyError:
            pass
        else:
            [self.bindings.pop(binding, None) for binding in bindings]

    def queue_bindings(self, queue: str) -> Iterable[binding_key_t]:
        return (
            queue_binding_t(key.exchange, key.routing_key, self.bindings[key])
            for key in self.queue_index[queue]
        )


class QoS(object):
    """Quality of Service guarantees.

    Only supports `prefetch_count` at this point.

    Arguments:
        channel (ChannelT): Connection channel.
        prefetch_count (int): Initial prefetch count (defaults to 0).
    """

    #: current prefetch count value
    prefetch_count: int = 0

    #: :class:`~collections.OrderedDict` of active messages.
    #: *NOTE*: Can only be modified by the consuming thread.
    _delivered: OrderedDict = None

    #: acks can be done by other threads than the consuming thread.
    #: Instead of a mutex, which doesn't perform well here, we mark
    #: the delivery tags as dirty, so subsequent calls to append() can remove
    #: them.
    _dirty: Set[MessageT] = None

    #: If disabled, unacked messages won't be restored at shutdown.
    restore_at_shutdown: bool = True

    def __init__(self, channel: ChannelT, prefetch_count: int = 0) -> None:
        self.channel = channel
        self.prefetch_count = prefetch_count or 0

        self._delivered = OrderedDict()
        self._delivered.restored = False
        self._dirty = set()
        self._quick_ack = self._dirty.add
        self._quick_append = self._delivered.__setitem__
        self._on_collect = Finalize(
            self, self.restore_unacked_once, exitpriority=1,
        )

    def can_consume(self) -> bool:
        """Return true if the channel can be consumed from.

        Used to ensure the client adhers to currently active
        prefetch limits.
        """
        pcount = self.prefetch_count
        return not pcount or len(self._delivered) - len(self._dirty) < pcount

    def can_consume_max_estimate(self) -> int:
        """Return the maximum number of messages allowed to be returned.

        Returns an estimated number of messages that a consumer may be allowed
        to consume at once from the broker.  This is used for services where
        bulk 'get message' calls are preferred to many individual 'get message'
        calls - like SQS.

        Returns:
            int: greater than zero.
        """
        pcount = self.prefetch_count
        if pcount:
            return max(pcount - (len(self._delivered) - len(self._dirty)), 0)

    def append(self, message: MessageT, delivery_tag: str) -> None:
        """Append message to transactional state."""
        if self._dirty:
            self._flush()
        self._quick_append(delivery_tag, message)

    def get(self, delivery_tag: str) -> MessageT:
        return self._delivered[delivery_tag]

    def _flush(self) -> None:
        """Flush dirty (acked/rejected) tags from."""
        dirty = self._dirty
        delivered = self._delivered
        while 1:
            try:
                dirty_tag = dirty.pop()
            except KeyError:
                break
            delivered.pop(dirty_tag, None)

    def ack(self, delivery_tag: str) -> None:
        """Acknowledge message and remove from transactional state."""
        self._quick_ack(delivery_tag)

    def reject(self, delivery_tag: str, requeue: bool = False) -> None:
        """Remove from transactional state and requeue message."""
        if requeue:
            self.channel._restore_at_beginning(self._delivered[delivery_tag])
        self._quick_ack(delivery_tag)

    async def restore_unacked(self) -> None:
        """Restore all unacknowledged messages."""
        self._flush()
        delivered = self._delivered
        errors = []
        restore = self.channel._restore
        pop_message = delivered.popitem

        while delivered:
            try:
                _, message = pop_message()
            except KeyError:  # pragma: no cover
                break

            try:
                await restore(message)
            except BaseException as exc:
                errors.append((exc, message))
        delivered.clear()
        return errors

    async def restore_unacked_once(self, stderr: IO = None) -> None:
        """Restore all unacknowledged messages at shutdown/gc collect.

        Note:
            Can only be called once for each instance, subsequent
            calls will be ignored.
        """
        self._on_collect.cancel()
        self._flush()
        stderr = sys.stderr if stderr is None else stderr
        state = self._delivered

        if not self.restore_at_shutdown or not self.channel.do_restore:
            return
        if getattr(state, 'restored', None):
            assert not state
            return
        try:
            if state:
                print(RESTORING_FMT.format(len(self._delivered)),
                      file=stderr)
                unrestored = await self.restore_unacked()

                if unrestored:
                    errors, messages = list(zip(*unrestored))
                    print(RESTORE_PANIC_FMT.format(len(errors), errors),
                          file=stderr)
                    emergency_dump_state(messages, stderr=stderr)
        finally:
            state.restored = True

    async def restore_visible(self, *args, **kwargs) -> None:
        """Restore any pending unackwnowledged messages.

        To be filled in for visibility_timeout style implementations.

        Note:
            This is implementation optional, and currently only
            used by the Redis transport.
        """
        ...


class Message(base.Message):
    """Message object."""

    def __init__(self, payload: Any,
                 channel: ChannelT = None, **kwargs) -> None:
        self._raw = payload
        properties = payload['properties']
        body = payload.get('body')
        if body:
            body = channel.decode_body(body, properties.get('body_encoding'))
        super(Message, self).__init__(
            body=body,
            channel=channel,
            delivery_tag=properties['delivery_tag'],
            content_type=payload.get('content-type'),
            content_encoding=payload.get('content-encoding'),
            headers=payload.get('headers'),
            properties=properties,
            delivery_info=properties.get('delivery_info'),
            postencode='utf-8',
            **kwargs)

    def serializable(self) -> Mapping:
        props = self.properties
        body, _ = self.channel.encode_body(self.body,
                                           props.get('body_encoding'))
        headers = dict(self.headers)
        # remove compression header
        headers.pop('compression', None)
        return {
            'body': body,
            'properties': props,
            'content-type': self.content_type,
            'content-encoding': self.content_encoding,
            'headers': headers,
        }


class AbstractChannel(object):
    """Abstract channel interface.

    This is an abstract class defining the channel methods
    you'd usually want to implement in a virtual channel.

    Note:
        Do not subclass directly, but rather inherit
        from :class:`Channel`.
    """

    async def _get(self, queue: str, timeout: float = None) -> MessageT:
        """Get next message from `queue`."""
        raise NotImplementedError('Virtual channels must implement _get')

    async def _put(self, queue: str, message: MessageT) -> None:
        """Put `message` onto `queue`."""
        raise NotImplementedError('Virtual channels must implement _put')

    async def _purge(self, queue: str) -> int:
        """Remove all messages from `queue`."""
        raise NotImplementedError('Virtual channels must implement _purge')

    async def _size(self, queue: str) -> int:
        """Return the number of messages in `queue` as an :class:`int`."""
        return 0

    async def _delete(self, queue: str, *args, **kwargs) -> None:
        """Delete `queue`.

        Note:
            This just purges the queue, if you need to do more you can
            override this method.
        """
        self._purge(queue)

    async def _new_queue(self, queue: str, **kwargs) -> None:
        """Create new queue.

        Note:
            Your transport can override this method if it needs
            to do something whenever a new queue is declared.
        """
        ...

    async def _has_queue(self, queue: str, **kwargs) -> bool:
        """Verify that queue exists.

        Returns:
            bool: Should return :const:`True` if the queue exists
                or :const:`False` otherwise.
        """
        return True

    async def _poll(self, cycle: Any, callback: Callable,
              timeout: float = None) -> Any:
        """Poll a list of queues for available messages."""
        return await cycle.get(callback)

    async def _get_and_deliver(self, queue: str, callback: Callable) -> None:
        message = await self._get(queue)
        await callback(message, queue)


class Channel(AbstractChannel, base.StdChannel):
    """Virtual channel.

    Arguments:
        connection (ConnectionT): The transport instance this
            channel is part of.
    """

    #: message class used.
    Message: type = Message

    #: QoS class used.
    QoS: type = QoS

    #: flag to restore unacked messages when channel
    #: goes out of scope.
    do_restore: bool = True

    #: mapping of exchange types and corresponding classes.
    exchange_type_classes: Mapping[str, type] = dict(
        STANDARD_EXCHANGE_TYPES)

    exchange_types: Mapping[str, ExchangeType] = None

    #: flag set if the channel supports fanout exchanges.
    supports_fanout: bool = False

    #: Binary <-> ASCII codecs.
    codecs: Mapping[str, Codec] = {'base64': Base64()}

    #: Default body encoding.
    #: NOTE: ``transport_options['body_encoding']`` will override this value.
    body_encoding: str = 'base64'

    #: counter used to generate delivery tags for this channel.
    _delivery_tags = count(1)

    #: Optional queue where messages with no route is delivered.
    #: Set by ``transport_options['deadletter_queue']``.
    deadletter_queue: str = None

    # List of options to transfer from :attr:`transport_options`.
    from_transport_options: Tuple[str, ...] = (
        'body_encoding',
        'deadletter_queue',
    )

    # Priority defaults
    default_priority = 0
    min_priority = 0
    max_priority = 9

    _consumers: Set[str]
    _tag_to_queue: Mapping[str, str]
    _active_queues: List[str]
    closed: bool = False
    _qos: QoS = None

    def __init__(self, connection: ConnectionT, **kwargs) -> None:
        self.connection = connection
        self._consumers = set()
        self._cycle = None
        self._tag_to_queue = {}
        self._active_queues = []
        self._qos = None
        self.closed = False

        # instantiate exchange types
        self.exchange_types = {
            typ: cls(self) for typ, cls in self.exchange_type_classes.items()
        }

        try:
            self.channel_id = self.connection._avail_channel_ids.pop()
        except IndexError:
            raise ResourceError(
                'No free channel ids, current={0}, channel_max={1}'.format(
                    len(self.connection.channels),
                    self.connection.channel_max), (20, 10),
            )

        topts = self.connection.client.transport_options
        for opt_name in self.from_transport_options:
            try:
                setattr(self, opt_name, topts[opt_name])
            except KeyError:
                pass

    async def exchange_declare(
            self,
            exchange: str = None,
            type: str = 'direct',
            durable: bool = False,
            auto_delete: bool = False,
            arguments: Mapping = None,
            nowait: bool = False,
            passive: bool = False) -> None:
        """Declare exchange."""
        type = type or 'direct'
        exchange = exchange or 'amq.%s' % type
        if passive:
            if exchange not in self.state.exchanges:
                raise ChannelError(
                    'NOT_FOUND - no exchange {0!r} in vhost {1!r}'.format(
                        exchange, self.connection.client.virtual_host or '/'),
                    (50, 10), 'Channel.exchange_declare', '404',
                )
            return
        try:
            prev = self.state.exchanges[exchange]
            if not self.typeof(exchange).equivalent(prev, exchange, type,
                                                    durable, auto_delete,
                                                    arguments):
                raise NotEquivalentError(NOT_EQUIVALENT_FMT.format(
                    exchange, self.connection.client.virtual_host or '/'))
        except KeyError:
            self.state.exchanges[exchange] = {
                'type': type,
                'durable': durable,
                'auto_delete': auto_delete,
                'arguments': arguments or {},
                'table': [],
            }

    async def exchange_delete(
            self,
            exchange: str,
            if_unused: bool = False,
            nowait: bool = False) -> None:
        """Delete `exchange` and all its bindings."""
        for rkey, _, queue in self.get_table(exchange):
            await self.queue_delete(queue, if_unused=True, if_empty=True)
        self.state.exchanges.pop(exchange, None)

    async def queue_declare(
            self,
            queue: str = None,
            passive: bool = False,
            **kwargs) -> queue_declare_ok_t:
        """Declare queue."""
        queue = queue or 'amq.gen-%s' % uuid()
        if passive and not await self._has_queue(queue, **kwargs):
            raise ChannelError(
                'NOT_FOUND - no queue {0!r} in vhost {1!r}'.format(
                    queue, self.connection.client.virtual_host or '/'),
                (50, 10), 'Channel.queue_declare', '404',
            )
        else:
            await self._new_queue(queue, **kwargs)
        return queue_declare_ok_t(queue, await self._size(queue), 0)

    async def queue_delete(
            self,
            queue: str,
            if_unused: bool = False,
            if_empty: bool = False,
            **kwargs) -> None:
        """Delete queue."""
        if if_empty and await self._size(queue):
            return
        for exchange, routing_key, args in self.state.queue_bindings(queue):
            meta = self.typeof(exchange).prepare_bind(
                queue, exchange, routing_key, args,
            )
            await self._delete(queue, exchange, *meta, **kwargs)
        self.state.queue_bindings_delete(queue)

    async def after_reply_message_received(self, queue: str) -> None:
        await self.queue_delete(queue)

    async def exchange_bind(
            self, destination: str,
            source: str = '',
            routing_key: str = '',
            nowait: bool = False,
            arguments: Mapping = None) -> None:
        raise NotImplementedError('transport does not support exchange_bind')

    async def exchange_unbind(
            self, destination: str,
            source: str = '',
            routing_key: str = '',
            nowait: bool = False,
            arguments: Mapping = None) -> None:
        raise NotImplementedError('transport does not support exchange_unbind')

    async def queue_bind(
            self, queue: str,
            exchange: str = None,
            routing_key: str = '',
            arguments: Mapping = None,
            **kwargs) -> None:
        """Bind `queue` to `exchange` with `routing key`."""
        exchange = exchange or 'amq.direct'
        if self.state.has_binding(queue, exchange, routing_key):
            return
        # Add binding:
        self.state.binding_declare(queue, exchange, routing_key, arguments)
        # Update exchange's routing table:
        table = self.state.exchanges[exchange].setdefault('table', [])
        meta = self.typeof(exchange).prepare_bind(
            queue, exchange, routing_key, arguments,
        )
        table.append(meta)
        if self.supports_fanout:
            await self._queue_bind(exchange, *meta)

    async def queue_unbind(
            self, queue: str,
            exchange: str = None,
            routing_key: str = '',
            arguments: Mapping = None,
            **kwargs) -> None:
        # Remove queue binding:
        self.state.binding_delete(queue, exchange, routing_key)
        try:
            table = self.get_table(exchange)
        except KeyError:
            return
        binding_meta = self.typeof(exchange).prepare_bind(
            queue, exchange, routing_key, arguments,
        )
        # TODO: the complexity of this operation is O(number of bindings).
        # Should be optimized.  Modifying table in place.
        table[:] = [meta for meta in table if meta != binding_meta]

    async def list_bindings(self) -> Iterable[queue_binding_t]:
        return (queue_binding_t(queue, exchange, rkey)
                for exchange in self.state.exchanges
                for rkey, pattern, queue in self.get_table(exchange))

    async def queue_purge(self, queue: str, **kwargs) -> int:
        """Remove all ready messages from queue."""
        return await self._purge(queue)

    def _next_delivery_tag(self) -> str:
        return uuid()

    async def basic_publish(
            self, message: MessageT, exchange: str, routing_key: str,
            **kwargs) -> None:
        """Publish message."""
        self._inplace_augment_message(message, exchange, routing_key)
        if exchange:
            return self.typeof(exchange).deliver(
                message, exchange, routing_key, **kwargs
            )
        # anon exchange: routing_key is the destination queue
        return await self._put(routing_key, message, **kwargs)

    def _inplace_augment_message(
            self, message: MessageT, exchange: str, routing_key: str) -> None:
        message['body'], body_encoding = self.encode_body(
            message['body'], self.body_encoding,
        )
        props = message['properties']
        props.update(
            body_encoding=body_encoding,
            delivery_tag=self._next_delivery_tag(),
        )
        props['delivery_info'].update(
            exchange=exchange,
            routing_key=routing_key,
        )

    async def basic_consume(
            self, queue: str,
            no_ack: bool = False,
            callback: Callable = None,
            consumer_tag: str = None,
            **kwargs) -> None:
        """Consume from `queue`."""
        self._tag_to_queue[consumer_tag] = queue
        self._active_queues.append(queue)

        async def _callback(raw_message):
            message = self.Message(raw_message, channel=self)
            if not no_ack:
                self.qos.append(message, message.delivery_tag)
            return await callback(message)

        self.connection._callbacks[queue] = _callback
        self._consumers.add(consumer_tag)

        self._reset_cycle()

    await def basic_cancel(self, consumer_tag: str) -> None:
        """Cancel consumer by consumer tag."""
        if consumer_tag in self._consumers:
            self._consumers.remove(consumer_tag)
            self._reset_cycle()
            queue = self._tag_to_queue.pop(consumer_tag, None)
            try:
                self._active_queues.remove(queue)
            except ValueError:
                pass
            self.connection._callbacks.pop(queue, None)

    async def basic_get(
            self, queue: str,
            no_ack: bool = False,
            **kwargs) -> Optional[MessageT]:
        """Get message by direct access (synchronous)."""
        try:
            message = self.Message(await self._get(queue), channel=self)
            if not no_ack:
                self.qos.append(message, message.delivery_tag)
            return message
        except Empty:
            pass

    async def basic_ack(self, delivery_tag: str, multiple: bool = False) -> None:
        """Acknowledge message."""
        self.qos.ack(delivery_tag)

    async def basic_recover(self, requeue: bool = False) -> None:
        """Recover unacked messages."""
        if requeue:
            return await self.qos.restore_unacked()
        raise NotImplementedError('Does not support recover(requeue=False)')

    async def basic_reject(self, delivery_tag: str, requeue: bool = False) -> None:
        """Reject message."""
        self.qos.reject(delivery_tag, requeue=requeue)

    async def basic_qos(
            self,
            prefetch_size: int = 0,
            prefetch_count: int = 0,
            apply_global: bool = False) -> None:
        """Change QoS settings for this channel.

        Note:
            Only `prefetch_count` is supported.
        """
        self.qos.prefetch_count = prefetch_count

    def get_exchanges(self) -> Sequence[str]:
        return list(self.state.exchanges)

    def get_table(self, exchange: str) -> Mapping:
        """Get table of bindings for `exchange`."""
        return self.state.exchanges[exchange]['table']

    def typeof(self, exchange: str, default: str = 'direct') -> ExchangeType:
        """Get the exchange type instance for `exchange`."""
        try:
            type = self.state.exchanges[exchange]['type']
        except KeyError:
            type = default
        return self.exchange_types[type]

    def _lookup(
            self, exchange: str, routing_key: str,
            default: str = None) -> Sequence[str]:
        """Find all queues matching `routing_key` for the given `exchange`.

        Returns:
            str: queue name -- must return the string `default`
                if no queues matched.
        """
        if default is None:
            default = self.deadletter_queue
        if not exchange:  # anon exchange
            return [routing_key or default]

        try:
            R = self.typeof(exchange).lookup(
                self.get_table(exchange),
                exchange, routing_key, default,
            )
        except KeyError:
            R = []

        if not R and default is not None:
            warnings.warn(UndeliverableWarning(UNDELIVERABLE_FMT.format(
                exchange=exchange, routing_key=routing_key)),
            )
            self._new_queue(default)
            R = [default]
        return R

    async def _restore(self, message: MessageT) -> None:
        """Redeliver message to its original destination."""
        delivery_info = message.delivery_info
        message = message.serializable()
        message['redelivered'] = True
        for queue in self._lookup(
                delivery_info['exchange'], delivery_info['routing_key']):
            await self._put(queue, message)

    async def _restore_at_beginning(self, message: MessageT) -> None:
        await self._restore(message)

    async def drain_events(
            self,
            timeout: float = None,
            callback: Callable = None) -> None:
        callback = callback or self.connection._deliver
        if self._consumers and self.qos.can_consume():
            if hasattr(self, '_get_many'):
                await self._get_many(self._active_queues, timeout=timeout)
            else:
                await self._poll(self.cycle, callback, timeout=timeout)
        raise Empty()

    def message_to_python(self, raw_message: Any) -> MessageT:
        """Convert raw message to :class:`Message` instance."""
        if not isinstance(raw_message, self.Message):
            return self.Message(payload=raw_message, channel=self)
        return raw_message

    def prepare_message(self, body: Any,
                        priority: int = None,
                        content_type: str = None,
                        content_encoding: str = None,
                        headers: Mapping = None,
                        properties: Mapping = None) -> Mapping:
        """Prepare message data."""
        properties = properties or {}
        properties.setdefault('delivery_info', {})
        properties.setdefault('priority', priority or self.default_priority)

        return {'body': body,
                'content-encoding': content_encoding,
                'content-type': content_type,
                'headers': headers or {},
                'properties': properties or {}}

    async def flow(self, active: bool = True) -> None:
        """Enable/disable message flow.

        Raises:
            NotImplementedError: as flow
                is not implemented by the base virtual implementation.
        """
        raise NotImplementedError('virtual channels do not support flow.')

    async def close(self) -> None:
        """Close channel.

        Cancel all consumers, and requeue unacked messages.
        """
        if not self.closed:
            self.closed = True
            for consumer in list(self._consumers):
                await self.basic_cancel(consumer)
            if self._qos:
                await self._qos.restore_unacked_once()
            if self._cycle is not None:
                self._cycle.close()
                self._cycle = None
            if self.connection is not None:
                await self.connection.close_channel(self)
        self.exchange_types = None

    def encode_body(self, body: Any, encoding: str = None) -> Tuple[Any, str]:
        if encoding:
            return self.codecs.get(encoding).encode(body), encoding
        return body, encoding

    def decode_body(self, body: Any, encoding: str = None) -> Any:
        if encoding:
            return self.codecs.get(encoding).decode(body)
        return body

    def _reset_cycle(self) -> None:
        self._cycle = FairCycle(
            self._get_and_deliver, self._active_queues, Empty)

    def __enter__(self) -> 'Channel':
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    async def __aenter__(self) -> 'Channel':
        return self

    async def __aexit__(self, *exc_info) -> None:
        ...

    @property
    def state(self) -> BrokerState:
        """Broker state containing exchanges and bindings."""
        return self.connection.state

    @property
    def qos(self) -> QoS:
        """:class:`QoS` manager for this channel."""
        if self._qos is None:
            self._qos = self.QoS(self)
        return self._qos

    @property
    def cycle(self) -> FairCycle:
        if self._cycle is None:
            self._reset_cycle()
        return self._cycle

    def _get_message_priority(self, message: MessageT,
                              *, reverse: bool = False) -> int:
        """Get priority from message.

        The value is limited to within a boundary of 0 to 9.

        Note:
            Higher value has more priority.
        """
        try:
            priority = max(
                min(int(message['properties']['priority']),
                    self.max_priority),
                self.min_priority,
            )
        except (TypeError, ValueError, KeyError):
            priority = self.default_priority

        return (self.max_priority - priority) if reverse else priority


class Management(base.Management):
    """Base class for the AMQP management API."""

    def __init__(self, transport: TransportT) -> None:
        super(Management, self).__init__(transport)
        self.channel = transport.client.channel()

    def get_bindings(self) -> Sequence[Mapping]:
        return [dict(destination=q, source=e, routing_key=r)
                for q, e, r in self.channel.list_bindings()]

    def close(self) -> None:
        self.channel.close()


class Transport(base.Transport):
    """Virtual transport.

    Arguments:
        client (kombu.Connection): The client this is a transport for.
    """

    Channel: type = Channel
    Cycle: type = FairCycle
    Management: type = Management

    #: Global :class:`BrokerState` containing declared exchanges and bindings.
    state: BrokerState = BrokerState()

    #: :class:`~kombu.utils.scheduling.FairCycle` instance
    #: used to fairly drain events from channels (set by constructor).
    cycle: FairCycle = None

    #: port number used when no port is specified.
    default_port: int = None

    #: active channels.
    channels: Sequence[ChannelT] = None

    #: queue/callback map.
    _callbacks: MutableMapping[str, Callable] = None

    #: Time to sleep between unsuccessful polls.
    polling_interval: float = 1.0

    #: Max number of channels
    channel_max: int = 65535

    implements = base.Transport.implements.extend(
        async=False,
        exchange_type=frozenset(['direct', 'topic']),
        heartbeats=False,
    )

    def __init__(self, client: ClientT, **kwargs):
        self.client = client
        self.channels = []
        self._avail_channels = []
        self._callbacks = {}
        self.cycle = self.Cycle(self._drain_channel, self.channels, Empty)
        polling_interval = client.transport_options.get('polling_interval')
        if polling_interval is not None:
            self.polling_interval = polling_interval
        self._avail_channel_ids = array(
            ARRAY_TYPE_H, range(self.channel_max, 0, -1),
        )

    def create_channel(self, connection: ConnectionT) -> ChannelT:
        try:
            return self._avail_channels.pop()
        except IndexError:
            channel = self.Channel(connection)
            self.channels.append(channel)
            return channel

    async def close_channel(self, channel: ChannelT) -> None:
        try:
            self._avail_channel_ids.append(channel.channel_id)
            try:
                self.channels.remove(channel)
            except ValueError:
                pass
        finally:
            channel.connection = None

    async def establish_connection(self) -> 'Transport':
        # creates channel to verify connection.
        # this channel is then used as the next requested channel.
        # (returned by ``create_channel``).
        self._avail_channels.append(self.create_channel(self))
        return self     # for drain events

    async def close_connection(self, connection: ConnectionT) -> None:
        self.cycle.close()
        for l in self._avail_channels, self.channels:
            while l:
                try:
                    channel = l.pop()
                except LookupError:  # pragma: no cover
                    pass
                else:
                    await channel.close()

    async def drain_events(self, connection: ConnectionT,
                           timeout: float = None) -> None:
        time_start = monotonic()
        get = self.cycle.get
        polling_interval = self.polling_interval
        while 1:
            try:
                await get(self._deliver, timeout=timeout)
            except Empty:
                if timeout is not None and monotonic() - time_start >= timeout:
                    raise socket.timeout()
                if polling_interval is not None:
                    await sleep(polling_interval)
            else:
                break

    async def _deliver(self, message: MessageT, queue: str) -> None:
        if not queue:
            raise KeyError(
                'Received message without destination queue: {0}'.format(
                    message))
        try:
            callback = self._callbacks[queue]
        except KeyError:
            logger.warn(W_NO_CONSUMERS, queue)
            self._reject_inbound_message(message)
        else:
            callback(message)

    def _reject_inbound_message(self, raw_message: Any) -> None:
        for channel in self.channels:
            if channel:
                message = channel.Message(raw_message, channel=channel)
                channel.qos.append(message, message.delivery_tag)
                channel.basic_reject(message.delivery_tag, requeue=True)
                break

    def on_message_ready(
            self, channel: ChannelT, message: MessageT, queue: str) -> None:
        if not queue or queue not in self._callbacks:
            raise KeyError(
                'Message for queue {0!r} without consumers: {1}'.format(
                    queue, message))
        self._callbacks[queue](message)

    def _drain_channel(self, channel: ChannelT, callback: Callable,
                       timeout: float = None) -> None:
        return channel.drain_events(callback=callback, timeout=timeout)

    @property
    def default_connection_params(self) -> Mapping:
        return {'port': self.default_port, 'hostnaime': 'localhost'}
