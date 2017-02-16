"""Generic process mailbox."""
import socket
import warnings

from collections import defaultdict, deque
from contextlib import contextmanager
from copy import copy
from itertools import count
from threading import local
from time import time
from typing import Any, Callable, Mapping, Set, Sequence, cast

from amqp.types import ChannelT

from .clocks import Clock, LamportClock
from .common import maybe_declare, oid_from
from .entity import Exchange, Queue
from .exceptions import InconsistencyError
from .log import get_logger
from .messaging import Consumer, Producer
from .typing import (
    ClientT, ConsumerT, ExchangeT, ProducerT, MessageT, ResourceT,
)
from .utils.functional import maybe_evaluate, reprcall
from .utils.objects import cached_property
from .utils.uuid import uuid

W_PIDBOX_IN_USE = """\
A node named {node.hostname} is already using this process mailbox!

Maybe you forgot to shutdown the other node or did not do so properly?
Or if you meant to start multiple nodes on the same host please make sure
you give each node a unique node name!
"""

__all__ = ['Node', 'Mailbox']
logger = get_logger(__name__)
debug, error = logger.debug, logger.error


class Node:
    """Mailbox node."""

    #: hostname of the node.
    hostname: str = None

    #: the :class:`Mailbox` this is a node for.
    mailbox: 'Mailbox' = None

    #: map of method name/handlers.
    handlers: Mapping[str, Callable] = None

    #: current context (passed on to handlers)
    state: Any = None

    #: current channel.
    channel: ChannelT = None

    def __init__(self,
                 hostname: str,
                 state: Any = None,
                 channel: ChannelT = None,
                 handlers: Mapping[str, Callable] = None,
                 mailbox: 'Mailbox' = None) -> None:
        self.channel = channel
        self.mailbox = mailbox
        self.hostname = hostname
        self.state = state
        self.adjust_clock = self.mailbox.clock.adjust
        if handlers is None:
            handlers = {}
        self.handlers = handlers

    def Consumer(self,
                 channel: ChannelT = None,
                 no_ack: bool = True,
                 accept: Set[str] = None,
                 **options) -> ConsumerT:
        queue = self.mailbox.get_queue(self.hostname)

        def verify_exclusive(name: str, messages: int, consumers: int) -> None:
            if consumers:
                warnings.warn(W_PIDBOX_IN_USE.format(node=self))
        queue.on_declared = verify_exclusive

        return Consumer(
            channel or self.channel, [queue], no_ack=no_ack,
            accept=self.mailbox.accept if accept is None else accept,
            **options
        )

    def handler(self, fun: Callable) -> Callable:
        self.handlers[fun.__name__] = fun
        return fun

    def on_decode_error(self, message: str, exc: Exception) -> None:
        error('Cannot decode message: %r', exc, exc_info=1)

    def listen(self,
               channel: ChannelT = None,
               callback: Callable = None) -> ConsumerT:
        consumer = self.Consumer(channel=channel,
                                 callbacks=[callback or self.handle_message],
                                 on_decode_error=self.on_decode_error)
        consumer.consume()
        return consumer

    def dispatch(self, method: str, arguments: Mapping = None,
                 reply_to: str = None, ticket: str = None, **kwargs) -> Any:
        arguments = arguments or {}
        debug('pidbox received method %s [reply_to:%s ticket:%s]',
              reprcall(method, (), kwargs=arguments), reply_to, ticket)
        handle = reply_to and self.handle_call or self.handle_cast
        try:
            reply = handle(method, arguments)
        except SystemExit:
            raise
        except Exception as exc:
            error('pidbox command error: %r', exc, exc_info=1)
            reply = {'error': repr(exc)}

        if reply_to:
            self.reply({self.hostname: reply},
                       exchange=reply_to['exchange'],
                       routing_key=reply_to['routing_key'],
                       ticket=ticket)
        return reply

    def handle(self, method: str, arguments: Mapping = {}) -> Any:
        return self.handlers[method](self.state, **arguments)

    def handle_call(self, method: str, arguments: Mapping) -> Any:
        return self.handle(method, arguments)

    def handle_cast(self, method: str, arguments: Mapping) -> Any:
        return self.handle(method, arguments)

    def handle_message(self, body: Any, message: MessageT = None) -> None:
        body = cast(Mapping, body)
        destination = body.get('destination')
        if message:
            self.adjust_clock(message.headers.get('clock') or 0)
        if not destination or self.hostname in destination:
            return self.dispatch(**body)
    dispatch_from_message = handle_message

    def reply(self, data: Any, exchange: str, routing_key: str, ticket: str,
              **kwargs) -> None:
        self.mailbox._publish_reply(data, exchange, routing_key, ticket,
                                    channel=self.channel,
                                    serializer=self.mailbox.serializer)


class Mailbox:
    """Process Mailbox."""

    node_cls: type = Node
    exchange_fmt: str = '%s.pidbox'
    reply_exchange_fmt: str = 'reply.%s.pidbox'

    #: Name of application.
    namespace: str = None

    #: Connection (if bound).
    connection: ClientT = None

    #: Exchange type (usually direct, or fanout for broadcast).
    type: str = 'direct'

    #: mailbox exchange (init by constructor).
    exchange: ExchangeT = None

    #: exchange to send replies to.
    reply_exchange: ExchangeT = None

    #: Only accepts json messages by default.
    accept: Set[str] = {'json'}

    #: Message serializer
    serializer: str = None

    def __init__(self, namespace: str,
                 type: str = 'direct',
                 connection: ClientT = None,
                 clock: Clock = None,
                 accept: Set[str] = None,
                 serializer: str = None,
                 producer_pool: ResourceT = None,
                 queue_ttl: float = None,
                 queue_expires: float = None,
                 reply_queue_ttl: float = None,
                 reply_queue_expires: float = 10.0) -> None:
        self.namespace = namespace
        self.connection = connection
        self.type = type
        self.clock = LamportClock() if clock is None else clock
        self.exchange = self._get_exchange(self.namespace, self.type)
        self.reply_exchange = self._get_reply_exchange(self.namespace)
        self._tls = local()
        self.unclaimed = defaultdict(deque)
        self.accept = self.accept if accept is None else accept
        self.serializer = self.serializer if serializer is None else serializer
        self.queue_ttl = queue_ttl
        self.queue_expires = queue_expires
        self.reply_queue_ttl = reply_queue_ttl
        self.reply_queue_expires = reply_queue_expires
        self._producer_pool = producer_pool

    def __call__(self, connection: ClientT) -> 'Mailbox':
        bound = copy(self)
        bound.connection = connection
        return bound

    def Node(self,
             hostname: str = None,
             state: Any = None,
             channel: ChannelT = None,
             handlers: Mapping[str, Callable] = None) -> Node:
        hostname = hostname or socket.gethostname()
        return self.node_cls(hostname, state, channel, handlers, mailbox=self)

    def call(self, destination: str, command: str,
             kwargs: Mapping[str, Any] = {},
             timeout: float = None,
             callback: Callable = None,
             channel: ChannelT = None) -> Sequence[Mapping]:
        return self._broadcast(command, kwargs, destination,
                               reply=True, timeout=timeout,
                               callback=callback,
                               channel=channel)

    def cast(self, destination: str, command: str,
             kwargs: Mapping[str, Any] = {}) -> None:
            self._broadcast(command, kwargs, destination, reply=False)

    def abcast(self, command: str, kwargs: Mapping[str, Any] = {}) -> None:
        self._broadcast(command, kwargs, reply=False)

    def multi_call(self, command: str,
                   kwargs: Mapping[str, Any] = {},
                   timeout: str = 1,
                   limit: int = None,
                   callback: Callable = None,
                   channel: ChannelT = None) -> Sequence[Mapping]:
        return self._broadcast(command, kwargs, reply=True,
                               timeout=timeout, limit=limit,
                               callback=callback,
                               channel=channel)

    def get_reply_queue(self) -> Queue:
        oid = self.oid
        return Queue(
            '%s.%s' % (oid, self.reply_exchange.name),
            exchange=self.reply_exchange,
            routing_key=oid,
            durable=False,
            auto_delete=True,
            expires=self.reply_queue_expires,
            message_ttl=self.reply_queue_ttl,
        )

    @cached_property
    def reply_queue(self) -> Queue:
        return self.get_reply_queue()

    def get_queue(self, hostname: str) -> Queue:
        return Queue(
            '%s.%s.pidbox' % (hostname, self.namespace),
            exchange=self.exchange,
            durable=False,
            auto_delete=True,
            expires=self.queue_expires,
            message_ttl=self.queue_ttl,
        )

    @contextmanager
    def producer_or_acquire(self,
                            producer: ProducerT = None,
                            channel: ChannelT = None) -> ProducerT:
        if producer:
            yield producer
        elif self.producer_pool:
            with self.producer_pool.acquire() as producer:
                yield producer
        else:
            yield Producer(channel, auto_declare=False)

    def _publish_reply(self, reply: Any,
                       exchange: str, routing_key: str, ticket: str,
                       channel: ChannelT = None,
                       producer: ProducerT = None,
                       **opts) -> None:
        chan = channel or self.connection.default_channel
        exchange = Exchange(exchange, exchange_type='direct',
                            delivery_mode='transient',
                            durable=False)
        with self.producer_or_acquire(producer, chan) as producer:
            try:
                producer.publish(
                    reply, exchange=exchange, routing_key=routing_key,
                    declare=[exchange], headers={
                        'ticket': ticket, 'clock': self.clock.forward(),
                    },
                    **opts
                )
            except InconsistencyError:
                # queue probably deleted and no one is expecting a reply.
                pass

    def _publish(self, type: str, arguments: Mapping[str, Any],
                 destination: str = None,
                 reply_ticket: str = None,
                 channel: ChannelT = None,
                 timeout: float = None,
                 serializer: str = None,
                 producer: ProducerT = None) -> None:
        message = {'method': type,
                   'arguments': arguments,
                   'destination': destination}
        chan = channel or self.connection.default_channel
        exchange = self.exchange
        if reply_ticket:
            maybe_declare(self.reply_queue(channel))
            message.update(ticket=reply_ticket,
                           reply_to={'exchange': self.reply_exchange.name,
                                     'routing_key': self.oid})
        serializer = serializer or self.serializer
        with self.producer_or_acquire(producer, chan) as producer:
            producer.publish(
                message, exchange=exchange.name, declare=[exchange],
                headers={'clock': self.clock.forward(),
                         'expires': time() + timeout if timeout else 0},
                serializer=serializer,
            )

    def _broadcast(self, command: str,
                   arguments: Mapping[str, Any] = None,
                   destination: str = None,
                   reply: bool = False,
                   timeout: float = 1.0,
                   limit: int = None,
                   callback: Callable = None,
                   channel: ChannelT = None,
                   serializer: str = None) -> Sequence[Mapping]:
        if destination is not None and \
                not isinstance(destination, (list, tuple)):
            raise ValueError(
                'destination must be a list/tuple not {0}'.format(
                    type(destination)))

        arguments = arguments or {}
        reply_ticket = reply and uuid() or None
        chan = channel or self.connection.default_channel

        # Set reply limit to number of destinations (if specified)
        if limit is None and destination:
            limit = destination and len(destination) or None

        serializer = serializer or self.serializer
        self._publish(command, arguments, destination=destination,
                      reply_ticket=reply_ticket,
                      channel=chan,
                      timeout=timeout,
                      serializer=serializer)

        if reply_ticket:
            return self._collect(reply_ticket, limit=limit,
                                 timeout=timeout,
                                 callback=callback,
                                 channel=chan)

    def _collect(self, ticket: str,
                 limit: str = None,
                 timeout: float = 1.0,
                 callback: Callable = None,
                 channel: ChannelT = None,
                 accept: Set[str] = None) -> Sequence[Mapping]:
        if accept is None:
            accept = self.accept
        chan = channel or self.connection.default_channel
        queue = self.reply_queue
        consumer = Consumer(channel, [queue], accept=accept, no_ack=True)
        responses = []
        unclaimed = self.unclaimed
        adjust_clock = self.clock.adjust

        try:
            return unclaimed.pop(ticket)
        except KeyError:
            pass

        def on_message(body: Any, message: MessageT) -> None:
            # ticket header added in kombu 2.5
            header = message.headers.get
            adjust_clock(header('clock') or 0)
            expires = header('expires')
            if expires and time() > expires:
                return
            this_id = header('ticket', ticket)
            if this_id == ticket:
                if callback:
                    callback(body)
                responses.append(body)
            else:
                unclaimed[this_id].append(body)

        consumer.register_callback(on_message)
        try:
            with consumer:
                for i in limit and range(limit) or count():
                    try:
                        self.connection.drain_events(timeout=timeout)
                    except socket.timeout:
                        break
                return responses
        finally:
            chan.after_reply_message_received(queue.name)

    def _get_exchange(self, namespace: str, type: str) -> Exchange:
        return Exchange(self.exchange_fmt % namespace,
                        type=type,
                        durable=False,
                        delivery_mode='transient')

    def _get_reply_exchange(self, namespace: str) -> Exchange:
        return Exchange(self.reply_exchange_fmt % namespace,
                        type='direct',
                        durable=False,
                        delivery_mode='transient')

    @cached_property
    def oid(self) -> str:
        try:
            return self._tls.OID
        except AttributeError:
            oid = self._tls.OID = oid_from(self)
            return oid

    @cached_property
    def producer_pool(self) -> ResourceT:
        return maybe_evaluate(self._producer_pool)
