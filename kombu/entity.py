"""Exchange and Queue declarations."""
from __future__ import absolute_import, unicode_literals

import numbers

from .abstract import MaybeChannelBound, Object
from .exceptions import ContentDisallowed
from .five import python_2_unicode_compatible, string_t
from .serialization import prepare_accept_content

TRANSIENT_DELIVERY_MODE = 1
PERSISTENT_DELIVERY_MODE = 2
DELIVERY_MODES = {'transient': TRANSIENT_DELIVERY_MODE,
                  'persistent': PERSISTENT_DELIVERY_MODE}

__all__ = ['Exchange', 'Queue', 'binding', 'maybe_delivery_mode']

INTERNAL_EXCHANGE_PREFIX = ('amq.',)


def _reprstr(s):
    s = repr(s)
    if isinstance(s, string_t) and s.startswith("u'"):
        return s[2:-1]
    return s[1:-1]


def pretty_bindings(bindings):
    return '[{0}]'.format(', '.join(map(str, bindings)))


def maybe_delivery_mode(
        v, modes=DELIVERY_MODES, default=PERSISTENT_DELIVERY_MODE):
    if v:
        return v if isinstance(v, numbers.Integral) else modes[v]
    return default


@python_2_unicode_compatible
class Exchange(MaybeChannelBound):
    """An Exchange declaration.

    Arguments:
        name (str): See :attr:`name`.
        type (str): See :attr:`type`.
        channel (kombu.Connection, ChannelT): See :attr:`channel`.
        durable (bool): See :attr:`durable`.
        auto_delete (bool): See :attr:`auto_delete`.
        delivery_mode (enum): See :attr:`delivery_mode`.
        arguments (Dict): See :attr:`arguments`.
        no_declare (bool): See :attr:`no_declare`

    Attributes:
        name (str): Name of the exchange.
            Default is no name (the default exchange).

        type (str):
            *This description of AMQP exchange types was shamelessly stolen
            from the blog post `AMQP in 10 minutes: Part 4`_ by
            Rajith Attapattu. Reading this article is recommended if you're
            new to amqp.*

            "AMQP defines four default exchange types (routing algorithms) that
            covers most of the common messaging use cases. An AMQP broker can
            also define additional exchange types, so see your broker
            manual for more information about available exchange types.

                * `direct` (*default*)

                    Direct match between the routing key in the message,
                    and the routing criteria used when a queue is bound to
                    this exchange.

                * `topic`

                    Wildcard match between the routing key and the routing
                    pattern specified in the exchange/queue binding.
                    The routing key is treated as zero or more words delimited
                    by `"."` and supports special wildcard characters. `"*"`
                    matches a single word and `"#"` matches zero or more words.

                * `fanout`

                    Queues are bound to this exchange with no arguments. Hence
                    any message sent to this exchange will be forwarded to all
                    queues bound to this exchange.

                * `headers`

                    Queues are bound to this exchange with a table of arguments
                    containing headers and values (optional). A special
                    argument named "x-match" determines the matching algorithm,
                    where `"all"` implies an `AND` (all pairs must match) and
                    `"any"` implies `OR` (at least one pair must match).

                    :attr:`arguments` is used to specify the arguments.


                .. _`AMQP in 10 minutes: Part 4`:
                    http://bit.ly/amqp-exchange-types

        channel (ChannelT): The channel the exchange is bound to (if bound).

        durable (bool): Durable exchanges remain active when a server restarts.
            Non-durable exchanges (transient exchanges) are purged when a
            server restarts.  Default is :const:`True`.

        auto_delete (bool): If set, the exchange is deleted when all queues
            have finished using it. Default is :const:`False`.

        delivery_mode (enum): The default delivery mode used for messages.
            The value is an integer, or alias string.

                * 1 or `"transient"`

                    The message is transient. Which means it is stored in
                    memory only, and is lost if the server dies or restarts.

                * 2 or "persistent" (*default*)
                    The message is persistent. Which means the message is
                    stored both in-memory, and on disk, and therefore
                    preserved if the server dies or restarts.

            The default value is 2 (persistent).

        arguments (Dict): Additional arguments to specify when the exchange
            is declared.

        no_declare (bool): Never declare this exchange
            (:meth:`declare` does nothing).
    """
    TRANSIENT_DELIVERY_MODE = TRANSIENT_DELIVERY_MODE
    PERSISTENT_DELIVERY_MODE = PERSISTENT_DELIVERY_MODE

    name = ''
    type = 'direct'
    durable = True
    auto_delete = False
    passive = False
    delivery_mode = None
    no_declare = False

    attrs = (
        ('name', None),
        ('type', None),
        ('arguments', None),
        ('durable', bool),
        ('passive', bool),
        ('auto_delete', bool),
        ('delivery_mode', lambda m: DELIVERY_MODES.get(m) or m),
        ('no_declare', bool),
    )

    def __init__(self, name='', type='', channel=None, **kwargs):
        super(Exchange, self).__init__(**kwargs)
        self.name = name or self.name
        self.type = type or self.type
        self.maybe_bind(channel)

    def __hash__(self):
        return hash(self.declaration_key)

    @property
    def declaration_key(self):
        return 'E|%s' % (self.name, )

    def _can_declare(self):
        return not self.no_declare and (
            self.name and not self.name.startswith(
                INTERNAL_EXCHANGE_PREFIX))

    def declare(self, nowait=False, passive=None):
        """Declare the exchange.

        Creates the exchange on the broker, unless passive is set
        in which case it will only assert that the exchange exists.

        Argument:
            nowait (bool): If set the server will not respond, and a
                response will not be waited for. Default is :const:`False`.
        """
        if self._can_declare():
            passive = self.passive if passive is None else passive
            return self.channel.exchange_declare(
                exchange=self.name, type=self.type, durable=self.durable,
                auto_delete=self.auto_delete, arguments=self.arguments,
                nowait=nowait, passive=passive,
            )

    def bind_to(self, exchange='', routing_key='',
                arguments=None, nowait=False, **kwargs):
        """Binds the exchange to another exchange.

        Arguments:
            nowait (bool): If set the server will not respond, and the call
                will not block waiting for a response.
                Default is :const:`False`.
        """
        if isinstance(exchange, Exchange):
            exchange = exchange.name
        return self.channel.exchange_bind(destination=self.name,
                                          source=exchange,
                                          routing_key=routing_key,
                                          nowait=nowait,
                                          arguments=arguments)

    def unbind_from(self, source='', routing_key='',
                    nowait=False, arguments=None):
        """Delete previously created exchange binding from the server."""
        if isinstance(source, Exchange):
            source = source.name
        return self.channel.exchange_unbind(destination=self.name,
                                            source=source,
                                            routing_key=routing_key,
                                            nowait=nowait,
                                            arguments=arguments)

    def Message(self, body, delivery_mode=None, priority=None,
                content_type=None, content_encoding=None,
                properties=None, headers=None):
        """Create message instance to be sent with :meth:`publish`.

        Arguments:
            body (Any): Message body.

            delivery_mode (bool): Set custom delivery mode.
                Defaults to :attr:`delivery_mode`.

            priority (int): Message priority, 0 to broker configured
                max priority, where higher is better.

            content_type (str): The messages content_type.  If content_type
                is set, no serialization occurs as it is assumed this is either
                a binary object, or you've done your own serialization.
                Leave blank if using built-in serialization as our library
                properly sets content_type.

            content_encoding (str): The character set in which this object
                is encoded. Use "binary" if sending in raw binary objects.
                Leave blank if using built-in serialization as our library
                properly sets content_encoding.

            properties (Dict): Message properties.

            headers (Dict): Message headers.
        """
        # XXX This method is unused by kombu itself AFAICT [ask].
        properties = {} if properties is None else properties
        properties['delivery_mode'] = maybe_delivery_mode(self.delivery_mode)
        return self.channel.prepare_message(body,
                                            properties=properties,
                                            priority=priority,
                                            content_type=content_type,
                                            content_encoding=content_encoding,
                                            headers=headers)

    def publish(self, message, routing_key=None, mandatory=False,
                immediate=False, exchange=None):
        """Publish message.

        Arguments:
            message (~kombu.Message): Message instance to publish.
            routing_key (str): Message routing key.
            mandatory (bool): Currently not supported.
            immediate (bool): Currently not supported.
        """
        exchange = exchange or self.name
        return self.channel.basic_publish(message,
                                          exchange=exchange,
                                          routing_key=routing_key,
                                          mandatory=mandatory,
                                          immediate=immediate)

    def delete(self, if_unused=False, nowait=False):
        """Delete the exchange declaration on server.

        Arguments:
            if_unused (bool): Delete only if the exchange has no bindings.
                Default is :const:`False`.
            nowait (bool): If set the server will not respond, and a
                response will not be waited for. Default is :const:`False`.
        """
        return self.channel.exchange_delete(exchange=self.name,
                                            if_unused=if_unused,
                                            nowait=nowait)

    def binding(self, routing_key='', arguments=None, unbind_arguments=None):
        return binding(self, routing_key, arguments, unbind_arguments)

    def __eq__(self, other):
        if isinstance(other, Exchange):
            return (self.name == other.name and
                    self.type == other.type and
                    self.arguments == other.arguments and
                    self.durable == other.durable and
                    self.auto_delete == other.auto_delete and
                    self.delivery_mode == other.delivery_mode)
        return NotImplemented

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return self._repr_entity(self)

    def __str__(self):
        return 'Exchange {0}({1})'.format(
            _reprstr(self.name) or repr(''), self.type,
        )

    @property
    def can_cache_declaration(self):
        return not self.auto_delete


@python_2_unicode_compatible
class binding(Object):
    """Represents a queue or exchange binding.

    Arguments:
        exchange (Exchange): Exchange to bind to.
        routing_key (str): Routing key used as binding key.
        arguments (Dict): Arguments for bind operation.
        unbind_arguments (Dict): Arguments for unbind operation.
    """

    attrs = (
        ('exchange', None),
        ('routing_key', None),
        ('arguments', None),
        ('unbind_arguments', None)
    )

    def __init__(self, exchange=None, routing_key='',
                 arguments=None, unbind_arguments=None):
        self.exchange = exchange
        self.routing_key = routing_key
        self.arguments = arguments
        self.unbind_arguments = unbind_arguments

    def declare(self, channel, nowait=False):
        """Declare destination exchange."""
        if self.exchange and self.exchange.name:
            ex = self.exchange(channel)
            ex.declare(nowait=nowait)

    def bind(self, entity, nowait=False):
        """Bind entity to this binding."""
        entity.bind_to(exchange=self.exchange,
                       routing_key=self.routing_key,
                       arguments=self.arguments,
                       nowait=nowait)

    def unbind(self, entity, nowait=False):
        """Unbind entity from this binding."""
        entity.unbind_from(self.exchange,
                           routing_key=self.routing_key,
                           arguments=self.unbind_arguments,
                           nowait=nowait)

    def __repr__(self):
        return '<binding: {0}>'.format(self)

    def __str__(self):
        return '{0}->{1}'.format(
            _reprstr(self.exchange.name), _reprstr(self.routing_key),
        )

    @property
    def declaration_key(self):
        return str(self)


@python_2_unicode_compatible
class Queue(MaybeChannelBound):
    """A Queue declaration.

    Arguments:
        name (str): See :attr:`name`.
        exchange (Exchange, str): See :attr:`exchange`.
        routing_key (str): See :attr:`routing_key`.
        channel (kombu.Connection, ChannelT): See :attr:`channel`.
        durable (bool): See :attr:`durable`.
        exclusive (bool): See :attr:`exclusive`.
        auto_delete (bool): See :attr:`auto_delete`.
        queue_arguments (Dict): See :attr:`queue_arguments`.
        binding_arguments (Dict): See :attr:`binding_arguments`.
        consumer_arguments (Dict): See :attr:`consumer_arguments`.
        no_declare (bool): See :attr:`no_declare`
        on_declared (Callable): See :attr:`on_declared`

    Attributes:
        name (str): Name of the queue.
            Default is no name (default queue destination).

        exchange (Exchange): The :class:`Exchange` the queue binds to.

        routing_key (str): The routing key (if any), also called *binding key*.

            The interpretation of the routing key depends on
            the :attr:`Exchange.type`.

            * direct exchange

                Matches if the routing key property of the message and
                the :attr:`routing_key` attribute are identical.

            * fanout exchange

                Always matches, even if the binding does not have a key.

            * topic exchange

                Matches the routing key property of the message by a primitive
                pattern matching scheme. The message routing key then consists
                of words separated by dots (`"."`, like domain names), and
                two special characters are available; star (`"*"`) and hash
                (`"#"`). The star matches any word, and the hash matches
                zero or more words. For example `"*.stock.#"` matches the
                routing keys `"usd.stock"` and `"eur.stock.db"` but not
                `"stock.nasdaq"`.

        channel (ChannelT): The channel the Queue is bound to (if bound).

        durable (bool): Durable queues remain active when a server restarts.
            Non-durable queues (transient queues) are purged if/when
            a server restarts.
            Note that durable queues do not necessarily hold persistent
            messages, although it does not make sense to send
            persistent messages to a transient queue.

            Default is :const:`True`.

        exclusive (bool): Exclusive queues may only be consumed from by the
            current connection. Setting the 'exclusive' flag
            always implies 'auto-delete'.

            Default is :const:`False`.

        auto_delete (bool): If set, the queue is deleted when all consumers
            have finished using it. Last consumer can be canceled
            either explicitly or because its channel is closed. If
            there was no consumer ever on the queue, it won't be
            deleted.

        queue_arguments (Dict): Additional arguments used when declaring
            the queue.  Can be used to to set the arguments value
            for RabbitMQ/AMQP's ``queue.declare``.

        binding_arguments (Dict): Additional arguments used when binding
            the queue.  Can be used to to set the arguments value
            for RabbitMQ/AMQP's ``queue.declare``.

        consumer_arguments (Dict): Additional arguments used when consuming
            from this queue.  Can be used to to set the arguments value
            for RabbitMQ/AMQP's ``basic.consume``.

        alias (str): Unused in Kombu, but applications can take advantage
            of this,  for example to give alternate names to queues with
            utomatically generated queue names.

        on_declared (Callable): Optional callback to be applied when the
            queue has been declared (the ``queue_declare`` operation is
            complete).  This must be a function with a signature that
            accepts at least 3 positional arguments:
            ``(name, messages, consumers)``.

        no_declare (bool): Never declare this queue, nor related
            entities (:meth:`declare` does nothing).
    """
    ContentDisallowed = ContentDisallowed

    name = ''
    exchange = Exchange('')
    routing_key = ''

    durable = True
    exclusive = False
    auto_delete = False
    no_ack = False

    attrs = (
        ('name', None),
        ('exchange', None),
        ('routing_key', None),
        ('queue_arguments', None),
        ('binding_arguments', None),
        ('consumer_arguments', None),
        ('durable', bool),
        ('exclusive', bool),
        ('auto_delete', bool),
        ('no_ack', None),
        ('alias', None),
        ('bindings', list),
        ('no_declare', bool),
    )

    def __init__(self, name='', exchange=None, routing_key='',
                 channel=None, bindings=None, on_declared=None,
                 **kwargs):
        super(Queue, self).__init__(**kwargs)
        self.name = name or self.name
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.bindings = set(bindings or [])
        self.on_declared = on_declared

        # allows Queue('name', [binding(...), binding(...), ...])
        if isinstance(exchange, (list, tuple, set)):
            self.bindings |= set(exchange)
        if self.bindings:
            self.exchange = None

        # exclusive implies auto-delete.
        if self.exclusive:
            self.auto_delete = True
        self.maybe_bind(channel)

    def bind(self, channel):
        on_declared = self.on_declared
        bound = super(Queue, self).bind(channel)
        bound.on_declared = on_declared
        return bound

    def __hash__(self):
        return hash(self.declaration_key)

    @property
    def declaration_key(self):
        return 'Q|%s|%s|%s' % (self.name, self.routing_key, '|'.join(sorted(b.declaration_key for b in self.bindings)))

    def when_bound(self):
        if self.exchange:
            self.exchange = self.exchange(self.channel)

    def declare(self, nowait=False):
        """Declares the queue, the exchange and binds the queue to
        the exchange."""
        if not self.no_declare:
            # - declare main binding.
            self._create_exchange(nowait=nowait)
            self._create_queue(nowait=nowait)
            self._create_bindings(nowait=nowait)
        return self.name

    def _create_exchange(self, nowait=False):
        if self.exchange:
            self.exchange.declare(nowait)

    def _create_queue(self, nowait=False):
        self.queue_declare(nowait, passive=False)
        if self.exchange and self.exchange.name:
            self.queue_bind(nowait)

    def _create_bindings(self, nowait=False):
        for B in self.bindings:
            B.declare(self.channel)
            B.bind(self, nowait=nowait)

    def queue_declare(self, nowait=False, passive=False):
        """Declare queue on the server.

        Arguments:
            nowait (bool): Do not wait for a reply.
            passive (bool): If set, the server will not create the queue.
                The client can use this to check whether a queue exists
                without modifying the server state.
        """
        ret = self.channel.queue_declare(queue=self.name,
                                         passive=passive,
                                         durable=self.durable,
                                         exclusive=self.exclusive,
                                         auto_delete=self.auto_delete,
                                         arguments=self.queue_arguments,
                                         nowait=nowait)
        if not self.name:
            self.name = ret[0]
        if self.on_declared:
            self.on_declared(*ret)
        return ret

    def queue_bind(self, nowait=False):
        """Create the queue binding on the server."""
        return self.bind_to(self.exchange, self.routing_key,
                            self.binding_arguments, nowait=nowait)

    def bind_to(self, exchange='', routing_key='',
                arguments=None, nowait=False):
        if isinstance(exchange, Exchange):
            exchange = exchange.name
        return self.channel.queue_bind(queue=self.name,
                                       exchange=exchange,
                                       routing_key=routing_key,
                                       arguments=arguments,
                                       nowait=nowait)

    def get(self, no_ack=None, accept=None):
        """Poll the server for a new message.

        This method provides direct access to the messages in a
        queue using a synchronous dialogue, designed for
        specific types of applications where synchronous functionality
        is more important than performance.

        Returns:
            ~kombu.Message: if a message was available,
                or :const:`None` otherwise.

        Arguments:
            no_ack (bool): If enabled the broker will
                automatically ack messages.
            accept (Set[str]): Custom list of accepted content types.
        """
        no_ack = self.no_ack if no_ack is None else no_ack
        message = self.channel.basic_get(queue=self.name, no_ack=no_ack)
        if message is not None:
            m2p = getattr(self.channel, 'message_to_python', None)
            if m2p:
                message = m2p(message)
            if message.errors:
                message._reraise_error()
            message.accept = prepare_accept_content(accept)
        return message

    def purge(self, nowait=False):
        """Remove all ready messages from the queue."""
        return self.channel.queue_purge(queue=self.name,
                                        nowait=nowait) or 0

    def consume(self, consumer_tag='', callback=None,
                no_ack=None, nowait=False):
        """Start a queue consumer.

        Consumers last as long as the channel they were created on, or
        until the client cancels them.

        Arguments:
            consumer_tag (str): Unique identifier for the consumer.
                The consumer tag is local to a connection, so two clients
                can use the same consumer tags. If this field is empty
                the server will generate a unique tag.

            no_ack (bool): If enabled the broker will automatically
                ack messages.

            nowait (bool): Do not wait for a reply.

            callback (Callable): callback called for each delivered message.
        """
        if no_ack is None:
            no_ack = self.no_ack
        return self.channel.basic_consume(
            queue=self.name,
            no_ack=no_ack,
            consumer_tag=consumer_tag or '',
            callback=callback,
            nowait=nowait,
            arguments=self.consumer_arguments)

    def cancel(self, consumer_tag):
        """Cancel a consumer by consumer tag."""
        return self.channel.basic_cancel(consumer_tag)

    def delete(self, if_unused=False, if_empty=False, nowait=False):
        """Delete the queue.

        Example:
            .. code-block:: console
                $ foo = 'blah'

        Arguments:
            if_unused (bool): If set, the server will only delete the queue
                if it has no consumers. A channel error will be raised
                if the queue has consumers.

            if_empty (bool): If set, the server will only delete the queue if
                it is empty. If it is not empty a channel error will be raised.

            nowait (bool): Do not wait for a reply.
        """
        return self.channel.queue_delete(queue=self.name,
                                         if_unused=if_unused,
                                         if_empty=if_empty,
                                         nowait=nowait)

    def queue_unbind(self, arguments=None, nowait=False):
        return self.unbind_from(self.exchange, self.routing_key,
                                arguments, nowait)

    def unbind_from(self, exchange='', routing_key='',
                    arguments=None, nowait=False):
        """Unbind queue by deleting the binding from the server."""
        return self.channel.queue_unbind(queue=self.name,
                                         exchange=exchange.name,
                                         routing_key=routing_key,
                                         arguments=arguments,
                                         nowait=nowait)

    def __eq__(self, other):
        if isinstance(other, Queue):
            return (self.name == other.name and
                    self.exchange == other.exchange and
                    self.routing_key == other.routing_key and
                    self.queue_arguments == other.queue_arguments and
                    self.binding_arguments == other.binding_arguments and
                    self.consumer_arguments == other.consumer_arguments and
                    self.durable == other.durable and
                    self.exclusive == other.exclusive and
                    self.auto_delete == other.auto_delete)
        return NotImplemented

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        if self.bindings:
            return self._repr_entity('Queue {name} -> {bindings}'.format(
                name=_reprstr(self.name),
                bindings=pretty_bindings(self.bindings),
            ))
        return self._repr_entity(
            'Queue {name} -> {0.exchange!r} -> {routing_key}'.format(
                self, name=_reprstr(self.name),
                routing_key=_reprstr(self.routing_key),
            ),
        )

    @property
    def can_cache_declaration(self):
        return not self.auto_delete

    @classmethod
    def from_dict(self, queue, **options):
        binding_key = options.get('binding_key') or options.get('routing_key')

        e_durable = options.get('exchange_durable')
        if e_durable is None:
            e_durable = options.get('durable')

        e_auto_delete = options.get('exchange_auto_delete')
        if e_auto_delete is None:
            e_auto_delete = options.get('auto_delete')

        q_durable = options.get('queue_durable')
        if q_durable is None:
            q_durable = options.get('durable')

        q_auto_delete = options.get('queue_auto_delete')
        if q_auto_delete is None:
            q_auto_delete = options.get('auto_delete')

        e_arguments = options.get('exchange_arguments')
        q_arguments = options.get('queue_arguments')
        b_arguments = options.get('binding_arguments')
        c_arguments = options.get('consumer_arguments')
        bindings = options.get('bindings')

        exchange = Exchange(options.get('exchange'),
                            type=options.get('exchange_type'),
                            delivery_mode=options.get('delivery_mode'),
                            routing_key=options.get('routing_key'),
                            durable=e_durable,
                            auto_delete=e_auto_delete,
                            arguments=e_arguments)
        return Queue(queue,
                     exchange=exchange,
                     routing_key=binding_key,
                     durable=q_durable,
                     exclusive=options.get('exclusive'),
                     auto_delete=q_auto_delete,
                     no_ack=options.get('no_ack'),
                     queue_arguments=q_arguments,
                     binding_arguments=b_arguments,
                     consumer_arguments=c_arguments,
                     bindings=bindings)

    def as_dict(self, recurse=False):
        res = super(Queue, self).as_dict(recurse)
        if not recurse:
            return res
        bindings = res.get('bindings')
        if bindings:
            res['bindings'] = [b.as_dict(recurse=True) for b in bindings]
        return res
