from kombu.abstract import MaybeChannelBound

TRANSIENT_DELIVERY_MODE = 1
PERSISTENT_DELIVERY_MODE = 2
DELIVERY_MODES = {"transient": TRANSIENT_DELIVERY_MODE,
                  "persistent": PERSISTENT_DELIVERY_MODE}


class Exchange(MaybeChannelBound):
    """An Exchange.

    :keyword name: See :attr:`name`.
    :keyword type: See :attr:`type`.
    :keyword channel: See :attr:`channel`.
    :keyword durable: See :attr:`durable`.
    :keyword auto_delete: See :attr:`auto_delete`.
    :keyword delivery_mode: See :attr:`delivery_mode`.
    :keyword arguments: See :attr:`arguments`.

    .. attribute:: name

        Name of the exchange. Default is no name (the default exchange).

    .. attribute:: type

        AMQP defines four default exchange types (routing algorithms) that
        covers most of the common messaging use cases. An AMQP broker can
        also define additional exchange types, so see your broker
        manual for more information about available exchange types.

            * ``direct`` (*default*)

                Direct match between the routing key in the message, and the
                routing criteria used when a queue is bound to this exchange.

            * ``topic``

                Wildcard match between the routing key and the routing pattern
                specified in the binding. The routing key is treated as zero
                or more words delimited by ``"."`` and supports special
                wildcard characters. ``"*"`` matches a single word and ``"#"``
                matches zero or more words.

            * ``fanout``

                Queues are bound to this exchange with no arguments. Hence any
                message sent to this exchange will be forwarded to all queues
                bound to this exchange.

            * ``headers``

                Queues are bound to this exchange with a table of arguments
                containing headers and values (optional). A special argument
                named "x-match" determines the matching algorithm, where
                ``"all"`` implies an ``AND`` (all pairs must match) and
                ``"any"`` implies ``OR`` (at least one pair must match).

                :attr:`arguments`` is used to specify the arguments.

            This description of AMQP exchange types was shamelessly stolen
            from the blog post `AMQP in 10 minutes: Part 4`_ by
            Rajith Attapattu. This article is recommended reading.

            .. _`AMQP in 10 minutes: Part 4`:
                http://bit.ly/amqp-exchange-types

    .. attribute:: channel

        The channel the exchange is bound to (if bound).

    .. attribute:: durable

        Durable exchanges remain active when a server restarts. Non-durable
        exchanges (transient exchanges) are purged when a server restarts.
        Default is ``True``.

    .. attribute:: auto_delete

        If set, the exchange is deleted when all queues have finished
        using it. Default is ``False``.

    .. attribute:: delivery_mode

        The default delivery mode used for messages. The value is an integer.

            * 1 or "transient"

                The message is transient. Which means it is stored in
                memory only, and is lost if the server dies or restarts.

            * 2 or "persistent" (*default*)
                The message is persistent. Which means the message is
                stored both in-memory, and on disk, and therefore
                preserved if the server dies or restarts.

        The default value is ``2`` (persistent).

    .. attribute:: arguments

        Additional arguments to specify when the exchange is declared.


    **Usage**

    Example creating an exchange declaration::

        >>> news_exchange = Exchange("news", type="topic")

    For now ``news_exchange`` is just a declaration, you can't perform
    actions on it. It just describes the name and options for the exchange.

    The exchange can be bound or unbound. Bound means the exchange is
    associated with a channel and operations can be performed on it.
    To bind the exchange you call the exchange with the channel as argument::

        >>> bound_exchange = news_exchange(channel)

    Now you can perform operations like :meth:`declare` or :meth:`delete`::

        >>> bound_exchange.declare()
        >>> message = bound_exchange.Message("Cure for cancer found!")
        >>> bound_exchange.publish(message, routing_key="news.science")
        >>> bound_exchange.delete()

    """
    TRANSIENT_DELIVERY_MODE = TRANSIENT_DELIVERY_MODE
    PERSISTENT_DELIVERY_MODE = PERSISTENT_DELIVERY_MODE
    name = ""
    type = "direct"
    durable = True
    auto_delete = False
    delivery_mode = PERSISTENT_DELIVERY_MODE

    attrs = (("name", None),
             ("type", None),
             ("arguments", None),
             ("durable", bool),
             ("auto_delete", bool),
             ("delivery_mode", lambda m: DELIVERY_MODES.get(m) or m))

    def __init__(self, name="", type="", channel=None, **kwargs):
        super(Exchange, self).__init__(**kwargs)
        self.name = name or self.name
        self.type = type or self.type
        self.maybe_bind(channel)

    def declare(self, nowait=False):
        """Declare the exchange.

        Creates the exchange on the broker.

        :keyword nowait: If set the server will not respond, and a
            response will not be waited for. Default is ``False``.

        """
        return self.channel.exchange_declare(exchange=self.name,
                                             type=self.type,
                                             durable=self.durable,
                                             auto_delete=self.auto_delete,
                                             arguments=self.arguments,
                                             nowait=nowait)

    def Message(self, body, delivery_mode=None,
                priority=None, content_type=None, content_encoding=None,
                properties=None, headers=None):
        """Create message instance to be sent with :meth:`publish`.

        :param body: Message body.

        :keyword delivery_mode: Set custom delivery mode. Defaults
            to :attr:`delivery_mode`.

        :keyword priority: Message priority, ``0`` to ``9``. (currently not
            supported by RabbitMQ).

        :keyword content_type: The messages content_type. If content_type
            is set, no serialization occurs as it is assumed this is either
            a binary object, or you've done your own serialization.
            Leave blank if using built-in serialization as our library
            properly sets content_type.

        :keyword content_encoding: The character set in which this object
            is encoded. Use "binary" if sending in raw binary objects.
            Leave blank if using built-in serialization as our library
            properly sets content_encoding.

        :keyword properties: Message properties.

        :keyword headers: Message headers.

        """
        properties = properties or {}
        properties["delivery_mode"] = delivery_mode or self.delivery_mode
        return self.channel.prepare_message(message_data,
                                            properties=properties,
                                            priority=priority,
                                            content_type=content_type,
                                            content_encoding=content_encoding,
                                            headers=headers)

    def publish(self, message, routing_key=None, mandatory=False,
            immediate=False):
        """Publish message.

        :param message: :meth:`Message` instance to publish.
        :param routing_key: Routing key.
        :param mandatory: Currently not supported.
        :param immediate: Currently not supported.

        """
        return self.channel.basic_publish(message,
                                          exchange=self.name,
                                          routing_key=routing_key,
                                          mandatory=mandatory,
                                          immediate=immediate)

    def delete(self, if_unused=False, nowait=False):
        """Delete the exchange declaration on server.

        :keyword if_unused: Delete only if the exchange has no bindings.
            Default is ``False``.

        :keyword nowait: If set the server will not respond, and a
            response will not be waited for. Default is ``False``.

        """
        return self.channel.exchange_delete(exchange=self.name,
                                            if_unused=if_unused,
                                            nowait=nowait)

    def __repr__(self):
        return super(Exchange, self).__repr__("Exchange %s(%s)" % (self.name,
                                                                   self.type))


class Binding(MaybeChannelBound):
    """A Queue declaration and its binding.

    :keyword name: See :attr:`name`.
    :keyword exchange: See :attr:`exchange`.
    :keyword routing_key: See :attr:`routing_key`.
    :keyword channel: See :attr:`channel`.
    :keyword durable: See :attr:`durable`.
    :keyword exclusive: See :attr:`exclusive`.
    :keyword auto_delete: See :attr:`auto_delete`.
    :keyword queue_arguments: See :attr:`queue_arguments`.
    :keyword binding_arguments: See :attr:`binding_arguments`.

    .. attribute:: name

        Name of the queue. Default is no name (default queue destination).

    .. attribute:: exchange

        The :class:`Exchange` the queue binds to.

    .. attribute:: routing_key

        The routing key (if any), also called *binding key*.

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
                of words separated by dots (``"."``, like domain names), and
                two special characters are available; star (``"*"``) and hash
                (``"#"``). The star matches any word, and the hash matches
                zero or more words. For example ``"*.stock.#"`` matches the
                routing keys ``"usd.stock"`` and ``"eur.stock.db"`` but not
                ``"stock.nasdaq"``.

    .. attribute:: channel

        The channel the Binding is bound to (if bound).

    .. attribute:: durable

        Durable queues remain active when a server restarts.
        Non-durable queues (transient queues) are purged if/when
        a server restarts.
        Note that durable queues do not necessarily hold persistent
        messages, although it does not make sense to send
        persistent messages to a transient queue.

        Default is ``True``.

    .. attribute:: exclusive

        Exclusive queues may only be consumed from by the
        current connection. Setting the 'exclusive' flag
        always implies 'auto-delete'.

        Default is ``False``.

    .. attribute:: auto_delete

        If set, the queue is deleted when all consumers have
        finished using it. Last consumer can be cancelled
        either explicitly or because its channel is closed. If
        there was no consumer ever on the queue, it won't be
        deleted.

    .. attribute:: queue_arguments

        Additional arguments used when declaring the queue.

    .. attribute:: binding_arguments

        Additional arguments used when binding the queue.

    **Usage**

    Example creating a binding for our exchange in the :class:`Exchange`
    example::

        >>> science_news = Binding("science_news",
        ...                        exchange=news_exchange,
        ...                        routing_key="news.science")

    For now ``science_news`` is just a declaration, you can't perform
    actions on it. It just describes the name and options for the binding.

    The binding can be bound or unbound. Bound means the binding is
    associated with a channel and operations can be performed on it.
    To bind the binding you call the binding with the channel as argument::

        >>> bound_science_news = science_news(channel)

    Now you can perform operations like :meth:`declare` or :meth:`purge`::

        >>> bound_sicence_news.declare()
        >>> bound_science_news.purge()
        >>> bound_science_news.delete()
    """

    name = ""
    exchange = None
    routing_key = ""

    durable = True
    exclusive = False
    auto_delete = False

    attrs = (("name", None),
             ("exchange", None),
             ("routing_key", None),
             ("queue_arguments", None),
             ("binding_arguments", None),
             ("durable", bool),
             ("exclusive", bool),
             ("auto_delete", bool))

    def __init__(self, name="", exchange=None, routing_key="", channel=None,
            **kwargs):
        super(Binding, self).__init__(**kwargs)
        self.name = name or self.name
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        # exclusive implies auto-delete.
        if self.exclusive:
            self.auto_delete = True
        self.maybe_bind(channel)

    def when_bound(self):
        self.exchange = self.exchange(self.channel)

    def declare(self, nowait=False):
        """Declares the queue, the exchange and binds the queue to
        the exchange."""
        return (self.exchange and self.exchange.declare(nowait),
                self.name and self.queue_declare(nowait, passive=False),
                self.name and self.queue_bind(nowait))

    def queue_declare(self, nowait=False, passive=False):
        """Declare queue on the server.

        :keyword nowait: Do not wait for a reply.
        :keyword passive: If set, the server will not create the queue. 
            The client can use this to check whether a queue exists
            without modifying the server state.

        """
        return self.channel.queue_declare(queue=self.name,
                                          passive=passive,
                                          durable=self.durable,
                                          exclusive=self.exclusive,
                                          auto_delete=self.auto_delete,
                                          arguments=self.queue_arguments,
                                          nowait=nowait)

    def queue_bind(self, nowait=False):
        """Create the queue binding on the server.

        :keyword nowait: Do not wait for a reply.

        """
        return self.channel.queue_bind(queue=self.name,
                                       exchange=self.exchange.name,
                                       routing_key=self.routing_key,
                                       arguments=self.binding_arguments,
                                       nowait=nowait)


    def get(self, no_ack=None):
        """Poll the server for a new message.

        Returns the message instance if a message was available,
        or :const:`None` otherwise.

        :keyword no_ack: If set messages received does not have to
            be acknowledged.

        This method provides a direct access to the messages in a
        queue using a synchronous dialogue that is designed for
        specific types of application where synchronous functionality
        is more important than performance.

        """
        message = self.channel.basic_get(queue=self.name, no_ack=no_ack)
        if message is not None:
            return self.channel.message_to_python(message)

    def purge(self, nowait=False):
        """Remove all messages from the queue."""
        return self.channel.queue_purge(queue=self.name, nowait=nowait) or 0

    def consume(self, consumer_tag=None, callback=None, no_ack=None,
            nowait=False):
        """Start a queue consumer.

        Consumers last as long as the channel they were created on, or
        until the client cancels them.

        :keyword consumer_tag: Unique identifier for the consumer. The
          consumer tag is local to a connection, so two clients
          can use the same consumer tags. If this field is empty
          the server will generate a unique tag.

        :keyword no_ack: If set messages received does not have to
            be acknowledged.

        :keyword nowait: Do not wait for a reply.

        :keyword callback: callback called for each delivered message

        """
        return self.channel.basic_consume(queue=self.name,
                                          no_ack=no_ack,
                                          consumer_tag=consumer_tag,
                                          callback=callback,
                                          nowait=nowait)

    def cancel(self, consumer_tag):
        """Cancel a consumer by consumer tag."""
        return self.channel.basic_cancel(consumer_tag)

    def delete(self, if_unused=False, if_empty=False, nowait=False):
        """Delete the queue.

        :keyword if_unused: If set, the server will only delete the queue
            if it has no consumers. A channel error will be raised
            if the queue has consumers.

        :keyword if_empty: If set, the server will only delete the queue
            if it is empty. If if's not empty a channel error will be raised.

        :keyword nowait: Do not wait for a reply.

        """
        return self.channel.queue_delete(queue=self.name,
                                         if_unused=if_unused,
                                         if_empty=if_empty,
                                         nowait=nowait)

    def unbind(self):
        """Delete the binding on the server."""
        return self.channel.queue_unbind(queue=self.name,
                                         exchange=self.exchange.name,
                                         routing_key=self.routing_key,
                                         arguments=self.binding_arguments)

    def __repr__(self):
        return super(Binding, self).__repr__(
                 "Binding %s -> %s -> %s" % (self.name,
                                             self.exchange,
                                             self.routing_key))
