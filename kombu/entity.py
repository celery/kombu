from copy import copy


def assert_is_bound(fun):

    def only_if_bound(self, *args, **kwargs):
        if self.is_bound:
            return fun(*args, **kwargs)
        raise NotBoundError("Can't call %s on unbound %s" % (
            fun.__name__, self.__class__.__name__))
    only_if_bound.__name__ = fun.__name__

    return only_if_bound


class MaybeChannelBound(object):
    """Mixin for classes that can be bound to an AMQP channel."""
    channel = None

    def bind(self, channel):
        """Create copy of the instance that is bound to a channel."""
        return copy(self).maybe_bind(channel)

    def maybe_bind(self, channel):
        """Bind instance to channel if not already bound."""
        if not self.is_bound:
            self.channel = channel
            self.when_bound()
        return self

    def when_bound(self):
        """Callback called when the class is bound."""
        pass

    @property
    def is_bound(self):
        """Returns ``True`` if the entity is bound."""
        return self.channel is not None

    def __repr__(self, item=""):
        if self.is_bound:
            return "<bound %s of %s>" % (item or self.__class__.__name__,
                                         self.channel)
        return "<unbound %s>" % (item, )


class Exchange(MaybeChannelBound):
    TRANSIENT_DELIVERY_MODE = 1
    PERSISTENT_DELIVERY_MODE = 2
    DELIVERY_MODES = {
        "transient": TRANSIENT_DELIVERY_MODE,
        "persistent": PERSISTENT_DELIVERY_MODE,
    }
    name = ""
    type = "direct"
    routing_key = ""
    delivery_mode = PERSISTENT_DELIVERY_MODE
    durable = True
    auto_delete = False
    _init_opts = ("durable", "auto_delete",
                  "delivery_mode", "auto_declare")

    def __init__(self, name="", type="", routing_key=None, channel=None,
            **kwargs):
        self.name = name or self.name
        self.type = type or self.type
        self.routing_key = routing_key or self.routing_key
        self.maybe_bind(channel)

        for opt_name in self._init_opts:
            opt_value = kwargs.get(opt_name)
            if opt_value is not None:
                setattr(self, opt_name, opt_value)

        self.delivery_mode = self.DELIVERY_MODES.get(self.delivery_mode,
                                                     self.delivery_mode)

    @assert_is_bound
    def declare(self):
        """Declare the exchange.

        Creates the exchange on the broker.

        """
        self.channel.exchange_declare(exchange=self.name,
                                      type=self.type,
                                      durable=self.durable,
                                      auto_delete=self.auto_delete)

    @assert_is_bound
    def create_message(self, message_data, delivery_mode=None,
                priority=None, content_type=None, content_encoding=None,
                properties=None):
        properties["delivery_mode"] = delivery_mode or self.delivery_mode
        return self.channel.prepare_message(message_data,
                                            properties=properties,
                                            priority=priority,
                                            content_type=content_type,
                                            content_encoding=content_encoding)

    @assert_is_bound
    def publish(self, message, routing_key=None,
            mandatory=False, immediate=False):
        if routing_key is None:
            routing_key = self.routing_key
        self.channel.basic_publish(message,
                                   exchange=self.name,
                                   routing_key=routing_key,
                                   mandatory=mandatory,
                                   immediate=immediate,
                                   headers=headers)

    def __copy__(self):
        return self.__class__(name=self.name,
                              type=self.type,
                              routing_key=self.routing_key,
                              channel=self.channel,
                              **dict((name, getattr(self, name))
                                        for name in self._init_opts))

    def __repr__(self):
        super(Exchange, self).__repr__("Exchange %s(%s)" % (self.name,
                                                            self.type))


class Binding(object):
    name = ""
    exchange = None
    routing_key = ""

    durable = True
    exclusive = False
    auto_delete = False
    warn_if_exists = False
    _init_opts = ("durable", "exclusive", "auto_delete",
                  "warn_if_exists")

    def __init__(self, name=None, exchange=None, routing_key=None,
            channel=None, **kwargs):
        # Binding.
        self.name = name or self.name
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.maybe_bind(channel)

        # Options
        for opt_name in self._init_opts:
            opt_value = kwargs.get(opt_name)
            if opt_value is not None:
                setattr(self, opt_name, opt_value)

        # exclusive implies auto-delete.
        if self.exclusive:
            self.auto_delete = True

    def when_bound(self):
        self.exchange = self.exchange.bind(self.channel)

    @assert_is_bound
    def declare(self):
        """Declares the queue, the exchange and binds the queue to
        the exchange."""
        if self.exchange:
            self.exchange.declare()
        if self.name:
            self.channel.queue_declare(queue=self.name,
                                       durable=self.durable,
                                       exclusive=self.exclusive,
                                       auto_delete=self.auto_delete)
            self.channel.queue_bind(queue=self.name,
                                    exchange=self.exchange.name,
                                    routing_key=self.routing_key)

    @assert_is_bound
    def get(self, no_ack=None):
        message = self.channel.basic_get(self.name, no_ack=no_ack)
        if message:
            return self.channel.message_to_python(message)

    @assert_is_bound
    def purge(self):
        return self.channel.queue_purge(self.name)

    @assert_is_bound
    def consume(self, consumer_tag, callback, no_ack=None, nowait=True):
        return self.channel.consume(queue=self.name,
                                    no_ack=no_ack,
                                    consumer_tag=consumer_tag,
                                    callback=callback,
                                    nowait=nowait)

    @assert_is_bound
    def cancel(self, consumer_tag):
        self.channel.basic_cancel(consumer_tag)

    def __copy__(self):
        return self.__class__(name=self.name,
                              exchange=self.exchange,
                              routing_key=self.routing_key,
                              channel=self.channel,
                              **dict((name, getattr(self, name)
                                            for name in self._init_opts)))

    def __repr__(self):
        super(Binding, self).__repr__(
                "Binding %s -> %s -> %s" % (self.name,
                                            self.exchange,
                                            self.routing_key))
