from kombu.abstract import MaybeChannelBound


TRANSIENT_DELIVERY_MODE = 1
PERSISTENT_DELIVERY_MODE = 2
DELIVERY_MODES = {"transient": TRANSIENT_DELIVERY_MODE,
                  "persistent": PERSISTENT_DELIVERY_MODE}


class Exchange(MaybeChannelBound):
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

        """
        return self.channel.exchange_declare(exchange=self.name,
                                             type=self.type,
                                             durable=self.durable,
                                             auto_delete=self.auto_delete,
                                             arguments=self.arguments,
                                             nowait=nowait)

    def create_message(self, message_data, delivery_mode=None,
                priority=None, content_type=None, content_encoding=None,
                properties=None, headers=None):
        properties = properties or {}
        properties["delivery_mode"] = delivery_mode or self.delivery_mode
        return self.channel.prepare_message(message_data,
                                            properties=properties,
                                            priority=priority,
                                            content_type=content_type,
                                            content_encoding=content_encoding,
                                            headers=headers)

    def publish(self, message, routing_key=None, mandatory=False,
            immediate=False, headers=None):
        return self.channel.basic_publish(message,
                                          exchange=self.name,
                                          routing_key=routing_key,
                                          mandatory=mandatory,
                                          immediate=immediate)

    def delete(self, if_unused=False, nowait=False):
        return self.channel.exchange_delete(exchange=self.name,
                                            if_unused=if_unused,
                                            nowait=nowait)

    def __repr__(self):
        return super(Exchange, self).__repr__("Exchange %s(%s)" % (self.name,
                                                                   self.type))


class Binding(MaybeChannelBound):
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
        chan = self.channel
        return (self.exchange and self.exchange.declare(),
                self.name and chan.queue_declare(queue=self.name,
                                            durable=self.durable,
                                            exclusive=self.exclusive,
                                            auto_delete=self.auto_delete,
                                            arguments=self.queue_arguments,
                                            nowait=nowait),
                self.name and chan.queue_bind(queue=self.name,
                                            exchange=self.exchange.name,
                                            routing_key=self.routing_key,
                                            arguments=self.binding_arguments,
                                            nowait=nowait))

    def get(self, no_ack=None):
        message = self.channel.basic_get(queue=self.name, no_ack=no_ack)
        if message is not None:
            return self.channel.message_to_python(message)

    def purge(self, nowait=False):
        return self.channel.queue_purge(queue=self.name, nowait=nowait) or 0

    def consume(self, consumer_tag, callback, no_ack=None, nowait=False):
        return self.channel.basic_consume(queue=self.name,
                                          no_ack=no_ack,
                                          consumer_tag=consumer_tag,
                                          callback=callback,
                                          nowait=nowait)

    def cancel(self, consumer_tag):
        return self.channel.basic_cancel(consumer_tag)

    def delete(self, if_unused=False, if_empty=False, nowait=False):
        return self.channel.queue_delete(queue=self.name,
                                         if_unused=if_unused,
                                         if_empty=if_empty,
                                         nowait=nowait)

    def unbind(self):
        return self.channel.queue_unbind(queue=self.name,
                                         exchange=self.exchange.name,
                                         routing_key=self.routing_key,
                                         arguments=self.binding_arguments)

    def __repr__(self):
        return super(Binding, self).__repr__(
                 "Binding %s -> %s -> %s" % (self.name,
                                             self.exchange,
                                             self.routing_key))
