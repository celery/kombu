class Exchange(object):
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

    def __init__(self, name="", type="", routing_key=None, **kwargs):
        self.name = name or self.name
        self.type = type or self.type
        self.routing_key = routing_key or self.routing_key
        for opt_name in self._init_opts:
            opt_value = kwargs.get(opt_name)
            if opt_value is not None:
                setattr(self, opt_name, opt_value)
        self.delivery_mode = self.DELIVERY_MODES.get(self.delivery_mode,
                                                     self.delivery_mode)

    def declare(self, channel):
        """Declare the exchange.

        Creates the exchange on the broker.

        """
        channel.exchange_declare(exchange=self.name,
                                 type=self.type,
                                 durable=self.durable,
                                 auto_delete=self.auto_delete)

    def create_message(self, channel, message_data, delivery_mode=None,
                priority=None, content_type=None, content_encoding=None,
                properties=None):
        properties["delivery_mode"] = delivery_mode or self.delivery_mode
        return channel.prepare_message(message_data,
                                       properties=properties,
                                       priority=priority,
                                       content_type=content_type,
                                       content_encoding=content_encoding)

    def publish(self, channel, message, routing_key=None,
            mandatory=False, immediate=False):
        if routing_key is None:
            routing_key = self.routing_key
        channel.basic_publish(message,
                        exchange=self.name, routing_key=routing_key,
                        mandatory=mandatory, immediate=immediate,
                        headers=headers)


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

    def __init__(self, name=None, exchange=None, routing_key=None, **kwargs):
        # Binding.
        self.name = name or self.name
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key

        # Options
        for opt_name in self._init_opts:
            opt_value = kwargs.get(opt_name)
            if opt_value is not None:
                setattr(self, opt_name, opt_value)

        # exclusive implies auto-delete.
        if self.exclusive:
            self.auto_delete = True

    def declare(self, channel):
        """Declares the queue, the exchange and binds the queue to
        the exchange."""
        if self.exchange:
            self.exchange.declare(channel)
        if self.name:
            channel.queue_declare(queue=self.name, durable=self.durable,
                                  exclusive=self.exclusive,
                                  auto_delete=self.auto_delete)
            channel.queue_bind(queue=self.name,
                               exchange=self.exchange.name,
                               routing_key=self.routing_key)

    def get(self, channel, no_ack=None):
        message = channel.basic_get(self.name, no_ack=no_ack)
        if message:
            return channel.message_to_python(message)

    def purge(self, channel):
        return channel.queue_purge(self.name)

    def consume(self, channel, consumer_tag, callback, no_ack=None,
            nowait=True):
        return channel.consume(queue=self.name, no_ack=no_ack,
                                        consumer_tag=consumer_tag,
                                        callback=callback,
                                        nowait=nowait)

    def cancel(self, channel, consumer_tag):
        channel.basic_cancel(consumer_tag)
