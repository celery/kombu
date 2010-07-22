from itertools import count

from kombu import entity
from kombu import messaging


def iterconsume(connection, consumer, no_ack=False, limit=None):
    consumer.consume(no_ack=no_ack)
    for iteration in count(0):
        if limit and iteration >= limit:
            raise StopIteration
        yield connection.drain_events()


def entry_to_binding(queue, **options):
    binding_key = options.get("binding_key") or options.get("routing_key")
    e_durable = options.get("exchange_durable") or options.get("durable")
    e_auto_delete = options.get("exchange_auto_delete") or \
                        options.get("auto_delete")
    q_durable = options.get("queue_durable") or options.get("durable")
    q_auto_delete = options.get("queue_auto_delete") or \
                        options.get("auto_delete")
    e_arguments = options.get("exchange_arguments")
    q_arguments = options.get("queue_arguments")
    b_arguments = options.get("binding_arguments")
    exchange = entity.Exchange(options.get("exchange"),
                               type=options.get("exchange_type"),
                               delivery_mode=options.get("delivery_mode"),
                               routing_key=options.get("routing_key"),
                               durable=e_durable,
                               auto_delete=e_auto_delete,
                               arguments=e_arguments)
    return entity.Binding(queue,
                          exchange=exchange,
                          routing_key=binding_key,
                          durable=q_durable,
                          exclusive=options.get("exclusive"),
                          auto_delete=q_auto_delete,
                          queue_arguments=q_arguments,
                          binding_arguments=b_arguments)


class Publisher(messaging.Producer):
    exchange = ""
    exchange_type = "direct"
    routing_key = ""
    durable = True
    auto_delete = False
    _closed = False

    def __init__(self, connection, exchange=None, routing_key=None,
            exchange_type=None, durable=None, auto_delete=None, **kwargs):
        self.connection = connection
        self.backend = connection.channel()

        self.exchange = exchange or self.exchange
        self.exchange_type = exchange_type or self.exchange_type
        self.routing_key = routing_key or self.routing_key

        if auto_delete is not None:
            self.auto_delete = auto_delete
        if durable is not None:
            self.durable = durable

        if not isinstance(self.exchange, entity.Exchange):
            self.exchange = entity.Exchange(name=self.exchange,
                                            type=self.exchange_type,
                                            routing_key=self.routing_key,
                                            auto_delete=self.auto_delete,
                                            durable=self.durable)

        super(Publisher, self).__init__(self.backend, self.exchange,
                **kwargs)

    def send(self, *args, **kwargs):
        return self.publish(*args, **kwargs)

    def close(self):
        self.backend.close()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class Consumer(messaging.Consumer):
    queue = ""
    exchange = ""
    routing_key = ""
    exchange_type = "direct"
    durable = True
    exclusive = False
    auto_delete = False
    exchange_type = "direct"
    _closed = False

    def __init__(self, connection, queue=None, exchange=None,
            routing_key=None, exchange_type=None, durable=None,
            exclusive=None, auto_delete=None, **kwargs):
        self.connection = connection
        self.backend = connection.channel()

        if durable is not None:
            self.durable = durable
        if exclusive is not None:
            self.exclusive = exclusive
        if auto_delete is not None:
            self.auto_delete = auto_delete

        self.queue = queue or self.queue
        self.exchange = exchange or self.exchange
        self.exchange_type = exchange_type or self.exchange_type
        self.routing_key = routing_key or self.routing_key

        exchange = entity.Exchange(self.exchange,
                                   type=self.exchange_type,
                                   routing_key=self.routing_key,
                                   auto_delete=self.auto_delete,
                                   durable=self.durable)
        binding = entity.Binding(self.queue,
                                 exchange=exchange,
                                 routing_key=self.routing_key,
                                 durable=self.durable,
                                 exclusive=self.exclusive,
                                 auto_delete=self.auto_delete)
        super(Consumer, self).__init__(self.backend, binding, **kwargs)

    def close(self):
        self.cancel()
        self.backend.close()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __iter__(self):
        return self.iterqueue(infinite=True)

    def fetch(self, no_ack=None, enable_callbacks=False):
        if no_ack is None:
            no_ack = self.no_ack
        message = self.bindings[0].get(no_ack)
        if message:
            if enable_callbacks:
                self.receive(message.payload, message)
        return message

    def process_next(self):
        raise NotImplementedError("Use fetch(enable_callbacks=True)")

    def discard_all(self, filterfunc=None):
        if filterfunc is not None:
            raise NotImplementedError(
                    "discard_all does not implement filters")
        return self.purge()

    def iterconsume(self, limit=None, no_ack=None):
        return iterconsume(self.connection, self, no_ack, limit)

    def wait(self, limit=None):
        it = self.iterconsume(limit)
        return list(it)

    def iterqueue(self, limit=None, infinite=False):
        for items_since_start in count():
            item = self.fetch()
            if (not infinite and item is None) or \
                    (limit and items_since_start >= limit):
                raise StopIteration
            yield item


class _CSet(messaging.Consumer):

    def __init__(self, connection, *args, **kwargs):
        self.connection = connection
        self.backend = connection.channel()
        super(_CSet, self).__init__(self.backend, *args, **kwargs)

    def iterconsume(self, limit=None, no_ack=False):
        return iterconsume(self.connection, self, no_ack, limit)

    def discard_all(self):
        return self.purge()

    def add_consumer_from_dict(self, queue, **options):
        self.bindings.append(entry_to_binding(queue, **options))

    def add_consumer(self, consumer):
        self.bindings.extend(consumer.bindings)

    def close(self):
        self.cancel()
        self.channel.close()


def ConsumerSet(connection, from_dict=None, consumers=None,
        callbacks=None, **kwargs):

    bindings = []
    if consumers:
        for consumer in consumers:
            map(bindings.extend, consumer.bindings)
    if from_dict:
        for queue_name, queue_options in from_dict.items():
            bindings.append(entry_to_binding(queue_name, **queue_options))

    return _CSet(connection, bindings, **kwargs)
