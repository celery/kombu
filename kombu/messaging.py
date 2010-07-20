from itertools import count

from kombu import serialization
from kombu.entity import Exchange, Binding
from kombu.utils import maybe_list


class Producer(object):
    exchange = None
    serializer = None
    auto_declare = True

    def __init__(self, channel, exchange=None, serializer=None,
            auto_declare=None):
        self.channel = channel
        self.exchange = exchange or self.exchange
        self.serializer = serializer or self.serializer
        if auto_declare is not None:
            self.auto_declare = auto_declare

        if self.exchange:
            self.exchange = self.exchange(self.channel)
            self.auto_declare and self.declare()

    def declare(self):
        self.exchange.declare()

    def prepare(self, message_data, serializer=None,
            content_type=None, content_encoding=None):
        # No content_type? Then we're serializing the data internally.
        if not content_type:
            serializer = serializer or self.serializer
            (content_type, content_encoding,
             message_data) = serialization.encode(message_data,
                                                  serializer=serializer)
        else:
            # If the programmer doesn't want us to serialize,
            # make sure content_encoding is set.
            if isinstance(message_data, unicode):
                if not content_encoding:
                    content_encoding = 'utf-8'
                message_data = message_data.encode(content_encoding)

            # If they passed in a string, we can't know anything
            # about it.  So assume it's binary data.
            elif not content_encoding:
                content_encoding = 'binary'

        return message_data, content_type, content_encoding

    def publish(self, message_data, routing_key=None, delivery_mode=None,
            mandatory=False, immediate=False, priority=0, content_type=None,
            content_encoding=None, serializer=None, headers=None):

        message_data, content_type, content_encoding = self.prepare(
                message_data, content_type, content_encoding)
        message = self.exchange.create_message(message_data,
                                               delivery_mode,
                                               priority,
                                               content_type,
                                               content_encoding,
                                               headers=headers)
        return self.exchange.publish(message, routing_key, mandatory,
                                     immediate)



class Consumer(object):
    no_ack = False
    auto_declare = True
    callbacks = None
    on_decode_error = None
    _next_tag = count(1).next # global

    def __init__(self, channel, bindings, no_ack=None, auto_declare=None,
            callbacks=None):
        self.channel = channel
        self.bindings = bindings
        if no_ack is not None:
            self.no_ack = no_ack
        if auto_declare is not None:
            self.auto_declare = auto_declare

        if callbacks is not None:
            self.callbacks = callbacks
        if self.callbacks is None:
            self.callbacks = []
        self._active_tags = {}

        self.bindings = [binding(self.channel)
                            for binding in maybe_list(self.bindings)]

        if self.auto_declare:
            self.declare()
            self.consume()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.cancel()

    def declare(self):
        for binding in self.bindings:
            binding.declare()

    def consume(self):
        H, T = self.bindings[:-1], self.bindings[-1]
        for binding in H:
            binding.consume(self._add_tag(binding),
                            self._receive_callback,
                            self.no_ack,
                            nowait=True)
        T.consume(self._add_tag(T),
                  self._receive_callback,
                  self.no_ack,
                  nowait=False)

    def _add_tag(self, binding):
        tag = self._active_tags[binding] = str(self._next_tag())
        return tag

    def _receive_callback(self, raw_message):
        message = self.channel.message_to_python(raw_message)
        try:
            decoded = message.payload
        except Exception, exc:
            if self.on_decode_error:
                return self.on_decode_error(message, exc)
            else:
                raise
        self.receive(decoded, message)

    def receive(self, message_data, message):
        if not self.callbacks:
            raise NotImplementedError("No consumer callbacks registered")
        for callback in self.callbacks:
            callback(message_data, message)

    def register_callback(self, callback):
        self.callbacks.append(callback)

    def purge(self):
        for binding in self.bindings:
            binding.purge()

    def cancel(self):
        for binding, tag in self._active_tags.items():
            binding.cancel(tag)
        self._active_tags = {}

    def flow(self, active):
        self.channel.flow(active)

    def qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
        return self.channel.basic_qos(prefetch_size,
                                      prefetch_count,
                                      apply_global)

    def recover(self, requeue=False):
        return self.channel.basic_recover(requeue)
