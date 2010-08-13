from itertools import count

from kombu import serialization
from kombu.compression import compress
from kombu.entity import Exchange, Queue
from kombu.entity import Binding # TODO Remove
from kombu.utils import maybe_list


class Producer(object):
    """Message Producer.

    :param channel: Connection channel.
    :keyword exchange: Exchange to publish to.
    :keyword routing_key: Default routing key.
    :keyword serializer: Default serializer. Default is ``"json"``.
    :keyword compression: Default compression method. Default is no
        compression.
    :keyword auto_declare: Automatically declare the exchange
      at instantiation. Default is ``True``.
    :keyword on_return: Callback to call for undeliverable messages,
        when ``mandatory`` or ``imediate`` is used. This callback
        needs the following signature:
        ``(exception, exchange, routing_key, message)``.

    .. attribute:: channel

        The connection channel to use.

    .. attribute:: exchange

        Exchange to publish to.

    .. attribute:: routing_key

        Default routing key.

    .. attribute:: serializer

        Default serializer to use. Default is autodetect.

    .. attribute:: auto_declare

        By default the exchange is declared at instantiation.
        If you want to declare manually you can set this to ``False``.

    """
    exchange = Exchange("")
    serializer = None
    auto_declare = True
    routing_key = ""
    compression = None
    on_return = None

    def __init__(self, channel, exchange=None, routing_key=None,
            serializer=None, auto_declare=None, compression=None,
            on_return=None):
        self.channel = channel
        self.exchange = exchange or self.exchange
        self.routing_key = routing_key or self.routing_key
        self.serializer = serializer or self.serializer
        self.compression = compression or self.compression
        self.on_return = on_return or self.on_return
        if auto_declare is not None:
            self.auto_declare = auto_declare

        if self.exchange:
            self.exchange = self.exchange(self.channel)
            self.auto_declare and self.declare()

        if self.on_return:
            self.channel.events["basic_return"].append(self.on_return)

    def declare(self):
        """Declare the exchange.

        This is done automatically at instantiation if :attr:`auto_declare`
        is set.

        """
        if self.exchange.name:
            self.exchange.declare()

    def _prepare(self, body, serializer=None,
            content_type=None, content_encoding=None, compression=None,
            headers=None):

        # No content_type? Then we're serializing the data internally.
        if not content_type:
            serializer = serializer or self.serializer
            (content_type, content_encoding,
             body) = serialization.encode(body, serializer=serializer)
        else:
            # If the programmer doesn't want us to serialize,
            # make sure content_encoding is set.
            if isinstance(body, unicode):
                if not content_encoding:
                    content_encoding = 'utf-8'
                body = body.encode(content_encoding)

            # If they passed in a string, we can't know anything
            # about it. So assume it's binary data.
            elif not content_encoding:
                content_encoding = 'binary'

        if compression:
            body, headers["compression"] = compress(body, compression)

        return body, content_type, content_encoding

    def publish(self, body, routing_key=None, delivery_mode=None,
            mandatory=False, immediate=False, priority=0, content_type=None,
            content_encoding=None, serializer=None, headers=None,
            compression=None):
        """Publish message to the specified exchange.

        :param body: Message body.
        :keyword routing_key: Message routing key.
        :keyword delivery_mode: See :attr:`delivery_mode`.
        :keyword mandatory: Currently not supported.
        :keyword immediate: Currently not supported.
        :keyword priority: Message priority. A number between 0 and 9.
        :keyword content_type: Content type. Default is autodetect.
        :keyword content_encoding: Content encoding. Default is autodetect.
        :keyword serializer: Serializer to use. Default is autodetect.
        :keyword headers: Mapping of arbitrary headers to pass along
          with the message body.

        """
        headers = headers or {}
        if routing_key is None:
            routing_key = self.routing_key
        if compression is None:
            compression = self.compression

        body, content_type, content_encoding = self._prepare(
                body, serializer, content_type, content_encoding,
                compression, headers)
        message = self.exchange.Message(body,
                                        delivery_mode,
                                        priority,
                                        content_type,
                                        content_encoding,
                                        headers=headers)
        return self.exchange.publish(message, routing_key, mandatory,
                                     immediate)


class Consumer(object):
    """Message consumer.

    :param channel: see :attr:`channel`.
    :param queues see :attr:`queues`.
    :keyword no_ack: see :attr:`no_ack`.
    :keyword auto_declare: see :attr:`auto_declare`
    :keyword callbacks: see :attr:`callbacks`.
    :keyword on_decode_error: see :attr:`on_decode_error`.

    .. attribute:: channel

        The connection channel to use.

    .. attribute:: queues

        A single :class:`~kombu.entity.Queue`, or a list of queues to
        consume from.

    .. attribute:: auto_declare

        By default the entities will be declared at instantiation,
        if you want to handle this manually you can set this to ``False``.

    .. attribute:: callbacks

        List of callbacks called in order when a message is received.

        The signature of the callbacks must take two arguments:
        ``(body, message)``, which is the decoded message body and
        the ``Message`` instance (a subclass of
        :class:`~kombu.transport.base.Message`).

    .. attribute:: on_decode_error

        Callback called when a message can't be decoded.

        The signature of the callback must take two arguments: ``(message,
        exc)``, which is the message that can't be decoded and the exception
        that occured while trying to decode it.

    """
    no_ack = False
    auto_declare = True
    callbacks = None
    on_decode_error = None
    _next_tag = count(1).next # global
    _consuming = False

    def __init__(self, channel, queues, no_ack=None, auto_declare=None,
            callbacks=None, on_decode_error=None):
        self.channel = channel
        self.queues = queues
        if no_ack is not None:
            self.no_ack = no_ack
        if auto_declare is not None:
            self.auto_declare = auto_declare
        if on_decode_error is not None:
            self.on_decode_error = on_decode_error

        if callbacks is not None:
            self.callbacks = callbacks
        if self.callbacks is None:
            self.callbacks = []
        self._active_tags = {}

        self.queues = [queue(self.channel)
                            for queue in maybe_list(self.queues)]

        if self.auto_declare:
            self.declare()

    def declare(self):
        """Declare queues, exchanges and bindings.

        This is done automatically at instantiation if :attr:`auto_declare`
        is set.

        """
        for queue in self.queues:
            queue.declare()

    def consume(self, delivery_tag=None, no_ack=None):
        """Register consumer on server.

        :keyword: delivery_tag: Unique delivery tag for this channel.
           If not specified a new tag will be automatically generated.

        """
        if no_ack is None:
            no_ack = self.no_ack
        if not self._consuming:
            H, T = self.queues[:-1], self.queues[-1]
            for queue in H:
                queue.consume(self._add_tag(queue, delivery_tag),
                                self._receive_callback,
                                no_ack=no_ack,
                                nowait=True)
            T.consume(self._add_tag(T),
                    self._receive_callback,
                    no_ack=no_ack,
                    nowait=False)
            self._consuming = False

    def receive(self, body, message):
        """Method called when a message is received.

        This dispatches to the registered :attr:`callbacks`.

        :param body: The decoded message body.
        :param message: The ``Message`` instance.

        :raises NotImplementedError: If no consumer callbacks have been
          registered.

        """
        if not self.callbacks:
            raise NotImplementedError("No consumer callbacks registered")
        for callback in self.callbacks:
            callback(body, message)

    def register_callback(self, callback):
        """Register a new callback to be called when a message
        is received.

        The signature of the callback needs to accept two arguments:
        ``(body, message)``, which is the decoded message body
        and the ``Message`` instance (a subclass of
        :class:`~kombu.transport.base.Message`.

        """
        self.callbacks.append(callback)

    def purge(self):
        """Purge messages from all queues.

        **WARNING**: This will *delete all ready messages*, there is no
        undo operation available.

        """
        return sum(queue.purge() for queue in self.queues)

    def cancel(self):
        """End all active queue consumers.

        This does not affect already delivered messages, but it does
        mean the server will not send any more messages for this consumer.

        """
        for queue, tag in self._active_tags.items():
            queue.cancel(tag)
        self._active_tags.clear()
        self._consuming = False

    def flow(self, active):
        """Enable/disable flow from peer.

        This is a simple flow-control mechanism that a peer can use
        to avoid overflowing its queues or otherwise finding itself
        receiving more messages than it can process.

        The peer that receives a request to stop sending content
        will finish sending the current content (if any), and then wait
        until flow is reactivated.

        """
        self.channel.flow(active)

    def qos(self, prefetch_size=0, prefetch_count=0, apply_global=False):
        """Specify quality of service.

        The client can request that messages should be sent in
        advance so that when the client finishes processing a message,
        the following message is already held locally, rather than needing
        to be sent down the channel. Prefetching gives a performance
        improvement.

        The prefetch window is Ignored if the :attr:`no_ack` option is set.

        :param prefetch_size: Specify the prefetch window in octets.
          The server will send a message in advance if it is equal to
          or smaller in size than the available prefetch size (and
          also falls within other prefetch limits). May be set to zero,
          meaning "no specific limit", although other prefetch limits
          may still apply.

        :param prefetch_count: Specify the prefetch window in terms of
          whole messages.

        :param apply_global: Apply new settings globally on all channels.
          Currently not supported by RabbitMQ.

        """
        return self.channel.basic_qos(prefetch_size,
                                      prefetch_count,
                                      apply_global)

    def recover(self, requeue=False):
        """Redeliver unacknowledged messages.

        Asks the broker to redeliver all unacknowledged messages
        on the specified channel.

        :keyword requeue: By default the messages will be redelivered
          to the original recipient. With ``requeue`` set to true, the
          server will attempt to requeue the message, potentially then
          delivering it to an alternative subscriber.

        """
        return self.channel.basic_recover(requeue=requeue)

    def _add_tag(self, queue, delivery_tag=None):
        tag = delivery_tag or str(self._next_tag())
        self._active_tags[queue] = tag
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

    def __enter__(self):
        self.consume()
        return self

    def __exit__(self, *args):
        self.cancel()
