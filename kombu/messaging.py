"""
kombu.messaging
===============

Sending and receiving messages.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from itertools import count

from .entity import Exchange, Queue
from .compression import compress
from .serialization import encode
from .utils import maybe_list

__all__ = ["Exchange", "Queue", "Producer", "Consumer"]


class Producer(object):
    """Message Producer.

    :param channel: Connection or channel.
    :keyword exchange: Optional default exchange.
    :keyword routing_key: Optional default routing key.
    :keyword serializer: Default serializer. Default is `"json"`.
    :keyword compression: Default compression method. Default is no
        compression.
    :keyword auto_declare: Automatically declare the default exchange
      at instantiation. Default is :const:`True`.
    :keyword on_return: Callback to call for undeliverable messages,
        when the `mandatory` or `immediate` arguments to
        :meth:`publish` is used. This callback needs the following
        signature: `(exception, exchange, routing_key, message)`.
        Note that the producer needs to drain events to use this feature.

    """
    #: The connection channel used.
    channel = None

    #: Default exchange.
    exchange = None

    # Default routing key.
    routing_key = ""

    #: Default serializer to use. Default is JSON.
    serializer = None

    #: Default compression method.  Disabled by default.
    compression = None

    #: By default the exchange is declared at instantiation.
    #: If you want to declare manually then you can set this
    #: to :const:`False`.
    auto_declare = True

    #: Basic return callback.
    on_return = None

    def __init__(self, channel, exchange=None, routing_key=None,
            serializer=None, auto_declare=None, compression=None,
            on_return=None):
        from .connection import BrokerConnection
        if isinstance(channel, BrokerConnection):
            channel = channel.default_channel
        self.channel = channel

        self.exchange = exchange or self.exchange
        if self.exchange is None:
            self.exchange = Exchange("")
        self.routing_key = routing_key or self.routing_key
        self.serializer = serializer or self.serializer
        self.compression = compression or self.compression
        self.on_return = on_return or self.on_return
        if auto_declare is not None:
            self.auto_declare = auto_declare

        self.exchange = self.exchange(self.channel)
        if self.auto_declare:
            self.declare()
        if self.on_return:
            self.channel.events["basic_return"].append(self.on_return)

    def declare(self):
        """Declare the exchange.

        This is done automatically at instantiation if :attr:`auto_declare`
        is set to :const:`True`.

        """
        if self.exchange.name:
            self.exchange.declare()

    def maybe_declare(self, entity, retry=False, **retry_policy):
        if entity:
            from .common import maybe_declare
            return maybe_declare(entity, self.channel, retry, **retry_policy)

    def publish(self, body, routing_key=None, delivery_mode=None,
            mandatory=False, immediate=False, priority=0, content_type=None,
            content_encoding=None, serializer=None, headers=None,
            compression=None, exchange=None, retry=False, retry_policy=None,
            declare=[], **properties):
        """Publish message to the specified exchange.

        :param body: Message body.
        :keyword routing_key: Message routing key.
        :keyword delivery_mode: See :attr:`delivery_mode`.
        :keyword mandatory: Currently not supported.
        :keyword immediate: Currently not supported.
        :keyword priority: Message priority. A number between 0 and 9.
        :keyword content_type: Content type. Default is auto-detect.
        :keyword content_encoding: Content encoding. Default is auto-detect.
        :keyword serializer: Serializer to use. Default is auto-detect.
        :keyword headers: Mapping of arbitrary headers to pass along
          with the message body.
        :keyword exchange: Override the exchange.  Note that this exchange
          must have been declared.
        :keyword properties: Additional properties, see the AMQP spec.

        """
        retry_policy = {} if retry_policy is None else retry_policy
        headers = headers or {}
        if routing_key is None:
            routing_key = self.routing_key
        if compression is None:
            compression = self.compression

        if isinstance(exchange, Exchange):
            exchange = exchange.name

        # Additional  entities to declare before publishing the message.
        for entity in declare:
            self.maybe_declare(entity, retry, **retry_policy)

        body, content_type, content_encoding = self._prepare(
                body, serializer, content_type, content_encoding,
                compression, headers)
        message = self.exchange.Message(body,
                                        delivery_mode,
                                        priority,
                                        content_type,
                                        content_encoding,
                                        headers=headers,
                                        properties=properties)
        publish = self._publish
        if retry:
            publish = self.connection.ensure(self, self._publish,
                                             **retry_policy)
        return publish(message, routing_key, mandatory, immediate, exchange)

    def _publish(self, message, routing_key, mandatory, immediate, exchange):
        return self.exchange.publish(message, routing_key, mandatory,
                                     immediate, exchange=exchange)

    def revive(self, channel):
        """Revive the producer after connection loss."""
        from .connection import BrokerConnection
        if isinstance(channel, BrokerConnection):
            channel = channel.default_channel
        self.channel = channel
        self.exchange.revive(channel)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.release()

    def release(self):
        pass
    close = release

    def _prepare(self, body, serializer=None,
            content_type=None, content_encoding=None, compression=None,
            headers=None):

        # No content_type? Then we're serializing the data internally.
        if not content_type:
            serializer = serializer or self.serializer
            (content_type, content_encoding,
             body) = encode(body, serializer=serializer)
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

    @property
    def connection(self):
        try:
            return self.channel.connection.client
        except AttributeError:
            pass


class Consumer(object):
    """Message consumer.

    :param channel: see :attr:`channel`.
    :param queues: see :attr:`queues`.
    :keyword no_ack: see :attr:`no_ack`.
    :keyword auto_declare: see :attr:`auto_declare`
    :keyword callbacks: see :attr:`callbacks`.
    :keyword on_decode_error: see :attr:`on_decode_error`.

    """
    #: The connection/channel to use for this consumer.
    channel = None

    #: A single :class:`~kombu.entity.Queue`, or a list of queues to
    #: consume from.
    queues = None

    #: Flag for message acknowledgment disabled/enabled.
    #: Enabled by default.
    no_ack = None

    #: By default all entities will be declared at instantiation, if you
    #: want to handle this manually you can set this to :const:`False`.
    auto_declare = True

    #: List of callbacks called in order when a message is received.
    #:
    #: The signature of the callbacks must take two arguments:
    #: `(body, message)`, which is the decoded message body and
    #: the `Message` instance (a subclass of
    #: :class:`~kombu.transport.base.Message`).
    callbacks = None

    #: Callback called when a message can't be decoded.
    #:
    #: The signature of the callback must take two arguments: `(message,
    #: exc)`, which is the message that can't be decoded and the exception
    #: that occurred while trying to decode it.
    on_decode_error = None

    _next_tag = count(1).next   # global

    def __init__(self, channel, queues=None, no_ack=None, auto_declare=None,
            callbacks=None, on_decode_error=None):
        from .connection import BrokerConnection
        if isinstance(channel, BrokerConnection):
            channel = channel.default_channel
        self.channel = channel

        self.queues = [] if queues is None else queues
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

    def register_callback(self, callback):
        """Register a new callback to be called when a message
        is received.

        The signature of the callback needs to accept two arguments:
        `(body, message)`, which is the decoded message body
        and the `Message` instance (a subclass of
        :class:`~kombu.transport.base.Message`.

        """
        self.callbacks.append(callback)

    def __enter__(self):
        self.consume()
        return self

    def __exit__(self, *exc_info):
        self.cancel()

    def add_queue(self, queue):
        queue = queue(self.channel)
        if self.auto_declare:
            queue.declare()
        self.queues.append(queue)
        return queue

    def consume(self, no_ack=None):
        """Register consumer on server.

        """
        if not self.queues:
            return
        if no_ack is None:
            no_ack = self.no_ack
        H, T = self.queues[:-1], self.queues[-1]
        for queue in H:
            self._basic_consume(queue, no_ack=no_ack, nowait=True)
        self._basic_consume(T, no_ack=no_ack, nowait=False)

    def cancel(self):
        """End all active queue consumers.

        This does not affect already delivered messages, but it does
        mean the server will not send any more messages for this consumer.

        """
        for tag in self._active_tags.values():
            self.channel.basic_cancel(tag)
        self._active_tags.clear()
    close = cancel

    def cancel_by_queue(self, queue):
        """Cancel consumer by queue name."""
        try:
            tag = self._active_tags.pop(queue)
        except KeyError:
            pass
        else:
            self.queues[:] = [q for q in self.queues if q.name != queue]
            self.channel.basic_cancel(tag)

    def consuming_from(self, queue):
        name = queue
        if isinstance(queue, Queue):
            name = queue.name
        return any(q.name == name for q in self.queues)

    def purge(self):
        """Purge messages from all queues.

        .. warning::
            This will *delete all ready messages*, there is no
            undo operation available.

        """
        return sum(queue.purge() for queue in self.queues)

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
          to the original recipient. With `requeue` set to true, the
          server will attempt to requeue the message, potentially then
          delivering it to an alternative subscriber.

        """
        return self.channel.basic_recover(requeue=requeue)

    def receive(self, body, message):
        """Method called when a message is received.

        This dispatches to the registered :attr:`callbacks`.

        :param body: The decoded message body.
        :param message: The `Message` instance.

        :raises NotImplementedError: If no consumer callbacks have been
          registered.

        """
        callbacks = self.callbacks
        if not callbacks:
            raise NotImplementedError("No consumer callbacks registered")
        [callback(body, message) for callback in callbacks]

    def revive(self, channel):
        """Revive consumer after connection loss."""
        for queue in self.queues:
            queue.revive(channel)
        self.channel = channel

    def _basic_consume(self, queue, consumer_tag=None,
            no_ack=no_ack, nowait=True):
        tag = self._active_tags.get(queue.name)
        if tag is None:
            tag = self._add_tag(queue, consumer_tag)
            queue.consume(tag,
                          self._receive_callback,
                          no_ack=no_ack,
                          nowait=nowait)
        return tag

    def _add_tag(self, queue, consumer_tag=None):
        tag = consumer_tag or str(self._next_tag())
        self._active_tags[queue.name] = tag
        return tag

    def _receive_callback(self, message):
        channel = self.channel
        try:
            m2p = getattr(channel, "message_to_python", None)
            if m2p:
                message = channel.message_to_python(message)
            decoded = message.decode()
        except Exception, exc:
            if not self.on_decode_error:
                raise
            self.on_decode_error(message, exc)
        else:
            self.receive(decoded, message)

    def __repr__(self):
        return "<Consumer: %s>" % (self.queues, )

    @property
    def connection(self):
        try:
            return self.channel.connection.client
        except AttributeError:
            pass
