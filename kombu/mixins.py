"""
kombu.mixins
============

Useful mixin classes.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import socket

from contextlib import contextmanager
from functools import partial
from itertools import count

from .messaging import Consumer
from .log import LogMixin
from .utils import cached_property, nested
from .utils.encoding import safe_repr
from .utils.limits import TokenBucket

__all__ = ["ConsumerMixin"]


class ConsumerMixin(LogMixin):
    """Convenience mixin for implementing consumer threads.

    It can be used outside of threads, with threads, or greenthreads
    (eventlet/gevent) too.

    The basic class would need a :attr:`connection` attribute
    which must be a :class:`~kombu.connection.BrokerConnection` instance,
    and define a :meth:`get_consumers` method that returns a list
    of :class:`kombu.messaging.Consumer` instances to use.
    Supporting multiple consumers is important so that multiple
    channels can be used for different QoS requirements.

    **Example**:

    .. code-block:: python


        class Worker(ConsumerMixin):
            task_queue = Queue("tasks", Exchange("tasks"), "tasks"))

            def __init__(self, connection):
                self.connection = None

            def get_consumers(self, Consumer, channel):
                return [Consumer(queues=[self.task_queue],
                                 callback=[self.on_task])]

            def on_task(self, body, message):
                print("Got task: %r" % (body, ))
                message.ack()

    **Additional handler methods**:

        * :meth:`extra_context`

            Optional extra context manager that will be entered
            after the connection and consumers have been set up.

            Takes arguments ``(connection, channel)``.

        * :meth:`on_connection_error`

            Handler called if the connection is lost/ or
            is unavailable.

            Takes arguments ``(exc, interval)``, where interval
            is the time in seconds when the connection will be retried.

            The default handler will log the exception.

        * :meth:`on_connection_revived`

            Handler called when the connection is re-established
            after connection failure.

            Takes no arguments.

        * :meth:`on_consume_ready`

            Handler called when the consumer is ready to accept
            messages.

            Takes arguments ``(connection, channel, consumers)``.
            Also keyword arguments to ``consume`` are forwarded
            to this handler.

        * :meth:`on_consume_end`

            Handler called after the consumers are cancelled.
            Takes arguments ``(connection, channel)``.

        * :meth:`on_iteration`

            Handler called for every iteration while draining
            events.

            Takes no arguments.

        * :meth:`on_decode_error`

            Handler called if a consumer was unable to decode
            the body of a message.

            Takes arguments ``(message, exc)`` where message is the
            original message object.

            The default handler will log the error and
            acknowledge the message, so if you override make
            sure to call super, or perform these steps yourself.

    """

    #: maximum number of retries trying to re-establish the connection,
    #: if the connection is lost/unavailable.
    connect_max_retries = None

    #: When this is set to true the consumer should stop consuming
    #: and return, so that it can be joined if it is the implementation
    #: of a thread.
    should_stop = False

    def get_consumers(self, Consumer, channel):
        raise NotImplementedError("Subclass responsibility")

    def on_connection_revived(self):
        pass

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        pass

    def on_consume_end(self, connection, channel):
        pass

    def on_iteration(self):
        pass

    def on_decode_error(self, message, exc):
        self.critical(
            "Can't decode message body: %r (type:%r encoding:%r raw:%r')",
                    exc, message.content_type, message.content_encoding,
                    safe_repr(message.body))
        message.ack()

    def on_connection_error(self, exc, interval):
        self.error("Broker connection error: %r. "
                   "Trying again in %s seconds.", exc, interval)

    @contextmanager
    def extra_context(self, connection, channel):
        yield

    def run(self):
        while not self.should_stop:
            try:
                if self.restart_limit.can_consume(1):
                    for _ in self.consume(limit=None):
                        pass
            except self.connection.connection_errors:
                self.error("Connection to broker lost. "
                           "Trying to re-establish the connection...")

    def consume(self, limit=None, timeout=None, safety_interval=1, **kwargs):
        elapsed = 0
        with self.Consumer() as (connection, channel, consumers):
            with self.extra_context(connection, channel):
                self.on_consume_ready(connection, channel, consumers, **kwargs)
                for i in limit and xrange(limit) or count():
                    if self.should_stop:
                        break
                    self.on_iteration()
                    try:
                        connection.drain_events(timeout=safety_interval)
                    except socket.timeout:
                        elapsed += safety_interval
                        if timeout and elapsed >= timeout:
                            raise socket.timeout()
                    except socket.error:
                        if not self.should_stop:
                            raise
                    else:
                        yield
                        elapsed = 0
        self.debug("consume exiting")

    def maybe_conn_error(self, fun):
        """Applies function but ignores any connection or channel
        errors raised."""
        try:
            fun()
        except (AttributeError, ) + \
                self.connection_errors + \
                self.channel_errors:
            pass

    @contextmanager
    def establish_connection(self):
        with self.connection.clone() as conn:
            conn.ensure_connection(self.on_connection_error,
                                   self.connect_max_retries)
            yield conn

    @contextmanager
    def Consumer(self):
        with self.establish_connection() as conn:
            self.on_connection_revived()
            self.info("Connected to %s", conn.as_uri())
            channel = conn.default_channel
            cls = partial(Consumer, channel,
                          on_decode_error=self.on_decode_error)
            with self._consume_from(*self.get_consumers(cls, channel)) as c:
                yield conn, channel, c
            self.debug("Consumers cancelled")
            self.on_consume_end(conn, channel)
        self.debug("Connection closed")

    def _consume_from(self, *consumers):
        return nested(*consumers)

    @cached_property
    def restart_limit(self):
        # the AttributeError that can be catched from amqplib
        # poses problems for the too often restarts protection
        # in Connection.ensure_connection
        return TokenBucket(1)

    @cached_property
    def connection_errors(self):
        return self.connection.connection_errors

    @cached_property
    def channel_errors(self):
        return self.connection.channel_errors
