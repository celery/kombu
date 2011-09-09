"""
kombu.mixins
============

Useful mixin classes.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import socket
import sys

from contextlib import nested, contextmanager
from functools import partial
from itertools import count

from kombu.messaging import Consumer

from kombu.utils import cached_property
from kombu.utils.limits import TokenBucket

__all__ = ["ConsumerMixin"]


class ConsumerMixin(object):
    connect_max_retries = None
    should_stop = False

    def get_consumers(self, Consumer, channel):
        raise NotImplementedError("Subclass responsibility")

    def on_connection_revived(self):
        pass

    def on_consume_ready(self, connection, channel):
        pass

    def on_iteration(self):
        pass

    @contextmanager
    def extra_context(self, connection, channel):
        yield

    def error(self, msg, *args):
        pass

    def info(self, msg, *args):
        pass

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
                self.on_consume_ready(connection, channel, **kwargs)
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
                        raise
                    else:
                        yield
                        elapsed = 0

    def on_connection_error(self, exc, interval):
        self.error("Broker connection error: %r. "
                   "Trying again in %s seconds.", exc, interval)

    @contextmanager
    def Consumer(self):
        with self.connection.clone() as conn:
            conn.ensure_connection(self.on_connection_error,
                                   self.connect_max_retries)
            self.on_connection_revived()
            self.info("Connected to %s", conn.as_uri())
            channel = conn.default_channel
            with self._consume_from(*self.get_consumers(
                    partial(Consumer, channel), channel)) as consumers:
                yield conn, channel, consumers

    @contextmanager
    def _consume_from(self, *consumers):
        with nested(*consumers) as context:
            yield context

    @cached_property
    def restart_limit(self):
        # the AttributeError that can be catched from amqplib
        # poses problems for the too often restarts protection
        # in Connection.ensure_connection
        return TokenBucket(1)
