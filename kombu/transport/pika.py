"""
kombu.transport.pika
====================

Pika transport.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import socket

from operator import attrgetter

from ..exceptions import StdChannelError, VersionMismatch
from . import base

from pika import channel  # must be here to raise import error
try:
    from pika import asyncore_adapter
except ImportError:
    raise VersionMismatch("Kombu only works with pika version 0.5.2")
from pika import blocking_adapter
from pika import connection
from pika import exceptions
from pika.spec import Basic, BasicProperties


DEFAULT_PORT = 5672


BASIC_PROPERTIES = ("content_type", "content_encoding",
                    "headers", "delivery_mode", "priority",
                    "correlation_id", "reply_to", "expiration",
                    "message_id", "timestamp", "type", "user_id",
                    "app_id", "cluster_id")


class Message(base.Message):

    def __init__(self, channel, amqp_message, **kwargs):
        channel_id, method, props, body = amqp_message
        propdict = dict(zip(BASIC_PROPERTIES,
                        attrgetter(*BASIC_PROPERTIES)(props)))

        kwargs.update({"body": body,
                       "delivery_tag": method.delivery_tag,
                       "content_type": props.content_type,
                       "content_encoding": props.content_encoding,
                       "headers": props.headers,
                       "properties": propdict,
                       "delivery_info": dict(
                            consumer_tag=getattr(method, "consumer_tag", None),
                            routing_key=method.routing_key,
                            delivery_tag=method.delivery_tag,
                            redelivered=method.redelivered,
                            exchange=method.exchange)})

        super(Message, self).__init__(channel, **kwargs)


class Channel(channel.Channel, base.StdChannel):
    Message = Message

    def basic_get(self, queue, no_ack):
        method = channel.Channel.basic_get(self, queue=queue, no_ack=no_ack)
        # pika returns semi-predicates (GetEmpty/GetOk).
        if isinstance(method, Basic.GetEmpty):
            return
        return None, method, method._properties, method._body

    def queue_purge(self, queue=None, nowait=False):
        return channel.Channel.queue_purge(self, queue=queue, nowait=nowait) \
                              .message_count

    def basic_publish(self, message, exchange, routing_key, mandatory=False,
            immediate=False):
        message_data, properties = message
        try:
            return channel.Channel.basic_publish(self,
                                                 exchange,
                                                 routing_key,
                                                 message_data,
                                                 properties,
                                                 mandatory,
                                                 immediate)
        finally:
            # Pika does not automatically flush the outbound buffer
            # TODO async: Needs to support `nowait`.
            self.handler.connection.flush_outbound()

    def basic_consume(self, queue, no_ack=False, consumer_tag=None,
            callback=None, nowait=False):

        # Kombu callbacks only take a single `message` argument,
        # but pika applies with 4 arguments, so need to wrap
        # these into a single tuple.
        def _callback_decode(channel, method, header, body):
            return callback((channel, method, header, body))

        return channel.Channel.basic_consume(self, _callback_decode,
                                             queue, no_ack,
                                             False, consumer_tag)

    def prepare_message(self, message_data, priority=None,
            content_type=None, content_encoding=None, headers=None,
            properties=None):
        properties = BasicProperties(priority=priority,
                                     content_type=content_type,
                                     content_encoding=content_encoding,
                                     headers=headers,
                                     **properties)
        return message_data, properties

    def message_to_python(self, raw_message):
        return self.Message(channel=self, amqp_message=raw_message)

    def basic_ack(self, delivery_tag):
        return channel.Channel.basic_ack(self, delivery_tag)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        super(Channel, self).close()
        if getattr(self, "handler", None):
            if getattr(self.handler, "connection", None):
                self.handler.connection.channels.pop(
                        self.handler.channel_number, None)
                self.handler.connection = None
            self.handler = None

    @property
    def channel_id(self):
        return self.channel_number


class BlockingConnection(blocking_adapter.BlockingConnection):
    Super = blocking_adapter.BlockingConnection

    def __init__(self, client, *args, **kwargs):
        self.client = client
        self.Super.__init__(self, *args, **kwargs)

    def channel(self):
        c = Channel(channel.ChannelHandler(self))
        c.connection = self
        return c

    def close(self):
        self.client = None
        self.Super.close(self)

    def ensure_drain_events(self, timeout=None):
        return self.drain_events(timeout=timeout)


class AsyncoreConnection(asyncore_adapter.AsyncoreConnection):
    _event_counter = 0
    Super = asyncore_adapter.AsyncoreConnection

    def __init__(self, client, *args, **kwargs):
        self.client = client
        self.Super.__init__(self, *args, **kwargs)

    def channel(self):
        c = Channel(channel.ChannelHandler(self))
        c.connection = self
        return c

    def ensure_drain_events(self, timeout=None):
        # asyncore connection does not raise socket.timeout when timing out
        # so need to do a little trick here to mimic the behavior
        # of sync connection.
        current_events = self._event_counter
        self.drain_events(timeout=timeout)
        if timeout and self._event_counter <= current_events:
            raise socket.timeout("timed out")

    def on_data_available(self, buf):
        self._event_counter += 1
        self.Super.on_data_available(self, buf)

    def close(self):
        self.client = None
        self.Super.close(self)


class SyncTransport(base.Transport):
    default_port = DEFAULT_PORT

    connection_errors = (socket.error,
                         exceptions.ConnectionClosed,
                         exceptions.ChannelClosed,
                         exceptions.LoginError,
                         exceptions.NoFreeChannels,
                         exceptions.DuplicateConsumerTag,
                         exceptions.UnknownConsumerTag,
                         exceptions.RecursiveOperationDetected,
                         exceptions.ContentTransmissionForbidden,
                         exceptions.ProtocolSyntaxError)

    channel_errors = (StdChannelError,
                      exceptions.ChannelClosed,
                      exceptions.DuplicateConsumerTag,
                      exceptions.UnknownConsumerTag,
                      exceptions.ProtocolSyntaxError)

    Message = Message
    Connection = BlockingConnection

    def __init__(self, client, **kwargs):
        self.client = client
        self.default_port = kwargs.get("default_port", self.default_port)

    def create_channel(self, connection):
        return connection.channel()

    def drain_events(self, connection, **kwargs):
        return connection.ensure_drain_events(**kwargs)

    def establish_connection(self):
        """Establish connection to the AMQP broker."""
        conninfo = self.client
        for name, default_value in self.default_connection_params.items():
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)
        credentials = connection.PlainCredentials(conninfo.userid,
                                                  conninfo.password)
        return self.Connection(self.client,
                               connection.ConnectionParameters(
                                    conninfo.hostname, port=conninfo.port,
                                    virtual_host=conninfo.virtual_host,
                                    credentials=credentials))

    def close_connection(self, connection):
        """Close the AMQP broker connection."""
        connection.close()

    @property
    def default_connection_params(self):
        return {"hostname": "localhost", "port": self.default_port,
                "userid": "guest", "password": "guest"}


class AsyncoreTransport(SyncTransport):
    Connection = AsyncoreConnection
