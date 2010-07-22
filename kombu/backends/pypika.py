import weakref
import functools
import itertools

import pika
from pika import channel

from carrot.backends.base import BaseMessage, BaseBackend

DEFAULT_PORT = 5672


class Message(BaseMessage):

    def __init__(self, channel, amqp_message, **kwargs):
        self.channel = channel
        self._amqp_message = amqp_message

        channel_id, method, header, body = amqp_message

        kwargs.update({"body": body,
                       "delivery_tag": method.delivery_tag,
                       "content_type": header.content_type,
                       "content_encoding": header.content_encoding,
                       "delivery_info": dict(
                            consumer_tag=method.consumer_tag,
                            routing_key=method.routing_key,
                            delivery_tag=method.delivery_tag,
                            exchange=method.exchange)})

        super(Message, self).__init__(channel, **kwargs)


class Channel(channel.Channel):
    Message = Message

    def basic_publish(self, message, exchange, routing_key, mandatory=False,
            immediate=False):
        message_data, properties = message
        return channel.Channel.basic_publish(self, exchange,
                                             routing_key,
                                             message_data,
                                             properties,
                                             mandatory,
                                             immediate)

    def basic_consume(self, queue, no_ack=False, consumer_tag=None,
            callback=None, nowait=False):

        def _callback_decode(channel, method, header, body):
            return callback((channel, method, header, body))

        return channel.Channel.basic_consume(self, _callback_decode,
                                             queue, no_ack,
                                             False, consumer_tag)

    def prepare_message(self, message_data, priority=None,
            content_type=None, content_encoding=None, headers=None,
            properties=None):
        """Encapsulate data into a AMQP message."""
        properties = pika.BasicProperties(priority=priority,
                                          content_type=content_type,
                                          content_encoding=content_encoding,
                                          headers=headers,
                                          **properties)
        return message_data, properties

    def message_to_python(self, raw_message):
        """Convert encoded message body back to a Python value."""
        return self.Message(channel=self, amqp_message=raw_message)


class BlockingConnection(pika.BlockingConnection):

    def channel(self):
        return Channel(channel.ChannelHandler(self))


class AsyncoreConnection(pika.AsyncoreConnection):

    def channel(self):
        return Channel(channel.ChannelHandler(self))


class SyncBackend(BaseBackend):
    default_port = DEFAULT_PORT

    Message = Message
    Connection = BlockingConnection

    def __init__(self, connection, **kwargs):
        self.connection = connection
        self.default_port = kwargs.get("default_port", self.default_port)

    def create_channel(self, connection):
        return connection.channel()

    def drain_events(self, connection, **kwargs):
        return connection.drain_events(**kwargs)

    def establish_connection(self):
        """Establish connection to the AMQP broker."""
        conninfo = self.connection
        if not conninfo.hostname:
            raise KeyError("Missing hostname for AMQP connection.")
        if conninfo.userid is None:
            raise KeyError("Missing user id for AMQP connection.")
        if conninfo.password is None:
            raise KeyError("Missing password for AMQP connection.")
        if not conninfo.port:
            conninfo.port = self.default_port

        credentials = pika.PlainCredentials(conninfo.userid,
                                            conninfo.password)
        return self.Connection(pika.ConnectionParameters(
                                    conninfo.hostname,
                                    port=conninfo.port,
                                    virtual_host=conninfo.virtual_host,
                                    credentials=credentials))

    def close_connection(self, connection):
        """Close the AMQP broker connection."""
        connection.close()


class AsyncoreBackend(SyncBackend):
    Connection = AsyncoreConnection
