"""
kombu.transport.amqplib
=======================

kamqp transport.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import socket

from kamqp import client_0_8 as amqp

from . import base

DEFAULT_PORT = 5672


class Message(base.Message):
    """A message received by the broker.

    .. attribute:: body

        The message body.

    .. attribute:: delivery_tag

        The message delivery tag, uniquely identifying this message.

    .. attribute:: channel

        The channel instance the message was received on.

    """

    def __init__(self, channel, msg, **kwargs):
        props = msg.properties
        super(Message, self).__init__(channel,
                body=msg.body,
                delivery_tag=msg.delivery_tag,
                content_type=props.get("content_type"),
                content_encoding=props.get("content_encoding"),
                delivery_info=msg.delivery_info,
                properties=msg.properties,
                headers=props.get("application_headers"),
                **kwargs)


class Channel(amqp.Channel, base.StdChannel):
    Message = Message

    def prepare_message(self, message_data, priority=None,
                content_type=None, content_encoding=None, headers=None,
                properties=None):
        """Encapsulate data into an AMQP message."""
        return amqp.Message(message_data, priority=priority,
                            content_type=content_type,
                            content_encoding=content_encoding,
                            application_headers=headers,
                            **properties)

    def message_to_python(self, raw_message):
        """Convert encoded message body back to a Python value."""
        return self.Message(self, raw_message)


class Connection(amqp.Connection):
    Channel = Channel


class Transport(base.Transport):
    Connection = Connection

    default_port = DEFAULT_PORT

    # it's very annoying that amqplib sometimes raises AttributeError
    # if the connection is lost, but nothing we can do about that here.
    connection_errors = (amqp.AMQPRecoverableError,
                         socket.error,
                         IOError,
                         OSError)
    channel_errors = (amqp.AMQPChannelError, )

    def __init__(self, client, **kwargs):
        self.client = client
        self.default_port = kwargs.get("default_port") or self.default_port

    def create_channel(self, connection):
        return connection.channel()

    def drain_events(self, connection, **kwargs):
        return connection.drain_events(**kwargs)

    def establish_connection(self):
        """Establish connection to the AMQP broker."""
        conninfo = self.client
        for name, default_value in self.default_connection_params.items():
            if not getattr(conninfo, name, None):
                setattr(conninfo, name, default_value)
        if conninfo.hostname == "localhost":
            conninfo.hostname = "127.0.0.1"
        conn = self.Connection(host=conninfo.host,
                               userid=conninfo.userid,
                               password=conninfo.password,
                               login_method=conninfo.login_method,
                               virtual_host=conninfo.virtual_host,
                               insist=conninfo.insist,
                               ssl=conninfo.ssl,
                               connect_timeout=conninfo.connect_timeout,
                               **conninfo.transport_options)
        conn.client = self.client
        return conn

    def close_connection(self, connection):
        """Close the AMQP broker connection."""
        connection.client = None
        connection.close()

    def verify_connection(self, connection):
        return connection.channels is not None

    @property
    def default_connection_params(self):
        return {"userid": "guest", "password": "guest",
                "port": self.default_port,
                "hostname": "localhost", "login_method": "AMQPLAIN"}
