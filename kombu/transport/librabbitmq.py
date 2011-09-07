"""
kombu.transport.librabbitmq
===========================

pylibrabbitmq transport.

:copyright: (c) 2010 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
import socket
import pylibrabbitmq as amqp

from pylibrabbitmq import ChannelError, ConnectionError

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

    def __init__(self, channel, message, **kwargs):
        props = message.properties
        info = message.delivery_info
        super(Message, self).__init__(channel,
                body=message.body,
                delivery_info=info,
                properties=props,
                delivery_tag=info["delivery_tag"],
                content_type=props["content_type"],
                content_encoding=props["content_encoding"],
                headers=props.get("application_headers"),
                **kwargs)


class Channel(amqp.Channel, base.StdChannel):
    Message = Message

    def prepare_message(self, body, priority=None,
                content_type=None, content_encoding=None, headers=None,
                properties=None):
        """Encapsulate data into a AMQP message."""
        properties = dict({"content_type": content_type,
                           "content_encoding": content_encoding,
                           "application_headers": headers,
                           "priority": priority}, **properties or {})
        return amqp.Message(body, properties=properties)

    def message_to_python(self, raw_message):
        """Convert encoded message body back to a Python value."""
        return self.Message(self, raw_message)


class Connection(amqp.Connection):
    Channel = Channel


class Transport(base.Transport):
    Connection = Connection

    default_port = DEFAULT_PORT
    connection_errors = (ConnectionError,
                         socket.error,
                         IOError,
                         OSError)
    channel_errors = (ChannelError, )

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
        conn = self.Connection(host=conninfo.host,
                               userid=conninfo.userid,
                               password=conninfo.password,
                               virtual_host=conninfo.virtual_host,
                               login_method=conninfo.login_method,
                               insist=conninfo.insist,
                               ssl=conninfo.ssl,
                               connect_timeout=conninfo.connect_timeout)
        conn.client = self.client
        return conn

    def close_connection(self, connection):
        """Close the AMQP broker connection."""
        connection.close()

    @property
    def default_connection_params(self):
        return {"userid": "guest", "password": "guest",
                "port": self.default_port,
                "hostname": "localhost", "login_method": "AMQPLAIN"}
