"""
kombu.transport.librabbitmq
===========================

pylibrabbitmq transport.

:copyright: (c) 2010 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
import socket
import pylibrabbitmq as amqp

from pylibrabbitmq import ChannelError, ConnectionError

from kombu.exceptions import StdChannelError

from . import base

DEFAULT_PORT = 5672


class Message(base.Message):

    def __init__(self, body, props, info, channel):
        super(Message, self).__init__(channel,
                body=body,
                delivery_info=info,
                properties=props,
                delivery_tag=info["delivery_tag"],
                content_type=props["content_type"],
                content_encoding=props["content_encoding"],
                headers=props.get("headers"))


class Channel(amqp.Channel, base.StdChannel):
    Message = Message

    def prepare_message(self, body, priority=None,
                content_type=None, content_encoding=None, headers=None,
                properties=None):
        """Encapsulate data into a AMQP message."""
        properties = properties if properties is not None else {}
        properties.update({"content_type": content_type,
                           "content_encoding": content_encoding,
                           "headers": headers,
                           "priority": priority})
        return amqp.Message(body, properties=properties)


class Connection(amqp.Connection):
    Channel = Channel


class Transport(base.Transport):
    Connection = Connection

    default_port = DEFAULT_PORT
    connection_errors = (ConnectionError,
                         socket.error,
                         IOError,
                         OSError)
    channel_errors = (StdChannelError, ChannelError, )

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
        self.client.drain_events = conn.drain_events
        return conn

    def close_connection(self, connection):
        """Close the AMQP broker connection."""
        connection.close()

    @property
    def default_connection_params(self):
        return {"userid": "guest", "password": "guest",
                "port": self.default_port,
                "hostname": "localhost", "login_method": "AMQPLAIN"}
