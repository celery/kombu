"""
kombu.transport.pyamqplib
=========================

amqplib transport.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
import socket

try:
    from ssl import SSLError
except ImportError:
    class SSLError(Exception):  # noqa
        pass

from amqplib import client_0_8 as amqp
from amqplib.client_0_8 import transport
from amqplib.client_0_8.channel import Channel as _Channel
from amqplib.client_0_8.exceptions import AMQPConnectionException
from amqplib.client_0_8.exceptions import AMQPChannelException

from kombu.transport import base

DEFAULT_PORT = 5672

# amqplib's handshake mistakenly identifies as protocol version 1191,
# this breaks in RabbitMQ tip, which no longer falls back to
# 0-8 for unknown ids.
transport.AMQP_PROTOCOL_HEADER = "AMQP\x01\x01\x08\x00"


class Connection(amqp.Connection):  # pragma: no cover

    def _dispatch_basic_return(self, channel, args, msg):
        reply_code = args.read_short()
        reply_text = args.read_shortstr()
        exchange = args.read_shortstr()
        routing_key = args.read_shortstr()

        exc = AMQPChannelException(reply_code, reply_text, (50, 60))
        if channel.events["basic_return"]:
            for callback in channel.events["basic_return"]:
                callback(exc, exchange, routing_key, msg)
        else:
            raise exc

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self._method_override = {(60, 50): self._dispatch_basic_return}

    def drain_events(self, allowed_methods=None, timeout=None):
        """Wait for an event on any channel."""
        return self.wait_multi(self.channels.values(), timeout=timeout)

    def wait_multi(self, channels, allowed_methods=None, timeout=None):
        """Wait for an event on a channel."""
        chanmap = dict((chan.channel_id, chan) for chan in channels)
        chanid, method_sig, args, content = self._wait_multiple(
                chanmap.keys(), allowed_methods, timeout=timeout)

        channel = chanmap[chanid]

        if content \
        and channel.auto_decode \
        and hasattr(content, 'content_encoding'):
            try:
                content.body = content.body.decode(content.content_encoding)
            except Exception:
                pass

        amqp_method = self._method_override.get(method_sig) or \
                        channel._METHOD_MAP.get(method_sig, None)

        if amqp_method is None:
            raise Exception('Unknown AMQP method (%d, %d)' % method_sig)

        if content is None:
            return amqp_method(channel, args)
        else:
            return amqp_method(channel, args, content)

    def read_timeout(self, timeout=None):
        if timeout is None:
            return self.method_reader.read_method()
        sock = self.transport.sock
        prev = sock.gettimeout()
        sock.settimeout(timeout)
        try:
            try:
                return self.method_reader.read_method()
            except SSLError, exc:
                # http://bugs.python.org/issue10272
                if "timed out" in str(exc):
                    raise socket.timeout()
                raise
        finally:
            sock.settimeout(prev)

    def _wait_multiple(self, channel_ids, allowed_methods, timeout=None):
        for channel_id in channel_ids:
            method_queue = self.channels[channel_id].method_queue
            for queued_method in method_queue:
                method_sig = queued_method[0]
                if (allowed_methods is None) \
                or (method_sig in allowed_methods) \
                or (method_sig == (20, 40)):
                    method_queue.remove(queued_method)
                    method_sig, args, content = queued_method
                    return channel_id, method_sig, args, content

        # Nothing queued, need to wait for a method from the peer
        read_timeout = self.read_timeout
        channels = self.channels
        wait = self.wait
        while 1:
            channel, method_sig, args, content = read_timeout(timeout)

            if (channel in channel_ids) \
            and ((allowed_methods is None) \
                or (method_sig in allowed_methods) \
                or (method_sig == (20, 40))):
                return channel, method_sig, args, content

            # Not the channel and/or method we were looking for. Queue
            # this method for later
            channels[channel].method_queue.append((method_sig, args, content))

            #
            # If we just queued up a method for channel 0 (the Connection
            # itself) it's probably a close method in reaction to some
            # error, so deal with it right away.
            #
            if channel == 0:
                wait()

    def channel(self, channel_id=None):
        try:
            return self.channels[channel_id]
        except KeyError:
            return Channel(self, channel_id)


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


class Channel(_Channel):
    Message = Message
    events = {"basic_return": []}

    def prepare_message(self, message_data, priority=None,
                content_type=None, content_encoding=None, headers=None,
                properties=None):
        """Encapsulate data into a AMQP message."""
        return amqp.Message(message_data, priority=priority,
                            content_type=content_type,
                            content_encoding=content_encoding,
                            application_headers=headers,
                            **properties)

    def message_to_python(self, raw_message):
        """Convert encoded message body back to a Python value."""
        return self.Message(self, raw_message)

    def close(self):
        try:
            super(Channel, self).close()
        finally:
            self.connection = None


class Transport(base.Transport):
    Connection = Connection

    default_port = DEFAULT_PORT

    # it's very annoying that amqplib sometimes raises AttributeError
    # if the connection is lost, but nothing we can do about that here.
    connection_errors = (AMQPConnectionException,
                         socket.error,
                         IOError,
                         OSError,
                         AttributeError)
    channel_errors = (AMQPChannelException, )

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
        if not conninfo.hostname:
            raise KeyError("Missing hostname for AMQP connection.")
        if conninfo.userid is None:
            conninfo.userid = "guest"
        if conninfo.password is None:
            conninfo.password = "guest"
        if conninfo.login_method is None:
            conninfo.login_method = "AMQPLAIN"
        if not conninfo.port:
            conninfo.port = self.default_port
        conn = self.Connection(host=conninfo.host,
                               userid=conninfo.userid,
                               password=conninfo.password,
                               login_method=conninfo.login_method,
                               virtual_host=conninfo.virtual_host,
                               insist=conninfo.insist,
                               ssl=conninfo.ssl,
                               connect_timeout=conninfo.connect_timeout)
        conn.client = self.client
        return conn

    def close_connection(self, connection):
        """Close the AMQP broker connection."""
        connection.client = None
        connection.close()

    def verify_connection(self, connection):
        return connection.channels is not None
