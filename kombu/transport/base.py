"""
kombu.transport.base
====================

Base transport interface.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from ..serialization import decode
from ..compression import decompress
from ..exceptions import MessageStateError

ACKNOWLEDGED_STATES = frozenset(["ACK", "REJECTED", "REQUEUED"])


class StdChannel(object):
    no_ack_consumers = None

    def Consumer(self, *args, **kwargs):
        from ..messaging import Consumer
        return Consumer(self, *args, **kwargs)

    def Producer(self, *args, **kwargs):
        from ..messaging import Producer
        return Producer(self, *args, **kwargs)

    def list_bindings(self):
        raise NotImplementedError("%r does not implement list_bindings" % (
            self.__class__, ))

    def after_reply_message_received(self, queue):
        """reply queue semantics: can be used to delete the queue
           after transient reply message received."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()


class Message(object):
    """Base class for received messages."""
    __slots__ = ("_state", "channel", "delivery_tag",
                 "content_type", "content_encoding",
                 "delivery_info", "headers",
                 "properties", "body",
                 "_decoded_cache",
                 "MessageStateError", "__dict__")
    MessageStateError = MessageStateError

    def __init__(self, channel, body=None, delivery_tag=None,
            content_type=None, content_encoding=None, delivery_info={},
            properties=None, headers=None, postencode=None,
            **kwargs):
        self.channel = channel
        self.delivery_tag = delivery_tag
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.delivery_info = delivery_info
        self.headers = headers or {}
        self.properties = properties or {}
        self._decoded_cache = None
        self._state = "RECEIVED"

        try:
            body = decompress(body, self.headers["compression"])
        except KeyError:
            pass
        if postencode and isinstance(body, unicode):
            body = body.encode(postencode)
        self.body = body

    def ack(self):
        """Acknowledge this message as being processed.,
        This will remove the message from the queue.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.channel.no_ack_consumers is not None:
            try:
                consumer_tag = self.delivery_info["consumer_tag"]
            except KeyError:
                pass
            else:
                if consumer_tag in self.channel.no_ack_consumers:
                    return
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.channel.basic_ack(self.delivery_tag)
        self._state = "ACK"

    def ack_log_error(self, logger, errors):
        try:
            self.ack()
        except errors, exc:
            logger.critical("Couldn't ack %r, reason:%r",
                    self.delivery_tag, exc, exc_info=True)

    def reject(self):
        """Reject this message.

        The message will be discarded by the server.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.channel.basic_reject(self.delivery_tag, requeue=False)
        self._state = "REJECTED"

    def requeue(self):
        """Reject this message and put it back on the queue.

        You must not use this method as a means of selecting messages
        to process.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.channel.basic_reject(self.delivery_tag, requeue=True)
        self._state = "REQUEUED"

    def decode(self):
        """Deserialize the message body, returning the original
        python structure sent by the publisher."""
        return decode(self.body, self.content_type,
                      self.content_encoding)

    @property
    def acknowledged(self):
        """Set to true if the message has been acknowledged."""
        return self._state in ACKNOWLEDGED_STATES

    @property
    def payload(self):
        """The decoded message body."""
        if not self._decoded_cache:
            self._decoded_cache = self.decode()
        return self._decoded_cache


class Transport(object):
    """Base class for transports."""

    #: The :class:`~kombu.connection.BrokerConnection` owning this instance.
    client = None

    #: Default port used when no port has been specified.
    default_port = None

    #: Tuple of errors that can happen due to connection failure.
    connection_errors = ()

    #: Tuple of errors that can happen due to channel/method failure.
    channel_errors = ()

    def __init__(self, client, **kwargs):
        self.client = client

    def establish_connection(self):
        raise NotImplementedError("Subclass responsibility")

    def close_connection(self, connection):
        raise NotImplementedError("Subclass responsibility")

    def create_channel(self, connection):
        raise NotImplementedError("Subclass responsibility")

    def close_channel(self, connection):
        raise NotImplementedError("Subclass responsibility")

    def drain_events(self, connection, **kwargs):
        raise NotImplementedError("Subclass responsibility")

    def verify_connection(self, connection):
        return True

    @property
    def default_connection_params(self):
        return {}
