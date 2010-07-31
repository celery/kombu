"""

Backend base classes.

"""
from kombu import serialization
from kombu.compression import decompress
from kombu.exceptions import MessageStateError

ACKNOWLEDGED_STATES = frozenset(["ACK", "REJECTED", "REQUEUED"])


class BaseMessage(object):
    """Base class for received messages."""
    _state = None

    MessageStateError = MessageStateError

    def __init__(self, channel, body=None, delivery_tag=None,
            content_type=None, content_encoding=None, delivery_info={},
            properties=None, headers=None,
            **kwargs):
        self.channel = channel
        self.body = body
        self.delivery_tag = delivery_tag
        self.content_type = content_type
        self.content_encoding = content_encoding
        self.delivery_info = delivery_info
        self.headers = headers or {}
        self.properties = properties or {}
        self._decoded_cache = None
        self._state = "RECEIVED"

        compression = self.headers.get("compression")
        if compression:
            self.body = decompress(self.body, compression)

    def decode(self):
        """Deserialize the message body, returning the original
        python structure sent by the publisher."""
        return serialization.decode(self.body, self.content_type,
                                    self.content_encoding)

    @property
    def payload(self):
        """The decoded message."""
        if not self._decoded_cache:
            self._decoded_cache = self.decode()
        return self._decoded_cache

    def ack(self):
        """Acknowledge this message as being processed.,
        This will remove the message from the queue.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.channel.basic_ack(self.delivery_tag)
        self._state = "ACK"

    def reject(self):
        """Reject this message.

        The message will be discarded by the server.

        :raises MessageStateError: If the message has already been
            acknowledged/requeued/rejected.

        """
        if self.acknowledged:
            raise self.MessageStateError(
                "Message already acknowledged with state: %s" % self._state)
        self.channel.basic_reject(self.delivery_tag)
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

    @property
    def acknowledged(self):
        return self._state in ACKNOWLEDGED_STATES


class BaseBackend(object):
    """Base class for backends."""
    client = None
    default_port = None
    connection_errors = ()
    channel_errors = ()

    def __init__(self, client, **kwargs):
        self.client = client

    def create_channel(self, connection):
        raise NotImplementedError("Subclass responsibility")

    def drain_events(self, connection, **kwargs):
        raise NotImplementedError("Subclass responsibility")

    def establish_connection(self):
        raise NotImplementedError("Subclass responsibility")

    def close_connection(self, connection):
        raise NotImplementedError("Subclass responsibility")
