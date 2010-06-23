"""

Backend base classes.

"""
from kombu import serialization

ACKNOWLEDGED_STATES = frozenset(["ACK", "REJECTED", "REQUEUED"])


class MessageStateError(Exception):
    """The message has already been acknowledged."""


class BaseMessage(object):
    """Base class for received messages."""
    _state = None

    MessageStateError = MessageStateError

    def __init__(self, channel, body=None, delivery_tag=None,
            content_type=None, content_encoding=None, delivery_info={}, **kwargs):
        self.channel = channel
        self._decoded_cache = None
        self._state = "RECEIVED"

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
    default_port = None

    def __init__(self, connection, **kwargs):
        self.connection = connection

    def get_channel(self):
        raise NotImplementedError("Subclass responsibility")

    def establish_connection(self):
        raise NotImplementedError("Subclass responsibility")

    def close_connection(self):
        raise NotImplementedError("Subclass responsibility")
