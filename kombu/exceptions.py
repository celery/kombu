"""
kombu.exceptions
================

Exceptions.

"""
from socket import timeout as TimeoutError

from amqp import ChannelError, ConnectionError, ResourceError

__all__ = [
    'NotBoundError', 'MessageStateError', 'TimeoutError',
    'LimitExceeded', 'ConnectionLimitExceeded',
    'ChannelLimitExceeded', 'ConnectionError', 'ChannelError',
    'VersionMismatch', 'SerializerNotInstalled', 'ResourceError',
    'SerializationError', 'EncodeError', 'DecodeError', 'HttpError',
]


class KombuError(Exception):
    """Common subclass for all Kombu exceptions."""
    ...


class SerializationError(KombuError):
    """Failed to serialize/deserialize content."""


class EncodeError(SerializationError):
    """Cannot encode object."""
    ...


class DecodeError(SerializationError):
    """Cannot decode object."""


class NotBoundError(KombuError):
    """Trying to call channel dependent method on unbound entity."""
    ...


class MessageStateError(KombuError):
    """The message has already been acknowledged."""
    ...


class LimitExceeded(KombuError):
    """Limit exceeded."""
    ...


class ConnectionLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous connections exceeded."""
    ...


class ChannelLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous channels exceeded."""
    ...


class VersionMismatch(KombuError):
    ...


class SerializerNotInstalled(KombuError):
    """Support for the requested serialization type is not installed"""
    ...


class ContentDisallowed(SerializerNotInstalled):
    """Consumer does not allow this content-type."""
    ...


class InconsistencyError(ConnectionError):
    """Data or environment has been found to be inconsistent,
    depending on the cause it may be possible to retry the operation."""
    ...


class HttpError(Exception):

    def __init__(self, code, message=None, response=None):
        self.code = code
        self.message = message
        self.response = response
        super().__init__(code, message, response)

    def __str__(self):
        return 'HTTP {0.code}: {0.message}'.format(self)
