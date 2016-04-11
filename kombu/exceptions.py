"""
kombu.exceptions
================

Exceptions.

"""
from __future__ import absolute_import, unicode_literals

from socket import timeout as TimeoutError

from amqp import ChannelError, ConnectionError, ResourceError

from kombu.five import python_2_unicode_compatible

__all__ = [
    'NotBoundError', 'MessageStateError', 'TimeoutError',
    'LimitExceeded', 'ConnectionLimitExceeded',
    'ChannelLimitExceeded', 'ConnectionError', 'ChannelError',
    'VersionMismatch', 'SerializerNotInstalled', 'ResourceError',
    'SerializationError', 'EncodeError', 'DecodeError', 'HttpError',
]


class KombuError(Exception):
    """Common subclass for all Kombu exceptions."""
    pass


class SerializationError(KombuError):
    """Failed to serialize/deserialize content."""


class EncodeError(SerializationError):
    """Cannot encode object."""
    pass


class DecodeError(SerializationError):
    """Cannot decode object."""


class NotBoundError(KombuError):
    """Trying to call channel dependent method on unbound entity."""
    pass


class MessageStateError(KombuError):
    """The message has already been acknowledged."""
    pass


class LimitExceeded(KombuError):
    """Limit exceeded."""
    pass


class ConnectionLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous connections exceeded."""
    pass


class ChannelLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous channels exceeded."""
    pass


class VersionMismatch(KombuError):
    pass


class SerializerNotInstalled(KombuError):
    """Support for the requested serialization type is not installed"""
    pass


class ContentDisallowed(SerializerNotInstalled):
    """Consumer does not allow this content-type."""
    pass


class InconsistencyError(ConnectionError):
    """Data or environment has been found to be inconsistent,
    depending on the cause it may be possible to retry the operation."""
    pass


@python_2_unicode_compatible
class HttpError(Exception):

    def __init__(self, code, message=None, response=None):
        self.code = code
        self.message = message
        self.response = response
        super(HttpError, self).__init__(code, message, response)

    def __str__(self):
        return 'HTTP {0.code}: {0.message}'.format(self)
