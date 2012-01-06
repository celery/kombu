"""
kombu.exceptions
================

Exceptions.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import socket

__all__ = ["NotBoundError", "MessageStateError", "TimeoutError",
           "LimitExceeded", "ConnectionLimitExceeded",
           "ChannelLimitExceeded", "StdChannelError", "VersionMismatch",
           "SerializerNotInstalled"]

TimeoutError = socket.timeout


class NotBoundError(Exception):
    """Trying to call channel dependent method on unbound entity."""
    pass


class MessageStateError(Exception):
    """The message has already been acknowledged."""
    pass


class LimitExceeded(Exception):
    """Limit exceeded."""
    pass


class ConnectionLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous connections exceeded."""
    pass


class ChannelLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous channels exceeded."""
    pass


class StdChannelError(Exception):
    pass


class VersionMismatch(Exception):
    pass


class SerializerNotInstalled(StandardError):
    """Support for the requested serialization type is not installed"""
    pass
