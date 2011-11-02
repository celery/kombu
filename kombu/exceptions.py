"""
kombu.exceptions
================

Exceptions.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""


class NotBoundError(Exception):
    """Trying to call channel dependent method on unbound entity."""
    pass


class MessageStateError(Exception):
    """The message has already been acknowledged."""
    pass


class TimeoutError(Exception):
    """Operation timed out."""
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
