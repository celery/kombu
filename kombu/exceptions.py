class NotBoundError(Exception):
    """Trying to call channel dependent method on unbound entity."""


class MessageStateError(Exception):
    """The message has already been acknowledged."""


class EnsureExhausted(Exception):
    """ensure() limit exceeded."""


class TimeoutError(Exception):
    """Operation timed out."""


class LimitExceeded(Exception):
    """Limit exceeded."""
    pass


class ConnectionLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous connections exceeded."""
    pass


class ChannelLimitExceeded(LimitExceeded):
    """Maximum number of simultaenous channels exceeded."""
    pass
