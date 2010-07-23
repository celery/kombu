class NotBoundError(Exception):
    """Trying to call channel dependent method on unbound entity."""


class MessageStateError(Exception):
    """The message has already been acknowledged."""


class EnsureExhausted(Exception):
    """ensure() limit exceeded."""


class PoolExhausted(Exception):
    """All connections acquired."""


class PoolLimitExceeded(Exception):
    """Can't add more connections to pool."""

class TimeoutError(Exception):
    """Operation timed out."""

