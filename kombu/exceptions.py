class NotBoundError(Exception):
    """Trying to call channel dependent method on unbound entity."""


class MessageStateError(Exception):
    """The message has already been acknowledged."""
