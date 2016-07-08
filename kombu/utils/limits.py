"""
kombu.utils.limits
==================

Token bucket implementation for rate limiting.

"""
from collections import deque
from time import monotonic
from typing import Any, Optional

from .typing import Float

__all__ = ['TokenBucket']


class TokenBucket:
    """Token Bucket Algorithm.

    See http://en.wikipedia.org/wiki/Token_Bucket
    Most of this code was stolen from an entry in the ASPN Python Cookbook:
    http://code.activestate.com/recipes/511490/

    .. admonition:: Thread safety

        This implementation is not thread safe. Access to a `TokenBucket`
        instance should occur within the critical section of any multithreaded
        code.

    """

    #: The rate in tokens/second that the bucket will be refilled.
    fill_rate = None

    #: Maximum number of tokens in the bucket.
    capacity = 1.0    # type: float

    #: Timestamp of the last time a token was taken out of the bucket.
    timestamp = None  # type: Optional[float]

    def __init__(self, fill_rate: Optional[Float],
                 capacity: Float=1.0) -> None:
        self.capacity = float(capacity)
        self._tokens = capacity
        self.fill_rate = float(fill_rate)
        self.timestamp = monotonic()
        self.contents = deque()  # type: deque

    def add(self, item: Any) -> None:
        self.contents.append(item)

    def pop(self) -> Any:
        return self.contents.popleft()

    def clear_pending(self) -> None:
        self.contents.clear()

    def can_consume(self, tokens: int=1) -> bool:
        """Return :const:`True` if the number of tokens can be consumed
        from the bucket.  If they can be consumed, a call will also consume the
        requested number of tokens from the bucket. Calls will only consume
        `tokens` (the number requested) or zero tokens -- it will never consume
        a partial number of tokens."""
        if tokens <= self._get_tokens():
            self._tokens -= tokens
            return True
        return False

    def expected_time(self, tokens: int=1) -> float:
        """Return the time (in seconds) when a new token is expected
        to be available. This will not consume any tokens from the bucket."""
        _tokens = self._get_tokens()
        tokens_f = max(tokens, _tokens)
        return (tokens_f - _tokens) / self.fill_rate

    def _get_tokens(self) -> float:
        if self._tokens < self.capacity:
            now = monotonic()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
        return self._tokens
