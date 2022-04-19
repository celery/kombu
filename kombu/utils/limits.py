"""Token bucket implementation for rate limiting."""

from __future__ import annotations

from time import monotonic

__all__ = ('TokenBucket',)


class TokenBucket:
    """Token Bucket Algorithm.

    See Also:
        https://en.wikipedia.org/wiki/Token_Bucket

        Most of this code was stolen from an entry in the ASPN Python Cookbook:
        https://code.activestate.com/recipes/511490/

    Warning:
        Thread Safety: This implementation is not thread safe.
        Access to a `TokenBucket` instance should occur within the critical
        section of any multithreaded code.
    """

    def __init__(
        self,
        fill_rate: str | int | float,
        capacity: int = 1
    ) -> None:
        #: Maximum number of tokens in the bucket.
        self.capacity = self._tokens = float(capacity)

        #: Rate in tokens/second that the bucket will be refilled.
        self.fill_rate = float(fill_rate)

        #: Timestamp of the last time a token was taken out of the bucket.
        self.timestamp: float = monotonic()

    def can_consume(self, tokens: int = 1) -> bool:
        """Check if one or more tokens can be consumed.

        Returns:
            bool: true if the number of tokens can be consumed
                from the bucket.  If they can be consumed, a call will also
                consume the requested number of tokens from the bucket.
                Calls will only consume `tokens` (the number requested)
                or zero tokens -- it will never consume a partial number
                of tokens.
        """
        if tokens <= self._get_tokens():
            self._tokens -= tokens
            return True
        return False

    def expected_time(self, tokens: float = 1.0) -> float:
        """Return estimated time of token availability.

        Returns:
            float: the time in seconds.
        """
        _tokens = self._get_tokens()
        tokens = max(tokens, _tokens)
        return (tokens - _tokens) / self.fill_rate

    def _get_tokens(self) -> float:
        if self._tokens < self.capacity:
            now = monotonic()
            delta = self.fill_rate * (now - self.timestamp)
            self._tokens = min(self.capacity, self._tokens + delta)
            self.timestamp = now
        return self._tokens
