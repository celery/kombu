"""Time Utilities."""
from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from math import isfinite
from threading import TIMEOUT_MAX
from time import monotonic

from kombu.exceptions import PublicationTimeout

__all__ = ('maybe_s_to_ms', 'publication_timeout')

_publication_deadline: ContextVar[Deadline | None] = ContextVar(
    'kombu_publication_deadline', default=None)


class Deadline:
    """One monotonic time budget, including retries and nested calls."""

    def __init__(self, timeout: float) -> None:
        try:
            valid = (isinstance(timeout, (int, float)) and
                     not isinstance(timeout, bool) and
                     isfinite(timeout) and 0 <= timeout <= TIMEOUT_MAX)
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError(
                'publication_timeout must be a finite non-negative number '
                '<= threading.TIMEOUT_MAX')
        self.expires = monotonic() + timeout

    def remaining(self, limit: float | None = None) -> float:
        remaining = self.expires - monotonic()
        if remaining <= 0:
            raise PublicationTimeout()
        return remaining if limit is None else min(limit, remaining)

    def check(self) -> None:
        self.remaining()


def current_publication_deadline() -> Deadline | None:
    return _publication_deadline.get()


@contextmanager
def publication_timeout(timeout: float | None = None) -> Iterator[Deadline | None]:
    """Bound cooperative publication work in the current execution context."""
    deadline = previous = current_publication_deadline()
    if timeout is not None:
        candidate = Deadline(timeout)
        if previous is None or candidate.expires < previous.expires:
            deadline = candidate
    token = _publication_deadline.set(deadline)
    try:
        if deadline is not None:
            deadline.check()
        yield deadline
        if deadline is not None:
            deadline.check()
    finally:
        _publication_deadline.reset(token)


def maybe_s_to_ms(v: int | float | None) -> int | None:
    """Convert seconds to milliseconds, but return None for None."""
    return int(float(v) * 1000.0) if v is not None else v
