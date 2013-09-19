from __future__ import absolute_import

__all__ = ['DummyLock', 'LaxBoundedSemaphore']


class LaxBoundedSemaphore(object):
    """Asynchronous Bounded Semaphore.

    Lax means that the value will stay within the specified
    range even if released more times than it was acquired.

    Example:

        >>> x = LaxBoundedSemaphore(2)

        >>> def callback(i):
        ...     say('HELLO {0!r}'.format(i))

        >>> x.acquire(callback, 1)
        HELLO 1

        >>> x.acquire(callback, 2)
        HELLO 2

        >>> x.acquire(callback, 3)
        >>> x._waiters   # private, do not access directly
        [(callback, 3)]

        >>> x.release()
        HELLO 3

    """

    def __init__(self, value):
        self.initial_value = self.value = value
        self._waiting = set()

    def acquire(self, callback, *partial_args):
        """Acquire semaphore, applying ``callback`` if
        the resource is available.

        :param callback: The callback to apply.
        :param \*partial_args: partial arguments to callback.

        """
        if self.value <= 0:
            self._waiting.append((callback, partial_args))
            return False
        else:
            self.value = max(self.value - 1, 0)
            callback(*partial_args)
            return True

    def release(self):
        """Release semaphore.

        If there are any waiters this will apply the first waiter
        that is waiting for the resource (FIFO order).

        """
        self.value = min(self.value + 1, self.initial_value)
        if self._waiting:
            waiter, args = self._waiting.pop()
            waiter(*args)

    def grow(self, n=1):
        """Change the size of the semaphore to accept more users."""
        self.initial_value += n
        self.value += n
        [self.release() for _ in range(n)]

    def shrink(self, n=1):
        """Change the size of the semaphore to accept less users."""
        self.initial_value = max(self.initial_value - n, 0)
        self.value = max(self.value - n, 0)

    def clear(self):
        """Reset the sempahore, which also wipes out any waiting callbacks."""
        self._waiting[:] = []
        self.value = self.initial_value

    def __repr__(self):
        return '<{0} at {1:#x} value:{2} waiting:{3}>'.format(
            self.__class__.__name__, id(self), self.value, len(self.waiting),
        )


class DummyLock(object):
    """Pretending to be a lock."""

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        pass
