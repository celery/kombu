"""Scheduling Utilities."""
from itertools import count
from typing import Any, Callable, Iterable, Sequence, Union
from typing import List  # noqa
from .imports import symbol_by_name

__all__ = [
    'FairCycle', 'priority_cycle', 'round_robin_cycle', 'sorted_cycle',
]

CYCLE_ALIASES = {
    'priority': 'kombu.utils.scheduling:priority_cycle',
    'round_robin': 'kombu.utils.scheduling:round_robin_cycle',
    'sorted': 'kombu.utils.scheduling:sorted_cycle',
}


class FairCycle:
    """Cycle between resources.

    Consume from a set of resources, where each resource gets
    an equal chance to be consumed from.

    Arguments:
        fun (Callable): Callback to call.
        resources (Sequence): List of resources.
        predicate (type): Exception predicate.
    """

    def __init__(self, fun: Callable, resources: Sequence,
                 predicate: Any = Exception) -> None:
        self.fun = fun
        self.resources = resources
        self.predicate = predicate
        self.pos = 0

    def _next(self) -> Any:
        while 1:
            try:
                resource = self.resources[self.pos]
                self.pos += 1
                return resource
            except IndexError:
                self.pos = 0
                if not self.resources:
                    raise self.predicate()

    def get(self, callback: Callable, **kwargs) -> Any:
        """Get from next resource."""
        for tried in count(0):  # for infinity
            resource = self._next()
            try:
                return self.fun(resource, callback, **kwargs)
            except self.predicate:
                # reraise when retries exchausted.
                if tried >= len(self.resources) - 1:
                    raise

    def close(self) -> None:
        ...

    def __repr__(self) -> str:
        return '<FairCycle: {self.pos}/{size} {self.resources}>'.format(
            self=self, size=len(self.resources))


class BaseCycle:
    ...


class round_robin_cycle(BaseCycle):
    """Iterator that cycles between items in round-robin."""

    def __init__(self, it: Iterable = None) -> None:
        self.items = list(it if it is not None else [])

    def update(self, it: Sequence) -> None:
        """Update items from iterable."""
        self.items[:] = it

    def consume(self, n: int) -> Any:
        """Consume n items."""
        return self.items[:n]

    def rotate(self, last_used: Any) -> Any:
        """Move most recently used item to end of list."""
        items = self.items
        try:
            items.append(items.pop(items.index(last_used)))
        except ValueError:
            pass
        return last_used


class priority_cycle(round_robin_cycle):
    """Cycle that repeats items in order."""

    def rotate(self, last_used: Any) -> Any:
        # Unused in this implementation.
        ...


class sorted_cycle(priority_cycle):
    """Cycle in sorted order."""

    def consume(self, n: int) -> Any:
        """Consume n items."""
        return sorted(self.items[:n])


def cycle_by_name(name: Union[str, BaseCycle]) -> BaseCycle:
    """Get cycle class by name."""
    return symbol_by_name(name, CYCLE_ALIASES)
