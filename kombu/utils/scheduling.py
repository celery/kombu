"""
    kombu.utils.scheduling
    ~~~~~~~~~~~~~~~~~~~~~~

    Consumer utilities.

"""
from __future__ import absolute_import, unicode_literals

from itertools import count
from typing import Any, Callable, Iterable, Optional, Sequence, Union
from typing import List  # noqa

from kombu.five import python_2_unicode_compatible

from . import symbol_by_name

__all__ = [
    'FairCycle', 'priority_cycle', 'round_robin_cycle', 'sorted_cycle',
]


@python_2_unicode_compatible
class FairCycle:
    """Consume from a set of resources, where each resource gets
    an equal chance to be consumed from."""

    def __init__(self, fun: Callable, resources: Sequence,
                 predicate: Any=Exception) -> None:
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

    def get(self, **kwargs) -> Any:
        for tried in count(0):  # for infinity
            resource = self._next()

            try:
                return self.fun(resource, **kwargs), resource
            except self.predicate:
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

    def __init__(self, it: Optional[Iterable]=None) -> None:
        self.items = list(it if it is not None else [])  # type: List

    def update(self, it: Sequence) -> None:
        self.items[:] = it

    def consume(self, n: int) -> Any:
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

    def rotate(self, last_used: Any) -> Any:
        ...


class sorted_cycle(priority_cycle):

    def consume(self, n: int) -> Any:
        return sorted(self.items[:n])


CYCLE_ALIASES = {
    'priority': 'kombu.utils.scheduling:priority_cycle',
    'round_robin': 'kombu.utils.scheduling:round_robin_cycle',
    'sorted': 'kombu.utils.scheduling:sorted_cycle',
}


def cycle_by_name(name: Union[str, BaseCycle]) -> BaseCycle:
    return symbol_by_name(name, CYCLE_ALIASES)
