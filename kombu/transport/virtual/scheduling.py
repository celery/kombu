"""
    kombu.transport.virtual.scheduling
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Consumer utilities.

    :copyright: (c) 2009 - 2010 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from itertools import count


class FairCycle(object):
    """Consume from a set of resources, where each resource gets
    an equal chance to be consumed from."""

    def __init__(self, fun, resources, predicate=Exception):
        self.fun = fun
        self.resources = resources
        self.predicate = predicate
        self.pos = 0

    def _next(self):
        while 1:
            try:
                resource = self.resources[self.pos]
                self.pos += 1
                return resource
            except IndexError:
                self.pos = 0

    def get(self):
        for tried in count(0):
            resource = self._next()

            try:
                return self.fun(resource), resource
            except self.predicate:
                if tried >= len(self.resources) - 1:
                    raise
