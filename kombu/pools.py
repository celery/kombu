"""
kombu.pools
===========

Public resource pools.

:copyright: (c) 2009 - 2012 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import os

from itertools import chain

from .connection import Resource
from .messaging import Producer
from .utils import EqualityDict

__all__ = ["ProducerPool", "PoolGroup", "register_group",
           "connections", "producers", "get_limit", "set_limit", "reset"]
_limit = [200]
_used = [False]
_groups = []
use_global_limit = object()
disable_limit_protection = os.environ.get("KOMBU_DISABLE_LIMIT_PROTECTION")


class ProducerPool(Resource):

    def __init__(self, connections, *args, **kwargs):
        self.connections = connections
        super(ProducerPool, self).__init__(*args, **kwargs)

    def Producer(self, connection):
        return Producer(connection)

    def _acquire_connection(self):
        return self.connections.acquire(block=True)

    def create_producer(self):
        return self.Producer(self._acquire_connection())

    def new(self):
        return lambda: self.create_producer()

    def setup(self):
        if self.limit:
            for _ in xrange(self.limit):
                self._resource.put_nowait(self.new())

    def prepare(self, p):
        if callable(p):
            p = p()
        if not p.channel:
            connection = self._acquire_connection()
            p.revive(connection.default_channel)
        return p

    def release(self, resource):
        if resource.connection:
            resource.connection.release()
        resource.channel = None
        super(ProducerPool, self).release(resource)


class PoolGroup(EqualityDict):

    def __init__(self, limit=None):
        self.limit = limit

    def create(self, resource, limit):
        raise NotImplementedError("PoolGroups must define ``create``")

    def __missing__(self, resource):
        limit = self.limit
        if limit is use_global_limit:
            limit = get_limit()
        if not _used[0]:
            _used[0] = True
        k = self[resource] = self.create(resource, limit)
        return k


def register_group(group):
    _groups.append(group)
    return group


class Connections(PoolGroup):

    def create(self, connection, limit):
        return connection.Pool(limit=limit)
connections = register_group(Connections(limit=use_global_limit))


class Producers(PoolGroup):

    def create(self, connection, limit):
        return ProducerPool(connections[connection], limit=limit)
producers = register_group(Producers(limit=use_global_limit))


def _all_pools():
    return chain(*[(g.itervalues() if g else iter([])) for g in _groups])


def get_limit():
    return _limit[0]


def set_limit(limit, force=False, reset_after=False):
    limit = limit or 0
    glimit = _limit[0] or 0
    if limit < glimit:
        if not disable_limit_protection and (_used[0] and not force):
            raise RuntimeError("Can't lower limit after pool in use.")
        reset_after = True
    if limit != glimit:
        _limit[0] = limit
        for pool in _all_pools():
            pool.limit = limit
        if reset_after:
            reset()
    return limit


def reset(*args, **kwargs):
    for pool in _all_pools():
        try:
            pool.force_close_all()
        except Exception:
            pass
    for group in _groups:
        group.clear()
    _used[0] = False

try:
    from multiprocessing.util import register_after_fork
    register_after_fork(connections, reset)
except ImportError:  # pragma: no cover
    pass
