from kombu.connection import Resource
from kombu.messaging import Producer

from itertools import chain

__all__ = ["ProducerPool", "connections", "producers", "set_limit", "reset"]
_limit = [200]
_groups = []


def register_group(group):
    _groups.append(group)
    return group


class ProducerPool(Resource):

    def __init__(self, connections, *args, **kwargs):
        self.connections = connections
        super(ProducerPool, self).__init__(*args, **kwargs)

    def Producer(self, connection):
        return Producer(connection)

    def create_producer(self):
        return self.Producer(self.connections.acquire(block=True))

    def new(self):
        return lambda: self.create_producer()

    def setup(self):
        if self.limit:
            for _ in xrange(self.limit):
                self._resource.put_nowait(self.new())

    def prepare(self, p):
        if callable(p):
            p = p()
        if not p.connection:
            p.connection = self.connections.acquire(block=True)
            p.revive(p.connection.default_channel)
        return p

    def release(self, resource):
        resource.connection.release()
        resource.connection = None
        super(ProducerPool, self).release(resource)


class HashingDict(dict):

    def __getitem__(self, key):
        h = hash(key)
        if h not in self:
            return self.__missing__(key)
        return dict.__getitem__(self, h)

    def __setitem__(self, key, value):
        return dict.__setitem__(self, hash(key), value)

    def __delitem__(self, key):
        return dict.__delitem__(self, hash(key))


class PoolGroup(HashingDict):

    def create(self, resource, limit):
        raise NotImplementedError("PoolGroups must define ``create``")

    def __missing__(self, resource):
        k = self[resource] = self.create(resource, get_limit())
        return k


class _Connections(PoolGroup):

    def create(self, connection, limit):
        return connection.Pool(limit=limit)
connections = register_group(_Connections())


class _Producers(HashingDict):

    def create(self, connection, limit):
        return ProducerPool(connections[connection], limit=limit)
producers = register_group(_Producers())


def _all_pools():
    return chain(*[(g.itervalues() if g else iter([])) for g in _groups])


def get_limit():
    return _limit[0]


def set_limit(limit):
    if _limit[0] != limit:
        _limit[0] = limit
        for pool in _all_pools():
            pool.limit = limit
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


try:
    from multiprocessing.util import register_after_fork
    register_after_fork(connections, reset)
except ImportError:
    pass
