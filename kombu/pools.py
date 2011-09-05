from kombu.connection import Resource
from kombu.messaging import Producer

from itertools import chain

__all__ = ["ProducerPool", "connections", "producers", "set_limit", "reset"]
_limit = [200]


class ProducerPool(Resource):
    Producer = Producer

    def __init__(self, connections, *args, **kwargs):
        self.connections = connections
        super(ProducerPool, self).__init__(*args, **kwargs)

    def create_producer(self):
        conn = self.connections.acquire(block=True)
        channel = conn.channel()
        producer = self.Producer(channel)
        producer.connection = conn
        conn._producer_chan = channel
        return producer

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
            if not getattr(p.connection, "_producer_chan", None):
                p.connection._producer_chan = p.connection.channel()
            p.revive(p.connection._producer_chan)
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


class _Connections(HashingDict):

    def __missing__(self, connection):
        k = self[connection] = connection.Pool(limit=_limit[0])
        return k
connections = _Connections()


class _Producers(HashingDict):

    def __missing__(self, conn):
        k = self[conn] = ProducerPool(connections[conn], limit=_limit[0])
        return k
producers = _Producers()


def _all_pools():
    return chain(connections.itervalues() if connections else iter([]),
                 producers.itervalues() if producers else iter([]))


def set_limit(limit):
    _limit[0] = limit
    for pool in _all_pools():
        pool.limit = limit
    return limit


def reset():
    global connections
    global producers
    for pool in _all_pools():
        try:
            pool.force_close_all()
        except Exception:
            pass
    connections = _Connections()
    producers._Producers()


try:
    from multiprocessing.util import register_after_fork
    register_after_fork(connections, reset)
except ImportError:
    pass
