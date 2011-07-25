from kombu.connection import Resource
from kombu.messaging import Producer


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
