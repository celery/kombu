from Queue import Empty

from anyjson import serialize, deserialize
from redis import Redis
from redis import exceptions

from kombu.transport import virtual

DEFAULT_PORT = 6379
DEFAULT_DB = 0


class Channel(virtual.Channel):
    _client = None
    supports_fanout = True
    keyprefix_fanout = "_kombu.fanout.%s"
    keyprefix_queue = "_kombu.binding.%s"
    sep = '\x06\x16'

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        self.client.sadd(self.keyprefix_queue % (exchange, ),
                         self.sep.join([routing_key or "",
                                        pattern or "",
                                        queue or ""]))

    def get_table(self, exchange):
        members = self.client.smembers(self.keyprefix_queue % (exchange, ))
        return [tuple(val.split(self.sep)) for val in members]

    def _new_queue(self, queue, **kwargs):
        pass

    def _get(self, queue):
        item = self.client.rpop(queue)
        if item:
            return deserialize(item)
        raise Empty()

    def _size(self, queue):
        return self.client.llen(queue)

    def _get_many(self, queues, timeout=None):
        dest__item = self.client.brpop(queues, timeout=timeout)
        if dest__item:
            dest, item = dest__item
            return deserialize(item), dest
        raise Empty()

    def _put(self, queue, message, **kwargs):
        self.client.lpush(queue, serialize(message))

    def _purge(self, queue):
        size = self.client.llen(queue)
        self.client.delete(queue)
        return size

    def close(self):
        super(Channel, self).close()
        try:
            self.client.bgsave()
        except exceptions.ResponseError:
            pass

    def _open(self):
        conninfo = self.connection.client
        database = conninfo.virtual_host
        if not isinstance(database, int):
            if not database or database == "/":
                database = DEFAULT_DB
            elif database.startswith("/"):
                database = database[1:]
            try:
                database = int(database)
            except ValueError:
                raise ValueError(
                    "Database name must be int between 0 and limit - 1")

        return Redis(host=conninfo.hostname,
                     port=conninfo.port or DEFAULT_PORT,
                     db=database,
                     password=conninfo.password)

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client


class Transport(virtual.Transport):
    Channel = Channel

    interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (exceptions.ConnectionError,
                         exceptions.AuthenticationError)
    channel_errors = (exceptions.ConnectionError,
                      exceptions.InvalidData,
                      exceptions.InvalidResponse,
                      exceptions.ResponseError)
