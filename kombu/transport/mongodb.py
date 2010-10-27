
"""
kombu.transport.mongodb
=======================

MongoDB transport.

:copyright: (c) 2009 - 2010 by Flavio Percoco Premoli.
:license: BSD, see LICENSE for more details.

"""
from Queue import Empty

from anyjson import serialize, deserialize
from pymongo import errors
from pymongo.connection import Connection

from kombu.transport import virtual

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 27017

__author__ = "Flavio [FlaPer87] Percoco Premoli <flaper87@flaper87.org>"


class Channel(virtual.Channel):
    _client = None
    _mongoconn = None
    _database = None

    def _new_queue(self, queue, **kwargs):
        pass

    def _get(self, queue):
        try:
            msg = self.client.database.command("findandmodify",
                        "messages", query={"queue": queue}, remove=True)
        except OperationFailure:
            raise Empty()
        return msg["value"]["payload"]

    def _size(self, queue):
        return self.client.count()

    def _put(self, queue, message, **kwargs):
        self.client.insert({"payload": serialize(message), "queue": queue})

    def _purge(self, queue):
        size = self._size(queue)
        self.client.remove({"queue": queue})

    def close(self):
        super(Channel, self).close()
        self._mongoconn.end_request()
        self._database = None
        self._mongoconn = None

    def _open(self):
        conninfo = self.connection.client
        mongoconn = Connection(host=conninfo.hostname, port=conninfo.port)
        dbname = conninfo.virtual_host
        if not dbname or dbname == "/":
            dbname = "kombu_default"
        database = getattr(mongoconn, connection, dbname)
        col = self.database.messages
        col.ensure_index([("queue", 1)])
        self._database = database
        self._mongoconn = mongoconn
        return col

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client


class Transport(virtual.Transport):
    Channel = Channel

    interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (errors.ConnectionFailure, )
    channel_errors = (errors.ConnectionFailure,
                      errors.OperationFailure, )
