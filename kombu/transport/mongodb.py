"""
kombu.transport.mongodb
=======================

MongoDB transport.

:copyright: (c) 2010 - 2012 by Flavio Percoco Premoli.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from Queue import Empty

import pymongo
from pymongo import errors
from anyjson import loads, dumps
from pymongo.connection import Connection

from . import virtual

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 27017

__author__ = "Flavio [FlaPer87] Percoco Premoli <flaper87@flaper87.org>"


class Channel(virtual.Channel):
    _client = None

    def _new_queue(self, queue, **kwargs):
        pass

    def _get(self, queue):
        try:
            msg = self.client.database.command("findandmodify", "messages",
                    query={"queue": queue},
                    sort={"_id": pymongo.ASCENDING}, remove=True)
        except errors.OperationFailure, exc:
            if "No matching object found" in exc.args[0]:
                raise Empty()
            raise
        # as of mongo 2.0 empty results won't raise an error
        if msg['value'] is None:
            raise Empty()
        return loads(msg["value"]["payload"])

    def _size(self, queue):
        return self.client.find({"queue": queue}).count()

    def _put(self, queue, message, **kwargs):
        self.client.insert({"payload": dumps(message), "queue": queue})

    def _purge(self, queue):
        size = self._size(queue)
        self.client.remove({"queue": queue})
        return size

    def close(self):
        super(Channel, self).close()
        if self._client:
            self._client.database.connection.end_request()

    def _open(self):
        conninfo = self.connection.client
        mongoconn = Connection(host=conninfo.hostname, port=conninfo.port)
        dbname = conninfo.virtual_host
        version = mongoconn.server_info()["version"]
        if tuple(map(int, version.split(".")[:2])) < (1, 3):
            raise NotImplementedError(
                "Kombu requires MongoDB version 1.3+, but connected to %s" % (
                    version, ))
        if not dbname or dbname == "/":
            dbname = "kombu_default"
        database = getattr(mongoconn, dbname)
        if conninfo.userid:
            database.authenticate(conninfo.userid, conninfo.password)
        col = database.messages
        col.ensure_index([("queue", 1)])
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
