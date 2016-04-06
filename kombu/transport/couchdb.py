"""
kombu.transport.couchdb
=======================

CouchDB transport.

:copyright: (c) 2010 - 2013 by David Clymer.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import, unicode_literals

import socket

from kombu.five import Empty
from kombu.utils import uuid4
from kombu.utils.encoding import bytes_to_str
from kombu.utils.json import loads, dumps

from . import virtual

try:
    import pycouchdb
    from pycouchdb import exceptions
    from requests import exceptions as requests_exceptions
except ImportError:  # pragma: no cover
    pycouchdb = exceptions = requests_exceptions = None   # noqa

DEFAULT_PORT = 5984
DEFAULT_DATABASE = 'kombu_default'

__author__ = 'David Clymer <david@zettazebra.com>'


def create_message_view(container):
    _id = '_design/kombu'
    try:
        existing = container.get(_id)
    except exceptions.NotFound:
        existing = {'_id': _id, 'views': {}}
    existing['language'] = 'javascript'
    existing['views']['messages'] = {
        'map': """
        function (doc) {
          if (doc.queue && doc.payload)
            emit(doc.queue, doc);
        }
        """
    }
    container.save(existing)


class Channel(virtual.Channel):
    _client = None

    view_created = False

    def _put(self, queue, message, **kwargs):
        self.client.save({'_id': uuid4().hex,
                          'queue': queue,
                          'payload': dumps(message)})

    def _get(self, queue):
        result = self._query(queue, limit=1)
        if not result:
            raise Empty()

        try:
            item = result[0]['value']
        except (KeyError, IndexError):
            raise Empty()
        self.client.delete(item['_id'])
        return loads(bytes_to_str(item['payload']))

    def _purge(self, queue):
        result = self._query(queue)
        for item in result:
            self.client.delete(item['value']['_id'])
        return len(result)

    def _size(self, queue):
        return len(self._query(queue))

    def _open(self):
        conninfo = self.connection.client
        dbname = conninfo.virtual_host
        proto = conninfo.ssl and 'https' or 'http'
        if not dbname or dbname == '/':
            dbname = DEFAULT_DATABASE
        port = conninfo.port or DEFAULT_PORT

        if conninfo.userid and conninfo.password:
            server = pycouchdb.Server('%s://%s:%s@%s:%s/' % (
                proto, conninfo.userid, conninfo.password,
                conninfo.hostname, port),
                authmethod='basic')
        else:
            server = pycouchdb.Server('%s://%s:%s/' % (
                proto, conninfo.hostname, port))
        try:
            return server.database(dbname)
        except exceptions.NotFound:
            return server.create(dbname)

    def _query(self, queue, **kwargs):
        if not self.view_created:
            # if the message view is not yet set up, we'll need it now.
            create_message_view(self.client)
            self.view_created = True
        return list(self.client.query('kombu/messages', key=queue, **kwargs))

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client


class Transport(virtual.Transport):
    Channel = Channel

    polling_interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (
        virtual.Transport.connection_errors + (
            socket.error,
            getattr(exceptions, 'AuthenticationFailed', None),
            getattr(requests_exceptions, 'HTTPError', None),
            getattr(requests_exceptions, 'ConnectionError', None),
            getattr(requests_exceptions, 'SSLError', None),
            getattr(requests_exceptions, 'Timeout', None)
        )
    )
    channel_errors = (
        virtual.Transport.channel_errors + (
            getattr(exceptions, 'Error', None),
            getattr(exceptions, 'UnexpectedError', None),
            getattr(exceptions, 'NotFound', None),
            getattr(exceptions, 'Conflict', None),
            getattr(exceptions, 'ResourceNotFound', None),
            getattr(exceptions, 'GenericError', None)
        )
    )
    driver_type = 'couchdb'
    driver_name = 'couchdb'

    def __init__(self, *args, **kwargs):
        if pycouchdb is None:
            raise ImportError('Missing pycouchdb library (pip install pycouchdb)')  # noqa
        super(Transport, self).__init__(*args, **kwargs)

    def driver_version(self):
        return pycouchdb.__version__
