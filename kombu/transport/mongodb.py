"""
kombu.transport.mongodb
=======================

MongoDB transport.

:copyright: (c) 2010 - 2013 by Flavio Percoco Premoli.
:license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import datetime

import pymongo
from pymongo import errors
from pymongo import MongoClient, uri_parser
from pymongo.cursor import CursorType

from kombu.five import Empty
from kombu.syn import _detect_environment
from kombu.utils.encoding import bytes_to_str
from kombu.utils.json import loads, dumps

from . import virtual

DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 27017

DEFAULT_MESSAGES_COLLECTION = 'messages'
DEFAULT_ROUTING_COLLECTION = 'messages.routing'
DEFAULT_BROADCAST_COLLECTION = 'messages.broadcast'
DEFAULT_QUEUES_COLLECTION = 'messages.queues'


class BroadcastCursor(object):
    """Cursor for broadcast queues."""

    def __init__(self, cursor):
        self._cursor = cursor

        self.purge(rewind=False)

    def get_size(self):
        return self._cursor.count() - self._offset

    def close(self):
        self._cursor.close()

    def purge(self, rewind=True):
        if rewind:
            self._cursor.rewind()

        # Fast forward the cursor past old events
        self._offset = self._cursor.count()
        self._cursor = self._cursor.skip(self._offset)

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            try:
                msg = next(self._cursor)
            except pymongo.errors.OperationFailure as exc:
                # In some cases tailed cursor can become invalid
                # and have to be reinitalized
                if 'not valid at server' in exc.message:
                    self.purge()

                    continue

                raise
            else:
                break

        self._offset += 1

        return msg
    next = __next__


class Channel(virtual.Channel):
    _client = None
    supports_fanout = True

    # Mutable containers. Shared by all class instances
    _fanout_queues = {}

    # Options
    connect_timeout = None
    ssl = False
    capped_queue_size = 100000
    ttl = False

    from_transport_options = (
        virtual.Channel.from_transport_options
        + ('connect_timeout', 'ssl', 'ttl', 'capped_queue_size'))


    def __init__(self, *vargs, **kwargs):
        super(Channel, self).__init__(*vargs, **kwargs)

        self._broadcast_cursors = {}

        # Evaluate connection
        self._create_client()

    # AbstractChannel/Channel interface implementation

    def _new_queue(self, queue, **kwargs):
        if self.ttl:
            self.get_queues().update({'_id': queue},
                                     {'_id': queue,
                                      'options': kwargs,
                                      'expire_at': self.get_expire(kwargs, 'x-expires')},
                                     upsert=True)

    def _get(self, queue):
        if queue in self._fanout_queues:
            try:
                msg = next(self.get_broadcast_cursor(queue))
            except StopIteration:
                msg = None
        else:
            msg = self.get_messages().find_and_modify(
                query={'queue': queue},
                sort=[('priority', pymongo.ASCENDING),
                      ('_id', pymongo.ASCENDING)],
                remove=True,
            )

        if self.ttl:
            self.update_queues_expire(queue)

        if msg is None:
            raise Empty()

        return loads(bytes_to_str(msg['payload']))

    def _size(self, queue):
        if queue in self._fanout_queues:
            return self.get_broadcast_cursor(queue).get_size()

        return self.get_messages().find({'queue': queue}).count()

    def _put(self, queue, message, **kwargs):
        data = {
            'payload': dumps(message),
            'queue': queue,
            'priority': self._get_message_priority(message, reverse=True)
        }

        if self.ttl:
            data['expire_at'] = self.get_expire(queue, 'x-message-ttl')

        self.get_messages().insert(data)

    def _purge(self, queue):
        size = self._size(queue)

        if queue in self._fanout_queues:
            self.get_broadcast_cursor(queue).purge()
        else:
            self.get_messages().remove({'queue': queue})

        return size

    def get_table(self, exchange):
        """Get table of bindings for ``exchange``."""
        localRoutes = frozenset(self.state.exchanges[exchange]['table'])
        brokerRoutes = self.get_messages().routing.find(
            {'exchange': exchange}
        )

        return localRoutes | frozenset((r['routing_key'],
                                        r['pattern'],
                                        r['queue']) for r in brokerRoutes)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == 'fanout':
            self.create_broadcast_cursor(exchange, routing_key, pattern, queue)
            self._fanout_queues[queue] = exchange

        lookup = {'exchange': exchange,
                  'queue': queue,
                  'routing_key': routing_key,
                  'pattern': pattern}

        data = lookup.copy()

        if self.ttl:
            data['expire_at'] = self.get_expire(queue, 'x-expires')

        self.get_routing().update(lookup, data, upsert=True)

    def queue_delete(self, queue, **kwargs):
        self.get_routing().remove({'queue': queue})

        if self.ttl:
            self.get_queues().remove({'_id': queue})

        super(Channel, self).queue_delete(queue, **kwargs)

        if queue in self._fanout_queues:
            try:
                cursor = self._broadcast_cursors.pop(queue)
            except KeyError:
                pass
            else:
                cursor.close()

                self._fanout_queues.pop(queue)

    # Implementation details

    def _parse_uri(self, scheme='mongodb://'):
        # See mongodb uri documentation:
        # http://docs.mongodb.org/manual/reference/connection-string/
        client = self.connection.client
        hostname = client.hostname

        if not hostname.startswith(scheme):
            hostname = scheme + hostname

        if not hostname[len(scheme):]:
            hostname += DEFAULT_HOST

        if client.userid and '@' not in hostname:
            head, tail = hostname.split('://')

            credentials = client.userid
            if client.password:
                credentials += ':' + client.password

            hostname = head + '://' + credentials + '@' + tail

        port = client.port if client.port is not None else DEFAULT_PORT

        parsed = uri_parser.parse_uri(hostname, port)

        dbname = parsed['database'] or client.virtual_host

        if dbname in ('/', None):
            dbname = 'kombu_default'

        options = {
            'auto_start_request': True,
            'ssl': self.ssl,
            'connectTimeoutMS': (int(self.connect_timeout * 1000)
                                 if self.connect_timeout else None),
        }
        options.update(parsed['options'])

        return hostname, dbname, options

    def _prepare_client_options(self, options):
        if pymongo.version_tuple >= (3,):
            options.pop('auto_start_request', None)
        return options

    def _open(self, scheme='mongodb://'):
        hostname, dbname, options = self._parse_uri(scheme=scheme)

        conf = self._prepare_client_options(options)
        conf['host'] = hostname

        env = _detect_environment()
        if env == 'gevent':
            from gevent import monkey
            monkey.patch_all()
        elif env == 'eventlet':
            from eventlet import monkey_patch
            monkey_patch()

        mongoconn = MongoClient(**conf)
        database = mongoconn[dbname]

        version_str = mongoconn.server_info()['version']
        version = tuple(map(int, version_str.split('.')))

        if version < (1, 3):
            raise NotImplementedError('Kombu requires MongoDB version 1.3+'
                                      '(server is {0})'.format(version_str))
        elif self.ttl and version < (2, 2):
            raise NotImplementedError('Kombu requires MongoDB version 2.2+'
                                      '(server is {0}) for TTL indexes support'.format(version_str))

        self._create_broadcast(database)

        self._client = database

    def _create_broadcast(self, database):
        '''Create capped collection for broadcast messages.'''
        if DEFAULT_BROADCAST_COLLECTION in database.collection_names():
            return

        database.create_collection(DEFAULT_BROADCAST_COLLECTION,
                                   size=self.capped_queue_size, capped=True)

    def _ensure_indexes(self):
        '''Ensure indexes on collections.'''
        messages = self.get_messages()
        messages.ensure_index(
            [('queue', 1), ('priority', 1), ('_id', 1)], background=True,
        )

        self.get_broadcast().ensure_index([('queue', 1)])

        routing = self.get_routing()
        routing.ensure_index([('queue', 1), ('exchange', 1)])

        if self.ttl:
            messages.ensure_index([('expire_at', 1)], expireAfterSeconds=0)
            routing.ensure_index([('expire_at', 1)], expireAfterSeconds=0)

            self.get_queues().ensure_index([('expire_at', 1)], expireAfterSeconds=0)

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Deliver fanout message."""
        self.get_broadcast().insert({'payload': dumps(message),
                                     'queue': exchange})

    def _create_client(self):
        '''Actualy creates connection'''
        self._open()
        self._ensure_indexes()

    @property
    def client(self):
        if self._client is None:
            self._create_client()
        return self._client

    def get_messages(self):
        return self.client[DEFAULT_MESSAGES_COLLECTION]

    def get_routing(self):
        return self.client[DEFAULT_ROUTING_COLLECTION]

    def get_broadcast(self):
        return self.client[DEFAULT_BROADCAST_COLLECTION]

    def get_queues(self):
        return self.client[DEFAULT_QUEUES_COLLECTION]

    def get_broadcast_cursor(self, queue):
        try:
            return self._broadcast_cursors[queue]
        except KeyError:
            # Cursor may be absent when Channel created more than once.
            # _fanout_queues is a class-level mutable attribute so it's
            # shared over all Channel instances.
            return self.create_broadcast_cursor(
                self._fanout_queues[queue], None, None, queue,
            )

    def create_broadcast_cursor(self, exchange, routing_key, pattern, queue):
        if pymongo.version_tuple >= (3, ):
            query = dict(filter={'queue': exchange},
                         sort=[('$natural', 1)],
                         cursor_type=CursorType.TAILABLE
                         )
        else:
            query = dict(query={'queue': exchange},
                         sort=[('$natural', 1)],
                         tailable=True
                         )

        cursor = self.get_broadcast().find(**query)
        ret = self._broadcast_cursors[queue] = BroadcastCursor(cursor)
        return ret

    def get_expire(self, queue, argument):
        """Gets expiration header named `argument` of queue definition.
        `queue` must be either queue name or options itself.
        """
        if isinstance(queue, basestring):
            doc = self.get_queues().find_one({'_id': queue})

            if not doc:
                return

            data = doc['options']
        else:
            data = queue

        try:
            value = data['arguments'][argument]
        except (KeyError, TypeError):
            return

        return self.get_now() + datetime.timedelta(milliseconds=value)

    def update_queues_expire(self, queue):
        """Updates expiration field on queues documents
        """
        expire_at = self.get_expire(queue, 'x-expires')

        if not expire_at:
            return

        self.get_routing().update({'queue': queue}, {'$set': {'expire_at': expire_at}},
                                  multiple=True)
        self.get_queues().update({'_id': queue}, {'$set': {'expire_at': expire_at}},
                                 multiple=True)

    def get_now(self):
        return datetime.datetime.utcnow()


class Transport(virtual.Transport):
    Channel = Channel

    can_parse_url = True
    polling_interval = 1
    default_port = DEFAULT_PORT
    connection_errors = (
        virtual.Transport.connection_errors + (errors.ConnectionFailure,)
    )
    channel_errors = (
        virtual.Transport.channel_errors + (
            errors.ConnectionFailure,
            errors.OperationFailure)
    )
    driver_type = 'mongodb'
    driver_name = 'pymongo'

    implements = virtual.Transport.implements.extend(
        exchange_types=frozenset(['direct', 'topic', 'fanout']),
    )

    def driver_version(self):
        return pymongo.version
