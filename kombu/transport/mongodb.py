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
from kombu.utils import cached_property

from . import virtual


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
    supports_fanout = True

    # Mutable container. Shared by all class instances
    _fanout_queues = {}

    # Options
    connect_timeout = None
    ssl = False
    capped_queue_size = 100000
    ttl = False

    default_hostname = '127.0.0.1'
    default_port = 27017
    default_database = 'kombu_default'

    messages_collection = 'messages'
    routing_collection = 'messages.routing'
    broadcast_collection = 'messages.broadcast'
    queues_collection = 'messages.queues'

    from_transport_options = (
        virtual.Channel.from_transport_options
        + ('connect_timeout', 'ssl', 'ttl', 'capped_queue_size',
           'default_hostname', 'default_port', 'default_database',
           'messages_collection', 'routing_collection',
           'broadcast_collection', 'queues_collection'))


    def __init__(self, *vargs, **kwargs):
        super(Channel, self).__init__(*vargs, **kwargs)

        self._broadcast_cursors = {}

        # Evaluate connection
        self.client

    # AbstractChannel/Channel interface implementation

    def _new_queue(self, queue, **kwargs):
        if self.ttl:
            self.queues.update({'_id': queue},
                               {'_id': queue,
                                'options': kwargs,
                                'expire_at': self._get_expire(kwargs, 'x-expires')},
                                upsert=True)

    def _get(self, queue):
        if queue in self._fanout_queues:
            try:
                msg = next(self._get_broadcast_cursor(queue))
            except StopIteration:
                msg = None
        else:
            msg = self.messages.find_and_modify(
                query={'queue': queue},
                sort=[('priority', pymongo.ASCENDING),
                      ('_id', pymongo.ASCENDING)],
                remove=True,
            )

        if self.ttl:
            self._update_queues_expire(queue)

        if msg is None:
            raise Empty()

        return loads(bytes_to_str(msg['payload']))

    def _size(self, queue):
        if queue in self._fanout_queues:
            return self._get_broadcast_cursor(queue).get_size()

        return self.messages.find({'queue': queue}).count()

    def _put(self, queue, message, **kwargs):
        data = {
            'payload': dumps(message),
            'queue': queue,
            'priority': self._get_message_priority(message, reverse=True)
        }

        if self.ttl:
            data['expire_at'] = self._get_expire(queue, 'x-message-ttl')

        self.messages.insert(data)

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Deliver fanout message."""
        self.broadcast.insert({'payload': dumps(message),
                               'queue': exchange})

    def _purge(self, queue):
        size = self._size(queue)

        if queue in self._fanout_queues:
            self._get_broadcast_cursor(queue).purge()
        else:
            self.messages.remove({'queue': queue})

        return size

    def get_table(self, exchange):
        """Get table of bindings for ``exchange``."""
        localRoutes = frozenset(self.state.exchanges[exchange]['table'])
        brokerRoutes = self.routing.find(
            {'exchange': exchange}
        )

        return localRoutes | frozenset((r['routing_key'],
                                        r['pattern'],
                                        r['queue']) for r in brokerRoutes)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        if self.typeof(exchange).type == 'fanout':
            self._create_broadcast_cursor(exchange, routing_key, pattern, queue)
            self._fanout_queues[queue] = exchange

        lookup = {'exchange': exchange,
                  'queue': queue,
                  'routing_key': routing_key,
                  'pattern': pattern}

        data = lookup.copy()

        if self.ttl:
            data['expire_at'] = self._get_expire(queue, 'x-expires')

        self.routing.update(lookup, data, upsert=True)

    def queue_delete(self, queue, **kwargs):
        self.routing.remove({'queue': queue})

        if self.ttl:
            self.queues.remove({'_id': queue})

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
            hostname += self.default_hostname

        if client.userid and '@' not in hostname:
            head, tail = hostname.split('://')

            credentials = client.userid
            if client.password:
                credentials += ':' + client.password

            hostname = head + '://' + credentials + '@' + tail

        port = client.port if client.port else self.default_port

        parsed = uri_parser.parse_uri(hostname, port)

        dbname = parsed['database'] or client.virtual_host

        if dbname in ('/', None):
            dbname = self.default_database

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

        return database

    def _create_broadcast(self, database):
        '''Create capped collection for broadcast messages.'''
        if self.broadcast_collection in database.collection_names():
            return

        database.create_collection(self.broadcast_collection,
                                   size=self.capped_queue_size,
                                   capped=True)

    def _ensure_indexes(self, database):
        '''Ensure indexes on collections.'''
        messages = database[self.messages_collection]
        messages.ensure_index(
            [('queue', 1), ('priority', 1), ('_id', 1)], background=True,
        )

        database[self.broadcast_collection].ensure_index([('queue', 1)])

        routing = database[self.routing_collection]
        routing.ensure_index([('queue', 1), ('exchange', 1)])

        if self.ttl:
            messages.ensure_index([('expire_at', 1)], expireAfterSeconds=0)
            routing.ensure_index([('expire_at', 1)], expireAfterSeconds=0)

            database[self.queues_collection].ensure_index([('expire_at', 1)], expireAfterSeconds=0)

    def _create_client(self):
        '''Actualy creates connection'''
        database = self._open()
        self._create_broadcast(database)
        self._ensure_indexes(database)

        return database

    @cached_property
    def client(self):
        return self._create_client()

    @cached_property
    def messages(self):
        return self.client[self.messages_collection]

    @cached_property
    def routing(self):
        return self.client[self.routing_collection]

    @cached_property
    def broadcast(self):
        return self.client[self.broadcast_collection]

    @cached_property
    def queues(self):
        return self.client[self.queues_collection]

    def _get_broadcast_cursor(self, queue):
        try:
            return self._broadcast_cursors[queue]
        except KeyError:
            # Cursor may be absent when Channel created more than once.
            # _fanout_queues is a class-level mutable attribute so it's
            # shared over all Channel instances.
            return self._create_broadcast_cursor(
                self._fanout_queues[queue], None, None, queue,
            )

    def _create_broadcast_cursor(self, exchange, routing_key, pattern, queue):
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

        cursor = self.broadcast.find(**query)
        ret = self._broadcast_cursors[queue] = BroadcastCursor(cursor)
        return ret

    def _get_expire(self, queue, argument):
        """Gets expiration header named `argument` of queue definition.
        `queue` must be either queue name or options itself.
        """
        if isinstance(queue, basestring):
            doc = self.queues.find_one({'_id': queue})

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

    def _update_queues_expire(self, queue):
        """Updates expiration field on queues documents
        """
        expire_at = self._get_expire(queue, 'x-expires')

        if not expire_at:
            return

        self.routing.update({'queue': queue}, {'$set': {'expire_at': expire_at}},
                            multiple=True)
        self.queues.update({'_id': queue}, {'$set': {'expire_at': expire_at}},
                           multiple=True)

    def get_now(self):
        return datetime.datetime.utcnow()


class Transport(virtual.Transport):
    Channel = Channel

    can_parse_url = True
    polling_interval = 1
    default_port = Channel.default_port
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
