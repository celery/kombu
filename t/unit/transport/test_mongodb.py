from __future__ import annotations

import datetime
from queue import Empty
from unittest.mock import MagicMock, call, patch

import pytest

from kombu import Connection

pymongo = pytest.importorskip('pymongo')

# must import following after above validation to avoid error
# and skip tests if missing
# these are used to define real spec of the corresponding mocks,
# to ensure called methods exist in real objects
# pylint: disable=C0413
from pymongo.collection import Collection  # isort:skip # noqa: E402
from pymongo.database import Database  # isort:skip # noqa: E402
from kombu.transport.mongodb import BroadcastCursor  # isort:skip # noqa: E402


def _create_mock_connection(url='', **kwargs):
    from kombu.transport import mongodb

    class _Channel(mongodb.Channel):
        # reset _fanout_queues for each instance
        _fanout_queues = {}

        collections = {}
        now = datetime.datetime.utcnow()

        def _create_client(self):
            # not really a 'MongoClient',
            # but an actual pre-established Database connection
            mock = MagicMock(name='client', spec=Database)

            # we need new mock object for every collection
            def get_collection(name):
                try:
                    return self.collections[name]
                except KeyError:
                    mock = self.collections[name] = MagicMock(
                        name='collection:%s' % name,
                        spec=Collection,
                    )

                    return mock

            mock.__getitem__.side_effect = get_collection

            return mock

        def get_now(self):
            return self.now

    class Transport(mongodb.Transport):
        Channel = _Channel

    return Connection(url, transport=Transport, **kwargs)


class test_mongodb_uri_parsing:

    def test_defaults(self):
        url = 'mongodb://'

        channel = _create_mock_connection(url).default_channel

        hostname, dbname, options = channel._parse_uri()

        assert dbname == 'kombu_default'
        assert hostname == 'mongodb://127.0.0.1'

    def test_custom_host(self):
        url = 'mongodb://localhost'
        channel = _create_mock_connection(url).default_channel
        hostname, dbname, options = channel._parse_uri()

        assert dbname == 'kombu_default'

    def test_custom_port(self):
        url = 'mongodb://localhost:27018'
        channel = _create_mock_connection(url).default_channel
        hostname, dbname, options = channel._parse_uri()

        assert hostname == 'mongodb://localhost:27018'

    def test_replicaset_hosts(self):
        url = 'mongodb://mongodb1.example.com:27317,mongodb2.example.com:27017/?replicaSet=test_rs'
        channel = _create_mock_connection(url).default_channel
        hostname, dbname, options = channel._parse_uri()

        assert hostname == 'mongodb://mongodb1.example.com:27317,mongodb2.example.com:27017/?replicaSet=test_rs'  # noqa
        assert options['replicaset'] == 'test_rs'

    def test_custom_database(self):
        url = 'mongodb://localhost/dbname'
        channel = _create_mock_connection(url).default_channel
        hostname, dbname, options = channel._parse_uri()

        assert dbname == 'dbname'

    def test_custom_credentials(self):
        url = 'mongodb://localhost/dbname'
        channel = _create_mock_connection(
            url, userid='foo', password='bar').default_channel
        hostname, dbname, options = channel._parse_uri()

        assert hostname == 'mongodb://foo:bar@localhost/dbname'
        assert dbname == 'dbname'

    def test_correct_readpreference(self):
        url = 'mongodb://localhost/dbname?readpreference=nearest'
        channel = _create_mock_connection(url).default_channel
        hostname, dbname, options = channel._parse_uri()
        assert options['readpreference'] == 'nearest'


class BaseMongoDBChannelCase:

    def _get_method(self, cname, mname):
        collection = getattr(self.channel, cname)
        method = getattr(collection, mname.split('.', 1)[0])

        for bit in mname.split('.')[1:]:
            method = getattr(method.return_value, bit)

        return method

    def set_operation_return_value(self, cname, mname, *values):
        method = self._get_method(cname, mname)

        if len(values) == 1:
            method.return_value = values[0]
        else:
            method.side_effect = values

    def declare_broadcast_queue(self, queue):
        self.channel.exchange_declare('fanout_exchange', type='fanout')

        self.channel._queue_bind('fanout_exchange', 'foo', '*', queue)

        assert queue in self.channel._broadcast_cursors

    def get_broadcast(self, queue):
        return self.channel._broadcast_cursors[queue]

    def set_broadcast_return_value(self, queue, *values):
        self.declare_broadcast_queue(queue)

        cursor = MagicMock(name='cursor', spec=BroadcastCursor)
        cursor.__iter__.return_value = iter(values)

        self.channel._broadcast_cursors[queue]._cursor = iter(cursor)

    def assert_collection_accessed(self, *collections):
        self.channel.client.__getitem__.assert_has_calls(
            [call(c) for c in collections], any_order=True)

    def assert_operation_has_calls(self, cname, mname, calls, any_order=False):
        method = self._get_method(cname, mname)

        method.assert_has_calls(calls, any_order=any_order)

    def assert_operation_called_with(self, cname, mname, *args, **kwargs):
        self.assert_operation_has_calls(cname, mname, [call(*args, **kwargs)])


class test_mongodb_channel(BaseMongoDBChannelCase):

    def setup_method(self):
        self.connection = _create_mock_connection()
        self.channel = self.connection.default_channel

    # Tests for "public" channel interface

    def test_new_queue(self):
        self.channel._new_queue('foobar')
        self.channel.client.assert_not_called()

    def test_get(self):

        self.set_operation_return_value('messages', 'find_one_and_delete', {
            '_id': 'docId', 'payload': '{"some": "data"}',
        })

        event = self.channel._get('foobar')
        self.assert_collection_accessed('messages')
        self.assert_operation_called_with(
            'messages', 'find_one_and_delete',
            {'queue': 'foobar'},
            sort=[
                ('priority', pymongo.ASCENDING),
            ],
        )

        assert event == {'some': 'data'}

        self.set_operation_return_value(
            'messages',
            'find_one_and_delete',
            None,
        )
        with pytest.raises(Empty):
            self.channel._get('foobar')

    def test_get_fanout(self):
        self.set_broadcast_return_value('foobar', {
            '_id': 'docId1', 'payload': '{"some": "data"}',
        })

        event = self.channel._get('foobar')
        self.assert_collection_accessed('messages.broadcast')
        assert event == {'some': 'data'}

        with pytest.raises(Empty):
            self.channel._get('foobar')

    def test_put(self):
        self.channel._put('foobar', {'some': 'data'})

        self.assert_collection_accessed('messages')
        self.assert_operation_called_with('messages', 'insert_one', {
            'queue': 'foobar',
            'priority': 9,
            'payload': '{"some": "data"}',
        })

    def test_put_fanout(self):
        self.declare_broadcast_queue('foobar')

        self.channel._put_fanout('foobar', {'some': 'data'}, 'foo')

        self.assert_collection_accessed('messages.broadcast')
        self.assert_operation_called_with('broadcast', 'insert_one', {
            'queue': 'foobar', 'payload': '{"some": "data"}',
        })

    def test_size(self):
        self.set_operation_return_value('messages', 'count_documents', 77)

        result = self.channel._size('foobar')
        self.assert_collection_accessed('messages')
        self.assert_operation_called_with(
            'messages', 'count_documents', {'queue': 'foobar'},
        )

        assert result == 77

    def test_size_fanout(self):
        self.declare_broadcast_queue('foobar')

        cursor = MagicMock(name='cursor', spec=BroadcastCursor)
        cursor.get_size.return_value = 77
        self.channel._broadcast_cursors['foobar'] = cursor

        result = self.channel._size('foobar')

        assert result == 77

    def test_purge(self):
        self.set_operation_return_value('messages', 'count_documents', 77)

        result = self.channel._purge('foobar')
        self.assert_collection_accessed('messages')
        self.assert_operation_called_with(
            'messages', 'delete_many', {'queue': 'foobar'},
        )

        assert result == 77

    def test_purge_fanout(self):
        self.declare_broadcast_queue('foobar')

        cursor = MagicMock(name='cursor', spec=BroadcastCursor)
        cursor.get_size.return_value = 77
        self.channel._broadcast_cursors['foobar'] = cursor

        result = self.channel._purge('foobar')

        cursor.purge.assert_any_call()

        assert result == 77

    def test_get_table(self):
        state_table = [('foo', '*', 'foo')]
        stored_table = [('bar', '*', 'bar')]

        self.channel.exchange_declare('test_exchange')
        self.channel.state.exchanges['test_exchange']['table'] = state_table

        self.set_operation_return_value('routing', 'find', [{
            '_id': 'docId',
            'routing_key': stored_table[0][0],
            'pattern': stored_table[0][1],
            'queue': stored_table[0][2],
        }])

        result = self.channel.get_table('test_exchange')
        self.assert_collection_accessed('messages.routing')
        self.assert_operation_called_with(
            'routing', 'find', {'exchange': 'test_exchange'},
        )

        assert set(result) == frozenset(state_table) | frozenset(stored_table)

    def test_queue_bind(self):
        self.channel._queue_bind('test_exchange', 'foo', '*', 'foo')
        self.assert_collection_accessed('messages.routing')
        self.assert_operation_called_with(
            'routing', 'update_one',
            {'queue': 'foo', 'pattern': '*',
             'routing_key': 'foo', 'exchange': 'test_exchange'},
            {'$set': {'queue': 'foo', 'pattern': '*',
             'routing_key': 'foo', 'exchange': 'test_exchange'}},
            upsert=True,
        )

    def test_queue_delete(self):
        self.channel.queue_delete('foobar')
        self.assert_collection_accessed('messages.routing')
        self.assert_operation_called_with(
            'routing', 'delete_many', {'queue': 'foobar'},
        )

    def test_queue_delete_fanout(self):
        self.declare_broadcast_queue('foobar')

        cursor = MagicMock(name='cursor', spec=BroadcastCursor)
        self.channel._broadcast_cursors['foobar'] = cursor

        self.channel.queue_delete('foobar')

        cursor.close.assert_any_call()

        assert 'foobar' not in self.channel._broadcast_cursors
        assert 'foobar' not in self.channel._fanout_queues

    # Tests for channel internals

    def test_create_broadcast(self):
        self.channel._create_broadcast(self.channel.client)

        self.channel.client.create_collection.assert_called_with(
            'messages.broadcast', capped=True, size=100000,
        )

    def test_create_broadcast_exists(self):
        # simulate already created collection
        self.channel.client.list_collection_names.return_value = [
            'messages.broadcast'
        ]

        broadcast = self.channel._create_broadcast(self.channel.client)
        self.channel.client.create_collection.assert_not_called()
        assert broadcast is None  # not returned since not created

    def test_get_broadcast_cursor_created(self):
        self.channel._fanout_queues['foobar'] = 'fanout_exchange'
        created_cursor = self.channel._get_broadcast_cursor('foobar')
        cached_cursor = self.channel._broadcast_cursors['foobar']
        assert cached_cursor is created_cursor

    def test_get_broadcast_cursor_exists(self):
        self.declare_broadcast_queue('foobar')
        cached_cursor = self.channel._broadcast_cursors['foobar']
        getter_cursor = self.channel._get_broadcast_cursor('foobar')
        assert cached_cursor is getter_cursor

    def test_ensure_indexes(self):
        self.channel._ensure_indexes(self.channel.client)

        self.assert_operation_called_with(
            'messages', 'create_index',
            [('queue', 1), ('priority', 1), ('_id', 1)],
            background=True,
        )
        self.assert_operation_called_with(
            'broadcast', 'create_index',
            [('queue', 1)],
        )
        self.assert_operation_called_with(
            'routing', 'create_index', [('queue', 1), ('exchange', 1)],
        )

    def test_create_broadcast_cursor(self):

        with patch.object(pymongo, 'version_tuple', (2, )):
            self.channel._create_broadcast_cursor(
                'fanout_exchange', 'foo', '*', 'foobar',
            )

            self.assert_collection_accessed('messages.broadcast')
            self.assert_operation_called_with(
                'broadcast', 'find',
                tailable=True,
                query={'queue': 'fanout_exchange'},
            )

        if pymongo.version_tuple >= (3, ):
            self.channel._create_broadcast_cursor(
                'fanout_exchange1', 'foo', '*', 'foobar',
            )

            self.assert_collection_accessed('messages.broadcast')
            self.assert_operation_called_with(
                'broadcast', 'find',
                cursor_type=pymongo.CursorType.TAILABLE,
                filter={'queue': 'fanout_exchange1'},
            )

    def test_open_rc_version(self):

        def server_info(self):
            return {'version': '3.6.0-rc'}

        with patch.object(pymongo.MongoClient, 'server_info', server_info):
            self.channel._open()


class test_mongodb_channel_ttl(BaseMongoDBChannelCase):

    def setup_method(self):
        self.connection = _create_mock_connection(
            transport_options={'ttl': True},
        )
        self.channel = self.connection.default_channel

        self.expire_at = (
            self.channel.get_now() + datetime.timedelta(milliseconds=777))

    # Tests

    def test_new_queue(self):
        self.channel._new_queue('foobar')

        self.assert_operation_called_with(
            'queues', 'update_one',
            {'_id': 'foobar'},
            {'$set': {'_id': 'foobar', 'options': {}, 'expire_at': None}},
            upsert=True,
        )

    def test_get(self):

        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-expires': 777}},
        })

        self.set_operation_return_value('messages', 'find_one_and_delete', {
            '_id': 'docId', 'payload': '{"some": "data"}',
        })

        self.channel._get('foobar')
        self.assert_collection_accessed('messages', 'messages.queues')
        self.assert_operation_called_with(
            'messages', 'find_one_and_delete',
            {'queue': 'foobar'},
            sort=[
                ('priority', pymongo.ASCENDING),
            ],
        )
        self.assert_operation_called_with(
            'routing', 'update_many',
            {'queue': 'foobar'},
            {'$set': {'expire_at': self.expire_at}},
        )

    def test_put(self):
        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-message-ttl': 777}},
        })

        self.channel._put('foobar', {'some': 'data'})

        self.assert_collection_accessed('messages')
        self.assert_operation_called_with('messages', 'insert_one', {
            'queue': 'foobar',
            'priority': 9,
            'payload': '{"some": "data"}',
            'expire_at': self.expire_at,
        })

    def test_queue_bind(self):
        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-expires': 777}},
        })

        self.channel._queue_bind('test_exchange', 'foo', '*', 'foo')
        self.assert_collection_accessed('messages.routing')
        self.assert_operation_called_with(
            'routing', 'update_one',
            {'queue': 'foo', 'pattern': '*',
             'routing_key': 'foo', 'exchange': 'test_exchange'},
            {'$set': {
                'queue': 'foo', 'pattern': '*',
                'routing_key': 'foo', 'exchange': 'test_exchange',
                'expire_at': self.expire_at
            }},
            upsert=True,
        )

    def test_queue_delete(self):
        self.channel.queue_delete('foobar')
        self.assert_collection_accessed('messages.queues')
        self.assert_operation_called_with(
            'queues', 'delete_one', {'_id': 'foobar'})

    def test_ensure_indexes(self):
        self.channel._ensure_indexes(self.channel.client)

        self.assert_operation_called_with(
            'messages', 'create_index', [('expire_at', 1)],
            expireAfterSeconds=0)

        self.assert_operation_called_with(
            'routing', 'create_index', [('expire_at', 1)],
            expireAfterSeconds=0)

        self.assert_operation_called_with(
            'queues', 'create_index', [('expire_at', 1)], expireAfterSeconds=0)

    def test_get_queue_expire(self):
        result = self.channel._get_queue_expire(
            {'arguments': {'x-expires': 777}}, 'x-expires')

        self.channel.client.assert_not_called()

        assert result == self.expire_at

        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-expires': 777}},
        })

        result = self.channel._get_queue_expire('foobar', 'x-expires')
        assert result == self.expire_at

    def test_get_message_expire(self):
        assert self.channel._get_message_expire({
            'properties': {'expiration': 777},
        }) == self.expire_at
        assert self.channel._get_message_expire({}) is None

    def test_update_queues_expire(self):
        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-expires': 777}},
        })
        self.channel._update_queues_expire('foobar')

        self.assert_collection_accessed('messages.routing', 'messages.queues')
        self.assert_operation_called_with(
            'routing', 'update_many',
            {'queue': 'foobar'},
            {'$set': {'expire_at': self.expire_at}},
        )
        self.assert_operation_called_with(
            'queues', 'update_many',
            {'_id': 'foobar'},
            {'$set': {'expire_at': self.expire_at}},
        )


class test_mongodb_channel_calc_queue_size(BaseMongoDBChannelCase):

    def setup_method(self):
        self.connection = _create_mock_connection(
            transport_options={'calc_queue_size': False})
        self.channel = self.connection.default_channel

        self.expire_at = (
            self.channel.get_now() + datetime.timedelta(milliseconds=777))

    # Tests

    def test_size(self):
        self.set_operation_return_value('messages', 'count_documents', 77)

        result = self.channel._size('foobar')

        self.assert_operation_has_calls('messages', 'find', [])

        assert result == 0


class test_mongodb_transport(BaseMongoDBChannelCase):
    def setup_method(self):
        self.connection = _create_mock_connection()

    def test_driver_version(self):
        version = self.connection.transport.driver_version()
        assert version == pymongo.__version__
