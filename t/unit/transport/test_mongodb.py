import datetime
import pytest
from queue import Empty

from case import MagicMock, call, patch, skip

from kombu import Connection


def _create_mock_connection(url='', **kwargs):
    from kombu.transport import mongodb  # noqa

    class _Channel(mongodb.Channel):
        # reset _fanout_queues for each instance
        _fanout_queues = {}

        collections = {}
        now = datetime.datetime.utcnow()

        def _create_client(self):
            mock = MagicMock(name='client')

            # we need new mock object for every collection
            def get_collection(name):
                try:
                    return self.collections[name]
                except KeyError:
                    mock = self.collections[name] = MagicMock(
                        name='collection:%s' % name)

                    return mock

            mock.__getitem__.side_effect = get_collection

            return mock

        def get_now(self):
            return self.now

    class Transport(mongodb.Transport):
        Channel = _Channel

    return Connection(url, transport=Transport, **kwargs)


@skip.unless_module('pymongo')
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

    def declare_droadcast_queue(self, queue):
        self.channel.exchange_declare('fanout_exchange', type='fanout')

        self.channel._queue_bind('fanout_exchange', 'foo', '*', queue)

        assert queue in self.channel._broadcast_cursors

    def get_broadcast(self, queue):
        return self.channel._broadcast_cursors[queue]

    def set_broadcast_return_value(self, queue, *values):
        self.declare_droadcast_queue(queue)

        cursor = MagicMock(name='cursor')
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


@skip.unless_module('pymongo')
class test_mongodb_channel(BaseMongoDBChannelCase):

    def setup(self):
        self.connection = _create_mock_connection()
        self.channel = self.connection.default_channel

    # Tests for "public" channel interface

    def test_new_queue(self):
        self.channel._new_queue('foobar')
        self.channel.client.assert_not_called()

    def test_get(self):
        import pymongo

        self.set_operation_return_value('messages', 'find_and_modify', {
            '_id': 'docId', 'payload': '{"some": "data"}',
        })

        event = self.channel._get('foobar')
        self.assert_collection_accessed('messages')
        self.assert_operation_called_with(
            'messages', 'find_and_modify',
            query={'queue': 'foobar'},
            remove=True,
            sort=[
                ('priority', pymongo.ASCENDING),
            ],
        )

        assert event == {'some': 'data'}

        self.set_operation_return_value('messages', 'find_and_modify', None)
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
        self.assert_operation_called_with('messages', 'insert', {
            'queue': 'foobar',
            'priority': 9,
            'payload': '{"some": "data"}',
        })

    def test_put_fanout(self):
        self.declare_droadcast_queue('foobar')

        self.channel._put_fanout('foobar', {'some': 'data'}, 'foo')

        self.assert_collection_accessed('messages.broadcast')
        self.assert_operation_called_with('broadcast', 'insert', {
            'queue': 'foobar', 'payload': '{"some": "data"}',
        })

    def test_size(self):
        self.set_operation_return_value('messages', 'find.count', 77)

        result = self.channel._size('foobar')
        self.assert_collection_accessed('messages')
        self.assert_operation_called_with(
            'messages', 'find', {'queue': 'foobar'},
        )

        assert result == 77

    def test_size_fanout(self):
        self.declare_droadcast_queue('foobar')

        cursor = MagicMock(name='cursor')
        cursor.get_size.return_value = 77
        self.channel._broadcast_cursors['foobar'] = cursor

        result = self.channel._size('foobar')

        assert result == 77

    def test_purge(self):
        self.set_operation_return_value('messages', 'find.count', 77)

        result = self.channel._purge('foobar')
        self.assert_collection_accessed('messages')
        self.assert_operation_called_with(
            'messages', 'remove', {'queue': 'foobar'},
        )

        assert result == 77

    def test_purge_fanout(self):
        self.declare_droadcast_queue('foobar')

        cursor = MagicMock(name='cursor')
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
            'routing', 'update',
            {'queue': 'foo', 'pattern': '*',
             'routing_key': 'foo', 'exchange': 'test_exchange'},
            {'queue': 'foo', 'pattern': '*',
             'routing_key': 'foo', 'exchange': 'test_exchange'},
            upsert=True,
        )

    def test_queue_delete(self):
        self.channel.queue_delete('foobar')
        self.assert_collection_accessed('messages.routing')
        self.assert_operation_called_with(
            'routing', 'remove', {'queue': 'foobar'},
        )

    def test_queue_delete_fanout(self):
        self.declare_droadcast_queue('foobar')

        cursor = MagicMock(name='cursor')
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

    def test_ensure_indexes(self):
        self.channel._ensure_indexes(self.channel.client)

        self.assert_operation_called_with(
            'messages', 'ensure_index',
            [('queue', 1), ('priority', 1), ('_id', 1)],
            background=True,
        )
        self.assert_operation_called_with(
            'broadcast', 'ensure_index',
            [('queue', 1)],
        )
        self.assert_operation_called_with(
            'routing', 'ensure_index', [('queue', 1), ('exchange', 1)],
        )

    def test_create_broadcast_cursor(self):
        import pymongo

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
        import pymongo

        def server_info(self):
            return {'version': '3.6.0-rc'}

        with patch.object(pymongo.MongoClient, 'server_info', server_info):
            self.channel._open()


@skip.unless_module('pymongo')
class test_mongodb_channel_ttl(BaseMongoDBChannelCase):

    def setup(self):
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
            'queues', 'update',
            {'_id': 'foobar'},
            {'_id': 'foobar', 'options': {}, 'expire_at': None},
            upsert=True,
        )

    def test_get(self):
        import pymongo

        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-expires': 777}},
        })

        self.set_operation_return_value('messages', 'find_and_modify', {
            '_id': 'docId', 'payload': '{"some": "data"}',
        })

        self.channel._get('foobar')
        self.assert_collection_accessed('messages', 'messages.queues')
        self.assert_operation_called_with(
            'messages', 'find_and_modify',
            query={'queue': 'foobar'},
            remove=True,
            sort=[
                ('priority', pymongo.ASCENDING),
            ],
        )
        self.assert_operation_called_with(
            'routing', 'update',
            {'queue': 'foobar'},
            {'$set': {'expire_at': self.expire_at}},
            multi=True,
        )

    def test_put(self):
        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-message-ttl': 777}},
        })

        self.channel._put('foobar', {'some': 'data'})

        self.assert_collection_accessed('messages')
        self.assert_operation_called_with('messages', 'insert', {
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
            'routing', 'update',
            {'queue': 'foo', 'pattern': '*',
             'routing_key': 'foo', 'exchange': 'test_exchange'},
            {'queue': 'foo', 'pattern': '*',
             'routing_key': 'foo', 'exchange': 'test_exchange',
             'expire_at': self.expire_at},
            upsert=True,
        )

    def test_queue_delete(self):
        self.channel.queue_delete('foobar')
        self.assert_collection_accessed('messages.queues')
        self.assert_operation_called_with(
            'queues', 'remove', {'_id': 'foobar'})

    def test_ensure_indexes(self):
        self.channel._ensure_indexes(self.channel.client)

        self.assert_operation_called_with(
            'messages', 'ensure_index', [('expire_at', 1)],
            expireAfterSeconds=0)

        self.assert_operation_called_with(
            'routing', 'ensure_index', [('expire_at', 1)],
            expireAfterSeconds=0)

        self.assert_operation_called_with(
            'queues', 'ensure_index', [('expire_at', 1)], expireAfterSeconds=0)

    def test_get_expire(self):
        result = self.channel._get_expire(
            {'arguments': {'x-expires': 777}}, 'x-expires')

        self.channel.client.assert_not_called()

        assert result == self.expire_at

        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-expires': 777}},
        })

        result = self.channel._get_expire('foobar', 'x-expires')
        assert result == self.expire_at

    def test_update_queues_expire(self):
        self.set_operation_return_value('queues', 'find_one', {
            '_id': 'docId', 'options': {'arguments': {'x-expires': 777}},
        })
        self.channel._update_queues_expire('foobar')

        self.assert_collection_accessed('messages.routing', 'messages.queues')
        self.assert_operation_called_with(
            'routing', 'update',
            {'queue': 'foobar'},
            {'$set': {'expire_at': self.expire_at}},
            multi=True,
        )
        self.assert_operation_called_with(
            'queues', 'update',
            {'_id': 'foobar'},
            {'$set': {'expire_at': self.expire_at}},
            multi=True,
        )


@skip.unless_module('pymongo')
class test_mongodb_channel_calc_queue_size(BaseMongoDBChannelCase):

    def setup(self):
        self.connection = _create_mock_connection(
            transport_options={'calc_queue_size': False})
        self.channel = self.connection.default_channel

        self.expire_at = (
            self.channel.get_now() + datetime.timedelta(milliseconds=777))

    # Tests

    def test_size(self):
        self.set_operation_return_value('messages', 'find.count', 77)

        result = self.channel._size('foobar')

        self.assert_operation_has_calls('messages', 'find', [])

        assert result == 0
