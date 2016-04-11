from __future__ import absolute_import, unicode_literals

from kombu.tests.case import Case, Mock, skip
from kombu.transport.consul import Channel
from kombu.five import Empty


@skip.unless_module('consul')
class TestConsul(Case):

    mock_consul = None
    channel = None

    def setup(self):
        self.mock_consul = Mock()
        self.mock_consul.kv = Mock()
        self.mock_consul.session = Mock()
        self.mock_consul.agent = Mock()

        self.channel = Channel()
        self.channel.consul = self.mock_consul

    def test_driver_version(self):
        self.assertTrue(self.channel.transport.driver_version())

    def test_failed_get(self):
        self.channel._obtain_lock = Mock(return_value=False)
        self.assertRaises(Empty, self.channel._get('empty'))

    def test_test_purge(self):
        self.channel._destroy_session = Mock(return_value=True)
        self.mock_consul.kv.delete = Mock(return_value=True)
        self.assertTrue(self.channel._purge('foo'))

    def test_variables(self):
        self.assertEqual(self.channel.session_ttl, 30)
        self.assertEqual(self.channel.timeout, '10s')

    def test_lock_key(self):
        key = self.channel._lock_key('myqueue')
        self.assertEqual(key, 'kombu/myqueue.lock')

    def test_key_prefix(self):
        key = self.channel._key_prefix('myqueue')
        self.assertEqual(key, 'kombu/myqueue')

    def test_get_or_create_session(self):
        queue = 'myqueue'
        session_id = '123456'
        self.mock_consul.session.create = Mock(return_value=session_id)
        self.assertEqual(self._get_or_create_session(queue), session_id)

    def test_create_delete_queue(self):
        channel = Channel()
        channel.consul = self.mock_consul

        queue = 'mynewqueue'

        self.mock_consul.kv.put = Mock(return_value=True)
        self.assertTrue(channel._new_queue(queue))

        self.mock_consul.kv.delete = Mock(return_value=True)
        self.channel._destroy_session = Mock()
        self._delete(queue)

    def test_size(self):
        keys = list()
        keys.append((1, dict()))
        keys.append((2, dict()))

        self.mock_consul.kv.get = Mock(return_value=keys)

        self.assertEqual(self.channel._size(), 2)

    def test_get(self):
        self.channel._obtain_lock = Mock(return_value=True)
        self.channel._release_lock = Mock(return_value=True)

        keys = list()
        keys.append((1, dict()))
        keys.append((2, dict()))
        self.mock_consul.kv.get = Mock(return_value=keys)

        self.mock_consul.kv.delete = Mock(return_value=True)

        self.assertIsNotNone(self.channel._get('myqueue'))

    def test_put(self):
        self.mock_consul.kv.put = Mock(return_value=True)
        self.assertIsNone(self.channel.put('myqueue', 'mydata'))
