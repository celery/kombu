from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock, skip

from kombu.five import Empty
from kombu.transport.consul import Channel, Transport


@skip.unless_module('consul')
class test_Consul:

    def setup(self):
        self.connection = Mock()
        self.connection.client.transport_options = {}
        self.connection.client.port = 303
        self.consul = self.patching('consul.Consul').return_value
        self.channel = Channel(connection=self.connection)

    def test_driver_version(self):
        assert Transport(self.connection.client).driver_version()

    def test_failed_get(self):
        self.channel._acquire_lock = Mock(return_value=False)
        self.channel.client.kv.get.return_value = (1, None)
        with pytest.raises(Empty):
            self.channel._get('empty')()

    def test_test_purge(self):
        self.channel._destroy_session = Mock(return_value=True)
        self.consul.kv.delete = Mock(return_value=True)
        assert self.channel._purge('foo')

    def test_variables(self):
        assert self.channel.session_ttl == 30
        assert self.channel.timeout == '10s'

    def test_lock_key(self):
        key = self.channel._lock_key('myqueue')
        assert key == 'kombu/myqueue.lock'

    def test_key_prefix(self):
        key = self.channel._key_prefix('myqueue')
        assert key == 'kombu/myqueue'

    def test_get_or_create_session(self):
        queue = 'myqueue'
        session_id = '123456'
        self.consul.session.create.return_value = session_id
        assert self.channel._get_or_create_session(queue) == session_id

    def test_create_delete_queue(self):
        queue = 'mynewqueue'

        self.consul.kv.put.return_value = True
        assert self.channel._new_queue(queue)

        self.consul.kv.delete.return_value = True
        self.channel._destroy_session = Mock()
        self.channel._delete(queue)

    def test_size(self):
        self.consul.kv.get.return_value = [(1, {}), (2, {})]
        assert self.channel._size('q') == 2

    def test_get(self):
        self.channel._obtain_lock = Mock(return_value=True)
        self.channel._release_lock = Mock(return_value=True)

        self.consul.kv.get.return_value = [1, [
            {'Key': 'myqueue', 'ModifyIndex': 1, 'Value': '1'},
        ]]

        self.consul.kv.delete.return_value = True

        assert self.channel._get('myqueue') is not None

    def test_put(self):
        self.consul.kv.put.return_value = True
        assert self.channel._put('myqueue', 'mydata') is None
