import pytest
from queue import Empty

from case import Mock, patch, skip

from kombu.transport.etcd import Channel, Transport


@skip.unless_module('etcd')
class test_Etcd:

    def setup(self):
        self.connection = Mock()
        self.connection.client.transport_options = {}
        self.connection.client.port = 2739
        self.client = self.patch('etcd.Client').return_value
        self.channel = Channel(connection=self.connection)

    def test_driver_version(self):
        assert Transport(self.connection.client).driver_version()

    def test_failed_get(self):
        self.channel._acquire_lock = Mock(return_value=False)
        self.channel.client.read.side_effect = IndexError
        with patch('etcd.Lock'):
            with pytest.raises(Empty):
                self.channel._get('empty')()

    def test_test_purge(self):
        with patch('etcd.Lock'):
            self.client.delete = Mock(return_value=True)
            assert self.channel._purge('foo')

    def test_key_prefix(self):
        key = self.channel._key_prefix('myqueue')
        assert key == 'kombu/myqueue'

    def test_create_delete_queue(self):
        queue = 'mynewqueue'

        with patch('etcd.Lock'):
            self.client.write.return_value = self.patch('etcd.EtcdResult')
            assert self.channel._new_queue(queue)

            self.client.delete.return_value = self.patch('etcd.EtcdResult')
            self.channel._delete(queue)

    def test_size(self):
        with patch('etcd.Lock'):
            self.client.read.return_value = self.patch(
                'etcd.EtcdResult', _children=[{}, {}])
            assert self.channel._size('q') == 2

    def test_get(self):
        with patch('etcd.Lock'):
            self.client.read.return_value = self.patch(
                'etcd.EtcdResult',
                _children=[{'key': 'myqueue', 'modifyIndex': 1, 'value': '1'}])
            assert self.channel._get('myqueue') is not None

    def test_put(self):
        with patch('etcd.Lock'):
            self.client.write.return_value = self.patch('etcd.EtcdResult')
            assert self.channel._put('myqueue', 'mydata') is None
