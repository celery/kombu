from __future__ import absolute_import, unicode_literals

import mock

from kombu.five import Empty

from kombu.transport.etcd import Channel, Transport

from kombu.tests.case import Case, Mock, skip


@skip.unless_module('etcd')
class test_Etcd(Case):

    def setup(self):
        self.connection = Mock()
        self.connection.client.transport_options = {}
        self.connection.client.port = 2739
        self.client = self.patch('etcd.Client').return_value
        self.channel = Channel(connection=self.connection)

    def test_driver_version(self):
        self.assertTrue(Transport(self.connection.client).driver_version())

    def test_failed_get(self):
        self.channel._acquire_lock = Mock(return_value=False)
        self.channel.client.read.side_effect = IndexError
        with mock.patch('etcd.Lock'):
            with self.assertRaises(Empty):
                self.channel._get('empty')()

    def test_test_purge(self):
        with mock.patch('etcd.Lock'):
            self.client.delete = Mock(return_value=True)
            self.assertTrue(self.channel._purge('foo'))

    def test_key_prefix(self):
        key = self.channel._key_prefix('myqueue')
        self.assertEqual(key, 'kombu/myqueue')

    def test_create_delete_queue(self):
        queue = 'mynewqueue'

        with mock.patch('etcd.Lock'):
            self.client.write.return_value = self.patch('etcd.EtcdResult')
            self.assertTrue(self.channel._new_queue(queue))

            self.client.delete.return_value = self.patch('etcd.EtcdResult')
            self.channel._delete(queue)

    def test_size(self):
        with mock.patch('etcd.Lock'):
            self.client.read.return_value = self.patch(
                'etcd.EtcdResult', _children=[{}, {}])
            self.assertEqual(self.channel._size('q'), 2)

    def test_get(self):
        with mock.patch('etcd.Lock'):
            self.client.read.return_value = self.patch(
                'etcd.EtcdResult',
                _children=[{'key': 'myqueue', 'modifyIndex': 1, 'value': '1'}])
            self.assertIsNotNone(self.channel._get('myqueue'))

    def test_put(self):
        with mock.patch('etcd.Lock'):
            self.client.write.return_value = self.patch('etcd.EtcdResult')
            self.assertIsNone(self.channel._put('myqueue', 'mydata'))
