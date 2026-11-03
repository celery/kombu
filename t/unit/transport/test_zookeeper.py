from __future__ import annotations

from unittest.mock import MagicMock, Mock, call, patch

import pytest

from kombu import Connection
from kombu.transport import zookeeper

pytest.importorskip('kazoo')


class test_Channel:
    def setup_method(self):
        self.connection = self.create_connection()
        self.channel = self.connection.default_channel

    def create_connection(self, **kwargs):
        return Connection(transport=zookeeper.Transport, **kwargs)

    def teardown_method(self):
        self.connection.close()

    def test_put_puts_bytes_to_queue(self):
        class AssertQueue:
            def put(self, value, priority):
                assert isinstance(value, bytes)

        self.channel._queues['foo'] = AssertQueue()
        self.channel._put(queue='foo', message='bar')

    @pytest.mark.parametrize('input,expected', (
        ('', '/'),
        ('/root', '/root'),
        ('/root/', '/root'),
    ))
    def test_virtual_host_normalization(self, input, expected):
        with self.create_connection(virtual_host=input) as conn:
            assert conn.default_channel._vhost == expected

    def test_queue_cache_is_not_shared_across_connections(self):
        with self.create_connection(virtual_host='/a') as conn_a, \
                self.create_connection(virtual_host='/b') as conn_b:
            channel_a = conn_a.default_channel
            channel_b = conn_b.default_channel
            channel_a._client = Mock(name='client_a')
            channel_b._client = Mock(name='client_b')

            with patch.object(zookeeper, 'Queue', MagicMock()) as Queue:
                channel_a._get_queue('orders')
                channel_b._get_queue('orders')

            assert Queue.call_args_list == [
                call(channel_a._client, '/a/orders'),
                call(channel_b._client, '/b/orders'),
            ]
