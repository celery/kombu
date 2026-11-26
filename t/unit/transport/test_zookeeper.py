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

    def test_close_stops_the_kazoo_client(self):
        client = Mock()
        self.channel._client = client
        self.channel.close()
        assert client.mock_calls == [call.stop(), call.close()]

    def test_connection_close_stops_the_kazoo_client(self):
        client = Mock()
        self.channel._client = client
        self.connection.close()
        assert client.mock_calls == [call.stop(), call.close()]

    def test_close_without_a_client_does_not_open_one(self):
        assert self.channel._client is None
        with patch.object(zookeeper.Channel, '_open') as _open:
            self.channel.close()
        _open.assert_not_called()

    def test_second_close_does_not_stop_the_client_again(self):
        client = Mock()
        self.channel._client = client
        self.channel.close()
        self.channel.close()
        assert client.mock_calls == [call.stop(), call.close()]

    def test_unacked_restore_runs_before_the_client_stops(self):
        client = Mock()
        self.channel._client = client

        def restore_unacked_once():
            assert client.mock_calls == []

        qos = Mock()
        qos.restore_unacked_once.side_effect = restore_unacked_once
        self.channel._qos = qos

        self.channel.close()

        qos.restore_unacked_once.assert_called_once_with()
        assert client.mock_calls == [call.stop(), call.close()]

    def test_close_closes_the_client_when_stop_fails(self):
        client = Mock()
        client.stop.side_effect = RuntimeError('stop failed')
        self.channel._client = client
        with pytest.raises(RuntimeError):
            self.channel.close()
        client.close.assert_called_once_with()
        client.stop.assert_called_once_with()
        assert self.channel._client is None

    @pytest.mark.parametrize('stop_fails', [False, True])
    def test_close_retains_the_client_when_cleanup_fails(self, stop_fails):
        client = Mock()
        if stop_fails:
            client.stop.side_effect = RuntimeError('stop failed')
        client.close.side_effect = RuntimeError('close failed')
        self.channel._client = client

        with pytest.raises(RuntimeError, match='close failed'):
            self.channel.close()

        assert self.channel._client is client
        assert client.mock_calls == [call.stop(), call.close()]

        client.stop.side_effect = None
        client.close.side_effect = None
        self.channel.close()

        assert self.channel._client is None
        assert client.mock_calls == [
            call.stop(), call.close(), call.stop(), call.close(),
        ]
        self.channel.close()
        assert client.stop.call_count == 2
        assert client.close.call_count == 2
