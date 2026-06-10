from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import pytest

from kombu import Connection
from kombu.transport import nats

pytest.importorskip('nats')


@pytest.mark.env('nats')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_Channel:
    def setup_method(self):
        self.connection = self.create_connection()
        self.channel = self.connection.default_channel

    def create_connection(self, **kwargs):
        return Connection(transport=nats.Transport, connect_timeout=60, **kwargs)

    def teardown_method(self):
        self.connection.close()

    def test_get_returns_message(self):
        message = {'body': 'test message'}
        self.channel._put('test_queue', message)
        result = self.channel._get('test_queue')
        assert result['body'] == 'test message'

    def test_delete_removes_queue(self):
        self.channel._put('test_queue', {'body': 'test'})
        self.channel._delete('test_queue')
        assert not self.channel._has_queue('test_queue')

    def test_size_returns_queue_size(self):
        self.channel._put('test_queue', {'body': 'test1'})
        self.channel._put('test_queue', {'body': 'test2'})
        assert self.channel._size('test_queue') == 2

    def test_new_queue_creates_queue(self):
        queue = self.channel._new_queue('test_queue')
        assert queue == 'test_queue'
        assert self.channel._has_queue('test_queue')

    def test_has_queue_returns_true_for_existing_queue(self):
        self.channel._new_queue('test_queue')
        assert self.channel._has_queue('test_queue')

    def test_has_queue_returns_false_for_nonexistent_queue(self):
        assert not self.channel._has_queue('nonexistent_queue')

    def test_ack_msg_acknowledges_message(self):
        message = AsyncMock()
        self.channel.ack_msg(message)
        message.nats_ack.assert_called_once()

    def test_nak_msg_negatively_acknowledges_message(self):
        message = AsyncMock()
        self.channel.nak_msg(message)
        message.nats_nak.assert_called_once()

    def test_term_msg_terminates_message(self):
        message = AsyncMock()
        self.channel.term_msg(message)
        message.nats_term.assert_called_once()

    def test_custom_stream_config(self):
        stream_config = {
            'max_msgs': 1000,
            'max_bytes': 1024 * 1024,
            'max_age': 3600,
        }
        conn = self.create_connection(transport_options={'stream_config': stream_config})
        channel = conn.default_channel
        assert channel.options['stream_config'] == stream_config

    def test_custom_consumer_config(self):
        consumer_config = {
            'ack_policy': 'explicit',
            'deliver_policy': 'all',
        }
        conn = self.create_connection(transport_options={'consumer_config': consumer_config})
        channel = conn.default_channel
        assert channel.options['consumer_config'] == consumer_config

    def test_custom_wait_time(self):
        wait_time = 10
        conn = self.create_connection(transport_options={'wait_time_seconds': wait_time})
        channel = conn.default_channel
        assert channel.wait_time_seconds == wait_time

    def test_custom_connection_wait_time(self):
        wait_time = 15
        conn = self.create_connection(transport_options={'connection_wait_time_seconds': wait_time})
        channel = conn.default_channel
        assert channel.connection_wait_time_seconds == wait_time


@pytest.mark.env('nats')
@pytest.mark.flaky(reruns=5, reruns_delay=2)
class test_Transport:
    def setup_method(self):
        self.client = Mock()
        self.client.transport_options = {}
        self.transport = nats.Transport(self.client)

    def test_driver_version(self):
        assert self.transport.driver_version()

    @patch('kombu.transport.nats.Client')
    def test_verify_connection(self, mock_client_class):
        mock_client = AsyncMock()
        mock_client_class.return_value = mock_client
        mock_client.connect.return_value = None
        mock_client.close.return_value = None

        connection = Mock()
        connection.client.port = None
        connection.client.hostname = None

        assert self.transport.verify_connection(connection)
        mock_client.connect.assert_called_once()
        mock_client.close.assert_called_once()

    @patch('kombu.transport.nats.Client')
    def test_verify_connection_fails(self, mock_client_class):
        mock_client = AsyncMock()
        mock_client_class.return_value = mock_client
        mock_client.connect.side_effect = ValueError

        connection = Mock()
        connection.client.port = None
        connection.client.hostname = None

        assert not self.transport.verify_connection(connection)
        mock_client.connect.assert_called_once()

    def test_connection_errors(self):
        assert self.transport.connection_errors == nats.NATS_CONNECTION_ERRORS

    def test_channel_errors(self):
        assert self.transport.channel_errors == nats.NATS_CHANNEL_ERRORS

    def test_default_port(self):
        assert self.transport.default_port == nats.DEFAULT_PORT

    def test_driver_type(self):
        assert self.transport.driver_type == 'nats'

    def test_driver_name(self):
        assert self.transport.driver_name == 'nats'
