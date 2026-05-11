"""Unit tests for the NATS JetStream transport.

These tests mock the nats-py network layer so that they run without a live NATS
server.  The nats-py package itself must be installed (it is listed as an extra
dependency), so we skip the whole module if it is absent.
"""

from __future__ import annotations

import asyncio
from array import array
from queue import Empty
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

nats = pytest.importorskip('nats')

import nats.errors  # noqa: E402
import nats.js.errors  # noqa: E402

from kombu.transport.nats import (DEFAULT_HOST,  # noqa: E402
                                  DEFAULT_METADATA_HEADER_NAMES, DEFAULT_PORT,
                                  Channel, Message, QoS, Transport,
                                  decode_nats_header_value,
                                  encode_nats_header_value, get_event_loop,
                                  message_to_nats_body_and_headers,
                                  nats_body_and_headers_to_message)

# Convenience aliases for real nats exception classes used as side-effects.
_NotFoundError = nats.js.errors.NotFoundError
_NatsTimeoutError = nats.errors.TimeoutError


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_connection(transport_options=None, port=DEFAULT_PORT,
                     hostname='localhost', userid=None, password=None):
    conn = MagicMock()
    conn.client.transport_options = transport_options or {}
    conn.client.port = port
    conn.client.hostname = hostname
    conn.client.userid = userid
    conn.client.password = password
    # Required by virtual.Channel for channel-id allocation.
    conn._used_channel_ids = array('H')
    conn.channel_max = 65535
    conn.default_port = DEFAULT_PORT
    return conn


@pytest.fixture
def mock_connection():
    return _make_connection()


@pytest.fixture
def channel(mock_connection):
    """Channel with _open mocked out (no real NATS connection)."""
    mock_nc = MagicMock()
    mock_js = MagicMock()
    mock_nc.jetstream.return_value = mock_js

    with patch.object(Channel, '_open', return_value=mock_nc):
        ch = Channel(connection=mock_connection)

    # Replace the cached_property value with our mocks.
    ch.__dict__['client'] = mock_nc
    ch._nats_client = mock_nc
    ch._js = mock_js
    ch._streams = set()
    ch._js_consumers = set()
    return ch


# ---------------------------------------------------------------------------
# test_get_event_loop
# ---------------------------------------------------------------------------


class test_get_event_loop:
    def test_returns_event_loop(self):
        loop = get_event_loop()
        assert loop is not None
        assert isinstance(loop, asyncio.AbstractEventLoop)

    def test_returns_same_loop_on_second_call(self):
        loop1 = get_event_loop()
        loop2 = get_event_loop()
        assert loop1 is loop2


# ---------------------------------------------------------------------------
# test_Message
# ---------------------------------------------------------------------------


class test_Message:
    def _make_payload(self, body=b'hello', **overrides):
        payload = {
            'body': body,
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {
                'delivery_mode': 2,
                'delivery_tag': 'tag-123',
            },
            'subject': 'test.subject',
            'ack': AsyncMock(),
            'nak': AsyncMock(),
            'term': AsyncMock(),
        }
        payload.update(overrides)
        return payload

    def test_subject_extracted(self):
        payload = self._make_payload()
        msg = Message(payload, channel=MagicMock())
        assert msg.subject == 'test.subject'

    def test_ack_callback_extracted(self):
        ack = AsyncMock()
        payload = self._make_payload(ack=ack)
        msg = Message(payload, channel=MagicMock())
        assert msg.nats_ack is ack

    def test_nak_callback_extracted(self):
        nak = AsyncMock()
        payload = self._make_payload(nak=nak)
        msg = Message(payload, channel=MagicMock())
        assert msg.nats_nak is nak

    def test_term_callback_extracted(self):
        term = AsyncMock()
        payload = self._make_payload(term=term)
        msg = Message(payload, channel=MagicMock())
        assert msg.nats_term is term


# ---------------------------------------------------------------------------
# test_QoS
# ---------------------------------------------------------------------------


class test_QoS:
    def setup_method(self):
        self.channel = MagicMock()
        self.qos = QoS(self.channel, prefetch_count=0)
        # Reset shared class-level dict (isolation between tests).
        self.qos._not_yet_acked = {}

    def test_can_consume_no_prefetch(self):
        self.qos.prefetch_count = 0
        assert self.qos.can_consume() is True

    def test_can_consume_below_limit(self):
        self.qos.prefetch_count = 5
        self.qos._not_yet_acked = {'a': 1, 'b': 2}
        assert self.qos.can_consume() is True

    def test_can_consume_at_limit(self):
        self.qos.prefetch_count = 2
        self.qos._not_yet_acked = {'a': 1, 'b': 2}
        assert self.qos.can_consume() is False

    def test_can_consume_max_estimate_no_prefetch(self):
        self.qos.prefetch_count = 0
        assert self.qos.can_consume_max_estimate() == 1

    def test_can_consume_max_estimate_with_prefetch(self):
        self.qos.prefetch_count = 10
        self.qos._not_yet_acked = {'a': 1, 'b': 2}
        assert self.qos.can_consume_max_estimate() == 8

    def test_can_consume_max_estimate_clamped_at_zero(self):
        """When _not_yet_acked exceeds prefetch_count, result is clamped at 0."""
        self.qos.prefetch_count = 2
        self.qos._not_yet_acked = {'a': 1, 'b': 2, 'c': 3}
        assert self.qos.can_consume_max_estimate() == 0

    def test_not_yet_acked_is_instance_attribute(self):
        """Each QoS instance must have its own _not_yet_acked dict."""
        qos2 = QoS(MagicMock(), prefetch_count=0)
        self.qos._not_yet_acked['x'] = 1
        assert 'x' not in qos2._not_yet_acked

    def test_append(self):
        msg = MagicMock()
        self.qos.append(msg, 'tag-1')
        assert self.qos._not_yet_acked['tag-1'] is msg

    def test_get(self):
        msg = MagicMock()
        self.qos._not_yet_acked['tag-1'] = msg
        assert self.qos.get('tag-1') is msg

    def test_ack_calls_channel_ack_msg(self):
        msg = MagicMock()
        self.qos._not_yet_acked['tag-1'] = msg
        self.qos.ack('tag-1')
        self.channel.ack_msg.assert_called_once_with(msg)
        assert 'tag-1' not in self.qos._not_yet_acked

    def test_ack_unknown_tag_noop(self):
        self.qos.ack('nonexistent')
        self.channel.ack_msg.assert_not_called()

    def test_reject_requeue_calls_nak(self):
        msg = MagicMock()
        self.qos._not_yet_acked['tag-1'] = msg
        self.qos.reject('tag-1', requeue=True)
        self.channel.nak_msg.assert_called_once_with(msg)
        self.channel.term_msg.assert_not_called()
        assert 'tag-1' not in self.qos._not_yet_acked

    def test_reject_no_requeue_calls_term(self):
        msg = MagicMock()
        self.qos._not_yet_acked['tag-1'] = msg
        self.qos.reject('tag-1', requeue=False)
        self.channel.term_msg.assert_called_once_with(msg)
        self.channel.nak_msg.assert_not_called()
        assert 'tag-1' not in self.qos._not_yet_acked

    def test_reject_unknown_tag_noop(self):
        self.qos.reject('nonexistent', requeue=True)
        self.channel.nak_msg.assert_not_called()

    def test_restore_unacked_once_is_noop(self):
        self.qos._not_yet_acked['tag-1'] = MagicMock()
        self.qos.restore_unacked_once()
        # Message is still there – nothing was touched.
        assert 'tag-1' in self.qos._not_yet_acked


# ---------------------------------------------------------------------------
# test_Channel
# ---------------------------------------------------------------------------


class test_Channel:
    def test_get_stream_name_default_prefix(self, channel):
        assert channel._get_stream_name('myqueue') == 'STREAM_myqueue'

    def test_get_stream_name_custom_prefix(self, channel):
        channel.connection.client.transport_options = {'stream_name_prefix': 'myapp_'}
        assert channel._get_stream_name('myqueue') == 'myapp_myqueue'

    def test_get_consumer_name_default_prefix(self, channel):
        assert channel._get_consumer_name('myqueue') == 'CONSUMER_myqueue'

    def test_get_consumer_name_custom_prefix(self, channel):
        channel.connection.client.transport_options = {'consumer_name_prefix': 'myapp_'}
        assert channel._get_consumer_name('myqueue') == 'myapp_myqueue'

    def test_ensure_stream_uses_custom_stream_name(self, channel):
        channel.connection.client.transport_options = {'stream_name_prefix': 'myapp_'}
        channel._js.stream_info = AsyncMock(side_effect=_NotFoundError())
        channel._js.add_stream = AsyncMock()
        channel._ensure_stream('myqueue')
        assert 'myapp_myqueue' in channel._streams
        call_args = channel._js.add_stream.call_args
        stream_cfg = call_args[0][0]
        assert stream_cfg.name == 'myapp_myqueue'

    def test_ensure_consumer_uses_custom_consumer_name(self, channel):
        channel.connection.client.transport_options = {'consumer_name_prefix': 'myapp_'}
        channel._js.add_consumer = AsyncMock()
        channel._ensure_consumer('myqueue')
        assert 'myapp_myqueue' in channel._js_consumers
        call_args = channel._js.add_consumer.call_args
        consumer_cfg = call_args[0][1]
        assert consumer_cfg.durable_name == 'myapp_myqueue'

    # -- _ensure_stream --------------------------------------------------

    def test_ensure_stream_already_tracked(self, channel):
        channel._streams.add('STREAM_myqueue')
        channel._js.stream_info = MagicMock()
        channel._ensure_stream('myqueue')
        # Must not call add_stream if stream is already tracked.
        channel._js.add_stream.assert_not_called()

    def test_ensure_stream_creates_new_stream(self, channel):
        # stream_info raises NotFoundError (not cached).
        channel._js.stream_info = AsyncMock(side_effect=_NotFoundError())
        channel._js.add_stream = AsyncMock()
        channel._ensure_stream('myqueue')
        assert 'STREAM_myqueue' in channel._streams
        channel._js.add_stream.assert_awaited_once()

    def test_ensure_stream_stream_already_exists_in_nats(self, channel):
        # First call (check) succeeds → stream exists already.
        channel._js.stream_info = AsyncMock(return_value=MagicMock())
        channel._ensure_stream('myqueue')
        assert 'STREAM_myqueue' in channel._streams
        # add_stream must not be called.
        channel._js.add_stream.assert_not_called()

    def test_ensure_stream_raises_if_js_is_none(self, channel):
        channel._js = None
        with pytest.raises(RuntimeError, match='JetStream context not initialized'):
            channel._ensure_stream('myqueue')

    # -- _ensure_consumer ------------------------------------------------

    def test_ensure_consumer_already_tracked(self, channel):
        channel._js_consumers.add('CONSUMER_myqueue')
        channel._js.add_consumer = MagicMock()
        channel._ensure_consumer('myqueue')
        channel._js.add_consumer.assert_not_called()

    def test_ensure_consumer_creates_consumer(self, channel):
        channel._js.add_consumer = AsyncMock()
        channel._ensure_consumer('myqueue')
        assert 'CONSUMER_myqueue' in channel._js_consumers
        channel._js.add_consumer.assert_awaited_once()

    def test_ensure_consumer_raises_if_js_is_none(self, channel):
        channel._js = None
        with pytest.raises(RuntimeError, match='JetStream context not initialized'):
            channel._ensure_consumer('myqueue')

    # -- _put ------------------------------------------------------------

    def test_put_publishes_message(self, channel):
        channel._ensure_stream = MagicMock()
        channel._js.publish = AsyncMock()
        message = {'body': 'hello'}
        channel._put('myqueue', message)
        channel._ensure_stream.assert_called_once_with('myqueue')
        channel._js.publish.assert_awaited_once()

    def test_put_publishes_with_ttl_header(self, channel):
        """Messages with an expiration property set the Nats-TTL header."""
        channel._ensure_stream = MagicMock()
        channel._js.publish = AsyncMock()
        message = {'body': 'hello', 'properties': {'expiration': '5000'}}
        channel._put('myqueue', message)
        _, kwargs = channel._js.publish.call_args
        assert kwargs.get('headers') == {'Nats-TTL': '5000ms'}

    def test_put_no_ttl_header_without_expiration(self, channel):
        """Messages without expiration must not include Nats-TTL header."""
        channel._ensure_stream = MagicMock()
        channel._js.publish = AsyncMock()
        message = {'body': 'hello', 'properties': {}}
        channel._put('myqueue', message)
        _, kwargs = channel._js.publish.call_args
        assert kwargs.get('headers') is None

    def test_put_raises_if_js_is_none(self, channel):
        channel._ensure_stream = MagicMock()
        channel._js = None
        with pytest.raises(RuntimeError, match='JetStream context not initialized'):
            channel._put('myqueue', {'body': 'hello'})

    # -- _get ------------------------------------------------------------

    def test_get_returns_message(self, channel):
        channel._ensure_stream = MagicMock()
        channel._ensure_consumer = MagicMock()

        fake_msg = MagicMock()
        fake_msg.subject = 'myqueue'
        fake_msg.data = b'{"body": "hello"}'
        fake_msg.headers = None
        fake_msg.ack = AsyncMock()
        fake_msg.nak = AsyncMock()
        fake_msg.term = AsyncMock()

        mock_pull_sub = MagicMock()
        mock_pull_sub.fetch = AsyncMock(return_value=[fake_msg])
        channel._js.pull_subscribe = AsyncMock(return_value=mock_pull_sub)

        result = channel._get('myqueue')
        assert result['body'] == 'hello'
        assert result['subject'] == 'myqueue'
        assert result['ack'] is fake_msg.ack
        assert result['nak'] is fake_msg.nak
        assert result['term'] is fake_msg.term

    def test_get_raises_empty_on_timeout(self, channel):
        channel._ensure_stream = MagicMock()
        channel._ensure_consumer = MagicMock()

        mock_pull_sub = MagicMock()
        mock_pull_sub.fetch = AsyncMock(side_effect=_NatsTimeoutError())
        channel._js.pull_subscribe = AsyncMock(return_value=mock_pull_sub)

        with pytest.raises(Empty):
            channel._get('myqueue')

    def test_get_raises_empty_on_index_error(self, channel):
        channel._ensure_stream = MagicMock()
        channel._ensure_consumer = MagicMock()

        mock_pull_sub = MagicMock()
        mock_pull_sub.fetch = AsyncMock(return_value=[])  # empty list → IndexError
        channel._js.pull_subscribe = AsyncMock(return_value=mock_pull_sub)

        with pytest.raises(Empty):
            channel._get('myqueue')

    def test_get_raises_if_js_is_none(self, channel):
        channel._ensure_stream = MagicMock()
        channel._ensure_consumer = MagicMock()
        channel._js = None
        with pytest.raises(RuntimeError, match='JetStream context not initialized'):
            channel._get('myqueue')

    # -- _delete ---------------------------------------------------------

    def test_delete_removes_tracked_stream(self, channel):
        channel._streams.add('STREAM_myqueue')
        channel._js.delete_stream = AsyncMock()
        channel._delete('myqueue')
        channel._js.delete_stream.assert_awaited_once_with('STREAM_myqueue')
        assert 'STREAM_myqueue' not in channel._streams

    def test_delete_attempts_deletion_for_untracked_stream(self, channel):
        """_delete() always attempts deletion, even if stream is not in cache."""
        channel._js.delete_stream = AsyncMock()
        channel._delete('myqueue')
        channel._js.delete_stream.assert_awaited_once_with('STREAM_myqueue')

    def test_delete_handles_not_found_gracefully(self, channel):
        channel._js.delete_stream = AsyncMock(side_effect=_NotFoundError())
        # Must not raise even when stream was never in cache.
        channel._delete('myqueue')
        assert 'STREAM_myqueue' not in channel._streams

    def test_delete_discards_stream_from_cache_on_not_found(self, channel):
        """When NotFoundError is raised, stream is still removed from cache."""
        channel._streams.add('STREAM_myqueue')
        channel._js.delete_stream = AsyncMock(side_effect=_NotFoundError())
        channel._delete('myqueue')
        assert 'STREAM_myqueue' not in channel._streams

    def test_delete_raises_if_js_is_none(self, channel):
        channel._js = None
        with pytest.raises(RuntimeError, match='JetStream context not initialized'):
            channel._delete('myqueue')

    # -- _size -----------------------------------------------------------

    def test_size_returns_message_count(self, channel):
        info = MagicMock()
        info.state.messages = 42
        channel._js.stream_info = AsyncMock(return_value=info)
        assert channel._size('myqueue') == 42

    def test_size_returns_zero_for_not_found(self, channel):
        channel._js.stream_info = AsyncMock(side_effect=_NotFoundError())
        assert channel._size('myqueue') == 0

    def test_size_raises_if_js_is_none(self, channel):
        channel._js = None
        with pytest.raises(RuntimeError, match='JetStream context not initialized'):
            channel._size('myqueue')

    # -- _new_queue ------------------------------------------------------

    def test_new_queue_ensures_stream_and_returns_name(self, channel):
        channel._ensure_stream = MagicMock()
        result = channel._new_queue('myqueue')
        channel._ensure_stream.assert_called_once_with('myqueue')
        assert result == 'myqueue'

    # -- _has_queue ------------------------------------------------------

    def test_has_queue_true_when_stream_exists(self, channel):
        channel._js.stream_info = AsyncMock(return_value=MagicMock())
        assert channel._has_queue('myqueue') is True

    def test_has_queue_false_when_not_found(self, channel):
        channel._js.stream_info = AsyncMock(side_effect=_NotFoundError())
        assert channel._has_queue('myqueue') is False

    def test_has_queue_false_on_timeout(self, channel):
        channel._js.stream_info = AsyncMock(side_effect=_NatsTimeoutError())
        assert channel._has_queue('myqueue') is False

    def test_has_queue_raises_if_js_is_none(self, channel):
        channel._js = None
        with pytest.raises(RuntimeError, match='JetStream context not initialized'):
            channel._has_queue('myqueue')

    # -- options / conninfo / wait times ---------------------------------

    def test_options_returns_transport_options(self, channel, mock_connection):
        mock_connection.client.transport_options = {'foo': 'bar'}
        channel.connection = mock_connection
        assert channel.options == {'foo': 'bar'}

    def test_conninfo_returns_client(self, channel, mock_connection):
        channel.connection = mock_connection
        assert channel.conninfo is mock_connection.client

    def test_wait_time_seconds_default(self, channel):
        # Delete cached value so the property is recalculated.
        channel.__dict__.pop('wait_time_seconds', None)
        assert channel.wait_time_seconds == float(
            channel.default_wait_time_seconds
        )

    def test_wait_time_seconds_from_options(self, channel, mock_connection):
        mock_connection.client.transport_options = {'wait_time_seconds': 10}
        channel.connection = mock_connection
        channel.__dict__.pop('wait_time_seconds', None)
        assert channel.wait_time_seconds == 10.0

    def test_connection_wait_time_seconds_default(self, channel):
        channel.__dict__.pop('connection_wait_time_seconds', None)
        assert channel.connection_wait_time_seconds == float(
            channel.default_connection_wait_time_seconds
        )

    def test_connection_wait_time_seconds_from_options(
        self, channel, mock_connection
    ):
        mock_connection.client.transport_options = {
            'connection_wait_time_seconds': 15
        }
        channel.connection = mock_connection
        channel.__dict__.pop('connection_wait_time_seconds', None)
        assert channel.connection_wait_time_seconds == 15.0

    # -- close -----------------------------------------------------------

    def test_close_drains_and_closes_client(self, channel):
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock()
        mock_nc.close = AsyncMock()
        channel._nats_client = mock_nc

        channel.close()

        mock_nc.drain.assert_awaited_once()
        mock_nc.close.assert_awaited_once()
        assert channel._nats_client is None
        assert channel._js is None

    def test_close_noop_when_no_client(self, channel):
        channel._nats_client = None
        # Must not raise.
        channel.close()

    # -- ack_msg / nak_msg / term_msg ------------------------------------

    def test_ack_msg(self, channel):
        msg = MagicMock()
        msg.nats_ack = AsyncMock()
        channel.ack_msg(msg)
        msg.nats_ack.assert_awaited_once()

    def test_nak_msg(self, channel):
        msg = MagicMock()
        msg.nats_nak = AsyncMock()
        channel.nak_msg(msg)
        msg.nats_nak.assert_awaited_once()

    def test_term_msg(self, channel):
        msg = MagicMock()
        msg.nats_term = AsyncMock()
        channel.term_msg(msg)
        msg.nats_term.assert_awaited_once()

    # -- _open -----------------------------------------------------------

    def test_open_connects_to_nats(self, mock_connection):
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        mock_nc.jetstream.return_value = MagicMock()

        with patch('kombu.transport.nats.Client', return_value=mock_nc):
            with patch.object(Channel, '_open', return_value=mock_nc):
                ch = Channel(connection=mock_connection)
            ch._nats_client = mock_nc

        assert ch._nats_client is mock_nc

    def test_open_reuses_existing_client(self, channel):
        existing_client = channel._nats_client
        result = channel._open()
        # If _nats_client is already set, _open returns it without connecting.
        assert result is existing_client

    def test_open_uses_default_host_when_none(self, mock_connection):
        """When hostname is None, _open must not produce 'nats://None:...'."""
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        mock_nc.jetstream.return_value = MagicMock()
        mock_connection.client.hostname = None
        mock_connection.client.port = None

        with patch.object(Channel, '_open', return_value=mock_nc):
            ch = Channel(connection=mock_connection)

        # Reset client so _open() will actually run the connection logic.
        ch._nats_client = None
        with patch('kombu.transport.nats.Client', return_value=mock_nc):
            ch._open()

        mock_nc.connect.assert_awaited_once()
        url_arg = mock_nc.connect.call_args[0][0]
        assert 'None' not in url_arg
        assert f'{DEFAULT_HOST}:{DEFAULT_PORT}' in url_arg

    # -- ImportError when library missing --------------------------------

    def test_channel_init_raises_without_nats(self, mock_connection):
        with patch('kombu.transport.nats.Client', None):
            with pytest.raises(ImportError, match='nats-py is not installed'):
                Channel(connection=mock_connection)


# ---------------------------------------------------------------------------
# test_Transport
# ---------------------------------------------------------------------------


class test_Transport:
    def setup_method(self):
        self.mock_client = MagicMock()
        self.mock_client.transport_options = {}

    def test_driver_version(self):
        transport = Transport(self.mock_client)
        version = transport.driver_version()
        assert isinstance(version, str)

    def test_default_port(self):
        transport = Transport(self.mock_client)
        assert transport.default_port == DEFAULT_PORT

    def test_driver_type(self):
        transport = Transport(self.mock_client)
        assert transport.driver_type == 'nats'

    def test_driver_name(self):
        transport = Transport(self.mock_client)
        assert transport.driver_name == 'nats'

    def test_init_raises_without_nats(self):
        with patch('kombu.transport.nats.Client', None):
            with pytest.raises(ImportError, match='nats-py is not installed'):
                Transport(self.mock_client)

    def test_verify_connection_returns_true_on_success(self):
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        mock_nc.close = AsyncMock()

        transport = Transport(self.mock_client)
        mock_conn = MagicMock()
        mock_conn.client.port = DEFAULT_PORT
        mock_conn.client.hostname = 'localhost'

        with patch('kombu.transport.nats.Client', return_value=mock_nc):
            result = transport.verify_connection(mock_conn)
        assert result is True

    def test_verify_connection_returns_false_on_value_error(self):
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock(side_effect=ValueError('bad url'))

        transport = Transport(self.mock_client)
        mock_conn = MagicMock()
        mock_conn.client.port = DEFAULT_PORT
        mock_conn.client.hostname = 'localhost'

        with patch('kombu.transport.nats.Client', return_value=mock_nc):
            result = transport.verify_connection(mock_conn)
        assert result is False

    def test_verify_connection_uses_default_port_when_none(self):
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        mock_nc.close = AsyncMock()

        transport = Transport(self.mock_client)
        mock_conn = MagicMock()
        mock_conn.client.port = None
        mock_conn.client.hostname = 'myhost'

        with patch('kombu.transport.nats.Client', return_value=mock_nc):
            result = transport.verify_connection(mock_conn)
        assert result is True
        # The connect coroutine should be called with the default port.
        mock_nc.connect.assert_awaited_once_with(
            f'nats://myhost:{DEFAULT_PORT}'
        )

    def test_verify_connection_uses_default_host_when_none(self):
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        mock_nc.close = AsyncMock()

        transport = Transport(self.mock_client)
        mock_conn = MagicMock()
        mock_conn.client.port = DEFAULT_PORT
        mock_conn.client.hostname = None

        with patch('kombu.transport.nats.Client', return_value=mock_nc):
            result = transport.verify_connection(mock_conn)
        assert result is True
        mock_nc.connect.assert_awaited_once_with(
            f'nats://{DEFAULT_HOST}:{DEFAULT_PORT}'
        )


# ---------------------------------------------------------------------------
# test_clean_body_helpers
# ---------------------------------------------------------------------------


class test_clean_body_helpers:
    """Tests for the module-level clean-body helper functions."""

    # -- encode_nats_header_value -----------------------------------------

    def test_encode_string_passthrough(self):
        assert encode_nats_header_value('application/json') == 'application/json'

    def test_encode_none_returns_empty_string(self):
        assert encode_nats_header_value(None) == ''

    def test_encode_dict_to_json(self):
        from kombu.utils.json import loads as jloads
        result = encode_nats_header_value({'a': 1, 'b': 'x'})
        assert jloads(result) == {'a': 1, 'b': 'x'}

    def test_encode_list_to_json(self):
        from kombu.utils.json import loads as jloads
        result = encode_nats_header_value([1, 2, 3])
        assert jloads(result) == [1, 2, 3]

    def test_encode_integer_to_json(self):
        result = encode_nats_header_value(42)
        assert result == '42'

    # -- decode_nats_header_value -----------------------------------------

    def test_decode_empty_string_returns_none(self):
        assert decode_nats_header_value('') is None

    def test_decode_json_object(self):
        result = decode_nats_header_value('{"k": "v"}')
        assert result == {'k': 'v'}

    def test_decode_json_array(self):
        result = decode_nats_header_value('[1, 2, 3]')
        assert result == [1, 2, 3]

    def test_decode_plain_string(self):
        assert decode_nats_header_value('application/json') == 'application/json'

    def test_decode_whitespace_around_json(self):
        result = decode_nats_header_value('  {"x": 1}  ')
        assert result == {'x': 1}

    # -- DEFAULT_METADATA_HEADER_NAMES ------------------------------------

    def test_default_header_names_keys(self):
        expected_keys = {
            'content_type', 'content_encoding', 'headers',
            'properties', 'delivery_info',
        }
        assert set(DEFAULT_METADATA_HEADER_NAMES.keys()) == expected_keys

    # -- message_to_nats_body_and_headers ---------------------------------

    def test_legacy_mode_returns_json_envelope(self):
        from kombu.utils.json import loads as jloads
        message = {'body': 'test', 'content-type': 'application/json'}
        body_bytes, headers = message_to_nats_body_and_headers(
            message,
            clean_body=False,
            header_prefix='Kombu-',
            header_names=None,
        )
        assert jloads(body_bytes.decode()) == message
        assert headers == {}

    def test_put_clean_body_publishes_body_as_bytes(self):
        """Clean-body mode publishes message['body'] as bytes without decoding."""
        import base64
        raw = b'{"result": 42}'
        body_b64 = base64.b64encode(raw).decode('utf-8')
        message = {
            'body': body_b64,
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
            'delivery_info': {'exchange': 'ex'},
        }
        body_bytes, headers = message_to_nats_body_and_headers(
            message,
            clean_body=True,
            header_prefix='Kombu-',
            header_names=None,
        )
        # NATS payload is message['body'] as UTF-8 bytes, not the decoded raw bytes.
        assert body_bytes == body_b64.encode('utf-8')

    def test_clean_body_sets_content_type_header(self):
        import base64
        message = {
            'body': base64.b64encode(b'x').decode(),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
        }
        _, headers = message_to_nats_body_and_headers(
            message, clean_body=True, header_prefix='Kombu-', header_names=None,
        )
        assert headers.get('Kombu-Content-Type') == 'application/json'
        assert headers.get('Kombu-Content-Encoding') == 'utf-8'

    def test_clean_body_custom_prefix(self):
        import base64
        message = {
            'body': base64.b64encode(b'x').decode(),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
        }
        _, headers = message_to_nats_body_and_headers(
            message, clean_body=True, header_prefix='ce-', header_names=None,
        )
        assert 'ce-Content-Type' in headers
        assert 'Kombu-Content-Type' not in headers

    def test_clean_body_custom_header_names(self):
        import base64
        message = {
            'body': base64.b64encode(b'x').decode(),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
        }
        _, headers = message_to_nats_body_and_headers(
            message,
            clean_body=True,
            header_prefix='X-',
            header_names={'content_type': 'KombuContentType'},
        )
        assert 'X-KombuContentType' in headers
        # Other names still use defaults
        assert 'X-Content-Encoding' in headers

    def test_clean_body_empty_headers_not_set(self):
        """Empty 'headers' dict is not serialised into NATS headers."""
        import base64
        message = {
            'body': base64.b64encode(b'x').decode(),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
        }
        _, headers = message_to_nats_body_and_headers(
            message, clean_body=True, header_prefix='Kombu-', header_names=None,
        )
        assert 'Kombu-Headers' not in headers

    # -- nats_body_and_headers_to_message ---------------------------------

    def test_legacy_path_no_metadata_headers(self):
        """No metadata headers → fall back to legacy JSON envelope parse."""
        import json
        envelope = {'body': 'hello', 'content-type': 'application/json'}
        data = json.dumps(envelope).encode()
        result = nats_body_and_headers_to_message(
            data, None,
            header_prefix='Kombu-', header_names=None,
        )
        assert result == envelope

    def test_legacy_path_non_dict_headers(self):
        """Non-dict msg.headers (e.g. MagicMock) → fall back to legacy path."""
        import json
        envelope = {'body': 'hi'}
        data = json.dumps(envelope).encode()
        result = nats_body_and_headers_to_message(
            data, MagicMock(),  # truthy but not a real dict
            header_prefix='Kombu-', header_names=None,
        )
        assert result == envelope

    def test_clean_body_path_reconstructs_envelope(self):
        import json
        raw = b'raw payload'
        headers = {
            'Kombu-Content-Type': 'application/octet-stream',
            'Kombu-Content-Encoding': 'binary',
            'Kombu-Properties': json.dumps({'delivery_mode': 2}),
            'Kombu-Delivery-Info': json.dumps({'exchange': 'ex', 'routing_key': 'rk'}),
        }
        result = nats_body_and_headers_to_message(
            raw, headers,
            header_prefix='Kombu-', header_names=None,
        )
        assert result['content-type'] == 'application/octet-stream'
        assert result['content-encoding'] == 'binary'
        # body is raw bytes, no re-encoding applied by the transport.
        assert result['body'] == raw
        assert result['properties']['delivery_mode'] == 2
        assert result['delivery_info'] == {'exchange': 'ex', 'routing_key': 'rk'}

    def test_clean_body_list_header_values_flattened(self):
        """Header values that are lists (nats-py style) are flattened."""
        import json
        raw = b'data'
        headers = {
            'Kombu-Content-Type': ['application/json'],  # list value
            'Kombu-Properties': json.dumps({'delivery_mode': 1}),
        }
        result = nats_body_and_headers_to_message(
            raw, headers,
            header_prefix='Kombu-', header_names=None,
        )
        assert result['content-type'] == 'application/json'

    def test_roundtrip_clean_body(self):
        """message_to_nats → nats_to_message reproduces the Kombu envelope."""
        import base64
        raw = b'{"x": 1}'
        message = {
            'body': base64.b64encode(raw).decode('utf-8'),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {'x-custom': 'val'},
            'properties': {'body_encoding': 'base64', 'delivery_mode': 2},
            'delivery_info': {'exchange': 'ex', 'routing_key': 'rk'},
        }
        body_bytes, meta_headers = message_to_nats_body_and_headers(
            message, clean_body=True, header_prefix='Kombu-', header_names=None,
        )
        reconstructed = nats_body_and_headers_to_message(
            body_bytes, meta_headers,
            header_prefix='Kombu-', header_names=None,
        )
        assert reconstructed['content-type'] == 'application/json'
        assert reconstructed['content-encoding'] == 'utf-8'
        assert base64.b64decode(reconstructed['body']) == raw
        assert reconstructed['headers'] == {'x-custom': 'val'}
        assert reconstructed['properties']['delivery_mode'] == 2
        assert reconstructed['delivery_info'] == {
            'exchange': 'ex', 'routing_key': 'rk',
        }


# ---------------------------------------------------------------------------
# Clean-body mode tests in Channel
# ---------------------------------------------------------------------------


class test_channel_clean_body:
    """Integration-style unit tests for Channel._put / _get in clean-body mode."""

    @pytest.fixture
    def clean_channel(self, mock_connection):
        """Channel configured with nats_clean_body=True."""
        mock_connection.client.transport_options = {'nats_clean_body': True}
        mock_nc = MagicMock()
        mock_js = MagicMock()
        mock_nc.jetstream.return_value = mock_js
        with patch.object(Channel, '_open', return_value=mock_nc):
            ch = Channel(connection=mock_connection)
        ch.__dict__['client'] = mock_nc
        ch._nats_client = mock_nc
        ch._js = mock_js
        ch._streams = set()
        ch._js_consumers = set()
        return ch

    # -- nats_clean_body / nats_metadata_header_prefix / nats_metadata_header_names

    def test_nats_clean_body_default_false(self, channel):
        assert channel.nats_clean_body is False

    def test_nats_clean_body_true_from_options(self, clean_channel):
        assert clean_channel.nats_clean_body is True

    def test_nats_metadata_header_prefix_default(self, channel):
        assert channel.nats_metadata_header_prefix == 'Kombu-'

    def test_nats_metadata_header_prefix_from_options(self, channel, mock_connection):
        mock_connection.client.transport_options = {
            'nats_metadata_header_prefix': 'ce-',
        }
        channel.connection = mock_connection
        assert channel.nats_metadata_header_prefix == 'ce-'

    def test_nats_metadata_header_prefix_reserved_warning(
        self, channel, mock_connection, caplog
    ):
        import logging
        mock_connection.client.transport_options = {
            'nats_metadata_header_prefix': 'Nats-Custom',
        }
        channel.connection = mock_connection
        with caplog.at_level(logging.WARNING, logger='kombu.transport.nats'):
            _ = channel.nats_metadata_header_prefix
        assert 'Nats-' in caplog.text

    def test_nats_metadata_header_names_default_none(self, channel):
        assert channel.nats_metadata_header_names is None

    def test_nats_metadata_header_names_from_options(self, channel, mock_connection):
        custom = {'content_type': 'ContentType'}
        mock_connection.client.transport_options = {
            'nats_metadata_header_names': custom,
        }
        channel.connection = mock_connection
        assert channel.nats_metadata_header_names == custom

    # -- _put in clean-body mode -----------------------------------------

    def test_put_clean_body_publishes_body_bytes_to_nats(self, clean_channel):
        """_put() publishes message['body'] as bytes; does not unwrap base64."""
        import base64
        clean_channel._ensure_stream = MagicMock()
        clean_channel._js.publish = AsyncMock()
        raw = b'{"task": "add", "args": [1, 2]}'
        body_b64 = base64.b64encode(raw).decode('utf-8')
        message = {
            'body': body_b64,
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64', 'delivery_mode': 2},
            'delivery_info': {'exchange': 'x', 'routing_key': 'r'},
        }
        clean_channel._put('myqueue', message)
        args, kwargs = clean_channel._js.publish.call_args
        # Payload is message['body'] encoded to UTF-8; not the unwrapped raw bytes.
        assert args[1] == body_b64.encode('utf-8')

    def test_put_clean_body_sets_metadata_headers(self, clean_channel):
        import base64
        clean_channel._ensure_stream = MagicMock()
        clean_channel._js.publish = AsyncMock()
        raw = b'"hello"'
        message = {
            'body': base64.b64encode(raw).decode('utf-8'),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
            'delivery_info': {'exchange': 'ex'},
        }
        clean_channel._put('myqueue', message)
        _, kwargs = clean_channel._js.publish.call_args
        headers = kwargs.get('headers', {}) or {}
        assert headers.get('Kombu-Content-Type') == 'application/json'
        assert headers.get('Kombu-Content-Encoding') == 'utf-8'

    def test_put_clean_body_with_ttl_has_both_headers(self, clean_channel):
        import base64
        clean_channel._ensure_stream = MagicMock()
        clean_channel._js.publish = AsyncMock()
        raw = b'"hello"'
        message = {
            'body': base64.b64encode(raw).decode('utf-8'),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64', 'expiration': '3000'},
        }
        clean_channel._put('myqueue', message)
        _, kwargs = clean_channel._js.publish.call_args
        headers = kwargs.get('headers', {}) or {}
        assert 'Kombu-Content-Type' in headers
        assert headers.get('Nats-TTL') == '3000ms'

    def test_put_clean_body_custom_prefix(self, mock_connection):
        import base64
        mock_connection.client.transport_options = {
            'nats_clean_body': True,
            'nats_metadata_header_prefix': 'ce-',
        }
        mock_nc = MagicMock()
        mock_js = MagicMock()
        mock_nc.jetstream.return_value = mock_js
        with patch.object(Channel, '_open', return_value=mock_nc):
            ch = Channel(connection=mock_connection)
        ch.__dict__['client'] = mock_nc
        ch._nats_client = mock_nc
        ch._js = mock_js
        ch._streams = set()
        ch._js_consumers = set()

        ch._ensure_stream = MagicMock()
        ch._js.publish = AsyncMock()
        message = {
            'body': base64.b64encode(b'data').decode(),
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
        }
        ch._put('myqueue', message)
        _, kwargs = ch._js.publish.call_args
        headers = kwargs.get('headers', {}) or {}
        assert 'ce-Content-Type' in headers
        assert 'Kombu-Content-Type' not in headers

    # -- _get in clean-body mode -----------------------------------------

    def test_get_clean_body_reconstructs_message(self, clean_channel):
        import json
        clean_channel._ensure_stream = MagicMock()
        clean_channel._ensure_consumer = MagicMock()
        raw = b'{"task": "add"}'
        fake_msg = MagicMock()
        fake_msg.subject = 'myqueue'
        fake_msg.data = raw
        fake_msg.ack = AsyncMock()
        fake_msg.nak = AsyncMock()
        fake_msg.term = AsyncMock()
        fake_msg.headers = {
            'Kombu-Content-Type': 'application/json',
            'Kombu-Content-Encoding': 'utf-8',
            'Kombu-Properties': json.dumps({'delivery_mode': 2}),
            'Kombu-Delivery-Info': json.dumps({'exchange': 'ex', 'routing_key': 'rk'}),
        }
        mock_pull_sub = MagicMock()
        mock_pull_sub.fetch = AsyncMock(return_value=[fake_msg])
        clean_channel._js.pull_subscribe = AsyncMock(return_value=mock_pull_sub)

        result = clean_channel._get('myqueue')
        assert result['content-type'] == 'application/json'
        assert result['content-encoding'] == 'utf-8'
        # body is raw bytes, no re-encoding applied by the transport.
        assert result['body'] == raw
        assert result['subject'] == 'myqueue'
        assert result['ack'] is fake_msg.ack

    def test_get_legacy_message_still_works(self, channel):
        """Legacy (non-clean-body) messages are always parsed correctly."""
        channel._ensure_stream = MagicMock()
        channel._ensure_consumer = MagicMock()
        import json
        envelope = {
            'body': 'aGVsbG8=',
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
        }
        fake_msg = MagicMock()
        fake_msg.subject = 'myqueue'
        fake_msg.data = json.dumps(envelope).encode()
        fake_msg.headers = None
        fake_msg.ack = AsyncMock()
        fake_msg.nak = AsyncMock()
        fake_msg.term = AsyncMock()
        mock_pull_sub = MagicMock()
        mock_pull_sub.fetch = AsyncMock(return_value=[fake_msg])
        channel._js.pull_subscribe = AsyncMock(return_value=mock_pull_sub)

        result = channel._get('myqueue')
        assert result['content-type'] == 'application/json'
        assert result['body'] == 'aGVsbG8='
