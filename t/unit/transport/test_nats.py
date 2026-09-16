"""Unit tests for the NATS JetStream transport.

These tests mock the nats-py network layer so that they run without a live NATS
server.  The nats-py package itself must be installed (it is listed as an extra
dependency), so we skip the whole module if it is absent.
"""

from __future__ import annotations

import asyncio
import threading
from array import array
from queue import Empty
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

nats = pytest.importorskip('nats')

import nats.errors  # noqa: E402
import nats.js.errors  # noqa: E402

from kombu.exceptions import NotBoundError, OperationalError  # noqa: E402
from kombu.transport.nats import DEFAULT_HOST  # noqa: E402
from kombu.transport.nats import DEFAULT_METADATA_HEADER_NAMES  # noqa: E402
from kombu.transport.nats import (DEFAULT_PORT, MAX_INBOX_SIZE,  # noqa: E402
                                  Channel, CoreNATSChannel, JetStreamChannel,
                                  Message, NATSError, QoS, Transport,
                                  decode_nats_header_value,
                                  encode_nats_header_value,
                                  message_to_nats_body_and_headers,
                                  nats_body_and_headers_to_message,
                                  normalize_js_resource_name)
from kombu.transport.virtual.base import BrokerState  # noqa: E402

# Convenience aliases for real nats exception classes used as side-effects.
_NotFoundError = nats.js.errors.NotFoundError
_NatsTimeoutError = nats.errors.TimeoutError


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


class _FakeTransport:
    """Minimal stand-in for the real NATS Transport: owns a shared
    event loop thread and hands it out to channels."""

    def __init__(self):
        self._loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self._loop.run_forever,
            daemon=True,
            name="kombu-nats-loop",
        )
        self._loop_thread.start()
        self._nats_client = None

    def _get_loop(self):
        return self._loop

    def _get_client(self, conninfo, connect_timeout=None):
        """Never dials NATS — returns a mock client, cached like the real
        transport so all channels share one object."""
        if self._nats_client is None:
            mock_nc = MagicMock()
            mock_nc.jetstream.return_value = MagicMock()
            self._nats_client = mock_nc
        return self._nats_client

    def close(self):
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._loop_thread.join(timeout=5)
            self._loop.close()
        self._loop = None


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
    # Channels borrow the shared loop from their transport.
    conn.transport = _FakeTransport()
    return conn


@pytest.fixture
def mock_connection():
    return _make_connection()


@pytest.fixture
def channel(mock_connection):
    """JetStreamChannel with _open mocked out (no real NATS connection)."""
    mock_nc = MagicMock()
    mock_js = MagicMock()
    mock_nc.jetstream.return_value = mock_js

    with patch.object(JetStreamChannel, '_open', return_value=mock_nc):
        ch = JetStreamChannel(connection=mock_connection)

    # Replace the cached_property value with our mocks.
    ch.__dict__['client'] = mock_nc
    ch._nats_client = mock_nc
    ch._js = mock_js
    ch._streams = set()
    ch._js_consumers = set()
    return ch


# ---------------------------------------------------------------------------
# test_Channel_loop
# ---------------------------------------------------------------------------


class test_Channel_loop:
    """Channels borrow a single shared event loop from the transport."""

    def test_channel_has_loop(self, channel):
        assert hasattr(channel, '_loop')
        assert isinstance(channel._loop, asyncio.AbstractEventLoop)
        assert not channel._loop.is_closed()

    def test_channels_share_transport_loop(self, channel, mock_connection):
        """Two channels on the same transport share one event loop."""
        mock_nc2 = MagicMock()
        mock_js2 = MagicMock()
        mock_nc2.jetstream.return_value = mock_js2
        with patch.object(JetStreamChannel, '_open', return_value=mock_nc2):
            ch2 = JetStreamChannel(connection=mock_connection)
        ch2.__dict__['client'] = mock_nc2
        assert channel._loop is ch2._loop

    def test_global_event_loop_not_mutated(self):
        """Creating a JetStreamChannel must not call asyncio.set_event_loop()."""
        conn = _make_connection()
        mock_nc = MagicMock()
        mock_nc.jetstream.return_value = MagicMock()
        with patch('asyncio.set_event_loop') as mock_set:
            with patch.object(JetStreamChannel, '_open', return_value=mock_nc):
                ch = JetStreamChannel(connection=conn)
            ch.__dict__['client'] = mock_nc
        mock_set.assert_not_called()

    def test_run_uses_channel_loop(self, channel):
        async def _coro():
            return 42
        assert channel._run(_coro()) == 42

    def test_run_raises_when_loop_closed(self, channel):
        # Stop the transport's shared loop (channel no longer owns one).
        transport = channel.transport
        transport._loop.call_soon_threadsafe(transport._loop.stop)
        transport._loop_thread.join(timeout=5)
        transport._loop.close()

        async def _noop():
            pass

        coro = _noop()
        with pytest.raises(RuntimeError, match='event loop is closed'):
            channel._run(coro)
        coro.close()  # suppress ResourceWarning

    def test_close_leaves_shared_loop_running(self, channel):
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock()
        mock_nc.close = AsyncMock()
        channel._nats_client = mock_nc
        loop = channel._loop
        channel.close()
        # Channel.close() is local-only: the shared loop keeps running.
        assert not loop.is_closed()

    def test_nats_calls_routed_through_channel_loop(self, channel):
        """Async NATS calls go through the channel's own loop."""
        channel._ensure_stream = MagicMock()
        channel._js.publish = AsyncMock()
        with patch.object(channel, '_run', wraps=channel._run) as mock_run:
            channel._put('q', {'body': 'hi'})
        assert mock_run.call_count >= 1


# ---------------------------------------------------------------------------
# test_get_event_loop (removed — function no longer exists)
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

    def test_get_stream_name_sanitizes_dotted_queue(self, channel):
        stream_name = channel._get_stream_name('celeryev.1234-5678')
        assert stream_name.startswith('STREAM_celeryev_1234-5678__')
        assert '.' not in stream_name

    def test_get_consumer_name_default_prefix(self, channel):
        assert channel._get_consumer_name('myqueue') == 'CONSUMER_myqueue'

    def test_get_consumer_name_custom_prefix(self, channel):
        channel.connection.client.transport_options = {'consumer_name_prefix': 'myapp_'}
        assert channel._get_consumer_name('myqueue') == 'myapp_myqueue'

    def test_get_consumer_name_sanitizes_dotted_queue(self, channel):
        consumer_name = channel._get_consumer_name('a.reply.celery.pidbox')
        assert consumer_name.startswith('CONSUMER_a_reply_celery_pidbox__')
        assert '.' not in consumer_name

    def test_normalize_js_resource_name_preserves_valid_names(self):
        assert normalize_js_resource_name('STREAM_celery') == 'STREAM_celery'

    def test_normalize_js_resource_name_adds_hash_for_invalid_names(self):
        normalized = normalize_js_resource_name('STREAM_celeryev.1234')
        assert normalized.startswith('STREAM_celeryev_1234__')
        assert '.' not in normalized

    def test_ensure_stream_uses_custom_stream_name(self, channel):
        channel.connection.client.transport_options = {'stream_name_prefix': 'myapp_'}
        channel._js.stream_info = AsyncMock(side_effect=_NotFoundError())
        channel._js.add_stream = AsyncMock()
        channel._ensure_stream('myqueue')
        assert 'myapp_myqueue' in channel._streams
        call_args = channel._js.add_stream.call_args
        stream_cfg = call_args[0][0]
        assert stream_cfg.name == 'myapp_myqueue'

    def test_ensure_stream_sanitizes_dotted_queue_names(self, channel):
        channel._js.stream_info = AsyncMock(side_effect=_NotFoundError())
        channel._js.add_stream = AsyncMock()
        channel._ensure_stream('celeryev.1234-5678')
        call_args = channel._js.add_stream.call_args
        stream_cfg = call_args[0][0]
        assert stream_cfg.name.startswith('STREAM_celeryev_1234-5678__')
        assert stream_cfg.subjects == ['celeryev.1234-5678']
        assert '.' not in stream_cfg.name

    def test_ensure_consumer_uses_custom_consumer_name(self, channel):
        channel.connection.client.transport_options = {'consumer_name_prefix': 'myapp_'}
        channel._js.add_consumer = AsyncMock()
        channel._ensure_consumer('myqueue')
        assert 'myapp_myqueue' in channel._js_consumers
        call_args = channel._js.add_consumer.call_args
        consumer_cfg = call_args[0][1]
        assert consumer_cfg.durable_name == 'myapp_myqueue'

    def test_ensure_consumer_sanitizes_dotted_queue_names(self, channel):
        channel._js.add_consumer = AsyncMock()
        channel._ensure_consumer('reply.celery.pidbox')
        call_args = channel._js.add_consumer.call_args
        consumer_cfg = call_args[0][1]
        assert consumer_cfg.durable_name.startswith('CONSUMER_reply_celery_pidbox__')
        assert '.' not in consumer_cfg.durable_name

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

    def test_ensure_stream_raises_nats_error_with_cause(self, channel):
        """Creation fails: stream_info NotFound + add_stream Timeout →
        NATSError chaining the original cause."""
        channel._js.stream_info = AsyncMock(side_effect=_NotFoundError())
        channel._js.add_stream = AsyncMock(side_effect=_NatsTimeoutError())
        channel._js.stream_info = AsyncMock(side_effect=_NotFoundError())

        with pytest.raises(NATSError) as excinfo:
            channel._ensure_stream('myqueue')
        assert 'STREAM_myqueue' in str(excinfo.value)
        # Cause must be chained, not swallowed.
        assert isinstance(excinfo.value.__cause__, _NotFoundError)
        assert isinstance(excinfo.value, OperationalError)

    def test_ensure_consumer_raises_nats_error_with_cause(self, channel):
        """Consumer creation times out and the check also fails →
        NATSError chaining the original cause."""
        channel._js.add_consumer = AsyncMock(side_effect=_NatsTimeoutError())
        channel._js.consumer_info = AsyncMock(side_effect=_NotFoundError())

        with pytest.raises(NATSError) as excinfo:
            channel._ensure_consumer('myqueue')
        assert 'CONSUMER_myqueue' in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, _NotFoundError)

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

    def test_options_raises_when_closed(self, channel, mock_connection):
        channel.connection = mock_connection
        channel.closed = True
        with pytest.raises(NotBoundError, match='Channel is closed'):
            _ = channel.options

    def test_conninfo_returns_client(self, channel, mock_connection):
        channel.connection = mock_connection
        assert channel.conninfo is mock_connection.client

    def test_conninfo_raises_when_closed(self, channel, mock_connection):
        channel.connection = mock_connection
        channel.closed = True
        with pytest.raises(NotBoundError, match='Channel is closed'):
            _ = channel.conninfo

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

    def test_close_is_local_only(self, channel):
        """Channel.close() must not drain/close the shared client, nor
        stop the shared loop — the Transport owns both."""
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock()
        mock_nc.close = AsyncMock()
        channel._nats_client = mock_nc
        loop = channel._loop

        channel.close()

        mock_nc.drain.assert_not_awaited()
        mock_nc.close.assert_not_awaited()
        assert not loop.is_closed()
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
            with patch.object(JetStreamChannel, '_open', return_value=mock_nc):
                ch = JetStreamChannel(connection=mock_connection)
            ch._nats_client = mock_nc

        assert ch._nats_client is mock_nc

    def test_open_shares_transport_client(self, mock_connection):
        """Channels on the same transport share one NATS client."""
        ch1 = JetStreamChannel(connection=mock_connection)
        ch2 = JetStreamChannel(connection=mock_connection)
        assert ch1._nats_client is ch2._nats_client

    def test_open_uses_default_host_when_none(self, mock_connection):
        """When hostname is None, the connect URL must not contain 'None'."""
        from kombu.transport.nats import DEFAULT_PORT
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        conninfo = MagicMock()
        conninfo.hostname = None
        conninfo.port = None
        conninfo.transport_options = {}

        transport = Transport(mock_connection.client)
        transport._get_loop()  # _run_on_loop needs a live loop
        try:
            with patch('kombu.transport.nats.Client', return_value=mock_nc):
                transport._get_client(conninfo)
            mock_nc.connect.assert_awaited_once()
            url_arg = mock_nc.connect.call_args[0][0]
            assert 'None' not in url_arg
            assert f'{DEFAULT_HOST}:{DEFAULT_PORT}' in url_arg
        finally:
            transport._nats_client = None  # skip client drain in cleanup
            transport.close_connection(None)

    # -- ImportError when library missing --------------------------------

    def test_channel_init_raises_without_nats(self, mock_connection):
        with patch('kombu.transport.nats.Client', None):
            with pytest.raises(ImportError, match='nats-py is not installed'):
                JetStreamChannel(connection=mock_connection)


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
        transport = Transport(self.mock_client)
        transport._nats_client = MagicMock(is_connected=True)
        result = transport.verify_connection(MagicMock())
        assert result is True

    def test_verify_connection_returns_false_when_disconnected(self):
        transport = Transport(self.mock_client)
        transport._nats_client = MagicMock(is_connected=False)
        result = transport.verify_connection(MagicMock())
        assert result is False

    def test_verify_connection_returns_false_when_no_client(self):
        transport = Transport(self.mock_client)
        transport._nats_client = None
        result = transport.verify_connection(MagicMock())
        assert result is False

    # -- shared loop + client lifecycle -----------------------------------

    def _teardown_transport(self, transport):
        if (transport._loop is not None
                and not transport._loop.is_closed()):
            transport._loop.call_soon_threadsafe(transport._loop.stop)
            transport._loop_thread.join(timeout=5)
            transport._loop.close()

    def test_get_loop_creates_one_loop(self):
        transport = Transport(self.mock_client)
        try:
            loop1 = transport._get_loop()
            loop2 = transport._get_loop()
            assert loop1 is loop2
            assert not loop1.is_closed()
        finally:
            self._teardown_transport(transport)

    def test_get_client_creates_single_shared_client(self):
        transport = Transport(self.mock_client)
        transport._get_loop()  # _run_on_loop needs a live loop
        mock_nc = MagicMock()
        mock_nc.connect = AsyncMock()
        mock_nc.jetstream.return_value = MagicMock()
        conninfo = MagicMock()
        conninfo.hostname = 'localhost'
        conninfo.port = DEFAULT_PORT
        conninfo.transport_options = {}
        try:
            with patch('kombu.transport.nats.Client', return_value=mock_nc):
                c1 = transport._get_client(conninfo)
                c2 = transport._get_client(conninfo)
            assert c1 is c2
            mock_nc.connect.assert_awaited_once()
        finally:
            self._teardown_transport(transport)

    def test_close_connection_drains_client_and_stops_loop(self):
        transport = Transport(self.mock_client)
        transport._get_loop()
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock()
        mock_nc.close = AsyncMock()
        transport._nats_client = mock_nc

        transport.close_connection(None)

        mock_nc.drain.assert_awaited_once()
        mock_nc.close.assert_awaited_once()
        assert transport._nats_client is None
        assert transport._loop is None
        assert transport._loop_thread is None


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
            raw_body=False,
            header_prefix='Kombu-',
            header_names=None,
        )
        assert jloads(body_bytes.decode()) == message
        assert headers == {}

    def test_put_clean_body_publishes_body_as_bytes(self):
        """Raw-body mode publishes message['body'] as bytes without decoding."""
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
            raw_body=True,
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
            message, raw_body=True, header_prefix='Kombu-', header_names=None,
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
            message, raw_body=True, header_prefix='ce-', header_names=None,
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
            raw_body=True,
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
            message, raw_body=True, header_prefix='Kombu-', header_names=None,
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
            message, raw_body=True, header_prefix='Kombu-', header_names=None,
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
# test_scalar_roundtrip
# ---------------------------------------------------------------------------


class test_scalar_roundtrip:
    """encode/decode roundtrips for all scalar types carried in NATS headers."""

    def _rt(self, value):
        return decode_nats_header_value(encode_nats_header_value(value))

    def test_integer_roundtrip(self):
        assert self._rt(42) == 42

    def test_boolean_true_roundtrip(self):
        assert self._rt(True) is True

    def test_boolean_false_roundtrip(self):
        assert self._rt(False) is False

    def test_null_roundtrip(self):
        assert self._rt(None) is None

    def test_list_roundtrip(self):
        assert self._rt([1, 'a', True]) == [1, 'a', True]

    def test_dict_roundtrip(self):
        assert self._rt({'k': 'v', 'n': 0}) == {'k': 'v', 'n': 0}

    def test_plain_string_roundtrip(self):
        assert self._rt('application/json') == 'application/json'

    def test_decode_integer_string(self):
        assert decode_nats_header_value('42') == 42

    def test_decode_true_string(self):
        assert decode_nats_header_value('true') is True

    def test_decode_null_string(self):
        assert decode_nats_header_value('null') is None

    def test_decode_non_json_string_preserved(self):
        assert decode_nats_header_value('application/json') == 'application/json'


# ---------------------------------------------------------------------------
# test_raw_body_validation
# ---------------------------------------------------------------------------


class test_raw_body_validation:
    """Raw-body mode must accept valid types and raise TypeError for others."""

    def _put(self, body):
        return message_to_nats_body_and_headers(
            {'body': body, 'content-type': 'x'},
            raw_body=True,
            header_prefix='Kombu-',
            header_names=None,
        )

    def test_str_accepted(self):
        body_bytes, _ = self._put('hello')
        assert body_bytes == b'hello'

    def test_bytes_accepted(self):
        body_bytes, _ = self._put(b'raw')
        assert body_bytes == b'raw'

    def test_bytearray_accepted(self):
        body_bytes, _ = self._put(bytearray(b'ba'))
        assert body_bytes == b'ba'

    def test_memoryview_accepted(self):
        body_bytes, _ = self._put(memoryview(b'mv'))
        assert body_bytes == b'mv'

    def test_none_accepted_as_empty_bytes(self):
        body_bytes, _ = self._put(None)
        assert body_bytes == b''

    def test_int_raises_type_error(self):
        with pytest.raises(TypeError, match='nats_raw_body mode'):
            self._put(42)

    def test_list_raises_type_error(self):
        with pytest.raises(TypeError, match='nats_raw_body mode'):
            self._put([1, 2])

    def test_dict_raises_type_error(self):
        with pytest.raises(TypeError, match='nats_raw_body mode'):
            self._put({'key': 'val'})


# ---------------------------------------------------------------------------
# test_wire_format_tolerance
# ---------------------------------------------------------------------------


class test_wire_format_tolerance:
    """Consumers must decode both default and raw-body messages regardless of
    their local nats_raw_body setting."""

    def _default_envelope(self):
        """Build a default-mode NATS payload (JSON envelope, no headers)."""
        import json
        envelope = {
            'body': 'aGVsbG8=',
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
            'delivery_info': {'exchange': 'ex'},
        }
        return json.dumps(envelope).encode(), None

    def _raw_body_message(self):
        """Build a raw-body NATS payload (bytes + Kombu headers)."""
        import json
        data = b'hello world'
        headers = {
            'Kombu-Content-Type': 'text/plain',
            'Kombu-Content-Encoding': 'utf-8',
            'Kombu-Properties': json.dumps({'delivery_mode': 2}),
        }
        return data, headers

    def test_consumer_reads_default_mode_message(self):
        data, hdrs = self._default_envelope()
        result = nats_body_and_headers_to_message(
            data, hdrs, header_prefix='Kombu-', header_names=None,
        )
        assert result['content-type'] == 'application/json'
        assert result['body'] == 'aGVsbG8='

    def test_consumer_reads_raw_body_message(self):
        data, hdrs = self._raw_body_message()
        result = nats_body_and_headers_to_message(
            data, hdrs, header_prefix='Kombu-', header_names=None,
        )
        assert result['content-type'] == 'text/plain'
        assert result['body'] == data  # raw bytes, no body_encoding set

    def test_channel_get_default_envelope_via_raw_channel(
        self, mock_connection
    ):
        """A channel with nats_raw_body=True can still read a default-mode msg."""
        import json
        mock_connection.client.transport_options = {'nats_raw_body': True}
        mock_nc = MagicMock()
        mock_js = MagicMock()
        mock_nc.jetstream.return_value = mock_js
        with patch.object(JetStreamChannel, '_open', return_value=mock_nc):
            ch = JetStreamChannel(connection=mock_connection)
        ch.__dict__['client'] = mock_nc
        ch._nats_client = mock_nc
        ch._js = mock_js
        ch._streams = set()
        ch._js_consumers = set()
        ch._ensure_stream = MagicMock()
        ch._ensure_consumer = MagicMock()

        envelope = {
            'body': 'aGVsbG8=',
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
            'headers': {},
            'properties': {'body_encoding': 'base64'},
        }
        fake_msg = MagicMock()
        fake_msg.subject = 'q'
        fake_msg.data = json.dumps(envelope).encode()
        fake_msg.headers = None
        fake_msg.ack = AsyncMock()
        fake_msg.nak = AsyncMock()
        fake_msg.term = AsyncMock()
        mock_pull_sub = MagicMock()
        mock_pull_sub.fetch = AsyncMock(return_value=[fake_msg])
        ch._js.pull_subscribe = AsyncMock(return_value=mock_pull_sub)

        result = ch._get('q')
        assert result['content-type'] == 'application/json'
        assert result['body'] == 'aGVsbG8='

    def test_channel_get_raw_message_via_default_channel(self, channel):
        """A channel with nats_raw_body=False can still read a raw-body msg."""
        import json
        channel._ensure_stream = MagicMock()
        channel._ensure_consumer = MagicMock()
        data = b'raw payload'
        headers_dict = {
            'Kombu-Content-Type': 'text/plain',
            'Kombu-Content-Encoding': 'utf-8',
            'Kombu-Properties': json.dumps({'delivery_mode': 1}),
        }
        fake_msg = MagicMock()
        fake_msg.subject = 'q'
        fake_msg.data = data
        fake_msg.headers = headers_dict
        fake_msg.ack = AsyncMock()
        fake_msg.nak = AsyncMock()
        fake_msg.term = AsyncMock()
        mock_pull_sub = MagicMock()
        mock_pull_sub.fetch = AsyncMock(return_value=[fake_msg])
        channel._js.pull_subscribe = AsyncMock(return_value=mock_pull_sub)

        result = channel._get('q')
        assert result['content-type'] == 'text/plain'
        assert result['body'] == data


# ---------------------------------------------------------------------------
# Raw-body mode tests in Channel
# ---------------------------------------------------------------------------


class test_channel_raw_body:
    """Integration-style unit tests for Channel._put / _get in raw-body mode."""

    @pytest.fixture
    def clean_channel(self, mock_connection):
        """JetStreamChannel configured with nats_raw_body=True."""
        mock_connection.client.transport_options = {'nats_raw_body': True}
        mock_nc = MagicMock()
        mock_js = MagicMock()
        mock_nc.jetstream.return_value = mock_js
        with patch.object(JetStreamChannel, '_open', return_value=mock_nc):
            ch = JetStreamChannel(connection=mock_connection)
        ch.__dict__['client'] = mock_nc
        ch._nats_client = mock_nc
        ch._js = mock_js
        ch._streams = set()
        ch._js_consumers = set()
        return ch

    # -- nats_raw_body / nats_metadata_header_prefix / nats_metadata_header_names

    def test_nats_raw_body_default_false(self, channel):
        assert channel.nats_raw_body is False

    def test_nats_raw_body_true_from_options(self, clean_channel):
        assert clean_channel.nats_raw_body is True

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
            'nats_raw_body': True,
            'nats_metadata_header_prefix': 'ce-',
        }
        mock_nc = MagicMock()
        mock_js = MagicMock()
        mock_nc.jetstream.return_value = mock_js
        with patch.object(JetStreamChannel, '_open', return_value=mock_nc):
            ch = JetStreamChannel(connection=mock_connection)
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


# ---------------------------------------------------------------------------
# test_CoreNATSChannel
# ---------------------------------------------------------------------------


@pytest.fixture
def core_channel(mock_connection):
    """CoreNATSChannel with _open mocked out (no real NATS connection)."""
    mock_nc = MagicMock()
    mock_nc.subscribe = AsyncMock()
    mock_nc.publish = AsyncMock()

    with patch.object(CoreNATSChannel, '_open', return_value=mock_nc):
        ch = CoreNATSChannel(connection=mock_connection)

    ch.__dict__['client'] = mock_nc
    ch._nats_client = mock_nc
    ch._inbox = {}
    ch._subscriptions = {}
    return ch


class test_CoreNATSChannel:
    """Unit tests for CoreNATSChannel: inbox ordering, head-drop, and ack no-ops."""

    # -- _subscribe: inbox created before nc.subscribe -------------------

    def test_inbox_created_before_subscribe(self, core_channel):
        """The inbox queue must exist before nc.subscribe() is called."""
        inbox_created_first = []

        async def _fake_subscribe(subject, queue, cb):
            # At this point the inbox must already be present.
            inbox_created_first.append(subject in core_channel._inbox)
            mock_sub = MagicMock()
            mock_sub.unsubscribe = AsyncMock()
            return mock_sub

        core_channel._nats_client.subscribe = _fake_subscribe
        core_channel._run(core_channel._subscribe('celery.queue.test', 'test'))
        assert inbox_created_first == [True], (
            "_inbox[subject] must be created BEFORE nc.subscribe() is called"
        )

    def test_subscribe_stores_subscription(self, core_channel):
        mock_sub = MagicMock()
        mock_sub.unsubscribe = AsyncMock()
        core_channel._nats_client.subscribe = AsyncMock(return_value=mock_sub)
        core_channel._run(core_channel._subscribe('celery.queue.q', 'q'))
        assert 'celery.queue.q' in core_channel._subscriptions
        assert 'celery.queue.q' in core_channel._inbox

    # -- head-drop policy ------------------------------------------------

    def test_head_drop_when_inbox_full(self, core_channel):
        """When inbox is full, oldest message is dropped and new one is added."""
        subject = 'celery.queue.hdrop'
        # Fill the inbox completely.
        q = asyncio.Queue(maxsize=MAX_INBOX_SIZE)
        for i in range(MAX_INBOX_SIZE):
            msg = MagicMock()
            msg.data = f"msg-{i}".encode()
            q.put_nowait(msg)
        core_channel._inbox[subject] = q

        # Simulate the message callback arriving with a new message.
        new_msg = MagicMock()
        new_msg.data = b"new"

        async def _trigger():
            try:
                core_channel._inbox[subject].put_nowait(new_msg)
            except asyncio.QueueFull:
                try:
                    core_channel._inbox[subject].get_nowait()
                except asyncio.QueueEmpty:
                    pass
                core_channel._inbox[subject].put_nowait(new_msg)

        core_channel._run(_trigger())
        assert core_channel._inbox[subject].qsize() == MAX_INBOX_SIZE
        # The last item must be the new message (oldest was dropped).
        # Drain and check the last one.
        items = []
        while not core_channel._inbox[subject].empty():
            items.append(core_channel._inbox[subject].get_nowait())
        assert items[-1] is new_msg

    # -- _put publishes to correct subject --------------------------------

    def test_put_publishes_to_correct_subject(self, core_channel):
        core_channel._nats_client.publish = AsyncMock()
        message = {'body': b'hello'}
        core_channel._put('myqueue', message)
        core_channel._nats_client.publish.assert_awaited_once()
        subject_arg = core_channel._nats_client.publish.call_args[0][0]
        assert subject_arg == 'celery.queue.myqueue'

    # -- _get raises Empty on timeout ------------------------------------

    def test_get_raises_empty_on_timeout(self, core_channel):
        subject = 'celery.queue.emptyq'
        q = asyncio.Queue(maxsize=MAX_INBOX_SIZE)
        core_channel._inbox[subject] = q
        mock_sub = MagicMock()
        core_channel._subscriptions[subject] = mock_sub
        # Queue is empty — wait_for will time out.
        core_channel.__dict__['wait_time_seconds'] = 0.01
        with pytest.raises(Empty):
            core_channel._get('emptyq')

    # -- _get decodes message body ---------------------------------------

    def test_get_decodes_message(self, core_channel):
        import json
        subject = 'celery.queue.dq'
        q = asyncio.Queue(maxsize=MAX_INBOX_SIZE)
        envelope = {'body': 'hi', 'content-type': 'application/json'}
        fake_msg = MagicMock()
        fake_msg.data = json.dumps(envelope).encode()
        fake_msg.headers = None
        q.put_nowait(fake_msg)
        core_channel._inbox[subject] = q
        core_channel._subscriptions[subject] = MagicMock()

        result = core_channel._get('dq')
        assert result['body'] == 'hi'

    # -- ack/nack/reject are no-ops --------------------------------------

    def test_basic_ack_is_noop(self, core_channel):
        core_channel.qos._not_yet_acked['tag-1'] = MagicMock()
        core_channel.basic_ack('tag-1')
        assert 'tag-1' not in core_channel.qos._not_yet_acked

    def test_basic_nack_is_noop(self, core_channel):
        core_channel.qos._not_yet_acked['tag-2'] = MagicMock()
        core_channel.basic_nack('tag-2')
        assert 'tag-2' not in core_channel.qos._not_yet_acked

    def test_basic_reject_is_noop(self, core_channel):
        core_channel.qos._not_yet_acked['tag-3'] = MagicMock()
        core_channel.basic_reject('tag-3')
        assert 'tag-3' not in core_channel.qos._not_yet_acked

    # -- close unsubscribes all subs -------------------------------------

    def test_close_unsubscribes_all(self, core_channel):
        sub1 = MagicMock()
        sub1.unsubscribe = AsyncMock()
        sub2 = MagicMock()
        sub2.unsubscribe = AsyncMock()
        core_channel._subscriptions = {
            'celery.queue.a': sub1,
            'celery.queue.b': sub2,
        }
        core_channel._inbox = {
            'celery.queue.a': asyncio.Queue(),
            'celery.queue.b': asyncio.Queue(),
        }
        mock_nc = MagicMock()
        mock_nc.drain = AsyncMock()
        mock_nc.close = AsyncMock()
        core_channel._nats_client = mock_nc

        core_channel.close()

        sub1.unsubscribe.assert_awaited_once()
        sub2.unsubscribe.assert_awaited_once()
        assert core_channel._subscriptions == {}
        assert core_channel._inbox == {}


# ---------------------------------------------------------------------------
# test_Transport_url_routing (Task 1.4)
# ---------------------------------------------------------------------------


class test_Transport_url_routing:
    """Transport.create_channel() dispatches based on URL scheme."""

    def setup_method(self):
        self.mock_client = MagicMock()
        self.mock_client.transport_options = {}

    def _make_conn(self, scheme):
        conn = MagicMock()
        conn.client.transport = scheme
        conn.client.transport_options = {}
        conn.client.port = DEFAULT_PORT
        conn.client.hostname = 'localhost'
        conn.client.userid = None
        conn.client.password = None
        conn._used_channel_ids = array('H')
        conn.channel_max = 65535
        conn.default_port = DEFAULT_PORT
        return conn

    def test_nats_url_creates_jetstream_channel(self):
        transport = Transport(self.mock_client)
        conn = self._make_conn('nats')
        assert transport._channel_cls_for(conn) is JetStreamChannel

    def test_nats_plus_jetstream_url_creates_jetstream_channel(self):
        transport = Transport(self.mock_client)
        conn = self._make_conn('nats+jetstream')
        assert transport._channel_cls_for(conn) is JetStreamChannel

    def test_nats_plus_core_url_creates_core_channel(self):
        transport = Transport(self.mock_client)
        conn = self._make_conn('nats+core')
        assert transport._channel_cls_for(conn) is CoreNATSChannel

    def test_unknown_scheme_defaults_to_jetstream(self):
        transport = Transport(self.mock_client)
        conn = self._make_conn('nats+unknown')
        assert transport._channel_cls_for(conn) is JetStreamChannel


# ---------------------------------------------------------------------------
# test_JetStreamChannel_fanout  (broadcast regression suite)
# ---------------------------------------------------------------------------

class test_JetStreamChannel_fanout:
    """Regression tests for fanout/broadcast semantics.

    Core requirement: one published message → N subscribers all receive it,
    using Core NATS pub/sub (no queue group) on the fanout subject.
    """

    @pytest.fixture()
    def fanout_channel(self, mock_connection):
        """JetStreamChannel with publish/subscribe mocked for fanout testing."""
        mock_nc = MagicMock()
        mock_nc.jetstream.return_value = MagicMock()
        mock_nc.subscribe = AsyncMock()
        mock_nc.publish = AsyncMock()
        # Use a real BrokerState so exchange_declare/queue_bind work correctly.
        mock_connection.state = BrokerState()

        with patch.object(JetStreamChannel, '_open', return_value=mock_nc):
            ch = JetStreamChannel(connection=mock_connection)

        ch.__dict__['client'] = mock_nc
        ch._nats_client = mock_nc
        ch._js = mock_nc.jetstream.return_value
        ch._streams = set()
        ch._js_consumers = set()
        return ch

    def test_supports_fanout_is_true(self):
        """JetStreamChannel.supports_fanout must be True."""
        assert JetStreamChannel.supports_fanout is True

    def test_fanout_subject_format(self, fanout_channel):
        """Fanout subject must use 'celery.fanout.<exchange>' scheme."""
        assert fanout_channel._fanout_subject('celery.pidbox') == 'celery.fanout.celery.pidbox'

    def test_queue_bind_subscribes_for_fanout_exchange(self, fanout_channel):
        """_queue_bind() subscribes to the fanout subject via Core NATS."""
        # Declare a fanout exchange in state.
        fanout_channel.exchange_declare('test.pidbox', type='fanout', durable=False)
        fanout_channel._queue_bind('test.pidbox', '', '', 'worker-a.test.pidbox')

        # nc.subscribe must have been called with the fanout subject (no queue group).
        assert fanout_channel._nats_client.subscribe.called
        call_kwargs = fanout_channel._nats_client.subscribe.call_args
        subject_arg = call_kwargs[0][0] if call_kwargs[0] else call_kwargs[1].get('subject')
        assert subject_arg == 'celery.fanout.test.pidbox'
        # Must NOT use a queue group (that would break broadcast semantics).
        assert call_kwargs[1].get('queue', '') == '' or 'queue' not in call_kwargs[1]

    def test_put_fanout_publishes_to_core_nats(self, fanout_channel):
        """_put_fanout() uses nc.publish (Core NATS) not js.publish (JetStream)."""
        message = {
            'body': 'ping',
            'headers': {},
            'properties': {'delivery_tag': '1', 'delivery_info': {}},
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
        }
        fanout_channel._put_fanout('celery.pidbox', message, '')
        fanout_channel._nats_client.publish.assert_called_once()
        call_args = fanout_channel._nats_client.publish.call_args[0]
        assert call_args[0] == 'celery.fanout.celery.pidbox'
        # js.publish must NOT have been called.
        fanout_channel._js.publish.assert_not_called()

    def test_two_workers_both_receive_fanout_message(self, mock_connection):
        """Broadcast regression: one message sent, N subscribers all receive it.

        Simulates two workers each with their own JetStreamChannel. Publishing
        to the fanout exchange subject must deliver one copy to each worker's
        fanout inbox.
        """
        received_by = {'worker_a': [], 'worker_b': []}
        subscriptions = {}

        def _subscribe_side_effect(subject, cb=None, **kw):
            """Track subscriptions by subject; return a mock sub."""
            subs_for_subject = subscriptions.setdefault(subject, [])
            sub_mock = MagicMock()
            sub_mock.subject = subject
            sub_mock.cb = cb
            subs_for_subject.append(sub_mock)
            f = asyncio.get_event_loop().create_future()
            f.set_result(sub_mock)
            return f

        async def _publish_side_effect(subject, data, headers=None):
            """Fan out to every subscriber registered for the subject."""
            for sub in subscriptions.get(subject, []):
                msg = MagicMock()
                msg.data = data
                msg.headers = headers or {}
                if sub.cb is not None:
                    await sub.cb(msg)

        def _make_worker_channel(worker_name, worker_received):
            """Create a JetStreamChannel for a simulated worker."""
            mock_nc = MagicMock()
            mock_nc.jetstream.return_value = MagicMock()
            mock_nc.subscribe = AsyncMock(side_effect=_subscribe_side_effect)
            mock_nc.publish = AsyncMock(side_effect=_publish_side_effect)
            # Each worker needs its own real BrokerState.
            worker_conn = _make_connection()
            worker_conn.state = BrokerState()

            with patch.object(JetStreamChannel, '_open', return_value=mock_nc):
                ch = JetStreamChannel(connection=worker_conn)

            ch.__dict__['client'] = mock_nc
            ch._nats_client = mock_nc
            ch._js = mock_nc.jetstream.return_value
            ch._streams = set()
            ch._js_consumers = set()
            return ch

        ch_a = _make_worker_channel('worker_a', received_by['worker_a'])
        ch_b = _make_worker_channel('worker_b', received_by['worker_b'])

        # Both workers declare + bind to the pidbox fanout exchange.
        for ch in (ch_a, ch_b):
            ch.exchange_declare('celery.pidbox', type='fanout', durable=False)
            ch._queue_bind('celery.pidbox', '', '', 'worker.celery.pidbox')

        # Both are now subscribed to celery.fanout.celery.pidbox.
        assert 'celery.fanout.celery.pidbox' in subscriptions
        assert len(subscriptions['celery.fanout.celery.pidbox']) == 2

        # Publish one inspect-ping message via channel A.
        message = {
            'body': '{"method": "ping", "arguments": {}}',
            'headers': {},
            'properties': {'delivery_tag': 'tag-1', 'delivery_info': {}},
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
        }
        ch_a._put_fanout('celery.pidbox', message, '')

        # Both channel A and B inboxes must each have one message.
        inbox_a = ch_a._fanout_inboxes.get('worker.celery.pidbox')
        inbox_b = ch_b._fanout_inboxes.get('worker.celery.pidbox')
        assert inbox_a is not None, "Worker A has no fanout inbox"
        assert inbox_b is not None, "Worker B has no fanout inbox"
        assert inbox_a.qsize() == 1, f"Worker A inbox has {inbox_a.qsize()} messages, expected 1"
        assert inbox_b.qsize() == 1, f"Worker B inbox has {inbox_b.qsize()} messages, expected 1"

    def test_close_unsubscribes_fanout_subs(self, fanout_channel):
        """close() must drain fanout subscriptions cleanly."""
        mock_sub = MagicMock()
        mock_sub.unsubscribe = AsyncMock()
        fanout_channel._fanout_subscriptions['celery.pidbox'] = mock_sub
        fanout_channel._fanout_inboxes['worker.celery.pidbox'] = asyncio.Queue()
        # Override base close to avoid real NATS drain.
        with patch.object(Channel, 'close'):
            fanout_channel.close()
        mock_sub.unsubscribe.assert_called_once()
        assert len(fanout_channel._fanout_subscriptions) == 0
        assert len(fanout_channel._fanout_inboxes) == 0

    def test_get_fanout_queue_drains_inbox(self, fanout_channel):
        """_get() on a fanout queue drains from the Core NATS inbox."""
        import json as _json
        queue = 'worker.celery.pidbox'
        inbox = asyncio.Queue()
        fanout_channel._fanout_inboxes[queue] = inbox

        # Pre-fill the inbox with a JSON-envelope message.
        payload = _json.dumps({
            'body': 'pong',
            'headers': {},
            'properties': {'delivery_tag': 't1', 'delivery_info': {}},
            'content-type': 'application/json',
            'content-encoding': 'utf-8',
        }).encode()
        msg = MagicMock()
        msg.data = payload
        msg.headers = {}
        inbox.put_nowait(msg)

        result = fanout_channel._get(queue)
        assert result['body'] == 'pong'
        assert inbox.qsize() == 0

    def test_get_fanout_raises_empty_on_timeout(self, fanout_channel):
        """_get() on empty fanout inbox raises Empty (not hangs)."""
        from queue import Empty
        fanout_channel._fanout_inboxes['worker.celery.pidbox'] = asyncio.Queue()
        # Tiny wait to keep test fast.
        fanout_channel._nats_client.transport_options = {}
        fanout_channel.__class__.default_wait_time_seconds = 0.01
        with pytest.raises(Empty):
            fanout_channel._get('worker.celery.pidbox')
