"""NATS JetStream transport module for Kombu.

NATS JetStream transport using nats-py library.

**References**

- https://github.com/nats-io/nats.py
- https://docs.nats.io/nats-concepts/jetstream

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: No
* Supports TTL: Yes

Connection String
=================
Connection string has the following format:

.. code-block::

    nats://[USER:PASSWORD@]NATS_ADDRESS[:PORT]

Transport Options
=================
* ``connection_wait_time_seconds`` - Time in seconds to wait for connection
  to succeed. Default ``5``
* ``wait_time_seconds`` - Time in seconds to wait to receive messages.
  Default ``5``
* ``stream_config`` - Stream configuration. Must be a dict whose key-value pairs
  correspond with attributes in the NATS JetStream stream configuration.
* ``consumer_config`` - Consumer configuration. Must be a dict whose key-value pairs
  correspond with attributes in the NATS JetStream consumer configuration.
* ``stream_name_prefix`` - Prefix used when naming JetStream streams. Default ``"STREAM_"``.
  For example, setting ``stream_name_prefix`` to ``"myapp_"`` causes queue
  ``tasks`` to use a stream named ``myapp_tasks``.
* ``consumer_name_prefix`` - Prefix used when naming JetStream consumers. Default ``"CONSUMER_"``.
  For example, setting ``consumer_name_prefix`` to ``"myapp_"`` causes queue
  ``tasks`` to use a consumer named ``myapp_tasks``.
* ``nats_raw_body`` - If ``True``, publish the application payload directly as
  the NATS message body instead of wrapping it in a Kombu JSON envelope.
  Kombu metadata (content-type, properties, etc.) is carried in NATS headers
  with the configured prefix.  Default ``False`` (backward-compatible
  envelope-in-body behaviour).
* ``nats_metadata_header_prefix`` - Prefix applied to all Kombu metadata header
  names when ``nats_raw_body=True``.  Default ``"Kombu-"``.  Must not start
  with ``"Nats-"`` as that namespace is reserved for NATS/JetStream built-in
  headers.
* ``nats_metadata_header_names`` - Optional :class:`dict` that overrides
  individual Kombu metadata header *name suffixes* (without the prefix).
  Recognized keys: ``"content_type"``, ``"content_encoding"``,
  ``"headers"``, ``"properties"``, ``"delivery_info"``.  Any key not
  specified falls back to the default (``Content-Type``, ``Content-Encoding``,
  ``Headers``, ``Properties``, ``Delivery-Info``).

Per-message TTL is supported via the ``Nats-TTL`` JetStream header. When a
message is published with a Kombu ``expiration`` property (in milliseconds),
the transport sets the ``Nats-TTL`` header so NATS will expire the message
after that duration.  This header is applied in both default and raw-body mode.
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
from queue import Empty

from kombu.exceptions import NotBoundError, OperationalError
from kombu.transport import virtual
from kombu.utils import cached_property
from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import dumps, loads


class NATSError(OperationalError):
    """NATS transport operational error — a recoverable transport failure.

    Subclasses :class:`~kombu.exceptions.OperationalError` (which in turn
    subclasses :class:`~kombu.exceptions.KombuError`), so Celery/robust retry
    machinery treats it as retryable and operators can catch NATS-specific
    failures distinctly.
    """


try:
    import nats.aio.client
    import nats.aio.errors
    import nats.errors
    import nats.js.errors
    from nats.aio.client import Client
    from nats.js.api import (AckPolicy, ConsumerConfig, DeliverPolicy,
                             DiscardPolicy, RetentionPolicy, StorageType,
                             StreamConfig)
    from nats.js.client import JetStreamContext  # noqa: F401

    NATS_CONNECTION_ERRORS = (
        nats.aio.errors.ErrConnectionClosed,
        nats.aio.errors.ErrTimeout,
        nats.aio.errors.ErrNoServers,
    )
    NATS_CHANNEL_ERRORS = (nats.js.errors.NotFoundError,)

except ImportError:
    Client = None
    NATS_CONNECTION_ERRORS = NATS_CHANNEL_ERRORS = ()

from kombu.log import get_logger  # noqa: E402

logger = get_logger(__name__)

DEFAULT_PORT = 4222
DEFAULT_HOST = "localhost"

_JS_NAME_INVALID_CHARS = frozenset(".*>/\\")
_JS_NAME_HASH_LEN = 12
_JS_NAME_MAX_LENGTH = 240


# ---------------------------------------------------------------------------
# Clean-body mode: module-level constants and helpers
# ---------------------------------------------------------------------------

#: Allowed body types for raw-body mode.  Only these types can be safely
#: published as NATS message data.
_RAW_BODY_TYPES = (str, bytes, bytearray, memoryview, type(None))

#: Default Kombu metadata header name suffixes (used with the configured
#: prefix, e.g. ``"Kombu-"`` → ``"Kombu-Content-Type"``).
DEFAULT_METADATA_HEADER_NAMES: dict[str, str] = {
    "content_type": "Content-Type",
    "content_encoding": "Content-Encoding",
    "headers": "Headers",
    "properties": "Properties",
    "delivery_info": "Delivery-Info",
}


def encode_nats_header_value(value) -> str:
    """Encode a Python value as a NATS header string.

    Plain strings are returned unchanged.  All other types (dicts, lists,
    integers, …) are JSON-serialised so they can round-trip through NATS
    headers.  ``None`` maps to an empty string.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return dumps(value)


def decode_nats_header_value(raw: str):
    """Decode a NATS header string back to a Python value.

    Attempts JSON parsing for every non-empty value so that integers,
    booleans, ``null``, dicts, lists, and plain strings all round-trip
    correctly.  Falls back to returning the raw string if JSON parsing
    fails.  An empty string returns ``None``.
    """
    if not raw:
        return None
    stripped = raw.strip()
    try:
        return loads(stripped)
    except Exception:
        return raw


def _is_valid_js_resource_name(name: str) -> bool:
    """Return ``True`` when *name* is safe for JetStream API resource paths."""
    return bool(name) and name.isprintable() and not any(
        ch.isspace() or ch in _JS_NAME_INVALID_CHARS for ch in name
    )


def normalize_js_resource_name(name: str) -> str:
    """Normalize *name* for JetStream stream/consumer identifiers.

    Queue subjects may legally contain characters like ``.`` that are unsafe in
    JetStream API resource paths. Keep already-valid names unchanged, and
    derive a readable, collision-resistant fallback only when normalization is
    required.
    """
    if _is_valid_js_resource_name(name):
        return name

    digest = hashlib.sha1(name.encode("utf-8")).hexdigest()[:_JS_NAME_HASH_LEN]
    stem = "".join(
        ch if ch.isprintable() and not ch.isspace() and ch not in _JS_NAME_INVALID_CHARS
        else "_"
        for ch in name
    ).strip("_") or "js"
    max_stem_len = _JS_NAME_MAX_LENGTH - len(digest) - 2
    if len(stem) > max_stem_len:
        stem = stem[:max_stem_len]
    return f"{stem}__{digest}"


def message_to_nats_body_and_headers(
    message: dict,
    *,
    raw_body: bool,
    header_prefix: str,
    header_names: dict | None,
) -> tuple[bytes, dict]:
    """Convert a Kombu message dict to *(body_bytes, metadata_headers)*.

    **Default mode** (``raw_body=False``):
        *body_bytes* is the full Kombu JSON envelope serialised to bytes;
        *metadata_headers* is an empty dict.

    **Raw-body mode** (``raw_body=True``):
        *body_bytes* is ``message['body']`` converted to bytes exactly as
        Kombu/the application provided it — no additional encoding or
        decoding is applied.  *metadata_headers* holds Kombu metadata under
        the configured header prefix.

        Only :data:`_RAW_BODY_TYPES` are accepted; any other type raises
        :exc:`TypeError`.
    """
    if not raw_body:
        return str_to_bytes(dumps(message)), {}

    # Merge user-supplied header name overrides with defaults.
    names = {**DEFAULT_METADATA_HEADER_NAMES, **(header_names or {})}

    # Publish the body exactly as provided by Kombu's serializer layer.
    # Serialization/encoding is entirely the caller's responsibility.
    body = message.get("body", b"")
    if not isinstance(body, _RAW_BODY_TYPES):
        raise TypeError(
            f"nats_raw_body mode: message body must be one of "
            f"{[t.__name__ for t in _RAW_BODY_TYPES if t is not type(None)] + ['None']}, "
            f"got {type(body).__name__!r}"
        )
    if body is None:
        body_bytes = b""
    elif isinstance(body, str):
        body_bytes = body.encode("utf-8")
    elif isinstance(body, memoryview):
        body_bytes = bytes(body)
    else:
        body_bytes = bytes(body)

    # Build the metadata header dict, skipping absent/empty values.
    headers: dict[str, str] = {}

    def _set(field_key: str, value) -> None:
        if value:
            headers[f"{header_prefix}{names[field_key]}"] = \
                encode_nats_header_value(value)

    _set("content_type", message.get("content-type"))
    _set("content_encoding", message.get("content-encoding"))
    _set("headers", message.get("headers") or None)
    _set("properties", message.get("properties") or None)
    _set("delivery_info", message.get("delivery_info") or None)

    return body_bytes, headers


def _has_kombu_metadata_headers(
    flat_headers: dict,
    header_prefix: str,
    header_names: dict | None,
) -> bool:
    """Return ``True`` if *flat_headers* contains the Kombu content-type key."""
    names = {**DEFAULT_METADATA_HEADER_NAMES, **(header_names or {})}
    ct_key = f"{header_prefix}{names['content_type']}"
    return ct_key in flat_headers


def nats_body_and_headers_to_message(
    data: bytes,
    msg_headers,
    *,
    header_prefix: str,
    header_names: dict | None,
) -> dict:
    """Reconstruct a Kombu message dict from NATS *data* and *msg_headers*.

    If the configured metadata headers are present the message is decoded as
    raw-body mode.  Otherwise the payload is assumed to be a default Kombu
    JSON envelope and is parsed directly.  Consumers can therefore read both
    formats regardless of their local ``nats_raw_body`` setting.
    """
    # Normalise msg.headers: nats-py may give None, str values, or list values.
    flat: dict[str, str] = {}
    if isinstance(msg_headers, dict):
        for k, v in msg_headers.items():
            flat[k] = v[0] if isinstance(v, list) else v

    if not _has_kombu_metadata_headers(flat, header_prefix, header_names):
        # Default path: payload is a full Kombu JSON envelope.
        return loads(data.decode())

    names = {**DEFAULT_METADATA_HEADER_NAMES, **(header_names or {})}
    ct_key = f"{header_prefix}{names['content_type']}"

    # Raw-body path: reconstruct a Kombu envelope from the metadata headers.
    content_type = flat.get(ct_key) or ""
    content_encoding = flat.get(
        f"{header_prefix}{names['content_encoding']}"
    ) or "utf-8"
    msg_hdrs = decode_nats_header_value(
        flat.get(f"{header_prefix}{names['headers']}", "")
    )
    properties = decode_nats_header_value(
        flat.get(f"{header_prefix}{names['properties']}", "")
    )
    delivery_info = decode_nats_header_value(
        flat.get(f"{header_prefix}{names['delivery_info']}", "")
    )

    props: dict = dict(properties) if isinstance(properties, dict) else {}

    # Restore the body to the same type as when it was published.
    # body_encoding is preserved via the Properties header, so Kombu's
    # deserialization pipeline can handle it correctly without any
    # re-encoding on the transport side.
    body_encoding = props.get("body_encoding", "")
    body_value: str | bytes = data.decode("utf-8") if body_encoding else data

    return {
        "body": body_value,
        "content-type": content_type,
        "content-encoding": content_encoding,
        "headers": msg_hdrs if isinstance(msg_hdrs, dict) else {},
        "properties": props,
        "delivery_info": delivery_info if isinstance(delivery_info, dict) else {},
    }


class Message(virtual.Message):
    """Message object."""

    def __init__(self, payload, channel=None, **kwargs):
        self.subject = payload["subject"]
        self.nats_ack = payload["ack"]
        self.nats_nak = payload["nak"]
        self.nats_term = payload["term"]
        super().__init__(payload, channel=channel, **kwargs)


class QoS(virtual.QoS):
    """Quality of Service guarantees."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._not_yet_acked = {}

    def can_consume(self):
        """Return true if the channel can be consumed from."""
        return not self.prefetch_count or len(self._not_yet_acked) < self.prefetch_count

    def can_consume_max_estimate(self):
        if self.prefetch_count:
            return max(0, self.prefetch_count - len(self._not_yet_acked))
        return 1

    def append(self, message, delivery_tag):
        self._not_yet_acked[delivery_tag] = message

    def get(self, delivery_tag):
        return self._not_yet_acked[delivery_tag]

    def ack(self, delivery_tag):
        if delivery_tag not in self._not_yet_acked:
            return
        message = self._not_yet_acked.pop(delivery_tag)
        self.channel.ack_msg(message)

    def reject(self, delivery_tag, requeue=False):
        """Reject a message by delivery tag."""
        if delivery_tag not in self._not_yet_acked:
            return
        message = self._not_yet_acked.pop(delivery_tag)
        if requeue:
            self.channel.nak_msg(message)
        else:
            self.channel.term_msg(message)

    def restore_unacked_once(self, stderr=None):
        pass


class Channel(virtual.Channel):
    """Base NATS channel: owns the asyncio event loop and NATS client lifecycle.

    Subclasses provide message delivery semantics:
    - :class:`JetStreamChannel` — JetStream durable streams (at-least-once)
    - :class:`CoreNATSChannel`  — Core NATS push-subscribe (at-most-once)
    """

    QoS = QoS

    default_wait_time_seconds = 5
    default_connection_wait_time_seconds = 5

    def __init__(self, connection, *args, transport=None, **kwargs):
        if Client is None:
            raise ImportError("nats-py is not installed")
        # The transport owns the shared NATS client + event loop.  In
        # production it is always passed by ``Transport.create_channel``;
        # the fallbacks cover direct construction (tests, Kombu internals).
        self.transport = transport or getattr(connection, 'transport', None)\
            or connection

        super().__init__(connection, *args, **kwargs)

        port = self.connection.client.port or self.connection.default_port
        host = self.connection.client.hostname or DEFAULT_HOST
        logger.debug("Host: %s Port: %s", host, port)

        # Borrow the shared event loop from the transport
        self._loop = self.transport._get_loop()
        self._nats_client = None
        self._js = None

        self.client

    def _run(self, coro):
        """Submit *coro* to this channel's background event loop and block until done.

        Using :func:`asyncio.run_coroutine_threadsafe` instead of
        ``run_until_complete`` means *_run* is safe to call from any thread,
        including callbacks that fire while the loop is already processing
        another coroutine (e.g. Celery ack callbacks during task retry).
        """
        if self._loop.is_closed():
            raise RuntimeError("NATS event loop is closed")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def _open(self):
        """Open a connection to NATS (shared across channels)."""
        self._nats_client = self.transport._get_client(
            self.conninfo, connect_timeout=self.connection_wait_time_seconds)
        return self._nats_client

    @cached_property
    def client(self):
        """Get the NATS client."""
        return self._open()

    @property
    def options(self):
        """Get the transport options."""
        if self.closed:
            raise NotBoundError("Channel is closed")
        return self.connection.client.transport_options

    @property
    def conninfo(self):
        """Get the connection info."""
        if self.closed:
            raise NotBoundError("Channel is closed")
        return self.connection.client

    @cached_property
    def wait_time_seconds(self):
        """Get the wait time in seconds."""
        return float(
            self.options.get("wait_time_seconds", self.default_wait_time_seconds)
        )

    @cached_property
    def connection_wait_time_seconds(self):
        """Get the connection wait time in seconds."""
        return float(
            self.options.get(
                "connection_wait_time_seconds",
                self.default_connection_wait_time_seconds,
            )
        )

    @property
    def nats_raw_body(self) -> bool:
        """If ``True``, publish the application payload directly as msg.data.

        Kombu envelope metadata is carried in NATS headers instead.
        Default ``False`` (backward-compatible envelope-in-body behaviour).
        """
        return bool(self.options.get('nats_raw_body', False))

    @property
    def nats_metadata_header_prefix(self) -> str:
        """Prefix applied to Kombu metadata header names in raw-body mode.

        Default ``"Kombu-"``.  Must not start with ``"Nats-"`` as that
        namespace is reserved for NATS/JetStream built-in headers.
        """
        prefix = self.options.get('nats_metadata_header_prefix', 'Kombu-')
        if prefix.lower().startswith('nats-'):
            logger.warning(
                "nats_metadata_header_prefix %r begins with 'Nats-', which "
                "is reserved for NATS/JetStream semantics.  Choose a "
                "different prefix to avoid conflicts with built-in headers.",
                prefix,
            )
        return prefix

    @property
    def nats_metadata_header_names(self):
        """Optional dict of per-field Kombu metadata header name overrides.

        Keys: ``"content_type"``, ``"content_encoding"``, ``"headers"``,
        ``"properties"``, ``"delivery_info"``.  Only specified keys are
        overridden; others fall back to :data:`DEFAULT_METADATA_HEADER_NAMES`.
        """
        return self.options.get('nats_metadata_header_names', None)

    def close(self):
        """Close the channel (local cleanup only).

        The NATS client and event loop are shared with other channels of
        this transport, so they are *not* torn down here.  The Transport
        drains/closes the client and stops the loop in
        ``Transport.close_connection()`` once every channel is closed.
        """
        self._nats_client = None
        self._js = None
        super().close()


class JetStreamChannel(Channel):
    """NATS JetStream Channel (at-least-once, durable streams)."""

    Message = Message

    #: Set to True so that FanoutExchange.deliver() and queue_bind() activate
    #: the broadcast path (Core NATS pub/sub — no queue group — one copy per
    #: subscriber).  JetStream work-queue semantics are kept for regular queues.
    supports_fanout = True

    default_stream_name_prefix = "STREAM_"
    default_consumer_name_prefix = "CONSUMER_"

    def __init__(self, *args, **kwargs):
        self._streams: set = set()
        self._js_consumers: set = set()
        # Cache of pull subscriptions keyed by "{stream}:{consumer}"
        self._subscriptions: dict[str, object] = {}
        # fanout: exchange_name -> asyncio.Queue (fanout inbox per queue)
        self._fanout_inboxes: dict[str, asyncio.Queue] = {}
        # fanout: exchange_name -> nats subscription
        self._fanout_subscriptions: dict[str, object] = {}
        # fanout: queue_name -> exchange_name
        self._fanout_queue_to_exchange: dict[str, str] = {}
        super().__init__(*args, **kwargs)

    @staticmethod
    def _fanout_subject(exchange: str) -> str:
        """Core NATS subject for fanout exchange — no queue group, all receive."""
        return f"celery.fanout.{exchange}"

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        """Register a fanout subscription on Core NATS for *exchange*/*queue*.

        Called by the virtual base when ``supports_fanout = True`` and a queue
        is bound to a fanout exchange.  Each worker subscribes independently to
        the fanout subject WITHOUT a queue group so that ALL workers receive
        every message (broadcast semantics).
        """
        if self.typeof(exchange).type != 'fanout':
            return
        if exchange not in self._fanout_subscriptions:
            self._run(self._subscribe_fanout(exchange, queue))

    async def _subscribe_fanout(self, exchange: str, queue: str) -> None:
        """Open a Core NATS subscription for the fanout exchange."""
        subject = self._fanout_subject(exchange)
        inbox: asyncio.Queue = asyncio.Queue(maxsize=MAX_INBOX_SIZE)
        self._fanout_inboxes[queue] = inbox
        self._fanout_queue_to_exchange[queue] = exchange

        async def _fanout_handler(msg) -> None:
            try:
                inbox.put_nowait(msg)
            except asyncio.QueueFull:
                # Head-drop: discard oldest, accept newest.
                try:
                    inbox.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    inbox.put_nowait(msg)
                except asyncio.QueueFull:
                    pass

        sub = await self._nats_client.subscribe(subject, cb=_fanout_handler)
        self._fanout_subscriptions[exchange] = sub

    def get_table(self, exchange):
        """Return the routing table for *exchange* (required for fanout support)."""
        try:
            return self.state.exchanges[exchange].get('table', [])
        except KeyError:
            return []

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Broadcast *message* to ALL workers bound to *exchange*.

        Uses Core NATS ``nc.publish`` (not JetStream) on the fanout subject so
        that every subscriber receives a copy.
        """
        body_bytes, meta_headers = message_to_nats_body_and_headers(
            message,
            raw_body=self.nats_raw_body,
            header_prefix=self.nats_metadata_header_prefix,
            header_names=self.nats_metadata_header_names,
        )
        headers: dict | None = dict(meta_headers) if meta_headers else None
        subject = self._fanout_subject(exchange)
        self._run(self._nats_client.publish(subject, body_bytes, headers=headers))

    def _open(self):
        """Open NATS connection and acquire a JetStream context."""
        client = super()._open()
        if self._js is None:
            self._js = client.jetstream()
        return client

    def _get_stream_name(self, queue):
        """Get the stream name for a queue."""
        prefix = self.options.get("stream_name_prefix", self.default_stream_name_prefix)
        return normalize_js_resource_name(f"{prefix}{queue}")

    def _get_consumer_name(self, queue):
        """Get the consumer name for a queue."""
        prefix = self.options.get("consumer_name_prefix", self.default_consumer_name_prefix)
        return normalize_js_resource_name(f"{prefix}{queue}")

    def _ensure_stream(self, queue):
        """Ensure a stream exists for the queue."""
        stream_name = self._get_stream_name(queue)
        if stream_name in self._streams:
            return

        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        # First try to get stream info with a shorter timeout
        try:
            self._run(
                asyncio.wait_for(
                    self._js.stream_info(stream_name),
                    timeout=1.0  # Use a shorter timeout for the check
                )
            )
            self._streams.add(stream_name)
            return
        except (nats.js.errors.NotFoundError, nats.errors.TimeoutError):
            # Stream doesn't exist or timed out, we'll create it
            pass

        # Create the stream with a longer timeout
        stream_config = StreamConfig(
            name=stream_name,
            subjects=[queue],
            retention=RetentionPolicy.WORK_QUEUE,
            max_consumers=-1,
            max_msgs_per_subject=-1,
            max_msgs=-1,
            max_bytes=-1,
            max_age=0,
            max_msg_size=-1,
            storage=StorageType.MEMORY,
            discard=DiscardPolicy.OLD,
            num_replicas=1,
            duplicate_window=120.0,  # 2 minutes in seconds
            allow_direct=True,  # for debugging with nats cli
        )

        # Update with user-provided config
        user_cfg = self.options.get("stream_config") or {}

        try:
            self._run(
                asyncio.wait_for(
                    self._js.add_stream(stream_config, **user_cfg),
                    timeout=5.0  # Use a longer timeout for creation
                )
            )
            self._streams.add(stream_name)
        except nats.errors.TimeoutError:
            # If we timeout creating the stream, check if it was actually created
            try:
                self._run(
                    asyncio.wait_for(
                        self._js.stream_info(stream_name),
                        timeout=1.0
                    )
                )
                self._streams.add(stream_name)
            except (nats.js.errors.NotFoundError, nats.errors.TimeoutError) as exc:
                raise NATSError(
                    f"Failed to create stream {stream_name}") from exc

    def _ensure_consumer(self, queue):
        """Ensure a consumer exists for the queue."""
        consumer_name = self._get_consumer_name(queue)
        if consumer_name in self._js_consumers:
            return

        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        name = self._get_stream_name(queue)

        consumer_config = ConsumerConfig(
            durable_name=consumer_name,
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=queue,
        )

        # Update with user-provided config
        user_cfg = self.options.get("consumer_config") or {}

        try:
            self._run(
                asyncio.wait_for(
                    self._js.add_consumer(name, consumer_config, **user_cfg),
                    timeout=5.0  # Use a longer timeout for consumer creation
                )
            )
            self._js_consumers.add(consumer_name)
        except nats.errors.TimeoutError:
            # If we timeout creating the consumer, check if it was actually created
            try:
                self._run(
                    asyncio.wait_for(
                        self._js.consumer_info(name, consumer_name),
                        timeout=1.0
                    )
                )
                self._js_consumers.add(consumer_name)
            except (nats.js.errors.NotFoundError, nats.errors.TimeoutError) as exc:
                raise NATSError(
                    f"Failed to create consumer {consumer_name} for stream {name}"
                ) from exc

    def _put(self, queue, message, **kwargs):
        """Put a message on a queue."""
        self._ensure_stream(queue)
        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        body_bytes, meta_headers = message_to_nats_body_and_headers(
            message,
            raw_body=self.nats_raw_body,
            header_prefix=self.nats_metadata_header_prefix,
            header_names=self.nats_metadata_header_names,
        )

        # Start from the metadata headers (empty dict in default mode).
        headers: dict | None = dict(meta_headers) if meta_headers else None

        # Append the JetStream TTL header when expiration is set.
        expiration = (message.get('properties') or {}).get('expiration')
        if expiration:
            if headers is None:
                headers = {}
            headers['Nats-TTL'] = f"{expiration}ms"

        self._run(
            self._js.publish(queue, body_bytes, headers=headers)
        )

    def _get(self, queue, **kwargs):
        """Get a message from a queue.

        If *queue* is bound to a fanout exchange, drain one message from the
        Core NATS fanout inbox instead of polling JetStream.
        """
        # Fanout path: drain from the Core NATS subscription inbox.
        if queue in self._fanout_inboxes:
            inbox = self._fanout_inboxes[queue]

            async def _drain_one():
                return await asyncio.wait_for(
                    inbox.get(),
                    timeout=self.wait_time_seconds,
                )

            try:
                msg = self._run(_drain_one())
            except (asyncio.TimeoutError, TimeoutError):
                raise Empty()
            return nats_body_and_headers_to_message(
                msg.data,
                msg.headers,
                header_prefix=self.nats_metadata_header_prefix,
                header_names=self.nats_metadata_header_names,
            )

        # Normal JetStream path.
        self._ensure_stream(queue)
        self._ensure_consumer(queue)

        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        stream_name = self._get_stream_name(queue)
        consumer_name = self._get_consumer_name(queue)
        sub_key = f"{stream_name}:{consumer_name}"

        try:
            # Cache the pull subscription: creating a new one on every
            # _get() call adds a JetStream API round trip per message.
            if sub_key not in self._subscriptions:
                self._subscriptions[sub_key] = self._run(
                    self._js.pull_subscribe(
                        queue,
                        consumer_name,
                        stream=stream_name,
                    )
                )
            pull_sub = self._subscriptions[sub_key]
            msg = self._run(
                pull_sub.fetch(1, timeout=self.wait_time_seconds)
            )[0]

            body = nats_body_and_headers_to_message(
                msg.data,
                msg.headers,
                header_prefix=self.nats_metadata_header_prefix,
                header_names=self.nats_metadata_header_names,
            )
            body["subject"] = msg.subject
            body["ack"] = msg.ack
            body["nak"] = msg.nak
            body["term"] = msg.term
            return body
        except (IndexError, nats.errors.TimeoutError):
            pass
        raise Empty()

    def _delete(self, queue, *args, **kwargs):
        """Delete a queue."""
        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        stream_name = self._get_stream_name(queue)
        consumer_name = self._get_consumer_name(queue)

        # Clear cached pull subscription for this stream/consumer.
        self._subscriptions.pop(f"{stream_name}:{consumer_name}", None)

        try:
            self._run(self._js.delete_stream(stream_name))
        except nats.js.errors.NotFoundError:
            pass
        finally:
            self._streams.discard(stream_name)

    def _size(self, queue):
        """Return the number of messages in a queue."""
        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        try:
            info = self._run(
                self._js.stream_info(self._get_stream_name(queue))
            )
            return info.state.messages
        except nats.js.errors.NotFoundError:
            return 0

    def _new_queue(self, queue, **kwargs):
        """Declare a new queue."""
        self._ensure_stream(queue)
        return queue

    def _has_queue(self, queue, **kwargs):
        """Check if a queue exists."""
        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        try:
            self._run(
                self._js.stream_info(self._get_stream_name(queue))
            )
            return True
        except (nats.js.errors.NotFoundError, nats.errors.TimeoutError):
            return False

    def ack_msg(self, msg):
        self._run(msg.nats_ack())

    def nak_msg(self, msg):
        self._run(msg.nats_nak())

    def term_msg(self, msg):
        self._run(msg.nats_term())

    def close(self):
        """Unsubscribe fanout subscriptions then delegate to base close."""
        for sub in list(self._fanout_subscriptions.values()):
            try:
                self._run(sub.unsubscribe())
            except Exception:
                pass
        self._fanout_subscriptions.clear()
        self._fanout_inboxes.clear()
        self._fanout_queue_to_exchange.clear()
        super().close()


# ---------------------------------------------------------------------------
# Core NATS channel (at-most-once, push-subscribe)
# ---------------------------------------------------------------------------

#: Maximum number of unread messages held per-subject in Core NATS inbox queues.
#: When the inbox is full the oldest message is dropped (head-drop policy).
MAX_INBOX_SIZE = 1000


class CoreNATSChannel(Channel):
    """Core NATS channel — at-most-once delivery via push subscriptions.

    Uses plain Core NATS ``nc.publish`` / ``nc.subscribe`` with no JetStream
    streams or consumers.  Each subscribed subject gets a bounded
    :class:`asyncio.Queue` inbox; messages are head-dropped when full.

    Acknowledgement is a no-op: Core NATS has no broker-side ack.
    """

    def __init__(self, *args, **kwargs):
        self._inbox: dict[str, asyncio.Queue] = {}
        self._subscriptions: dict = {}
        super().__init__(*args, **kwargs)

    # -- Subscription management -----------------------------------------

    async def _subscribe(self, subject: str, queue_group: str) -> None:
        """Subscribe to *subject* with *queue_group*, backing it with an inbox queue.

        The inbox queue is created **before** ``nc.subscribe()`` is called so
        that the message callback never sees a missing key even on the very
        first delivery.
        """
        # Create the inbox queue first so the callback can always find it.
        self._inbox[subject] = asyncio.Queue(maxsize=MAX_INBOX_SIZE)

        async def _msg_handler(msg):
            try:
                self._inbox[subject].put_nowait(msg)
            except asyncio.QueueFull:
                # Head-drop: discard the oldest message and enqueue the new one.
                try:
                    self._inbox[subject].get_nowait()
                except asyncio.QueueEmpty:
                    pass
                self._inbox[subject].put_nowait(msg)

        sub = await self._nats_client.subscribe(
            subject, queue=queue_group, cb=_msg_handler
        )
        self._subscriptions[subject] = sub

    # -- Subject mapping -------------------------------------------------

    @staticmethod
    def _queue_to_subject(queue: str) -> str:
        """Map a Kombu queue name to a Core NATS subject."""
        return f"celery.queue.{queue}"

    # -- Virtual transport interface -------------------------------------

    def _put(self, queue, message, **kwargs):
        """Publish a message to the Core NATS subject for *queue*."""
        subject = self._queue_to_subject(queue)
        body_bytes, _ = message_to_nats_body_and_headers(
            message,
            raw_body=self.nats_raw_body,
            header_prefix=self.nats_metadata_header_prefix,
            header_names=self.nats_metadata_header_names,
        )
        self._run(self._nats_client.publish(subject, body_bytes))

    def _get(self, queue, **kwargs):
        """Drain one message from the inbox for *queue*; raise :exc:`Empty` on timeout."""
        subject = self._queue_to_subject(queue)
        if subject not in self._subscriptions:
            self._run(self._subscribe(subject, queue))

        async def _drain_one():
            return await asyncio.wait_for(
                self._inbox[subject].get(),
                timeout=self.wait_time_seconds,
            )

        try:
            msg = self._run(_drain_one())
        except (asyncio.TimeoutError, TimeoutError):
            raise Empty()

        return nats_body_and_headers_to_message(
            msg.data,
            msg.headers,
            header_prefix=self.nats_metadata_header_prefix,
            header_names=self.nats_metadata_header_names,
        )

    def _new_queue(self, queue, **kwargs):
        """Declare queue: subscribe to the corresponding subject if not already done."""
        subject = self._queue_to_subject(queue)
        if subject not in self._subscriptions:
            self._run(self._subscribe(subject, queue))
        return queue

    def _has_queue(self, queue, **kwargs):
        subject = self._queue_to_subject(queue)
        return subject in self._subscriptions

    def _size(self, queue):
        subject = self._queue_to_subject(queue)
        q = self._inbox.get(subject)
        return q.qsize() if q is not None else 0

    def _delete(self, queue, *args, **kwargs):
        subject = self._queue_to_subject(queue)
        sub = self._subscriptions.pop(subject, None)
        if sub is not None:
            try:
                self._run(sub.unsubscribe())
            except Exception:
                pass
        self._inbox.pop(subject, None)

    # -- Ack semantics (no-ops) ------------------------------------------

    def basic_ack(self, delivery_tag, multiple=False):
        """No-op: Core NATS has no broker-side acknowledgement."""
        self.qos._not_yet_acked.pop(delivery_tag, None)

    def basic_nack(self, delivery_tag, multiple=False, requeue=True):
        """No-op: Core NATS has no broker-side negative-acknowledgement."""
        self.qos._not_yet_acked.pop(delivery_tag, None)

    def basic_reject(self, delivery_tag, requeue=False):
        """No-op: Core NATS has no broker-side reject."""
        self.qos._not_yet_acked.pop(delivery_tag, None)

    # -- Close -----------------------------------------------------------

    def close(self):
        """Unsubscribe all active subscriptions, then close the channel."""
        for sub in list(self._subscriptions.values()):
            try:
                self._run(sub.unsubscribe())
            except Exception:
                pass
        self._subscriptions.clear()
        self._inbox.clear()
        super().close()


class Transport(virtual.Transport):
    """NATS JetStream Transport."""

    Channel = JetStreamChannel

    default_port = DEFAULT_PORT

    driver_type = "nats"
    driver_name = "nats"

    connection_errors = NATS_CONNECTION_ERRORS
    channel_errors = NATS_CHANNEL_ERRORS

    def __init__(self, client, **kwargs):
        if Client is None:
            raise ImportError("nats-py is not installed")
        super().__init__(client, **kwargs)
        # State shared by all channels of this transport: one NATS
        # client (TCP connection) and one event-loop thread.
        self._nats_client = None
        self._loop = None
        self._loop_thread = None
        self._loop_lock = threading.Lock()

    def _get_loop(self):
        """Return (and lazily create) the shared event loop thread."""
        with self._loop_lock:
            if self._loop is None or self._loop.is_closed():
                self._loop = asyncio.new_event_loop()
                self._loop_thread = threading.Thread(
                    target=self._loop.run_forever,
                    daemon=True,
                    name="kombu-nats-loop",
                )
                self._loop_thread.start()
            return self._loop

    def _run_on_loop(self, coro):
        """Submit *coro* to the shared event loop and block until done."""
        if self._loop is None or self._loop.is_closed():
            raise RuntimeError("NATS event loop is closed")
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def _get_client(self, conninfo, connect_timeout=None):
        """Return (and lazily create) the shared NATS client.

        Only the first channel to call this actually dials NATS; later
        channels reuse the same TCP connection.
        """
        if self._nats_client is None:
            self._nats_client = Client()
            if self._nats_client is None:
                raise RuntimeError("Failed to create NATS client")

            host = conninfo.hostname or DEFAULT_HOST
            port = conninfo.port or DEFAULT_PORT
            if connect_timeout is None:
                connect_timeout = \
                    conninfo.transport_options.get(
                        "connection_wait_time_seconds", 5)
            self._run_on_loop(
                self._nats_client.connect(
                    f"nats://{host}:{port}",
                    user=conninfo.userid,
                    password=conninfo.password,
                    connect_timeout=connect_timeout,
                    error_cb=self._on_nats_error,
                    reconnected_cb=self._on_reconnect,
                )
            )
        return self._nats_client

    async def _on_reconnect(self) -> None:
        """Called by nats-py after a reconnect — all cached pull
        subscriptions are stale and must be dropped."""
        logger.info("NATS reconnected, clearing pull subscription caches")
        for channel in self.channels:
            try:
                channel._subscriptions.clear()
            except AttributeError:
                pass

    async def _on_nats_error(self, exc: Exception) -> None:
        """Async error callback passed to nats-py on connect.

        Drain-timeout errors during transport shutdown are expected and
        handled in Python; suppress the default nats-py stderr print for
        them.  All other errors are forwarded to the Kombu logger.
        """
        if isinstance(exc, nats.errors.DrainTimeoutError):
            logger.debug("NATS drain timed out (suppressed): %s", exc)
        else:
            logger.warning("NATS error: %s", exc)

    def _channel_cls_for(self, connection) -> type:
        """Return the channel class appropriate for *connection*'s URL scheme.

        ``nats+core://`` → :class:`CoreNATSChannel`
        All other schemes (``nats://``, ``nats+jetstream://``) → :class:`JetStreamChannel`
        """
        scheme = getattr(connection.client, 'transport', 'nats') or 'nats'
        if scheme == 'nats+core':
            return CoreNATSChannel
        return JetStreamChannel

    def create_channel(self, connection):

        """Create a channel whose type is selected by the URL scheme."""
        try:
            return self._avail_channels.pop()
        except IndexError:
            channel = self._channel_cls_for(connection)(connection, transport=self)
            self.channels.append(channel)
            return channel

    def drain_events(self, connection, **kwargs):
        return super().drain_events(connection, **kwargs)

    def driver_version(self):
        """Get the NATS driver version."""
        return nats.aio.client.__version__

    def establish_connection(self):
        """Establish a connection to NATS."""
        return super().establish_connection()

    def close_connection(self, connection):
        """Close the connection: close all channels, drain the shared NATS
        client, then stop the shared event loop thread."""
        super().close_connection(connection)

        # Drain + close the shared client while the loop is still running.
        if self._nats_client is not None:
            try:
                try:
                    self._run_on_loop(self._nats_client.drain())
                except nats.errors.DrainTimeoutError:
                    logger.debug(
                        "NATS drain timed out during close; closing anyway")
                self._run_on_loop(self._nats_client.close())
            finally:
                self._nats_client = None

        # Now stop the shared loop.
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
            if self._loop_thread is not None:
                self._loop_thread.join(timeout=5)
            self._loop.close()
        self._loop = None
        self._loop_thread = None

    def verify_connection(self, connection):
        """Verify the connection works."""
        port = connection.client.port or self.default_port
        host = connection.client.hostname or DEFAULT_HOST

        logger.debug("Verify NATS connection to nats://%s:%s", host, port)

        client = Client()
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(client.connect(f"nats://{host}:{port}"))
            loop.run_until_complete(client.close())
            return True
        except ValueError:
            pass
        finally:
            loop.close()

        return False
