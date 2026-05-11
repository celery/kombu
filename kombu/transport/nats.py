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
* ``nats_clean_body`` - If ``True``, publish the serialized application payload
  directly as the NATS message body instead of wrapping it in a Kombu JSON
  envelope.  Kombu metadata (content-type, properties, etc.) is carried in NATS
  headers with the configured prefix.  Default ``False`` (backward-compatible
  envelope-in-body behaviour).
* ``nats_metadata_header_prefix`` - Prefix applied to all Kombu metadata header
  names when ``nats_clean_body=True``.  Default ``"Kombu-"``.  Must not start
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
after that duration.  This header is applied in both legacy and clean-body mode.
"""

from __future__ import annotations

import asyncio
from queue import Empty

from kombu.transport import virtual
from kombu.utils import cached_property
from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import dumps, loads

try:
    import nats.aio.client
    import nats.aio.errors
    import nats.errors
    import nats.js.errors
    from nats.aio.client import Client
    from nats.js.api import (AckPolicy, ConsumerConfig, DeliverPolicy,
                             DiscardPolicy, RetentionPolicy, StorageType,
                             StreamConfig)
    from nats.js.client import JetStreamContext

    NATS_CONNECTION_ERRORS = (
        nats.aio.errors.ErrConnectionClosed,
        nats.aio.errors.ErrTimeout,
        nats.aio.errors.ErrNoServers,
    )
    NATS_CHANNEL_ERRORS = (nats.js.errors.NotFoundError,)

except ImportError:
    Client = None
    NATS_CONNECTION_ERRORS = NATS_CHANNEL_ERRORS = ()

from kombu.log import get_logger

logger = get_logger(__name__)

DEFAULT_PORT = 4222
DEFAULT_HOST = "localhost"

_event_loop: asyncio.AbstractEventLoop | None = None


def get_event_loop() -> asyncio.AbstractEventLoop:
    """Get or create the global event loop."""
    global _event_loop
    if _event_loop is None:
        _event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_event_loop)
    return _event_loop


# ---------------------------------------------------------------------------
# Clean-body mode: module-level constants and helpers
# ---------------------------------------------------------------------------

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

    Values whose first non-whitespace character is ``{`` or ``[`` are
    JSON-parsed; all others are returned as plain strings.  An empty
    string returns ``None``.
    """
    if not raw:
        return None
    stripped = raw.strip()
    if stripped and stripped[0] in ('{', '['):
        try:
            return loads(stripped)
        except Exception:
            pass
    return raw


def message_to_nats_body_and_headers(
    message: dict,
    *,
    clean_body: bool,
    header_prefix: str,
    header_names: dict | None,
) -> tuple[bytes, dict]:
    """Convert a Kombu message dict to *(body_bytes, metadata_headers)*.

    **Default mode** (``clean_body=False``):
        *body_bytes* is the full Kombu JSON envelope serialised to bytes;
        *metadata_headers* is an empty dict.

    **Clean-body mode** (``clean_body=True``):
        *body_bytes* is ``message['body']`` converted to bytes exactly as
        Kombu/the application provided it — no additional encoding or
        decoding is applied.  *metadata_headers* holds Kombu metadata under
        the configured header prefix.
    """
    if not clean_body:
        return str_to_bytes(dumps(message)), {}

    # Merge user-supplied header name overrides with defaults.
    names = {**DEFAULT_METADATA_HEADER_NAMES, **(header_names or {})}

    # Publish the body exactly as provided by Kombu's serializer layer.
    # Serialization/encoding is entirely the caller's responsibility.
    body = message.get("body", b"")
    if isinstance(body, str):
        body_bytes = body.encode("utf-8")
    elif isinstance(body, (bytes, bytearray)):
        body_bytes = bytes(body)
    else:
        body_bytes = b""

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


def nats_body_and_headers_to_message(
    data: bytes,
    msg_headers,
    *,
    header_prefix: str,
    header_names: dict | None,
) -> dict:
    """Reconstruct a Kombu message dict from NATS *data* and *msg_headers*.

    If the configured metadata headers are absent the payload is assumed to
    be a legacy Kombu JSON envelope and is parsed directly.
    """
    names = {**DEFAULT_METADATA_HEADER_NAMES, **(header_names or {})}
    ct_key = f"{header_prefix}{names['content_type']}"

    # Normalise msg.headers: nats-py may give None, str values, or list values.
    flat: dict[str, str] = {}
    if isinstance(msg_headers, dict):
        for k, v in msg_headers.items():
            flat[k] = v[0] if isinstance(v, list) else v

    if ct_key not in flat:
        # Legacy path: payload is a full Kombu JSON envelope.
        return loads(data.decode())

    # Clean-body path: reconstruct a Kombu envelope from the metadata headers.
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
    """NATS JetStream Channel."""

    QoS = QoS
    Message = Message

    default_wait_time_seconds = 5
    default_connection_wait_time_seconds = 5
    default_stream_name_prefix = "STREAM_"
    default_consumer_name_prefix = "CONSUMER_"

    def __init__(self, *args, **kwargs):
        if Client is None:
            raise ImportError("nats-py is not installed")

        super().__init__(*args, **kwargs)

        port = self.connection.client.port or self.connection.default_port
        host = self.connection.client.hostname or DEFAULT_HOST

        logger.debug("Host: %s Port: %s", host, port)

        self._nats_client: Client | None = None
        self._js: JetStreamContext | None = None
        self._streams = set()
        self._js_consumers: set = set()

        # Evaluate connection
        self.client

    def _get_stream_name(self, queue):
        """Get the stream name for a queue."""
        prefix = self.options.get("stream_name_prefix", self.default_stream_name_prefix)
        return f"{prefix}{queue}"

    def _get_consumer_name(self, queue):
        """Get the consumer name for a queue."""
        prefix = self.options.get("consumer_name_prefix", self.default_consumer_name_prefix)
        return f"{prefix}{queue}"

    def _ensure_stream(self, queue):
        """Ensure a stream exists for the queue."""
        stream_name = self._get_stream_name(queue)
        if stream_name in self._streams:
            return

        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        # First try to get stream info with a shorter timeout
        try:
            loop = get_event_loop()
            loop.run_until_complete(
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
            loop = get_event_loop()
            loop.run_until_complete(
                asyncio.wait_for(
                    self._js.add_stream(stream_config, **user_cfg),
                    timeout=5.0  # Use a longer timeout for creation
                )
            )
            self._streams.add(stream_name)
        except nats.errors.TimeoutError:
            # If we timeout creating the stream, check if it was actually created
            try:
                loop.run_until_complete(
                    asyncio.wait_for(
                        self._js.stream_info(stream_name),
                        timeout=1.0
                    )
                )
                self._streams.add(stream_name)
            except (nats.js.errors.NotFoundError, nats.errors.TimeoutError):
                raise RuntimeError(f"Failed to create stream {stream_name}")

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
            loop = get_event_loop()
            loop.run_until_complete(
                asyncio.wait_for(
                    self._js.add_consumer(name, consumer_config, **user_cfg),
                    timeout=5.0  # Use a longer timeout for consumer creation
                )
            )
            self._js_consumers.add(consumer_name)
        except nats.errors.TimeoutError:
            # If we timeout creating the consumer, check if it was actually created
            try:
                loop.run_until_complete(
                    asyncio.wait_for(
                        self._js.consumer_info(name, consumer_name),
                        timeout=1.0
                    )
                )
                self._js_consumers.add(consumer_name)
            except (nats.js.errors.NotFoundError, nats.errors.TimeoutError):
                raise RuntimeError(f"Failed to create consumer {consumer_name} for stream {name}")

    def _put(self, queue, message, **kwargs):
        """Put a message on a queue."""
        self._ensure_stream(queue)
        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        body_bytes, meta_headers = message_to_nats_body_and_headers(
            message,
            clean_body=self.nats_clean_body,
            header_prefix=self.nats_metadata_header_prefix,
            header_names=self.nats_metadata_header_names,
        )

        # Start from the metadata headers (empty dict in legacy mode).
        headers: dict | None = dict(meta_headers) if meta_headers else None

        # Append the JetStream TTL header when expiration is set.
        expiration = (message.get('properties') or {}).get('expiration')
        if expiration:
            if headers is None:
                headers = {}
            headers['Nats-TTL'] = f"{expiration}ms"

        get_event_loop().run_until_complete(
            self._js.publish(queue, body_bytes, headers=headers)
        )

    def _get(self, queue, **kwargs):
        """Get a message from a queue."""
        self._ensure_stream(queue)
        self._ensure_consumer(queue)

        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        try:
            pull_sub = get_event_loop().run_until_complete(
                self._js.pull_subscribe(
                    queue,
                    self._get_consumer_name(queue),
                    stream=self._get_stream_name(queue),
                )
            )
            msg = get_event_loop().run_until_complete(
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
        try:
            get_event_loop().run_until_complete(self._js.delete_stream(stream_name))
        except nats.js.errors.NotFoundError:
            pass
        finally:
            self._streams.discard(stream_name)

    def _size(self, queue):
        """Return the number of messages in a queue."""
        if self._js is None:
            raise RuntimeError("JetStream context not initialized")

        try:
            info = get_event_loop().run_until_complete(
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
            get_event_loop().run_until_complete(
                self._js.stream_info(self._get_stream_name(queue))
            )
            return True
        except (nats.js.errors.NotFoundError, nats.errors.TimeoutError):
            return False

    def _open(self):
        """Open a new connection to NATS."""
        if self._nats_client is None:
            self._nats_client = Client()
            if self._nats_client is None:
                raise RuntimeError("Failed to create NATS client")

            host = self.conninfo.hostname or DEFAULT_HOST
            port = self.conninfo.port or DEFAULT_PORT
            get_event_loop().run_until_complete(
                self._nats_client.connect(
                    f"nats://{host}:{port}",
                    user=self.conninfo.userid,
                    password=self.conninfo.password,
                    connect_timeout=self.connection_wait_time_seconds,
                )
            )
            self._js = self._nats_client.jetstream()
        return self._nats_client

    @cached_property
    def client(self):
        """Get the NATS client."""
        return self._open()

    @property
    def options(self):
        """Get the transport options."""
        return self.connection.client.transport_options

    @property
    def conninfo(self):
        """Get the connection info."""
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
    def nats_clean_body(self) -> bool:
        """If ``True``, publish the application payload directly as msg.data.

        Kombu envelope metadata is carried in NATS headers instead.
        Default ``False`` (backward-compatible envelope-in-body behaviour).
        """
        return bool(self.options.get('nats_clean_body', False))

    @property
    def nats_metadata_header_prefix(self) -> str:
        """Prefix applied to Kombu metadata header names in clean-body mode.

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
        """Close the channel."""
        if self._nats_client is not None:
            loop = get_event_loop()
            loop.run_until_complete(self._nats_client.drain())
            loop.run_until_complete(self._nats_client.close())
            self._nats_client = None
            self._js = None

    def ack_msg(self, msg):
        get_event_loop().run_until_complete(msg.nats_ack())

    def nak_msg(self, msg):
        get_event_loop().run_until_complete(msg.nats_nak())

    def term_msg(self, msg):
        get_event_loop().run_until_complete(msg.nats_term())


class Transport(virtual.Transport):
    """NATS JetStream Transport."""

    Channel = Channel

    default_port = DEFAULT_PORT

    driver_type = "nats"
    driver_name = "nats"

    connection_errors = NATS_CONNECTION_ERRORS
    channel_errors = NATS_CHANNEL_ERRORS

    def __init__(self, client, **kwargs):
        if Client is None:
            raise ImportError("nats-py is not installed")
        super().__init__(client, **kwargs)

    def drain_events(self, connection, **kwargs):
        return super().drain_events(connection, **kwargs)

    def driver_version(self):
        """Get the NATS driver version."""
        return nats.aio.client.__version__

    def establish_connection(self):
        """Establish a connection to NATS."""
        return super().establish_connection()

    def close_connection(self, connection):
        """Close the connection to NATS."""
        return super().close_connection(connection)

    def verify_connection(self, connection):
        """Verify the connection works."""
        port = connection.client.port or self.default_port
        host = connection.client.hostname or DEFAULT_HOST

        logger.debug("Verify NATS connection to nats://%s:%s", host, port)

        client = Client()
        try:
            loop = get_event_loop()
            loop.run_until_complete(client.connect(f"nats://{host}:{port}"))
            loop.run_until_complete(client.close())
            return True
        except ValueError:
            pass

        return False
