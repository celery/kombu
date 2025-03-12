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
"""

from __future__ import annotations

import asyncio
from queue import Empty
from typing import TYPE_CHECKING

from kombu.transport import virtual
from kombu.utils import cached_property
from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import dumps, loads

try:
    import nats.aio.client
    import nats.aio.errors
    import nats.errors
    from nats.aio.client import Client
    from nats.js.api import (
        AckPolicy,
        ConsumerConfig,
        DeliverPolicy,
        DiscardPolicy,
        RetentionPolicy,
        StorageType,
        StreamConfig,
    )
    from nats.js.client import JetStreamContext
    from nats.js.errors import NotFoundError

    NATS_CONNECTION_ERRORS = (
        nats.aio.errors.ErrConnectionClosed,
        nats.aio.errors.ErrTimeout,
        nats.aio.errors.ErrNoServers,
    )
    NATS_CHANNEL_ERRORS = (NotFoundError,)

except ImportError:
    Client = None
    NATS_CONNECTION_ERRORS = NATS_CHANNEL_ERRORS = ()

from kombu.log import get_logger

logger = get_logger(__name__)

DEFAULT_PORT = 4222
DEFAULT_HOST = "localhost"


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

    _not_yet_acked = {}

    def can_consume(self):
        """Return true if the channel can be consumed from."""
        return not self.prefetch_count or len(self._not_yet_acked) < self.prefetch_count

    def can_consume_max_estimate(self):
        if self.prefetch_count:
            return self.prefetch_count - len(self._not_yet_acked)
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

    if TYPE_CHECKING:
        _nats_client: Client
        _js: JetStreamContext

    def __init__(self, *args, **kwargs):
        if Client is None:
            raise ImportError("nats-py is not installed")

        super().__init__(*args, **kwargs)

        port = self.connection.client.port or self.connection.default_port
        host = self.connection.client.hostname or DEFAULT_HOST

        logger.debug("Host: %s Port: %s", host, port)

        self._event_loop = asyncio.new_event_loop()

        self._nats_client = None
        self._js = None

        self._streams = set()

        # Evaluate connection
        self.client

    def _get_stream_name(self, queue):
        """Get the stream name for a queue."""
        return f"STREAM_{queue}"

    def _get_consumer_name(self, queue):
        """Get the consumer name for a queue."""
        return f"CONSUMER_{queue}"

    def _ensure_stream(self, queue):
        """Ensure a stream exists for the queue."""
        stream_name = self._get_stream_name(queue)
        if stream_name in self._streams:
            return

        try:
            self._event_loop.run_until_complete(self._js.stream_info(stream_name))
            self._streams.add(stream_name)
            return
        except NotFoundError:
            pass

        stream_config = StreamConfig(
            name=stream_name,
            # subjects=[f"{queue}.>"],
            subjects=[queue],
            retention=RetentionPolicy.WORK_QUEUE,
            max_consumers=-1,
            max_msgs_per_subject=1,
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

        self._event_loop.run_until_complete(
            self._js.add_stream(stream_config, **user_cfg)
        )
        self._streams.add(stream_name)

    def _ensure_consumer(self, queue):
        """Ensure a consumer exists for the queue."""
        consumer_name = self._get_consumer_name(queue)
        if consumer_name in self._consumers:
            return

        name = self._get_stream_name(queue)

        consumer_config = ConsumerConfig(
            durable_name=consumer_name,
            deliver_policy=DeliverPolicy.ALL,
            ack_policy=AckPolicy.EXPLICIT,
            # filter_subject=f"{queue}.>",
            filter_subject=queue,
        )

        # Update with user-provided config
        user_cfg = self.options.get("consumer_config") or {}

        self._event_loop.run_until_complete(
            self._js.add_consumer(name, consumer_config, **user_cfg)
        )
        self._consumers.add(consumer_name)

    def _put(self, queue, message, **kwargs):
        """Put a message on a queue."""
        self._ensure_stream(queue)
        # subject = f"{queue}.{message.get('id', '')}"
        subject = queue
        self._event_loop.run_until_complete(
            self._js.publish(subject, str_to_bytes(dumps(message)))
        )

    def _get(self, queue, **kwargs):
        """Get a message from a queue."""
        self._ensure_stream(queue)
        self._ensure_consumer(queue)

        try:
            pull_sub = self._event_loop.run_until_complete(
                self._js.pull_subscribe(
                    # f"{queue}.>",
                    queue,
                    self._get_consumer_name(queue),
                    stream=self._get_stream_name(queue),
                )
            )
            msg = self._event_loop.run_until_complete(
                pull_sub.fetch(1, timeout=self.wait_time_seconds)
            )[0]

            body = loads(msg.data.decode())
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
        stream_name = self._get_stream_name(queue)
        if stream_name in self._streams:
            try:
                self._event_loop.run_until_complete(self._js.delete_stream(stream_name))
                self._streams.remove(stream_name)
            except NotFoundError:
                pass

    def _size(self, queue):
        """Return the number of messages in a queue."""
        try:
            info = self._event_loop.run_until_complete(
                self._js.stream_info(self._get_stream_name(queue))
            )
            return info.state.messages
        except NotFoundError:
            return 0

    def _new_queue(self, queue, **kwargs):
        """Declare a new queue."""
        self._ensure_stream(queue)
        return queue

    def _has_queue(self, queue, **kwargs):
        """Check if a queue exists."""
        try:
            self._event_loop.run_until_complete(
                self._js.stream_info(self._get_stream_name(queue))
            )
            return True
        except NotFoundError:
            return False

    def _open(self):
        """Open a new connection to NATS."""
        if self._nats_client is None:
            self._nats_client = Client()
            self._event_loop.run_until_complete(
                self._nats_client.connect(
                    f"nats://{self.conninfo.hostname}:{self.conninfo.port or DEFAULT_PORT}",
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

    def close(self):
        """Close the channel."""
        if self._nats_client is not None:
            self._event_loop.run_until_complete(self._nats_client.drain())
            self._event_loop.run_until_complete(self._nats_client.close())
            self._nats_client = None
            self._js = None

        pending = asyncio.all_tasks(loop=self._event_loop)
        group = asyncio.gather(*pending)
        if not group.done():
            self._event_loop.run_until_complete(group)

        self._event_loop.close()

    def ack_msg(self, msg):
        self._event_loop.run_until_complete(msg.nats_ack())

    def nak_msg(self, msg):
        self._event_loop.run_until_complete(msg.nats_nak())

    def term_msg(self, msg):
        self._event_loop.run_until_complete(msg.nats_term())


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
        self._event_loop = asyncio.new_event_loop()

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
            self._event_loop.run_until_complete(client.connect(f"nats://{host}:{port}"))
            self._event_loop.run_until_complete(client.close())
            return True
        except ValueError:
            pass

        return False
