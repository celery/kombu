"""PGMQ Transport module for Kombu.

Uses the PostgreSQL Message Queue (`PGMQ`) extension as the message store.

PGMQ provides SQS-like semantics: messages are read with a visibility
timeout, must be explicitly deleted to acknowledge, and become visible
again automatically if not deleted before the timeout expires.

PGMQ Features supported by this transport
=========================================

Long Polling
------------

Long polling is enabled by setting the ``wait_time_seconds`` transport
option to a value greater than 0 (default ``10``).  When set, each read
uses PGMQ's ``read_with_poll`` and blocks up to that many seconds before
returning empty.

Polling Interval
----------------

The ``polling_interval`` transport option (default: 1 second) controls
the client-side delay between consecutive polls when a poll returns no
messages.  After a successful (non-empty) poll the next poll is
scheduled immediately; after an empty poll the transport waits
``polling_interval`` seconds before trying again.

Batch Reads
-----------

Like the SQS transport, ``drain_events`` uses ``_get_bulk`` to request up
to ``prefetch_count`` messages per queue on each poll, which improves
throughput when tasks are short-lived.

Topic Routing
-------------

PGMQ's native `topic routing`_ is used for fanout and topic exchanges.
Bindings are namespaced by exchange name so fanout and topic traffic do
not interfere.  Fanout queues bind to ``kombu.fanout.<exchange>``;
topic queues bind to ``<exchange>.<pattern>``.  Publishing to fanout or
topic exchanges calls ``send_topic`` once and PGMQ delivers copies to
every matching queue.

Direct exchanges continue to use Kombu's standard virtual routing
(``_put`` per queue).  PGMQ topic patterns use the same ``*`` and
``#`` wildcards as AMQP topic exchanges.

Delayed Delivery
----------------

Per-message delay is supported via Kombu's ``expiration`` publish argument
(milliseconds) or the ``DelaySeconds`` message property (seconds).  These
map to PGMQ's ``delay`` parameter on ``send`` and ``send_topic``.

FIFO Reads
----------

Set ``fifo_mode`` to ``grouped`` or ``round_robin`` to use PGMQ's
``read_grouped`` or ``read_grouped_rr`` read paths.  Create a FIFO index
with the ``x-pgmq-fifo-index`` queue argument or by enabling ``fifo_mode``
on a transport that creates new queues.

NOTIFY / LISTEN
---------------

Set ``use_notify`` to ``True`` to enable PGMQ queue notifications and
use PostgreSQL ``LISTEN``/``NOTIFY`` as a wake-up signal between polls
instead of sleeping for the full ``polling_interval``.

Partitioned Queues
------------------

Declare a queue with ``queue_arguments={'x-pgmq-partitioned': True}`` to
create a partitioned PGMQ queue.  Optional ``x-pgmq-partition-interval``
and ``x-pgmq-retention-interval`` arguments control partition sizing.

.. _topic routing: https://pgmq.github.io/pgmq-py/latest/topic_routing/

Connection String
=================

.. code-block::

    pgmq://USERNAME:PASSWORD@HOST:PORT/DATABASE

Examples:

.. code-block::

    pgmq://postgres:postgres@localhost:5432/postgres

Transport Options
=================

* ``queue_name_prefix``: String prefix prepended to queue names.
* ``visibility_timeout``: Seconds a message stays invisible after read
  (default ``1800``).
* ``wait_time_seconds``: Long-poll duration in seconds (default ``10``).
* ``poll_interval_ms``: Poll interval for long-poll reads (default ``100``).
* ``init_extension``: Create the PGMQ extension on connect (default ``True``).
* ``pool_size``: Connection pool size (default ``10``).
* ``conn_string``: Optional full PostgreSQL connection string. When set, it
  overrides host/port/database/username/password from the URL.
* ``fifo_mode``: ``grouped`` or ``round_robin`` to enable FIFO-ordered reads.
* ``use_notify``: Enable PGMQ ``NOTIFY``/``LISTEN`` wake-ups (default ``False``).
* ``notify_throttle_interval_ms``: Throttle interval for ``enable_notify``
  (default ``250``).
"""

from __future__ import annotations

import socket
import string
import threading
import warnings
from queue import Empty
from time import monotonic, sleep

from kombu.exceptions import OperationalError
from kombu.log import get_logger
from kombu.utils import scheduling
from kombu.utils.encoding import safe_str
from kombu.utils.objects import cached_property

from . import virtual
from .virtual.exchange import STANDARD_EXCHANGE_TYPES, TopicExchange

try:
    import psycopg
    from pgmq import PGMQueue
    from pgmq.messages import Message as PGMQMessage
except ImportError:  # pragma: no cover
    psycopg = None
    PGMQueue = None
    PGMQMessage = None

logger = get_logger(__name__)

DEFAULT_PORT = 5432

# PGMQ bulk reads mirror the SQS transport default batch size.
PGMQ_MAX_MESSAGES = 10

# Prefix used to namespace fanout publishes in PGMQ topic routing.
FANOUT_TOPIC_PREFIX = 'kombu.fanout'

# PGMQ queue names allow alphanumeric characters and underscores.
PUNCTUATIONS_TO_REPLACE = set(string.punctuation) - {'_'}
CHARS_REPLACE_TABLE = {
    **{ord(c): ord('_') for c in PUNCTUATIONS_TO_REPLACE}
}

FIFO_READ_METHODS = {
    'grouped': 'read_grouped',
    'round_robin': 'read_grouped_rr',
}


class PGMQTopicExchange(TopicExchange):
    """Topic exchange that publishes via PGMQ ``send_topic``."""

    def deliver(self, message, exchange, routing_key, **kwargs):
        self.channel._put_topic(exchange, message, routing_key, **kwargs)


class _NotifyWaiter:
    """Wait for PGMQ INSERT notifications on one or more queues."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._conn = None
        self._channels: set[str] = set()
        self._lock = threading.Lock()

    def register(self, queue_name: str) -> None:
        channel = f'pgmq.q_{queue_name}.INSERT'
        with self._lock:
            if channel in self._channels:
                return
            self._ensure_conn()
            self._conn.execute(f'LISTEN "{channel}";')
            self._channels.add(channel)

    def wait(self, timeout: float | None) -> bool:
        if not self._channels:
            return False
        self._ensure_conn()
        for _ in self._conn.notifies(timeout=timeout):
            return True
        return False

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None
            self._channels.clear()

    def _ensure_conn(self) -> None:
        if self._conn is None:
            self._conn = psycopg.connect(self._dsn, autocommit=True)


class Channel(virtual.Channel):
    """PGMQ channel."""

    supports_fanout = True

    exchange_types = {
        **STANDARD_EXCHANGE_TYPES,
        'topic': PGMQTopicExchange,
    }

    default_visibility_timeout = 1800  # 30 minutes, same as SQS.
    default_wait_time_seconds = 10
    default_init_extension = True
    default_pool_size = 10
    default_poll_interval_ms = 100
    default_notify_throttle_interval_ms = 250

    def __init__(self, *args, **kwargs):
        if PGMQueue is None:
            raise ImportError(
                'PGMQ transport requires the pgmq library: '
                'pip install kombu[pgmq]'
            )

        self._noack_queues: set[str] = set()
        super().__init__(*args, **kwargs)
        self.qos.restore_at_shutdown = False

    def basic_consume(self, queue, no_ack, callback, consumer_tag,
                      *args, **kwargs):
        if no_ack:
            self._noack_queues.add(queue)
        self._maybe_enable_notify(queue)
        return super().basic_consume(
            queue, no_ack, callback, consumer_tag, *args, **kwargs
        )

    def basic_cancel(self, consumer_tag):
        if consumer_tag in self._consumers:
            queue = self._tag_to_queue.get(consumer_tag)
            if queue is not None:
                self._noack_queues.discard(queue)
        return super().basic_cancel(consumer_tag)

    def drain_events(self, timeout=None, callback=None, **kwargs):
        """Return payload message(s) from one of our queues."""
        if not self._consumers or not self.qos.can_consume():
            raise Empty()
        self._poll(self.cycle, callback, timeout=timeout)

    def _reset_cycle(self):
        """Reset the consume cycle to use bulk reads."""
        self._cycle = scheduling.FairCycle(
            self._get_bulk, self._active_queues, Empty,
        )

    def entity_name(self, name, table=CHARS_REPLACE_TABLE) -> str:
        """Format an AMQP queue name into a valid PGMQ queue name."""
        return str(safe_str(name)).translate(table)

    def canonical_queue_name(self, queue_name: str) -> str:
        return self.entity_name(self.queue_name_prefix + queue_name)

    def _queue_name(self, queue: str) -> str:
        return self.canonical_queue_name(queue)

    def _fanout_topic_key(self, exchange: str) -> str:
        return f'{FANOUT_TOPIC_PREFIX}.{exchange}'

    def _topic_key(self, exchange: str, routing_key: str) -> str:
        return f'{exchange}.{routing_key}'

    def _pgmq_send_kwargs(self, message: dict) -> dict:
        """Build PGMQ ``send``/``send_topic`` keyword arguments."""
        kwargs = {}
        headers = dict(message.get('headers') or {})
        properties = message.get('properties') or {}

        if 'MessageGroupId' in properties:
            headers.setdefault('MessageGroupId', properties['MessageGroupId'])

        if headers:
            kwargs['headers'] = headers

        if 'DelaySeconds' in properties:
            kwargs['delay'] = int(properties['DelaySeconds'])
        else:
            expiration = properties.get('expiration')
            if expiration is not None:
                try:
                    delay = max(0, int(expiration) // 1000)
                except (TypeError, ValueError):
                    delay = 0
                if delay:
                    kwargs['delay'] = delay

        return kwargs

    def _new_queue(self, queue, **kwargs):
        queue_name = self._queue_name(queue)
        arguments = kwargs.get('arguments') or {}

        if arguments.get('x-pgmq-partitioned'):
            self.pgmq.create_partitioned_queue(
                queue_name,
                arguments.get('x-pgmq-partition-interval', 10000),
                arguments.get('x-pgmq-retention-interval', 100000),
            )
        else:
            self.pgmq.create_queue(queue_name)

        if arguments.get('x-pgmq-fifo-index') or self.fifo_mode:
            self.pgmq.create_fifo_index(queue_name)

        self._maybe_enable_notify(queue, queue_name=queue_name)

    def _maybe_enable_notify(self, queue, queue_name=None):
        if self.use_notify:
            queue_name = queue_name or self._queue_name(queue)
            self.pgmq.enable_notify(
                queue_name,
                throttle_interval_ms=self.notify_throttle_interval_ms,
            )
            self.connection._register_notify_queue(queue_name)

    def _has_queue(self, queue, **kwargs):
        queue_name = self._queue_name(queue)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            return any(
                record.queue_name == queue_name
                for record in self.pgmq.list_queues()
            )

    def _delete(self, queue, *args, **kwargs):
        queue_name = self._queue_name(queue)
        self.pgmq.drop_queue(queue_name)

    def _put(self, queue, message, **kwargs):
        queue_name = self._queue_name(queue)
        self.pgmq.send(queue_name, message, **self._pgmq_send_kwargs(message))

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        """Register queue bindings with PGMQ topic routing."""
        self._new_queue(queue)
        queue_name = self._queue_name(queue)
        exchange_type = self.typeof(exchange).type
        if exchange_type == 'fanout':
            self.pgmq.bind_topic(
                self._fanout_topic_key(exchange), queue_name)
        elif exchange_type == 'topic' and routing_key:
            self.pgmq.bind_topic(
                self._topic_key(exchange, routing_key), queue_name)

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Broadcast a message using PGMQ ``send_topic``."""
        self.pgmq.send_topic(
            self._fanout_topic_key(exchange),
            message,
            **self._pgmq_send_kwargs(message))

    def _put_topic(self, exchange, message, routing_key, **kwargs):
        """Route a message using PGMQ ``send_topic``."""
        self.pgmq.send_topic(
            self._topic_key(exchange, routing_key),
            message,
            **self._pgmq_send_kwargs(message))

    def _normalize_messages(self, result) -> list:
        if result is None:
            return []
        if isinstance(result, list):
            return result
        return [result]

    def _fifo_read_method(self) -> str | None:
        return FIFO_READ_METHODS.get(self.fifo_mode)

    def _receive_messages(self, queue: str, qty: int = 1) -> list:
        """Read up to ``qty`` messages from a PGMQ queue."""
        queue_name = self._queue_name(queue)

        if queue in self._noack_queues:
            return self._normalize_messages(
                self.pgmq.pop(queue_name, qty=qty))

        fifo_method = self._fifo_read_method()
        if fifo_method:
            if self.wait_time_seconds:
                read = getattr(self.pgmq, f'{fifo_method}_with_poll')
                return self._normalize_messages(read(
                    queue_name,
                    vt=self.visibility_timeout,
                    qty=qty,
                    max_poll_seconds=self.wait_time_seconds,
                    poll_interval_ms=self.poll_interval_ms,
                ))
            read = getattr(self.pgmq, fifo_method)
            return self._normalize_messages(read(
                queue_name,
                vt=self.visibility_timeout,
                qty=qty,
            ))

        if self.wait_time_seconds:
            return self.pgmq.read_with_poll(
                queue_name,
                vt=self.visibility_timeout,
                qty=qty,
                max_poll_seconds=self.wait_time_seconds,
                poll_interval_ms=self.poll_interval_ms,
            )

        return self._normalize_messages(self.pgmq.read(
            queue_name,
            vt=self.visibility_timeout,
            qty=qty,
        ))

    def _pgmq_message_to_python(self, pgmq_message, queue: str):
        payload = pgmq_message.message
        if queue in self._noack_queues:
            return payload

        properties = payload.setdefault('properties', {})
        delivery_info = properties.setdefault('delivery_info', {})
        queue_name = self._queue_name(queue)
        delivery_info['pgmq_queue'] = queue_name
        delivery_info['pgmq_msg_id'] = pgmq_message.msg_id
        delivery_info['pgmq_message'] = {
            'msg_id': pgmq_message.msg_id,
            'read_ct': pgmq_message.read_ct,
        }
        return payload

    def _pgmq_messages_to_python(self, messages, queue: str) -> list:
        return [
            self._pgmq_message_to_python(message, queue)
            for message in messages
        ]

    def _get_bulk(self, queue, callback=None, timeout=None,
                  max_if_unlimited=PGMQ_MAX_MESSAGES):
        """Try to retrieve multiple messages from ``queue``.

        The number of messages returned is limited by the prefetch count
        and ``PGMQ_MAX_MESSAGES``.
        """
        max_count = self._get_message_estimate(max_if_unlimited)
        if not max_count:
            raise Empty()

        messages = self._receive_messages(queue, qty=max_count)
        if not messages:
            raise Empty()

        for payload in self._pgmq_messages_to_python(messages, queue):
            self.connection._deliver(payload, queue)

    def _get(self, queue, timeout=None):
        """Try to retrieve a single message from ``queue``."""
        messages = self._receive_messages(queue, qty=1)
        if not messages:
            raise Empty()
        return self._pgmq_message_to_python(messages[0], queue)

    def _get_message_estimate(self, max_if_unlimited=PGMQ_MAX_MESSAGES):
        maxcount = self.qos.can_consume_max_estimate()
        return min(
            max_if_unlimited if maxcount is None else max(maxcount, 1),
            max_if_unlimited,
        )

    def basic_ack(self, delivery_tag, multiple=False):
        if multiple:
            raise NotImplementedError('multiple acks not implemented')

        try:
            delivery_info = self.qos.get(delivery_tag).delivery_info
        except KeyError:
            super().basic_ack(delivery_tag)
        else:
            queue = delivery_info.get('pgmq_queue')
            msg_id = delivery_info.get('pgmq_msg_id')
            if queue is not None and msg_id is not None:
                self.pgmq.delete(queue, msg_id)
            super().basic_ack(delivery_tag)

    def basic_reject(self, delivery_tag, requeue=False):
        try:
            delivery_info = self.qos.get(delivery_tag).delivery_info
        except KeyError:
            super().basic_reject(delivery_tag, requeue=requeue)
        else:
            queue = delivery_info.get('pgmq_queue')
            msg_id = delivery_info.get('pgmq_msg_id')
            if queue is not None and msg_id is not None:
                if requeue:
                    self.pgmq.set_vt(queue, msg_id, 0)
                else:
                    self.pgmq.delete(queue, msg_id)
            self.qos.reject(delivery_tag, requeue=False)

    def _restore(
        self,
        message,
        unwanted_delivery_info=(
            'pgmq_message', 'pgmq_msg_id', 'pgmq_queue',
        ),
    ):
        for unwanted_key in unwanted_delivery_info:
            message.delivery_info.pop(unwanted_key, None)
        return super()._restore(message)

    def _purge(self, queue):
        queue_name = self._queue_name(queue)
        return self.pgmq.purge(queue_name)

    def _size(self, queue):
        queue_name = self._queue_name(queue)
        metrics = self.pgmq.metrics(queue_name)
        return metrics.queue_length

    @property
    def conninfo(self):
        return self.connection.client

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @property
    def pgmq(self) -> PGMQueue:
        return self.connection._get_pgmq_client()

    @cached_property
    def queue_name_prefix(self) -> str:
        return self.transport_options.get('queue_name_prefix', '')

    @cached_property
    def visibility_timeout(self) -> int:
        return (self.transport_options.get('visibility_timeout') or
                self.default_visibility_timeout)

    @cached_property
    def wait_time_seconds(self) -> int:
        wait_time = self.transport_options.get('wait_time_seconds')
        if wait_time is None:
            # Backward compatibility with the initial transport option name.
            wait_time = self.transport_options.get('max_poll_seconds')
        return (wait_time if wait_time is not None else
                self.default_wait_time_seconds)

    @cached_property
    def init_extension(self) -> bool:
        return self.transport_options.get(
            'init_extension',
            self.default_init_extension,
        )

    @cached_property
    def pool_size(self) -> int:
        return self.transport_options.get(
            'pool_size',
            self.default_pool_size,
        )

    @cached_property
    def poll_interval_ms(self) -> int:
        return self.transport_options.get(
            'poll_interval_ms',
            self.default_poll_interval_ms,
        )

    @cached_property
    def fifo_mode(self) -> str | None:
        mode = self.transport_options.get('fifo_mode')
        if mode is None:
            return None
        if mode not in FIFO_READ_METHODS:
            raise ValueError(
                f'Invalid fifo_mode {mode!r}; expected one of '
                f'{sorted(FIFO_READ_METHODS)}')
        return mode

    @cached_property
    def use_notify(self) -> bool:
        return bool(self.transport_options.get('use_notify', False))

    @cached_property
    def notify_throttle_interval_ms(self) -> int:
        return self.transport_options.get(
            'notify_throttle_interval_ms',
            self.default_notify_throttle_interval_ms,
        )


class Transport(virtual.Transport):
    """PGMQ transport."""

    Channel = Channel

    # Let Kombu parse pgmq://user:pass@host:port/db into connection
    # components.  (Transports that set can_parse_url=True must parse the
    # URL themselves, e.g. sqlalchemy passes hostname to create_engine.)
    can_parse_url = False
    default_port = DEFAULT_PORT
    driver_type = 'pgmq'
    driver_name = 'pgmq'

    if psycopg:
        connection_errors = virtual.Transport.connection_errors + (
            psycopg.OperationalError,
            psycopg.InterfaceError,
        )

    def __init__(self, client, **kwargs):
        if PGMQueue is None:
            raise ImportError(
                'PGMQ transport requires the pgmq library: '
                'pip install kombu[pgmq]'
            )

        super().__init__(client, **kwargs)
        self._pgmq_client: PGMQueue | None = None
        self._notify_waiter: _NotifyWaiter | None = None

    def _get_pgmq_client(self) -> PGMQueue:
        if self._pgmq_client is None:
            self._pgmq_client = self._create_pgmq_client()
        return self._pgmq_client

    def _register_notify_queue(self, queue_name: str) -> None:
        if not self.client.transport_options.get('use_notify'):
            return
        waiter = self._get_notify_waiter()
        waiter.register(queue_name)

    def _get_notify_waiter(self) -> _NotifyWaiter:
        if self._notify_waiter is None:
            self._notify_waiter = _NotifyWaiter(
                self._get_pgmq_client().config.dsn)
        return self._notify_waiter

    def drain_events(self, connection, timeout=None):
        time_start = monotonic()
        get = self.cycle.get
        polling_interval = self.polling_interval
        if timeout and polling_interval and polling_interval > timeout:
            polling_interval = timeout
        use_notify = bool(self.client.transport_options.get('use_notify'))
        while 1:
            try:
                get(self._deliver, timeout=timeout)
            except Empty:
                if timeout is not None and monotonic() - time_start >= timeout:
                    raise socket.timeout()
                wait_time = polling_interval
                if timeout is not None:
                    remaining = timeout - (monotonic() - time_start)
                    if wait_time is None or wait_time > remaining:
                        wait_time = remaining
                if use_notify and wait_time:
                    self._get_notify_waiter().wait(wait_time)
                elif polling_interval is not None:
                    sleep(polling_interval)
            else:
                break

    def _create_pgmq_client(self) -> PGMQueue:
        conninfo = self.client
        transport_options = conninfo.transport_options
        visibility_timeout = (
            transport_options.get('visibility_timeout') or
            Channel.default_visibility_timeout
        )

        if conn_string := transport_options.get('conn_string'):
            return PGMQueue(
                conn_string=conn_string,
                vt=visibility_timeout,
                init_extension=transport_options.get(
                    'init_extension', Channel.default_init_extension),
                pool_size=transport_options.get(
                    'pool_size', Channel.default_pool_size),
            )

        database = conninfo.virtual_host
        if database in ('/', None, ''):
            database = 'postgres'
        elif database.startswith('/'):
            database = database[1:]

        return PGMQueue(
            host=conninfo.hostname or 'localhost',
            port=str(conninfo.port or self.default_port),
            database=database,
            username=conninfo.userid or 'postgres',
            password=conninfo.password or '',
            vt=visibility_timeout,
            init_extension=transport_options.get(
                'init_extension', Channel.default_init_extension),
            pool_size=transport_options.get(
                'pool_size', Channel.default_pool_size),
        )

    def establish_connection(self):
        if not self.verify_connection(self):
            raise OperationalError('Could not connect to PGMQ')
        self._avail_channels.append(self.create_channel(self))
        return self

    def close_connection(self, connection) -> None:
        try:
            super().close_connection(connection)
        finally:
            if self._notify_waiter is not None:
                self._notify_waiter.close()
                self._notify_waiter = None
            if self._pgmq_client is not None:
                pool = getattr(self._pgmq_client, 'pool', None)
                if pool is not None:
                    pool.close()
                self._pgmq_client = None

    def verify_connection(self, connection) -> bool:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter('ignore', UserWarning)
                self._get_pgmq_client().list_queues()
            return True
        except Exception:
            return False

    def driver_version(self) -> str:
        import pgmq as pgmq_module
        return getattr(pgmq_module, '__version__', 'Unknown')

    @classmethod
    def as_uri(cls, uri: str, include_password: bool = False,
               mask: str = '**') -> str:
        from kombu.utils.url import as_url, parse_url

        if '://' not in uri:
            return uri

        params = parse_url(uri)
        password = params['password'] if include_password else mask
        return as_url(
            'pgmq',
            params['hostname'],
            params['port'] or cls.default_port,
            params['userid'],
            password,
            params['virtual_host'],
        )
