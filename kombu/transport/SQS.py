"""Amazon SQS Transport.

Amazon SQS transport module for Kombu.  This package implements an AMQP-like
interface on top of Amazons SQS service, with the goal of being optimized for
high performance and reliability.

The default settings for this module are focused now on high performance in
task queue situations where tasks are small, idempotent and run very fast.

SQS Features supported by this transport:
  Long Polling:
    http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/
      sqs-long-polling.html

    Long polling is enabled by setting the `wait_time_seconds` transport
    option to a number > 1.  Amazon supports up to 20 seconds.  This is
    enabled with 10 seconds by default.

  Batch API Actions:
   http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/
     sqs-batch-api.html

    The default behavior of the SQS Channel.drain_events() method is to
    request up to the 'prefetch_count' messages on every request to SQS.
    These messages are stored locally in a deque object and passed back
    to the Transport until the deque is empty, before triggering a new
    API call to Amazon.

    This behavior dramatically speeds up the rate that you can pull tasks
    from SQS when you have short-running tasks (or a large number of workers).

    When a Celery worker has multiple queues to monitor, it will pull down
    up to 'prefetch_count' messages from queueA and work on them all before
    moving on to queueB.  If queueB is empty, it will wait up until
    'polling_interval' expires before moving back and checking on queueA.
"""

from __future__ import absolute_import, unicode_literals

import socket
import string

from vine import transform, ensure_promise, promise

from kombu.async import get_event_loop
from kombu.async.aws import sqs as _asynsqs
from kombu.async.aws.ext import boto, exception
from kombu.async.aws.sqs.connection import AsyncSQSConnection, SQSConnection
from kombu.async.aws.sqs.ext import regions
from kombu.async.aws.sqs.message import Message
from kombu.five import Empty, range, string_t, text_t
from kombu.log import get_logger
from kombu.utils import scheduling
from kombu.utils.encoding import bytes_to_str, safe_str
from kombu.utils.json import loads, dumps
from kombu.utils.objects import cached_property

from . import virtual

logger = get_logger(__name__)

# dots are replaced by dash, all other punctuation
# replaced by underscore.
CHARS_REPLACE_TABLE = {
    ord(c): 0x5f for c in string.punctuation if c not in '-_.'
}
CHARS_REPLACE_TABLE[0x2e] = 0x2d  # '.' -> '-'

#: SQS bulk get supports a maximum of 10 messages at a time.
SQS_MAX_MESSAGES = 10


def maybe_int(x):
    """Try to convert x' to int, or return x' if that fails."""
    try:
        return int(x)
    except ValueError:
        return x


class Channel(virtual.Channel):
    """SQS Channel."""

    default_region = 'us-east-1'
    default_visibility_timeout = 1800  # 30 minutes.
    default_wait_time_seconds = 10  # up to 20 seconds max
    domain_format = 'kombu%(vhost)s'
    _asynsqs = None
    _sqs = None
    _queue_cache = {}
    _noack_queues = set()

    def __init__(self, *args, **kwargs):
        if boto is None:
            raise ImportError('boto is not installed')
        super(Channel, self).__init__(*args, **kwargs)

        # SQS blows up if you try to create a new queue when one already
        # exists but with a different visibility_timeout.  This prepopulates
        # the queue_cache to protect us from recreating
        # queues that are known to already exist.
        self._update_queue_cache(self.queue_name_prefix)

        self.hub = kwargs.get('hub') or get_event_loop()

    def _update_queue_cache(self, queue_name_prefix):
        try:
            queues = self.sqs.get_all_queues(prefix=queue_name_prefix)
        except exception.SQSError as exc:
            if exc.status == 403:
                raise RuntimeError(
                    'SQS authorization error, access_key={0}'.format(
                        self.sqs.access_key))
            raise
        else:
            self._queue_cache.update({
                queue.name: queue for queue in queues
            })

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        if no_ack:
            self._noack_queues.add(queue)
        if self.hub:
            self._loop1(queue)
        return super(Channel, self).basic_consume(
            queue, no_ack, *args, **kwargs
        )

    def basic_cancel(self, consumer_tag):
        if consumer_tag in self._consumers:
            queue = self._tag_to_queue[consumer_tag]
            self._noack_queues.discard(queue)
        return super(Channel, self).basic_cancel(consumer_tag)

    def drain_events(self, timeout=None):
        """Return a single payload message from one of our queues.

        Raises:
            Queue.Empty: if no messages available.
        """
        # If we're not allowed to consume or have no consumers, raise Empty
        if not self._consumers or not self.qos.can_consume():
            raise Empty()

        # At this point, go and get more messages from SQS
        self._poll(self.cycle, self.connection._deliver, timeout=timeout)

    def _reset_cycle(self):
        """Reset the consume cycle.

        Returns:
            FairCycle: object that points to our _get_bulk() method
                rather than the standard _get() method.  This allows for
                multiple messages to be returned at once from SQS (
                based on the prefetch limit).
        """
        self._cycle = scheduling.FairCycle(
            self._get_bulk, self._active_queues, Empty,
        )

    def entity_name(self, name, table=CHARS_REPLACE_TABLE):
        """Format AMQP queue name into a legal SQS queue name."""
        return text_t(safe_str(name)).translate(table)

    def _new_queue(self, queue, **kwargs):
        """Ensure a queue with given name exists in SQS."""
        if not isinstance(queue, string_t):
            return queue
        # Translate to SQS name for consistency with initial
        # _queue_cache population.
        queue = self.entity_name(self.queue_name_prefix + queue)

        # The SQS ListQueues method only returns 1000 queues.  When you have
        # so many queues, it's possible that the queue you are looking for is
        # not cached.  In this case, we could update the cache with the exact
        # queue name first.
        if queue not in self._queue_cache:
            self._update_queue_cache(queue)
        try:
            return self._queue_cache[queue]
        except KeyError:
            q = self._queue_cache[queue] = self.sqs.create_queue(
                queue, self.visibility_timeout,
            )
            return q

    def _delete(self, queue, *args, **kwargs):
        """Delete queue by name."""
        super(Channel, self)._delete(queue)
        self._queue_cache.pop(queue, None)

    def _put(self, queue, message, **kwargs):
        """Put message onto queue."""
        q = self._new_queue(queue)
        m = Message()
        m.set_body(dumps(message))
        q.write(m)

    def _message_to_python(self, message, queue_name, queue):
        payload = loads(bytes_to_str(message.get_body()))
        if queue_name in self._noack_queues:
            queue.delete_message(message)
        else:
            try:
                properties = payload['properties']
                delivery_info = payload['properties']['delivery_info']
            except KeyError:
                # json message not sent by kombu?
                delivery_info = {}
                properties = {'delivery_info': delivery_info}
                payload.update({
                    'body': bytes_to_str(message.get_body()),
                    'properties': properties,
                })
        # set delivery tag to SQS receipt handle
        delivery_info.update({
            'sqs_message': message, 'sqs_queue': queue,
        })
        properties['delivery_tag'] = message.receipt_handle
        return payload

    def _messages_to_python(self, messages, queue):
        """Convert a list of SQS Message objects into Payloads.

        This method handles converting SQS Message objects into
        Payloads, and appropriately updating the queue depending on
        the 'ack' settings for that queue.

        Arguments:
            messages (SQSMessage): A list of SQS Message objects.
            queue (str): Name representing the queue they came from.

        Returns:
            List: A list of Payload objects
        """
        q = self._new_queue(queue)
        return [self._message_to_python(m, queue, q) for m in messages]

    def _get_bulk(self, queue,
                  max_if_unlimited=SQS_MAX_MESSAGES, callback=None):
        """Try to retrieve multiple messages off ``queue``.

        Where :meth:`_get` returns a single Payload object, this method
        returns a list of Payload objects.  The number of objects returned
        is determined by the total number of messages available in the queue
        and the number of messages the QoS object allows (based on the
        prefetch_count).

        Note:
            Ignores QoS limits so caller is responsible for checking
            that we are allowed to consume at least one message from the
            queue.  get_bulk will then ask QoS for an estimate of
            the number of extra messages that we can consume.

        Arguments:
            queue (str): The queue name to pull from.

        Returns:
            List[Message]
        """
        # drain_events calls `can_consume` first, consuming
        # a token, so we know that we are allowed to consume at least
        # one message.
        maxcount = self._get_message_estimate()
        if maxcount:
            q = self._new_queue(queue)
            messages = q.get_messages(num_messages=maxcount)

            if messages:
                for msg in self._messages_to_python(messages, queue):
                    self.connection._deliver(msg, queue)
                return
        raise Empty()

    def _get(self, queue):
        """Try to retrieve a single message off ``queue``."""
        q = self._new_queue(queue)
        messages = q.get_messages(num_messages=1)
        if messages:
            return self._messages_to_python(messages, queue)[0]
        raise Empty()

    def _loop1(self, queue, _=None):
        self.hub.call_soon(self._schedule_queue, queue)

    def _schedule_queue(self, queue):
        if queue in self._active_queues:
            if self.qos.can_consume():
                self._get_bulk_async(
                    queue, callback=promise(self._loop1, (queue,)),
                )
            else:
                self._loop1(queue)

    def _get_message_estimate(self, max_if_unlimited=SQS_MAX_MESSAGES):
        maxcount = self.qos.can_consume_max_estimate()
        return min(
            max_if_unlimited if maxcount is None else max(maxcount, 1),
            max_if_unlimited,
        )

    def _get_bulk_async(self, queue,
                        max_if_unlimited=SQS_MAX_MESSAGES, callback=None):
        maxcount = self._get_message_estimate()
        if maxcount:
            return self._get_async(queue, maxcount, callback=callback)
        # Not allowed to consume, make sure to notify callback..
        callback = ensure_promise(callback)
        callback([])
        return callback

    def _get_async(self, queue, count=1, callback=None):
        q = self._new_queue(queue)
        return self._get_from_sqs(
            q, count=count, connection=self.asynsqs,
            callback=transform(self._on_messages_ready, callback, q, queue),
        )

    def _on_messages_ready(self, queue, qname, messages):
        if messages:
            callbacks = self.connection._callbacks
            for raw_message in messages:
                message = self._message_to_python(raw_message, qname, queue)
                callbacks[qname](message)

    def _get_from_sqs(self, queue,
                      count=1, connection=None, callback=None):
        """Retrieve and handle messages from SQS.

        Uses long polling and returns :class:`~vine.promises.promise`.
        """
        connection = connection if connection is not None else queue.connection
        return connection.receive_message(
            queue, number_messages=count,
            wait_time_seconds=self.wait_time_seconds,
            callback=callback,
        )

    def _restore(self, message,
                 unwanted_delivery_info=('sqs_message', 'sqs_queue')):
        for unwanted_key in unwanted_delivery_info:
            # Remove objects that aren't JSON serializable (Issue #1108).
            message.delivery_info.pop(unwanted_key, None)
        return super(Channel, self)._restore(message)

    def basic_ack(self, delivery_tag, multiple=False):
        delivery_info = self.qos.get(delivery_tag).delivery_info
        try:
            queue = delivery_info['sqs_queue']
        except KeyError:
            pass
        else:
            queue.delete_message(delivery_info['sqs_message'])
        super(Channel, self).basic_ack(delivery_tag)

    def _size(self, queue):
        """Return the number of messages in a queue."""
        return self._new_queue(queue).count()

    def _purge(self, queue):
        """Delete all current messages in a queue."""
        q = self._new_queue(queue)
        # SQS is slow at registering messages, so run for a few
        # iterations to ensure messages are deleted.
        size = 0
        for i in range(10):
            size += q.count()
            if not size:
                break
        q.clear()
        return size

    def close(self):
        super(Channel, self).close()
        for conn in (self._sqs, self._asynsqs):
            if conn:
                try:
                    conn.close()
                except AttributeError as exc:  # FIXME ???
                    if "can't set attribute" not in str(exc):
                        raise

    def _get_regioninfo(self, regions):
        if self.regioninfo:
            return self.regioninfo
        if self.region:
            for _r in regions:
                if _r.name == self.region:
                    return _r

    def _aws_connect_to(self, fun, regions):
        conninfo = self.conninfo
        region = self._get_regioninfo(regions)
        is_secure = self.is_secure if self.is_secure is not None else True
        port = self.port if self.port is not None else conninfo.port
        return fun(region=region,
                   aws_access_key_id=conninfo.userid,
                   aws_secret_access_key=conninfo.password,
                   is_secure=is_secure,
                   port=port)

    @property
    def sqs(self):
        if self._sqs is None:
            self._sqs = self._aws_connect_to(SQSConnection, regions())
        return self._sqs

    @property
    def asynsqs(self):
        if self._asynsqs is None:
            self._asynsqs = self._aws_connect_to(
                AsyncSQSConnection, _asynsqs.regions(),
            )
        return self._asynsqs

    @property
    def conninfo(self):
        return self.connection.client

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @cached_property
    def visibility_timeout(self):
        return (self.transport_options.get('visibility_timeout') or
                self.default_visibility_timeout)

    @cached_property
    def queue_name_prefix(self):
        return self.transport_options.get('queue_name_prefix', '')

    @cached_property
    def supports_fanout(self):
        return False

    @cached_property
    def region(self):
        return self.transport_options.get('region') or self.default_region

    @cached_property
    def regioninfo(self):
        return self.transport_options.get('regioninfo')

    @cached_property
    def is_secure(self):
        return self.transport_options.get('is_secure')

    @cached_property
    def port(self):
        return self.transport_options.get('port')

    @cached_property
    def wait_time_seconds(self):
        return self.transport_options.get('wait_time_seconds',
                                          self.default_wait_time_seconds)


class Transport(virtual.Transport):
    """SQS Transport."""

    Channel = Channel

    polling_interval = 1
    wait_time_seconds = 0
    default_port = None
    connection_errors = (
        virtual.Transport.connection_errors +
        (exception.SQSError, socket.error)
    )
    channel_errors = (
        virtual.Transport.channel_errors + (exception.SQSDecodeError,)
    )
    driver_type = 'sqs'
    driver_name = 'sqs'

    implements = virtual.Transport.implements.extend(
        async=True,
        exchange_type=frozenset(['direct']),
    )
