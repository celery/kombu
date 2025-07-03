"""RocketMQ transport module for Kombu.

`RocketMQ`_ transport use `rocketmq-clients python`_ as the client.

To use this transport you must install the necessary dependencies. These
dependencies are available via PyPI and can be installed using the pip
command:

.. code-block:: console

    $ pip install kombu[rocketmq]

or to install the requirements manually:

.. code-block:: console

    $ pip install rocketmq-python-client==5.0.6


.. _`RocketMQ`: https://rocketmq.apache.org/
.. _`rocketmq-clients python`: https://github.com/apache/rocketmq-clients
.. _`RocketMQ Topic`: https://rocketmq.apache.org/docs/domainModel/02topic
.. _`RocketMQ Producer`: https://rocketmq.apache.org/docs/domainModel/04producer
.. _`RocketMQ Group`: https://rocketmq.apache.org/docs/domainModel/07consumergroup

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: Yes
* Supports Priority: No
* Supports TTL: No

Connection String
=================
Connection string has the following format:

.. code-block::

    rocketmq://[USER:PASSWORD@]ENDPOINT_ADDRESS[:PORT]

Concept
=================
As a pluggable transport, it uses RocketMQ to abstract certain AMQP
semantics with the trade-offs and limitations.

In RocketMQ, the two core concepts most closely related to publishing and consuming
are Topic and Consumer Group. These are used to simulate the AMQP protocol, which
introduces certain implementation and usage constraints:

* `RocketMQ Topic`_ is analogous to a Queue in AMQP, but it comes with stricter naming rules
  to avoid conflicts caused by duplicate names. For example, names like "amqp.queue"
  or "amqp-queue" may be considered conflicting, because we convert special characters
  to a dash.

* `RocketMQ Group`_ is used to support cluster-based consumption. Multiple consumer tags
  in AMQP can be logically grouped into a single Consumer Group, by default, we use
  CID-{queue} as consumer group of queues. When configuring multiple queues, be sure to maintain
  consistency of the `subscription relationship <https://rocketmq.apache.org/docs/domainModel/09subscription>`_.

* Queues are implicitly durable, ant they do not support features such as reply_to or
  exclusive queues.

Transport Options
=================

* ``request_timeout``: time in seconds, default 3
* ``tls_enable``: default False
* ``max_send_attempts``: max attempts when publishing, default 3
* ``separate_queue_producer``: (bool) whether to use separate producer for each queue, default False
* ``invisible_time``: time in seconds, how long a 1st delivered message will be invisible before re-delivery
* ``await_duration``: time in seconds, long polling support, wait to receive incoming messages
* ``backoff_policy``: the message visibility timeout between each re-delivery, default is
            [10, 30, 60, 90, 120, 240, 300, 360, 720]
* ``consumer_options``: specified group config

Connection Example
===================

.. code-block:: python

    with Connection('rocketmq://ak:sk@endpoint:port//') as conn:
    # or
    with Connection('rocketmq://ak:sk@endpoint:port//', transport_options={
                    'backoff_policy': [30, 60, 90, 120, 240, 300],
                    'consumer_options': {
                        'global_group_id': 'CID-ALL-IN-ONE',
                        'group_format': 'CID-{}',
                        'topic_config': {
                            'topic_xx': {
                                'group_id': 'CID-xx',
                                'filter_exp': 'exp'
                            }
                        }
                    }
                }) as conn:
    #  The group_format and global_group_id are exclusive, and global_group_id has higher priority.
    #  If topic config exists, use its specified group, and others use global or formatted one.

"""

from __future__ import annotations

import string
import threading
from dataclasses import dataclass
from queue import Empty

from kombu.connection import Connection
from kombu.exceptions import KombuError
from kombu.log import get_logger
from kombu.transport import virtual
from kombu.utils import scheduling
from kombu.utils.encoding import safe_str, str_to_bytes
from kombu.utils.functional import retry_over_time
from kombu.utils.json import dumps, loads

try:
    import rocketmq
    from rocketmq import ClientConfiguration, Credentials, FilterExpression
    from rocketmq import Message as RocketmqMessage
    from rocketmq import Producer, SimpleConsumer
    from rocketmq.grpc_protocol import FilterType
    from rocketmq.v5.util import Misc
    DEFAULT_FILTER_EXP = FilterExpression.TAG_EXPRESSION_SUB_ALL
except ImportError:
    rocketmq = None


logger = get_logger(__name__)

DEFAULT_PORT = 8080
GLOBAL_PRODUCER_KEY = '__GLOBAL_ROCKETMQ_PRODUCER_KEY_IN_KOMBU__'
MAX_INVISIBLE_TIME = 12 * 3600
MIN_INVISIBLE_TIME = 10
DEFAULT_INVISIBLE_TIME = 120
MAX_POLL_WAIT_TIME = 18
MIN_POLL_WAIT_TIME = 4
DEFAULT_POLL_WAIT_TIME = 10
MAX_BATCH_COUNT = 32
DEFAULT_BACKOFF_POLICY = [10, 30, 60, 90, 120, 240, 300, 360, 720]

CHARS_REPLACE_TABLE = {
    ord(c): ord('-') for c in string.punctuation if c not in ('_', '-')
}

ACK_ENDPOINTS_KEY = '_rmq_endpoints_'
ACK_MESSAGE_ID_KEY = '_rmq_message_id_'
ACK_GROUP_KEY = '_rmq_consumer_group_'
ACK_TOPIC_KEY = '_rmq_topic_'
ACK_HANDLE_KEY = '_rmq_receipt_handle_'
DELIVERY_ATTEMPT_KEY = '_rmq_delivery_attempt_'
AMQP_QUEUE_KEY = '_amqp_queue_'

DEFAULT_GROUP_FMT = 'CID-{}'
GLOBAL_GROUP_ID_KEY = 'global_group_id'
GROUP_FORMAT_KEY = 'group_format'
TOPIC_CONFIG_KEY = 'topic_config'
TOPIC_GROUP_ID_KEY = 'group_id'
TOPIC_FILTER_EXP_KEY = 'filter_exp'


class QoS(virtual.QoS):
    """RocketMQ Ack Emulation.

    `prefetch_count` default to 0, which disables prefetch limits,
    and the object can hold an arbitrary number of messages.
    """

    def __init__(self, channel, prefetch_count=0):
        self._not_yet_acked = {}
        super().__init__(channel, prefetch_count)

    def can_consume(self):
        """Return True if the :class:`Channel` can consume more messages.

        Used to ensure the client adheres to currently active prefetch
        limits.

        :returns: True, if this QoS object can accept more messages
            without violating the prefetch_count. If prefetch_count is 0,
            can_consume will always return True.
        :rtype: bool
        """
        return not self.prefetch_count or len(self._not_yet_acked) < self.prefetch_count

    def can_consume_max_estimate(self):
        """Return the maximum number of messages allowed to be polled from queue.

        Returns an estimated number of messages that a consumer may be allowed
        to consume at once from the broker. This is used for services where
        bulk 'get message' calls are preferred to many individual 'get message'
        calls - like SQS, gcpubsub, RocketMQ those support this feature.

        :returns: The number of estimated messages that can be fetched
                  without violating the prefetch_count. If ``prefetch_count``
                  is 0, then this method returns MAX_BATCH_COUNT(32).
        :rtype: int
        """
        if self.prefetch_count:
            return min(max(self.prefetch_count - len(self._not_yet_acked), 1), MAX_BATCH_COUNT)
        else:
            return MAX_BATCH_COUNT

    def append(self, message, delivery_tag):
        """Append message to the list of un-acked messages.

        Add a message, referenced by the delivery_tag, for ACKing, rejecting,
        or getting later. Messages are saved into a dict by delivery_tag.
        In RocketMQ, the lifecycle of message delivery is largely driven by the
        server, and this process is durable. Here we only need to temporarily
        store the messages for subsequent acknowledgment.
        """
        self._not_yet_acked[delivery_tag] = message

    def get(self, delivery_tag):
        return self._not_yet_acked[delivery_tag]

    def ack(self, delivery_tag):
        """Ack a message by delivery_tag.

        Positive acknowledgement in manual ack mode(no_ack=False).
        Note that a message will still be redelivered if we ack
        a message after invisible timeout.
        :raises KombuError: if operation fails after 2 attempts
        """
        if delivery_tag not in self._not_yet_acked:
            logger.warning(f'ack message not found. {delivery_tag}')
            return
        message = self._not_yet_acked.pop(delivery_tag)
        rocketmq_message = _message_to_rocketmq_ack_message(message)
        consumer = self.channel._get_consumer(rocketmq_message.topic, passive=True)
        _ack_rocketmq_message(consumer, rocketmq_message, True)

    def reject(self, delivery_tag, requeue=False):
        """Reject a message by delivery tag.

        Negative ack. If requeue is True, the broker will requeue the delivery,
        then it can be consumed later, note that requeue does not guarantee that
        the message will always be delivered to a different consumer.
        We use a back-off policy to control message visibility timeout between each retry.
        If requeue is False, that message will be discarded, and Dead Letter Queue
        is not supported for now.
        :raises Exception: if operation fails
        """
        if delivery_tag not in self._not_yet_acked:
            logger.warn(f'reject message not found. {delivery_tag}')
            return
        if requeue:
            message = self._not_yet_acked.pop(delivery_tag)
            rocketmq_message = _message_to_rocketmq_ack_message(message)
            delivery_attempt = _safe_str_to_int(rocketmq_message.properties[DELIVERY_ATTEMPT_KEY])
            next_invisible_time = self.channel.backoff_policy[
                min(delivery_attempt, len(self.channel.backoff_policy) - 1)
            ]
            consumer = self.channel._get_consumer(message.delivery_info[AMQP_QUEUE_KEY], passive=True)
            if not consumer:
                return
            retry_over_time(
                lambda: consumer.change_invisible_duration_async(rocketmq_message,
                                                                 next_invisible_time), Exception,
                max_retries=1, interval_start=1, interval_step=0, timeout=6
            )
        else:
            self.ack(delivery_tag)

    def restore_unacked(self):
        """Restore all unacknowledged messages.

        Nothing to do as RocketMQ broker holds the un-acked messages.
        """
        pass

    def restore_unacked_once(self, stderr=None):
        """Restore all unacknowledged messages at shutdown/gc collect.

        Nothing to do as RocketMQ broker holds the un-acked messages,
        but we clear all un-acked messages in memory.
        """
        self._not_yet_acked.clear()

    def discard_message(self, topic):
        discard_tags = [
            tag for tag, message in self._not_yet_acked.items()
            if getattr(message, 'delivery_info', {}).get(ACK_TOPIC_KEY) == topic
        ]
        for tag in discard_tags:
            self._not_yet_acked.pop(tag, None)


class Channel(virtual.Channel):
    """RocketMQ Channel."""

    QoS = QoS
    do_restore = False
    supports_fanout = True

    max_send_attempts = 3
    separate_queue_producer = False
    invisible_time = DEFAULT_INVISIBLE_TIME
    await_duration = DEFAULT_POLL_WAIT_TIME
    backoff_policy = DEFAULT_BACKOFF_POLICY
    consumer_options = {}
    tls_enable = False
    request_timeout = 3

    from_transport_options = virtual.Channel.from_transport_options + (
        'max_send_attempts',
        'separate_queue_producer',
        'invisible_time',
        'await_duration',
        'backoff_policy',
        'consumer_options',
        'tls_enable',
        'request_timeout',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.mutex = threading.Lock()
        # dict of rocketmq topic to producer
        self._rocketmq_producers: dict[str, Producer] = {}
        # dict of rocketmq group to consumer
        self._rocketmq_consumers: dict[str, SimpleConsumer] = {}
        # to prevent duplicate subscribe()
        self._bound_topics = set()
        # rocket topic which no_ack is True
        self._auto_ack_topics = set()
        # dict of rocket topic to consumer config
        self._group_config: dict[str, ConsumerConfig] = {}
        self._check_options()

    def exchange_declare(self, exchange=None, type='direct', durable=False,
                         auto_delete=False, arguments=None,
                         nowait=False, passive=False):
        """Create a new exchange.

        Create an exchange of a specific type, and optionally have the
        exchange be durable. If an exchange of the requested name already
        exists, no action is taken and no exceptions are raised. Durable
        exchanges will survive a broker restart, non-durable exchanges will
        not.

        It is an in-memory concept here, so force set False.
        """
        durable = False
        auto_delete = False
        nowait = False
        super().exchange_declare(exchange, type, durable, auto_delete, arguments, nowait, passive)

    def basic_consume(self, queue, no_ack, callback, consumer_tag, **kwargs):
        """Start a consumer that reads from a queue.

        `RocketMQ Topic`_

        Simulate AMQP queue semantics using RocketMQ topics,
        messages are available later through a synchronous call to
        :meth:`Transport.drain_events`. RocketMQ consumer instance is lazily initialized,
        so map AMQP queues to RocketMQ topics and revert back when needed.

        Each consumer is referenced by a consumer_tag, which is provided by
        the caller of this method.

        This method sets up the callback onto the self.connection object in a
        dict keyed by queue name. :meth:`~Transport.drain_events` is
        responsible for calling that callback upon message receipt.

        Due to the above callback implementation of :class:`~kombu.transport.virtual.Transport`,
        multiple consumers of the same queue in one Connection is not supported,
        so it is necessary to check this condition in this method. To achieve concurrent
        consumption within a process, it is recommended to use different Connection instances.

        All messages received are added to the QoS object to be
        saved for asynchronous ack.

        :meth:`basic_consume` transforms the message object type prior to
        calling the callback. Initially the message comes in as a
        :class:`rocketmq.Message`. This method unpacks the payload
        of the :class:`rocketmq.Message` and creates a new object of
        type self.Message.

        This method wraps the user delivered callback in a runtime-built
        function which provides the type transformation from
        :class:`rocketmq.Message` to
        :class:`~kombu.transport.virtual.Message`, and adds the message to
        the associated :class:`QoS` object for asynchronous ack if necessary.

        :param no_ack: If True, messages will not be saved for ack,
            but will be acked immediately. If False, then messages
            will be saved and can be acked later with a call to :meth:`basic_ack`.
        :type no_ack: bool
        :param callback: a callable that will be called when messages
            polled from the queue.
        :type callback: a callable object
        :param consumer_tag: a tag to reference the created consumer.
        This consumer_tag is needed to cancel the consumer.
        The consumer_tag in Kombu and the Consumer Group in RocketMQ
        represent different concepts and should be clearly distinguished.
        """
        if queue in self._active_queues:
            logger.error(f'queue: {queue} already registered')
            raise KombuError(f'queue: {queue} already registered on the same channel/connection: {self}')
        super().basic_consume(
            queue, no_ack, callback, consumer_tag, **kwargs
        )

        topic = _queue_to_topic(queue)
        self.connection._callbacks[topic] = self.connection._callbacks[queue]
        self.connection._topic_to_queue[topic] = queue
        if no_ack:
            self._auto_ack_topics.add(topic)

    def basic_cancel(self, consumer_tag):
        """Cancel consumer by consumer tag.

        Request the consumer stops reading messages from its queue.
        This method also cleans up all lingering references of the consumer.

        :param consumer_tag: The tag which refers to the consumer to be
            cancelled. Originally specified when the consumer was created
            as a parameter to :meth:`basic_consume`.
        """
        queue = self._tag_to_queue.get(consumer_tag, None)
        if queue:
            topic = _queue_to_topic(queue)
            self.connection._callbacks.pop(topic, None)
            self.connection._topic_to_queue.pop(topic, None)
            self._auto_ack_topics.discard(topic)
            consumer = self._get_consumer(queue, True)
            if consumer:
                consumer.unsubscribe(topic)
            self._bound_topics.discard(topic)
            self.qos.discard_message(topic)
            self._group_config.pop(topic, None)
        super().basic_cancel(consumer_tag)

    def _queue_bind(self, exchange, routing_key, pattern, queue):
        logger.debug(f'mock operation of _queue_bind() for fanout. '
                     f'{exchange}, {routing_key}, {pattern} {queue}')

    def _reset_cycle(self):
        """Reset the consume cycle, override to enable batch get.

        FairCycle: object that points to our _get_bulk() method rather than
        the standard _get() method. This allows for multiple messages to be
        returned at once from queue based on the prefetch limit.
        """
        self._cycle = scheduling.FairCycle(
            self._get_bulk, self._active_queues, Empty,
        )

    def _get_producer(self, queue, passive=False) -> Producer | None:
        """Create/get a :class:`rocketmq.Producer` instance for the given queue.

        `RocketMQ Producer <https://rocketmq.apache.org/docs/domainModel/04producer>`_

        :param queue: the AMQP queue name
        """
        real_topic = _queue_to_topic(queue)
        topic = real_topic if self.separate_queue_producer else GLOBAL_PRODUCER_KEY
        if topic in self._rocketmq_producers:
            return self._rocketmq_producers[topic]
        if passive:
            return None
        with self.mutex:
            if topic not in self._rocketmq_producers:
                credentials = Credentials(self.connection_info.userid, self.connection_info.password)
                client_config = ClientConfiguration(self.connection_info.host, credentials,
                                                    request_timeout=self.request_timeout)
                producer = Producer(client_config, [real_topic], tls_enable=self.tls_enable)
                producer.MAX_SEND_ATTEMPTS = self.max_send_attempts
                producer.startup()
                self._rocketmq_producers[topic] = producer
            return self._rocketmq_producers[topic]

    def _get_consumer(self, queue, passive=False) -> SimpleConsumer | None:
        """Create/get a :class:`rocketmq.SimpleConsumer` instance for the given queue.

        `RocketMQ group <https://rocketmq.apache.org/docs/domainModel/07consumergroup>`_

        :param queue: the AMQP queue name
        """
        topic = _queue_to_topic(queue)
        group_config = self._get_consumer_config(topic)
        group = group_config.consumer_group
        if group not in self._rocketmq_consumers:
            if passive:
                return None
            with self.mutex:
                if group not in self._rocketmq_consumers:
                    credentials = Credentials(self.connection_info.userid, self.connection_info.password)
                    client_config = ClientConfiguration(self.connection_info.host, credentials,
                                                        request_timeout=self.request_timeout)
                    consumer = SimpleConsumer(client_config, group, await_duration=self.await_duration,
                                              tls_enable=self.tls_enable)
                    # must start before submitting subscription
                    consumer.startup()
                    self._rocketmq_consumers[group] = consumer
        consumer = self._rocketmq_consumers[group]

        if topic in self._bound_topics:
            return consumer

        def _subscribe():
            consumer.subscribe(topic, FilterExpression(expression=group_config.filter_exp))
            self._bound_topics.add(topic)

        retry_over_time(
            _subscribe, Exception, max_retries=4, interval_max=4, timeout=20
        )
        return consumer

    def _get_consumer_config(self, topic) -> ConsumerConfig:
        """Define the consumer group config of topic.

        `RocketMQ Subscription <https://rocketmq.apache.org/docs/domainModel/09subscription>`_

        A consumer associated with a specific group can subscribe to one or more RocketMQ topics,
        thus we can use a global or separate Consumer for each topic.
        """
        if topic in self._group_config:
            return self._group_config[topic]

        consumer_options = self.consumer_options or {}
        topic_config = consumer_options.get(TOPIC_CONFIG_KEY, {})
        group_config = ConsumerConfig()

        if topic in topic_config:
            group_config.consumer_group = topic_config[topic].get(TOPIC_GROUP_ID_KEY)
            group_config.filter_exp = topic_config[topic].get(TOPIC_FILTER_EXP_KEY, DEFAULT_FILTER_EXP)
        elif GLOBAL_GROUP_ID_KEY in consumer_options:
            group_config.consumer_group = consumer_options[GLOBAL_GROUP_ID_KEY]
        elif GROUP_FORMAT_KEY in consumer_options:
            group_config.consumer_group = consumer_options[GROUP_FORMAT_KEY].format(topic)
        else:
            group_config.consumer_group = DEFAULT_GROUP_FMT.format(topic)
        if not group_config.consumer_group:
            raise ValueError('group is None or blank')
        self._group_config[topic] = group_config
        return group_config

    def _put(self, queue, message, **kwargs):
        """Synchronously send a single message onto a queue.

        An internal method which synchronously sends a single message onto
        a given queue. The whole message is regarded as the payload of
        RocketMQ message.

        External calls for put functionality should be done using
        :meth:`basic_publish`.

        :param message: The message to be sent as prepared by
            :meth:`basic_publish`.
        :type message: dict
        :raises rocketmq.v5.exception.ClientException: if operation fails
        """
        topic = _queue_to_topic(queue)
        producer = self._get_producer(queue)
        rocketmq_message = RocketmqMessage()
        rocketmq_message.topic = topic
        rocketmq_message.body = str_to_bytes(dumps(message))
        producer.send(rocketmq_message)

    def _put_fanout(self, exchange, message, routing_key, **kwargs):
        """Simulate fanout delivery.

        It is a client-side implementation.
        :raises rocketmq.v5.exception.ClientException: if operation fails, it might be a partial error.
        """
        queues = {queue for (queue, exch, key) in self.state.bindings if exch == exchange}
        if not queues:
            return

        rocketmq_message = RocketmqMessage()
        rocketmq_message.body = str_to_bytes(dumps(message))
        for queue in queues:
            producer = self._get_producer(queue)
            rocketmq_message.topic = _queue_to_topic(queue)
            producer.send(rocketmq_message)

    def _get_bulk(self, queue, callback=None):
        """Try to retrieve multiple messages from ``queue`` synchronously.

        Where :meth:`_get` returns a single Payload object, this method returns a list
        of Payload objects.  The number of objects returned is determined by the total
        number of messages available in the queue and the number of messages the QoS
        object allows (based on the prefetch_count).

        No return value is expected. We call _deliver(), which is responsible for invoking
        the associated callback with the queue.

        Unlike SQS, due to limitations in the underlying Consumer.receive() implementation,
        the method cannot specify a topic during polling. Therefore, when a consumer is
        subscribed to multiple topics, the topic of the messages returned by a single poll
        is undetermined. As a result, a mapping and replacement step is required here to
        ensure that the correct callback is invoked.

        If the topic is configured as auto-ack, messages will be acked before delivery.
        """
        consumer = self._get_consumer(queue)
        max_count = self.qos.can_consume_max_estimate()

        messages = retry_over_time(
            lambda: consumer.receive(max_count, self.invisible_time), Exception,
            max_retries=1, interval_start=2, interval_step=0, timeout=self.await_duration * 2 + 3
        )

        if not messages:
            pass

        for message in messages:
            if message.topic in self._auto_ack_topics:
                _ack_rocketmq_message(consumer, message, False)
            amqp_queue = self.connection._topic_to_queue[message.topic]
            payload = _rocketmq_message_to_payload(message, consumer.consumer_group, amqp_queue)
            self.connection._deliver(payload, amqp_queue)

    def _get(self, queue, callback=None) -> dict:
        """Get a message from the queue synchronously.

        If a message is available, a payload object is returned. If no message is
        available, an Empty exception is raised.

        This is an internal method. External calls for get functionality should be
        done using :meth:`basic_get`. If the topic is configured as auto-ack, messages
        will be acked before delivery.

        :raises: Empty if no message is available.
        """
        consumer = self._get_consumer(queue)
        messages = retry_over_time(
            lambda: consumer.receive(1, self.invisible_time), Exception,
            max_retries=1, interval_start=2, interval_step=0, timeout=self.await_duration * 2 + 3
        )
        if not messages:
            raise Empty()

        message = messages.pop()
        if message.topic in self._auto_ack_topics:
            _ack_rocketmq_message(consumer, message, False)
        amqp_queue = self.connection._topic_to_queue[message.topic]
        return _rocketmq_message_to_payload(message, consumer.consumer_group, amqp_queue)

    def _delete(self, queue, *args, **kwargs):
        """Delete a queue and all messages on that queue.

        This is an internal method. External calls for queue delete
        functionality should be done using :meth:`queue_delete`.
        """
        logger.debug(f'mock operation of _delete({queue})')

    def _size(self, queue):
        """Get the number of messages in a queue specified by name."""
        return 0

    def _new_queue(self, queue, **kwargs):
        """Ensure a queue with given name exists.

        No queue returned as we mock _has_queue().
        """
        logger.debug(f'mock operation of _new_queue({queue})')

    def _purge(self, queue):
        """Purge all undelivered messages from a queue specified by name.

        This is an internal method. External calls for purge functionality
        should be done using :meth:`queue_purge`, perhaps delete topic or
        just reset offset, but do nothing for now.
        """
        logger.debug(f'mock operation of _purge({queue})')
        return 0

    def _has_queue(self, queue, **kwargs):
        """Determine if the broker has a queue specified by name.

        Since RocketMQ does not provide native queue management APIs, this method
        always returns True. This helps avoid premature failures and allows users
        to manually create required topics beforehand.
        """
        logger.debug(f'mock operation of _has_queue({queue})')
        return True

    def _restore(self, message):
        """Redeliver message to its original destination."""
        logger.debug(f'mock operation of _restore({message})')

    def _check_options(self):
        if self.invisible_time > MAX_INVISIBLE_TIME or self.invisible_time < MIN_INVISIBLE_TIME:
            self.invisible_time = DEFAULT_INVISIBLE_TIME
        if self.await_duration > MAX_POLL_WAIT_TIME or self.await_duration < MIN_POLL_WAIT_TIME:
            self.await_duration = DEFAULT_POLL_WAIT_TIME

    def close(self):
        """Clean all associated resources and close the Channel."""
        super().close()
        for producer in self._rocketmq_producers.values():
            producer.shutdown()
        self._rocketmq_producers = {}

        for consumer in self._rocketmq_consumers.values():
            consumer.shutdown()
        self._rocketmq_consumers = {}

        self._bound_topics.clear()
        self._auto_ack_topics.clear()
        self._group_config = {}
        self.qos.restore_unacked_once()

    @property
    def connection_info(self) -> Connection:
        return self.connection.client


class Transport(virtual.Transport):
    """RocketMQ Transport."""

    def as_uri(self, uri: str, include_password=False, mask='**') -> str:
        """Customise the display format of the URI."""
        pass

    Channel = Channel
    default_port = DEFAULT_PORT
    driver_type = 'rocketmq'
    driver_name = 'rocketmq'

    implements = virtual.Transport.implements.extend(
        asynchronous=False,
        exchange_type=frozenset([
            'direct',
            'topic',
            'fanout',
            # 'headers'
        ]),
        heartbeats=False,
    )

    def __init__(self, client, **kwargs):
        if rocketmq is None:
            raise ImportError('The rocketmq-python-client library is not installed')
        super().__init__(client, **kwargs)

        # Since RocketMQ topics and AMQP queues are not directly equivalent,
        # this mapping is used to find the corresponding callback before message delivery.
        self._topic_to_queue = {}

    def driver_version(self):
        return Misc.SDK_VERSION


@dataclass
class ConsumerConfig:
    """wrapper of group config."""

    consumer_group: str = ''
    filter_type: FilterType = FilterType.TAG
    filter_exp: str = DEFAULT_FILTER_EXP


def _safe_str_to_int(s, default=0):
    try:
        return int(s)
    except (ValueError, TypeError):
        return default


def _queue_to_topic(queue):
    """Format AMQP queue name into a valid RocketMQ topic name.

    `Constraints <https://rocketmq.apache.org/docs/introduction/03limits>`_

    Note:
    ----
        We add no prefix for now. And there is a hard constraint that two different queue
        names must not map to the same topic name, to avoid routing conflicts during message
        delivery. This ensures each AMQP queue corresponds to a unique RocketMQ topic.

    :param queue: The AMQP queue name.
    :type queue: str
    :return: A valid RocketMQ topic name.
    :rtype: str
    """
    return safe_str(queue).translate(CHARS_REPLACE_TABLE)


def _rocketmq_message_to_payload(message: RocketmqMessage,
                                 group: str,
                                 amqp_queue: str) -> dict:
    """Unpack a RocketMQ message into a payload dictionary and enrich delivery_info.

    This function deserializes the message body and appends metadata required for
    acknowledgment (e.g., message ID, receipt handle, topic, etc.) to the
    `delivery_info` dictionary. These fields are used later during message ack
    operations to identify and control the message lifecycle in RocketMQ.

    :param message: The raw RocketMQ message received from the broker.
    :type message: RocketmqMessage
    :param group: The consumer group associated with this message.
    :type group: str
    :param amqp_queue: The original AMQP queue name mapped from the RocketMQ topic.
    :type amqp_queue: str
    :return: A payload dictionary containing the message body and extended delivery info.
    :rtype: dict
    """
    payload = loads(message.body)
    payload['properties']['delivery_info'].update({
        ACK_ENDPOINTS_KEY: message.endpoints,
        ACK_MESSAGE_ID_KEY: message.message_id,
        ACK_GROUP_KEY: group,
        ACK_TOPIC_KEY: message.topic,
        ACK_HANDLE_KEY: message.receipt_handle,
        DELIVERY_ATTEMPT_KEY: str(message.delivery_attempt),
        AMQP_QUEUE_KEY: amqp_queue
    })
    return payload


def _message_to_rocketmq_ack_message(message: virtual.Message) -> RocketmqMessage:
    """Convert a Kombu message into a RocketMQ message for acknowledgment.

    This function constructs a minimal `RocketmqMessage` object using metadata
    stored in the `delivery_info` of the Kombu message. This is used to generate
    an equivalent RocketMQ message representation solely for the purpose of
    acknowledging or modifying message visibility.

    :param message: The Kombu message instance containing delivery metadata.
    :type message: kombu.transport.virtual.Message
    :return: A RocketMQ message object with enough metadata to perform ack operations.
    :rtype: RocketmqMessage
    """
    ack_message = RocketmqMessage()
    ack_message.endpoints = message.delivery_info[ACK_ENDPOINTS_KEY]
    ack_message.message_id = message.delivery_info[ACK_MESSAGE_ID_KEY]
    ack_message.topic = message.delivery_info[ACK_TOPIC_KEY]
    ack_message.receipt_handle = message.delivery_info[ACK_HANDLE_KEY]
    # no setter
    ack_message.add_property(DELIVERY_ATTEMPT_KEY, message.delivery_info[DELIVERY_ATTEMPT_KEY])
    return ack_message


def _ack_rocketmq_message(consumer: SimpleConsumer, message: RocketmqMessage,
                          raise_exception=True, max_retries=1):
    """Acknowledge a RocketMQ message using the provided consumer.

    :raises KombuError: If acknowledgment fails and `raise_exception` is True.
    """
    if not consumer or not consumer.is_running:
        logger.warn(f'unexpected condition: consumer is None or not running. {message}')
        return
    try:
        retry_over_time(
            lambda: consumer.ack(message), Exception,
            max_retries=max_retries, interval_start=1, interval_step=0, timeout=6
        )
    except Exception as e:
        if raise_exception:
            raise KombuError('rocketmq ack message exception') from e
        logger.error(f'ack message failed after {max_retries + 1} times. {message}')
