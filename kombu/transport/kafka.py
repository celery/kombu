"""
kombu.transport.kafka
=====================

Kafka transport.

:copyright: (c) 2010 - 2013 by Mahendra M.
:license: BSD, see LICENSE for more details.

**Synopsis**

Connects to kafka (0.8.x) as <server>:<port>/<vhost>
The <vhost> becomes the group for all the clients. So we can use
it like a vhost

It is recommended that the queue be created in advance, by specifying the
number of partitions. The partition configuration determines how many
consumers can fetch data in parallel

**Limitations**
* The client API needs to modified to fetch data only from a single
  partition. This can be used for effective load balancing also.

"""

from __future__ import absolute_import

import calendar
import datetime
import time
from anyjson import loads, dumps

# from kombu.exceptions import StdConnectionError, StdChannelError
from kombu.five import Empty
from kombu.transport import virtual
from kombu.utils.compat import OrderedDict

try:
    import pykafka
    from pykafka import KafkaClient
    from pykafka.protocol import PartitionOffsetCommitRequest

    KAFKA_CONNECTION_ERRORS = ()
    KAFKA_CHANNEL_ERRORS = ()

except ImportError:
    kafka = None                                          # noqa
    KAFKA_CONNECTION_ERRORS = KAFKA_CHANNEL_ERRORS = ()   # noqa

DEFAULT_PORT = 9092
DEFAULT_KAFKA_GROUP = 'kombu-consumer-group'

__author__ = 'Mahendra M <mahendra.m@gmail.com>'


class QoS(object):
    def __init__(self, channel):
        self.prefetch_count = 1
        self._channel = channel
        self._not_yet_acked = OrderedDict()

    def can_consume(self):
        """Returns True if the :class:`Channel` can consume more messages, else
        False.

        :returns: True, if this QoS object can accept a message.
        :rtype: bool
        """
        return not self.prefetch_count or len(self._not_yet_acked) < self\
            .prefetch_count

    def can_consume_max_estimate(self):
        if self.prefetch_count:
            return self.prefetch_count - len(self._not_yet_acked)
        else:
            return 1

    def append(self, message, delivery_tag):
        self._not_yet_acked[delivery_tag] = message

    def get(self, delivery_tag):
        return self._not_yet_acked[delivery_tag]

    def ack(self, delivery_tag):
        message = self._not_yet_acked.pop(delivery_tag)
        exchange = message.properties['delivery_info']['exchange']
        consumer = self._channel._get_consumer(exchange)
        consumer.commit_offsets()

    def reject(self, delivery_tag, requeue=False):
        """Reject a message by delivery tag.

        If requeue is True, then the last consumed message is reverted so it'll
        be refetched on the next attempt. If False, that message is consumed and
        ignored.
        """
        if requeue:
            message = self._not_yet_acked.pop(delivery_tag)
            consumer = self._channel._get_consumer(message.exchange)
            consumer.set_offset(consumer.last_offset_consumed)
        else:
            self.ack(delivery_tag)

    def restore_unacked_once(self):
        pass


class Channel(virtual.Channel):
    QoS = QoS

    _client = None
    _kafka_group = None

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)

        self._kafka_consumers = {}
        self._kafka_producers = {}

    def fetch_offsets(self, client, topic, offset):
        """Fetch raw offset data from a topic.
        note: stolen from the pykafka cli
        :param client: KafkaClient connected to the cluster.
        :type client:  :class:`pykafka.KafkaClient`
        :param topic:  Name of the topic.
        :type topic:  :class:`pykafka.topic.Topic`
        :param offset: Offset to reset to. Can be earliest, latest or a
            datetime.  Using a datetime will reset the offset to the latest
            message published *before* the datetime.
        :type offset: :class:`pykafka.common.OffsetType` or
            :class:`datetime.datetime`
        :returns: {
                partition_id: :class:`pykafka.protocol.OffsetPartitionResponse`
            }
        """
        if offset.lower() == 'earliest':
            return topic.earliest_available_offsets()
        elif offset.lower() == 'latest':
            return topic.latest_available_offsets()
        else:
            offset = datetime.datetime.strptime(offset, "%Y-%m-%dT%H:%M:%S")
            offset = int(calendar.timegm(offset.utctimetuple())*1000)
            return topic.fetch_offset_limits(offset)

    def sanitize_queue_name(self, queue):
        """Need to sanitize the queue name, celery sometimes pushes in @
        signs"""
        return str(queue).replace('@', '')

    def _get_producer(self, queue):
        """Create/get a producer instance for the given topic/queue"""
        queue = self.sanitize_queue_name(queue)
        producer = self._kafka_producers.get(queue, None)
        if producer is None:
            producer = self.client.topics[queue].get_producer()
            self._kafka_producers[queue] = producer

        return producer

    def _get_consumer(self, queue):
        """Create/get a consumer instance for the given topic/queue"""
        queue = self.sanitize_queue_name(queue)
        consumer = self._kafka_consumers.get(queue, None)
        if consumer is None:
            topic = self.client.topics[queue]
            #consumer = topic.get_simple_consumer(
            consumer = topic.get_balanced_consumer(
                consumer_group=self._kafka_group,
                auto_commit_enable=False)
            self._kafka_consumers[queue] = consumer

        return consumer

    def _put(self, queue, message, **kwargs):
        """Put a message on the topic/queue"""
        queue = self.sanitize_queue_name(queue)
        producer = self._get_producer(queue)
        producer.produce(dumps(message))

    def _get(self, queue, **kwargs):
        """Get a message from the topic/queue"""
        consumer = self._get_consumer(queue)
        message = consumer.consume(block=False)

        if not message:
            raise Empty()

        return loads(message.value)

    def _purge(self, queue):
        """Purge all pending messages in the topic/queue, taken from the pykafka
        cli
        """
        queue = self.sanitize_queue_name(queue)

        # Don't auto-create topics.
        if queue not in self.client.topics:
            return 0

        topic = self.client.topics[queue]

        size = self._size(queue)

        # build offset commit requests
        offsets = topic.latest_available_offsets()
        tmsp = int(time.time() * 1000)
        reqs = [PartitionOffsetCommitRequest(queue,
                                             partition_id,
                                             res.offset[0],
                                             tmsp,
                                             'kombu')
                for partition_id, res in offsets.iteritems()]

        # Send them to the appropriate broker.
        broker = self.client.cluster.get_offset_manager(self._kafka_group)
        broker.commit_consumer_group_offsets(
            self._kafka_group, 1, 'kombu', reqs
        )

        return size

    def _delete(self, queue, *args, **kwargs):
        """Delete a queue/topic"""
        # We will just let it go through. There is no API defined yet
        # for deleting a queue/topic, need to be done through kafka itself
        pass

    def _size(self, queue):
        """Gets the number of pending messages in the topic/queue"""
        queue = self.sanitize_queue_name(queue)

        # Don't auto-create topics.
        if queue not in self.client.topics:
            return 0

        topic = self.client.topics[queue]

        latest = topic.latest_available_offsets()[0].offset[0]
        earliest = topic.earliest_available_offsets()[0].offset[0]
        return earliest + latest

    def _new_queue(self, queue, **kwargs):
        """Create a new queue if it does not exist"""
        # Just create a producer, the queue will be created automatically
        # Note: Please, please, please create the topic before hand,
        # preferably with high replication factor and loads of partitions
        queue = self.sanitize_queue_name(queue)
        self._get_producer(queue)

    def _has_queue(self, queue, **kwargs):
        """Check if a queue already exists"""
        queue = self.sanitize_queue_name(queue)

        client = self._open()

        if queue in client.topics:
            return True
        else:
            return False

    def _open(self):
        conninfo = self.connection.client
        port = conninfo.port or DEFAULT_PORT
        client = KafkaClient(
            hosts='{0}:{1}'.format(conninfo.hostname, int(port)),
            use_greenlets=False)
        return client

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
            self._kafka_group = self.connection.client.virtual_host[0:-1]
            # A kafka group must always be set.
            if not self._kafka_group:
                self._kafka_group = DEFAULT_KAFKA_GROUP
        return self._client

    def close(self):
        super(Channel, self).close()

        for producer in self._kafka_producers.itervalues():
            producer.stop()

        self._kafka_producers = {}

        for consumer in self._kafka_consumers.itervalues():
            consumer.stop()

        self._kafka_consumers = {}


class Transport(virtual.Transport):
    Channel = Channel

    default_port = DEFAULT_PORT

    # connection_errors = ( ) + KAFKA_CONNECTION_ERRORS
    # channel_errors = (socket.error) + KAFKA_CHANNEL_ERRORS

    driver_type = 'kafka'
    driver_name = 'pykafka'

    def __init__(self, *args, **kwargs):
        if pykafka is None:
            raise ImportError('The pykafka library is not installed')

        super(Transport, self).__init__(*args, **kwargs)

    def driver_version(self):
        return pykafka.__version__

    def establish_connection(self):
        return super(Transport, self).establish_connection()

    def close_connection(self, connection):
        return super(Transport, self).close_connection(connection)
