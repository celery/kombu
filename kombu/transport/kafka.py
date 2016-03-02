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

import socket

from json import loads, dumps
import copy

# from kombu.exceptions import StdConnectionError, StdChannelError
from kombu.five import Empty

from . import virtual

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

__author__ = 'Mahendra M <mahendra.m@gmail.com>'


class Channel(virtual.Channel):

    _client = None
    _kafka_group = None
    _kafka_consumers = {}
    _kafka_producers = {}

    def fetch_offsets(self, client, topic, offset):
        """Fetch raw offset data from a topic.
        note: stolen from the pykafka cli
        :param client: KafkaClient connected to the cluster.
        :type client:  :class:`pykafka.KafkaClient`
        :param topic:  Name of the topic.
        :type topic:  :class:`pykafka.topic.Topic`
        :param offset: Offset to reset to. Can be earliest, latest or a datetime.
            Using a datetime will reset the offset to the latest message published
            *before* the datetime.
        :type offset: :class:`pykafka.common.OffsetType` or
            :class:`datetime.datetime`
        :returns: {partition_id: :class:`pykafka.protocol.OffsetPartitionResponse`}
        """
        if offset.lower() == 'earliest':
            return topic.earliest_available_offsets()
        elif offset.lower() == 'latest':
            return topic.latest_available_offsets()
        else:
            offset = dt.datetime.strptime(offset, "%Y-%m-%dT%H:%M:%S")
            offset = int(calendar.timegm(offset.utctimetuple())*1000)
            return topic.fetch_offset_limits(offset)


    def sanitize_queue_name(self, queue):
        """Need to sanitize the queue name, celery sometimes pushes in @ signs"""
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
        """
        Create/get a consumer instance for the given topic/queue
        """
        queue = self.sanitize_queue_name(queue)
        consumer = self._kafka_consumers.get(queue, None)
        if consumer is None:
            consumer = self.client.topics[queue].get_simple_consumer(consumer_group=self._kafka_group,
                                                                     auto_commit_enable=True,
                                                                     auto_commit_interval_ms=5000,
                                                                     queued_max_messages=10)
            self._kafka_consumers[queue] = consumer

        return consumer

    def _put(self, queue, message, **kwargs):
        """Put a message on the topic/queue"""
        queue = self.sanitize_queue_name(queue)
        producer = self._get_producer(queue)
        producer.produce(dumps(message))

    def _get(self, queue, **kwargs):
        """Get a message from the topic/queue"""
        queue = self.sanitize_queue_name(queue)
        consumer = self._get_consumer(queue)
        message = consumer.consume(block=False)

        if not message:
            raise Empty()

        return loads(message.value)

    def _purge(self, queue):
        """Purge all pending messages in the topic/queue, taken from the pykafka cli"""
        queue = self.sanitize_queue_name(queue)
        #build offset commit requests
        offsets = self.fetch_offsets(self.client, queue, 0)

        tmsp = int(time.time() * 1000)
        reqs = [PartitionOffsetCommitRequest(queue,
                                             partition_id,
                                             res.offset[0],
                                             tmsp,
                                             'kafka-tools')
                for partition_id, res in offsets.iteritems()]

        # Send them to the appropriate broker.
        broker = self.client.cluster.get_offset_manager(self._kafka_group)
        broker.commit_consumer_group_offsets(
            args.consumer_group, 1, 'kafka-tools', reqs
        )

    def _delete(self, queue, *args, **kwargs):
        """Delete a queue/topic"""
        # We will just let it go through. There is no API defined yet
        # for deleting a queue/topic, need to be done through kafka itself
        pass

    def _size(self, queue):
        """Gets the number of pending messages in the topic/queue"""
        queue = self.sanitize_queue_name(queue)
        consumer = self._get_consumer(queue)
        latest = consumer.topic.latest_available_offsets()[0].offset[0]
        earliest = consumer.topic.earliest_available_offsets()[0].offset[0]
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

        if client.topics[queue]:
            return True
        else:
            return False

    def _open(self):
        conninfo = self.connection.client
        port = conninfo.port or DEFAULT_PORT
        client = KafkaClient(hosts='{0}:{1}'.format(conninfo.hostname, int(port)),
                             use_greenlets=True)
        return client

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
            self._kafka_group = self.connection.client.virtual_host[0:-1]
        return self._client


class Transport(virtual.Transport):
    Channel = Channel
    polling_interval = 1
    default_port = DEFAULT_PORT
    # connection_errors = ( ) + KAFKA_CONNECTION_ERRORS
    # channel_errors = (socket.error) + KAFKA_CHANNEL_ERRORS
    driver_type = 'kafka'
    driver_name = 'kafka'

    def __init__(self, *args, **kwargs):
        if pykafka is None:
            raise ImportError('The pykafka library is not installed')

        super(Transport, self).__init__(*args, **kwargs)

    def driver_version(self):
        return pykafka.__version__
