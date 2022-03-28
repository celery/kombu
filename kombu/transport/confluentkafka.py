from collections import OrderedDict
from queue import Empty

from kombu.transport import virtual
from kombu.utils import cached_property
from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import dumps, loads

try:
    import confluent_kafka
    from confluent_kafka import Consumer, Producer, TopicPartition
    from confluent_kafka.admin import AdminClient, NewTopic

    KAFKA_CONNECTION_ERRORS = ()
    KAFKA_CHANNEL_ERRORS = ()

except ImportError:
    confluent_kafka = None
    KAFKA_CONNECTION_ERRORS = KAFKA_CHANNEL_ERRORS = ()

from kombu.log import get_logger

logger = get_logger(__name__)

DEFAULT_PORT = 9092


class NoBrokersAvailable(confluent_kafka.KafkaException):
    retriable = True


class Message(virtual.Message):
    """Message object."""

    def __init__(self, payload, channel=None, **kwargs):
        self.topic = payload.get('topic')
        super().__init__(payload, channel=channel, **kwargs)


class QoS(virtual.QoS):
    _not_yet_acked = OrderedDict()

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
        if delivery_tag not in self._not_yet_acked:
            return
        message = self._not_yet_acked.pop(delivery_tag)
        consumer = self.channel._get_consumer(message.topic)
        consumer.commit()

    def reject(self, delivery_tag, requeue=False):
        """Reject a message by delivery tag.
        If requeue is True, then the last consumed message is reverted so
        it'll be refetched on the next attempt.
        If False, that message is consumed and ignored.
        """
        if requeue:
            message = self._not_yet_acked.pop(delivery_tag)
            consumer = self.channel._get_consumer(message.topic)
            for assignment in consumer.assignment():
                topic_partition = TopicPartition(message.topic,
                                                 assignment.partition)
                [committed_offset] = consumer.committed([topic_partition])
                consumer.seek(committed_offset)
        else:
            self.ack(delivery_tag)

    def restore_unacked_once(self, stderr=None):
        pass


class Channel(virtual.Channel):
    QoS = QoS
    Message = Message

    default_wait_time_seconds = 5
    _client = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._kafka_consumers = {}
        self._kafka_producers = {}

        self._client = self._open()

    def sanitize_queue_name(self, queue):
        """Need to sanitize the queue name, celery sometimes pushes in @
        signs"""
        return str(queue).replace('@', '')

    def _get_producer(self, queue):
        """Create/get a producer instance for the given topic/queue"""
        queue = self.sanitize_queue_name(queue)
        producer = self._kafka_producers.get(queue, None)
        if producer is None:
            producer = Producer(self.common_config)
            self._kafka_producers[queue] = producer

        return producer

    def _get_consumer(self, queue):
        """Create/get a consumer instance for the given topic/queue"""
        queue = self.sanitize_queue_name(queue)
        consumer = self._kafka_consumers.get(queue, None)
        if consumer is None:
            consumer = Consumer(
                {**self.common_config,
                 'group.id':
                     self.transport_options.get('kafka_consumer_group') or
                     f"{queue}-consumer-group",
                 'auto.offset.reset': 'earliest',
                 'enable.auto.commit': False,
                 }
            )
            consumer.subscribe([queue])
            self._kafka_consumers[queue] = consumer

        return consumer

    def _put(self, queue, message, **kwargs):
        """Put a message on the topic/queue"""
        queue = self.sanitize_queue_name(queue)
        producer = self._get_producer(queue)
        producer.produce(queue, str_to_bytes(dumps(message)))
        producer.flush()

    def _get(self, queue, **kwargs):
        """Get a message from the topic/queue"""
        queue = self.sanitize_queue_name(queue)
        consumer = self._get_consumer(queue)
        message = None

        try:
            message = consumer.poll(self.wait_time_seconds)
        except StopIteration:
            pass

        if not message:
            raise Empty()

        error = message.error()
        if error:
            logger.error(error)
            raise Empty()

        return {**loads(message.value()), "topic": message.topic()}

    def _delete(self, queue, *args, **kwargs):
        """Delete a queue/topic"""
        queue = self.sanitize_queue_name(queue)
        self._kafka_consumers[queue].close()
        self._kafka_consumers.pop(queue)
        self.client.delete_topics([queue])

    def _size(self, queue):
        """Gets the number of pending messages in the topic/queue"""
        queue = self.sanitize_queue_name(queue)

        consumer = self._kafka_consumers.get(queue, None)
        if consumer is None:
            return 0

        size = 0
        for assignment in consumer.assignment():
            topic_partition = TopicPartition(queue, assignment.partition)
            (_, end_offset) = consumer.get_watermark_offsets(topic_partition)
            [committed_offset] = consumer.committed([topic_partition])
            size += end_offset - committed_offset.offset
        return size

    def _new_queue(self, queue, **kwargs):
        """Create a new topic if it does not exist"""
        queue = self.sanitize_queue_name(queue)
        if queue not in self.client.list_topics().topics:
            self.client.create_topics(
                new_topics=[NewTopic(
                    queue,
                    num_partitions=self.transport_options.get('num_partitions',
                                                              1),
                    replication_factor=self.transport_options.get(
                        'replication_factor', 1)
                )])

    def _has_queue(self, queue, **kwargs):
        """Check if a topic already exists"""
        queue = self.sanitize_queue_name(queue)
        return queue in self.client.list_topics().topics

    def _open(self):
        client = AdminClient(self.common_config)

        try:
            # seems to be the only way to check connection
            client.list_topics(timeout=self.wait_time_seconds)
        except confluent_kafka.KafkaException as e:
            raise NoBrokersAvailable(e)

        return client

    @property
    def client(self):
        if self._client is None:
            self._client = self._open()
        return self._client

    @property
    def transport_options(self):
        return self.connection.client.transport_options

    @property
    def conninfo(self):
        return self.connection.client

    @cached_property
    def wait_time_seconds(self):
        return self.transport_options.get('wait_time_seconds',
                                          self.default_wait_time_seconds)

    @cached_property
    def common_config(self):
        config = {
            'bootstrap.servers':
                f'{self.conninfo.hostname}:{int(self.conninfo.port)}',
        }
        security_protocol = self.transport_options.get('security_protocol',
                                                       'plaintext')
        if security_protocol.lower() != 'plaintext':
            config.update({
                'security.protocol': security_protocol,
                'sasl.username': self.conninfo.userid,
                'sasl.password': self.conninfo.password,
                'sasl.mechanism': self.transport_options.get('sasl_mechanism'),
            })
        return config

    def close(self):
        super().close()
        self._kafka_producers = {}

        for consumer in self._kafka_consumers.values():
            consumer.close()

        self._kafka_consumers = {}


class Transport(virtual.Transport):
    def as_uri(self, uri: str, include_password=False, mask='**') -> str:
        pass

    Channel = Channel

    default_port = DEFAULT_PORT

    driver_type = 'kafka'
    driver_name = 'confluentkafka'

    recoverable_connection_errors = (
        NoBrokersAvailable,
    )

    def __init__(self, *args, **kwargs):
        if confluent_kafka is None:
            raise ImportError('The confluent-kafka library is not installed')

        super().__init__(*args, **kwargs)

    def driver_version(self):
        return confluent_kafka.__version__

    def establish_connection(self):
        return super().establish_connection()

    def close_connection(self, connection):
        return super().close_connection(connection)