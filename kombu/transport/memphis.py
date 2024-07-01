"""confluent-kafka transport module for Kombu.

Kafka transport using confluent-kafka library.

**References**

- http://docs.confluent.io/current/clients/confluent-kafka-python

**Limitations**

The confluent-kafka transport does not support PyPy environment.

Features
========
* Type: Virtual
* Supports Direct: Yes
* Supports Topic: Yes
* Supports Fanout: No
* Supports Priority: No
* Supports TTL: No

Connection String
=================
Connection string has the following format:

.. code-block::

    confluentkafka://[USER:PASSWORD@]KAFKA_ADDRESS[:PORT]

Transport Options
=================
* ``connection_wait_time_seconds`` - Time in seconds to wait for connection
  to succeed. Default ``5``
* ``wait_time_seconds`` - Time in seconds to wait to receive messages.
  Default ``5``
* ``security_protocol`` - Protocol used to communicate with broker.
  Visit https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for
  an explanation of valid values. Default ``plaintext``
* ``sasl_mechanism`` - SASL mechanism to use for authentication.
  Visit https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md for
  an explanation of valid values.
* ``num_partitions`` - Number of partitions to create. Default ``1``
* ``replication_factor`` - Replication factor of partitions. Default ``1``
* ``topic_config`` - Topic configuration. Must be a dict whose key-value pairs
  correspond with attributes in the
  http://kafka.apache.org/documentation.html#topicconfigs.
* ``kafka_common_config`` - Configuration applied to producer, consumer and
  admin client. Must be a dict whose key-value pairs correspond with attributes
  in the https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
* ``kafka_producer_config`` - Producer configuration. Must be a dict whose
  key-value pairs correspond with attributes in the
  https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
* ``kafka_consumer_config`` - Consumer configuration. Must be a dict whose
  key-value pairs correspond with attributes in the
  https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
* ``kafka_admin_config`` - Admin client configuration. Must be a dict whose
  key-value pairs correspond with attributes in the
  https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
"""

from __future__ import annotations

from queue import Empty

from kombu.transport import virtual
from kombu.utils import cached_property
from kombu.utils.encoding import str_to_bytes
from kombu.utils.json import dumps, loads
from . import base

try:
    from memphis import Memphis, Headers, MemphisError, MemphisConnectError, MemphisHeaderError, MemphisSchemaError

    # KAFKA_CONNECTION_ERRORS = ()
    # KAFKA_CHANNEL_ERRORS = ()

except ImportError:
    memphis = None
    connection_errors = MemphisConnectError
    

from kombu.log import get_logger

logger = get_logger(__name__)

DEFAULT_PORT = 9000

# class Message(base.Message):
#     """AMQP Message (librabbitmq)."""

#     def __init__(self, channel, props, info, body):
#         super().__init__(
#             channel=channel,
#             body=body,
#             delivery_info=info,
#             properties=props,
#             delivery_tag=info.get('delivery_tag'),
#             content_type=props.get('content_type'),
#             content_encoding=props.get('content_encoding'),
#             headers=props.get('headers'))


class QoS(virtual.QoS):
  """Quality of Service guarantees."""

  _not_yet_acked = {}

  def can_consume(self):
        """Return true if the channel can be consumed from.

        :returns: True, if this QoS object can accept a message.
        :rtype: bool
        """
        return not self.prefetch_count or len(self._not_yet_acked) < self \
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
    if requeue:
      message = self._not_yet_acked.pop(delivery_tag)
      consumer = self.channel._get_consumer(message.topic)
      for assignment in consumer.assignment():
        topic_partition = TopicPartition(message.topic, assignment.partition)
        [committed_offset] = consumer.committed([topic_partition])
        consumer.seek(committed_offset)
    else:
      self.ack(delivery_tag)

  def restore_unacked_once(self, stderr=None):
      pass

class Transport(base.Transport):
  """Memphis Transport."""
  @property
  def default_connection_params(self):
            return {
                'username': 'root',
                'connection_token': 'memphis',
                'port':  DEFAULT_PORT,
                'host': 'localhost',
            }

  def establish_connection(self):
            """Establish connection to the Memphis broker."""
            conninfo = self.client
            for name, default_value in self.default_connection_params.items():
                if not getattr(conninfo, name, None):
                    setattr(conninfo, name, default_value)
            opts = dict({
                'host': conninfo.hostname,
                'username': 'root',
                'connection_token': 'memphis',
                # 'timeout_ms': conninfo.timeout_ms,
            }, **conninfo.transport_options or {})
            conn = Memphis()
            conn.connect_sync(host= conninfo.hostname, username= 'root',connection_token= 'memphis')
            return conn
            # return super().establish_connection()
  def create_channel(self, connection):
        station = connection.station_sync(name="celery")
        station.basic_qos = QoS
        station.close = station.destroy_sync
        return station

  def close_connection(self, connection):
        connection.cluse_sync
