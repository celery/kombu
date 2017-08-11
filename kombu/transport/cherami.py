"""Cherami Transport."""

from clay import config

from cherami_client.client import Client
from tchannel.sync import TChannel as TChannelSyncClient

from kombu.async import get_event_loop
from kombu.log import get_logger
from kombu.utils.json import loads, dumps

from . import virtual

try:
    import cherami_client as cherami
except ImportError:
    cherami = None


logger = get_logger('kombu.transport.cherami')

DEFAULT_PORT = 4922

class Channel(virtual.Channel):
    """Cherami Channel."""

    # cherami-specific configs
    client_name = config.get('application_identifier')
    destination_path = config.get('destination')
    consumergroup_path = config.get('consumergroup')

    default_consume_interval = 1  # time between two calls to get messages
    default_message_batch_size = 2  # No. of messages to fetch when get message is called

    _client = None
    _publisher = None
    _consumer = None

    def __init__(self, *args, **kwargs):
        if cherami is None:
            raise ImportError('cherami-client is not installed')
        super(Channel, self).__init__(*args, **kwargs)

        # event_loop
        self.hub = kwargs.get('hub') or get_event_loop()

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        if self.hub:
            self._loop(queue)
        return super(Channel, self).basic_consume(
            queue, no_ack, *args, **kwargs
        )

    def _loop(self, queue, _=None):
        self.hub.call_repeatedly(self.default_consume_interval, self._get, queue)

    def _put(self, queue, message, **kwargs):
        # the first argument is the message id, cherami doesn't care about this, just passes along
        self.publisher.publish(str(0), dumps(message))

    def _get(self, queue, callback=None):
        try:
            results = self.consumer.receive(num_msgs=self.default_message_batch_size)
            self._on_message_ready(queue, results)
        except Exception as e:
            logger.info('Failed to receive messages: {0}'.format(str(e)))

    def _on_message_ready(self, queue, results):
        for res in results:
            delivery_token = res[0]
            message = res[1]
            try:
                self._handle_message(queue, message.payload.data)
                self.consumer.ack(delivery_token)
            except Exception as e:
                self.consumer.nack(delivery_token)
                logger.info('Failed to process a message:  {0}'.format(str(e)))

    def _handle_message(self, queue, data):
        message = loads(data)
        if message:
            callback = self.connection._callbacks[queue]
            callback(message)

    def _create_publisher(self):
        return self.client.create_publisher(self.destination_path)

    def _create_consumer(self):
        return self.client.create_consumer(self.destination_path, self.consumergroup_path)

    def close(self):
        super(Channel, self).close()
        if self._consumer:
            self._consumer.close()
        if self._publisher:
            self._publisher.close()

    @property
    def client(self):
        if self._client is None:
            tchannel = TChannelSyncClient(name=self.client_name, known_peers=[self.connection.host])
            self._client = Client(tchannel, logger)
        return self._client

    @property
    def publisher(self):
        if self._publisher is None:
            self._publisher = self._create_publisher()
            self._publisher.open()
        return self._publisher

    @property
    def consumer(self):
        if self._consumer is None:
            self._consumer = self._create_consumer()
            self._consumer.open()
        return self._consumer


class Transport(virtual.Transport):
    """Cherami Transport"""

    Channel = Channel

    default_port = DEFAULT_PORT
    driver_type = 'cherami'
    driver_name = 'cherami-client'

    implements = virtual.Transport.implements.extend(
        async=True,
        exchange_type=frozenset(['direct']),
    )

    # TODO: cherami-specific errors to add?
    connection_errors = (
        virtual.Transport.connection_errors,
    )
    channel_errors = (
        virtual.Transport.channel_errors,
    )

    def __init__(self, *args, **kwargs):
        if cherami is None:
            raise ImportError('Missing cherami client library (pip install cherami-client)')
        super(Transport, self).__init__(*args, **kwargs)

        self.host = self.client.host
