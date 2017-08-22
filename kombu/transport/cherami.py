"""Cherami Transport."""

from cherami_client.consumer import Consumer
from cherami_client.publisher import Publisher

from kombu.async import get_event_loop
from kombu.log import get_logger
from kombu.utils.json import loads, dumps

from . import virtual


DEFAULT_PORT = 4922

logger = get_logger('kombu.transport.cherami')

class Channel(virtual.Channel):
    """Cherami Channel."""

    default_message_batch_size = 2  # No. of messages to fetch when get message is called

    _publisher = None
    _consumer = None

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)

        # event_loop
        self.hub = kwargs.get('hub') or get_event_loop()

        # cherami-client instance kwargs
        self.kwargs = self.connection.kwargs

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        if self.hub:
            self.hub.call_soon(self._get, queue)
        return super(Channel, self).basic_consume(
            queue, no_ack, *args, **kwargs
        )

    def _put(self, queue, message, **kwargs):
        # the first argument is the message id, cherami doesn't care about this, just passes along
        self.publisher.publish(str(0), dumps(message))

    def _get(self, queue, callback=None):
        while True:
            try:
                results = self.consumer.receive(num_msgs=self.default_message_batch_size)
                self._on_message_ready(queue, results)
            except Exception as e:
                logger.info('Failed to receive messages: {0}'.format(e))

    def _on_message_ready(self, queue, results):
        for res in results:
            delivery_token = res[0]
            message = res[1]
            try:
                self._handle_message(queue, message.payload.data)
                self.consumer.ack(delivery_token)
            except Exception as e:
                self.consumer.nack(delivery_token)
                logger.info('Failed to process a message:  {0}'.format(e))

    def _handle_message(self, queue, data):
        message = loads(data)
        if message:
            callback = self.connection._callbacks[queue]
            callback(message)

    def close(self):
        super(Channel, self).close()
        if self._consumer:
            self._consumer.close()
        if self._publisher:
            self._publisher.close()

    @property
    def publisher(self):
        if self._publisher is None:
            if isinstance(self.kwargs['cherami_publisher'], Publisher):
                self._publisher = self.kwargs['cherami_publisher']
                self._publisher.open()
            else:
                raise Exception('Invalid cherami publisher instance! ')
        return self._publisher


    @property
    def consumer(self):
        if self._consumer is None:
            if isinstance(self.kwargs['cherami_consumer'], Consumer):
                self._consumer = self.kwargs['cherami_consumer']
                self._consumer.open()
            else:
                raise Exception('Invalid cherami consumer instance! ')
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

    connection_errors = (
        virtual.Transport.connection_errors,
    )
    channel_errors = (
        virtual.Transport.channel_errors,
    )

    def __init__(self, *args, **kwargs):
        super(Transport, self).__init__(*args, **kwargs)

        self.kwargs = kwargs
