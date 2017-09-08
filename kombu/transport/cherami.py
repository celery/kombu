"""Cherami Transport."""
from __future__ import absolute_import, unicode_literals

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

    # No. of messages to fetch when get message is called
    default_message_batch_size = 1

    _publisher = None
    _consumer = None

    # message fetching limit
    prefetch_limit = 0
    prefetched = 0

    def __init__(self, *args, **kwargs):
        super(Channel, self).__init__(*args, **kwargs)

        # event_loop
        self.hub = kwargs.get('hub') or get_event_loop()

        # cherami-client instance kwargs
        self.kwargs = self.connection.kwargs

        # delivery_tag(Transport.QoS) -> cherami_delivery_token(cherami)
        self.delivery_map = {}

    def basic_consume(self, queue, no_ack, *args, **kwargs):
        # set the prefetch_limit specified by the celery configuration
        self.prefetch_limit = self.qos.prefetch_count

        if self.hub:
            self.hub.call_soon(self._get, queue)
        # no_ack is always True because cherami is in charge of message ack
        return super(Channel, self).basic_consume(
            queue, True, *args, **kwargs
        )

    def _put(self, queue, message, **kwargs):
        # the first argument is the message id,
        # cherami doesn't care about this, just passes along
        self.publisher.publish(str(0), dumps(message))

    def _get(self, queue, callback=None):
        if self.prefetched < self.prefetch_limit:
            try:
                results = self.consumer.receive(
                    num_msgs=self.default_message_batch_size)
                self._on_message_ready(queue, results)
            except Exception as e:
                logger.info('Failed to receive messages: {0}'.format(e))
        else:
            self.hub.call_soon(self._get, queue)

    def _on_message_ready(self, queue, results):
        for res in results:
            self.prefetched += 1
            cherami_delivery_token = res[0]
            message = res[1]
            try:
                self._handle_message(queue,
                                     message.payload.data,
                                     cherami_delivery_token)
            except Exception as e:
                self.prefetched -= 1
                self.consumer.nack(cherami_delivery_token)
                logger.info('Failed to process a message:  {0}'.format(e))
        # done processing messages, consume again
        self.hub.call_soon(self._get, queue)

    def _handle_message(self, queue, data, cherami_delivery_token):
        message = loads(data)

        # saves the mapping for ack
        delivery_tag = message['properties']['delivery_tag']
        self.delivery_map[delivery_tag] = cherami_delivery_token

        self.connection._deliver(message, queue)

    def basic_ack(self, delivery_tag, multiple=False):
        # get the delivery_token for cherami ack
        cherami_delivery_token = self.delivery_map[delivery_tag]
        self.consumer.ack(cherami_delivery_token)

        # removes the mapping
        self.delivery_map.pop(delivery_tag, None)
        self.prefetched -= 1

    @property
    def publisher(self):
        if self._publisher is None:
            if isinstance(self.kwargs['cherami_publisher'], Publisher):
                self._publisher = self.kwargs['cherami_publisher']
            else:
                raise Exception('Invalid cherami publisher instance! ')
        return self._publisher

    @property
    def consumer(self):
        if self._consumer is None:
            if isinstance(self.kwargs['cherami_consumer'], Consumer):
                self._consumer = self.kwargs['cherami_consumer']
            else:
                raise Exception('Invalid cherami consumer instance! ')
        return self._consumer


class Transport(virtual.Transport):
    """Cherami Transport."""

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

        # cherami-client instance kwargs
        self.kwargs = kwargs
