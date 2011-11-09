.. currentmodule:: kombu.messaging
.. _guide-consumers:

===========
 Consumers
===========

.. _consumer-basics:

Basics
======

The :class:`Consumer` takes a connection (or channel) and a list of queues to
consume from.  Several consumers can be mixed to consume from different
channels, as they all bind to the same connection, and ``drain_events`` will
drain events from all channels on that connection.


Draining events from a single consumer:

.. code-block:: python

    with Consumer(connection, queues):
        connection.drain_events(timeout=1)


Draining events from several consumers:

.. code-block:: python

    from kombu.utils import nested

    with connection.channel(), connection.channel() as (channel1, channel2):
        consumers = [Consumer(channel1, queues1),
                     Consumer(channel2, queues2)]
        with nested(\*consumers):
            connection.drain_events(timeout=1)


Or using :class:`~kombu.mixins.ConsumerMixin`:

.. code-block:: python

    from kombu.mixins import ConsumerMixin

    class C(ConsumerMixin):

        def __init__(self, connection):
            self.connection = connection

        def get_consumers(self, Consumer, channel):
            return [Consumer(queues, callbacks=[self.on_message])]

        def on_message(self, body, message):
            print("RECEIVED MESSAGE: %r" % (body, ))
            message.ack()

    C(connection).run()


and with multiple channels again:

.. code-block:: python

    from kombu.messaging import Consumer
    from kombu.mixins import ConsumerMixin

    class C(ConsumerMixin):
        channel2 = None

        def __init__(self, connection):
            self.connection = connection

        def get_consumers(self, _, default_channel):
            self.channel2 = default_channel.connection.channel()
            return [Consumer(default_channel, queues1,
                             callbacks=[self.on_message]),
                    Consumer(self.channel2, queues2,
                             callbacks=[self.on_special_message])]

        def on_consumer_end(self, connection, default_channel):
            if self.channel2:
                self.channel2.close()

    C(connection).run()


Reference
=========

.. module:: kombu.messaging

.. autoclass:: Consumer
    :noindex:
    :members:
