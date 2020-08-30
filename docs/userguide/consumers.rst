.. _guide-consumers:

===========
 Consumers
===========

.. _consumer-basics:

Basics
======

The :class:`~kombu.messaging.Consumer` takes a connection (or channel) and a list of queues to
consume from. Several consumers can be mixed to consume from different
channels, as they all bind to the same connection, and ``drain_events`` will
drain events from all channels on that connection.

.. note::

    Kombu since 3.0 will only accept json/binary or text messages by default,
    to allow deserialization of other formats you have to specify them
    in the ``accept`` argument (in addition to setting the right content type for your messages):

    .. code-block:: python

        >>> Consumer(conn, accept=['json', 'pickle', 'msgpack', 'yaml'])

You can create a consumer using a Connection. Consumer is consuming from single queue with name `'queue'`:

.. code-block:: python

    >>> queue = Queue('queue', routing_key='queue')
    >>> consumer = connection.Consumer(queue)

You can also instantiate Consumer directly, it takes a channel or a connection as an argument. This consumer also
consumes from single queue with name `'queue'`:

.. code-block:: python

    >>> queue = Queue('queue', routing_key='queue')
    >>> with Connection('amqp://') as conn:
    ...     with conn.channel() as channel:
    ...         consumer = Consumer(channel, queue)

Consumer needs to specify handler of received data. This handler specified in form of callback. Callback function is called
by kombu library every time a new message is received. Callback is called with two parameters ``body`` containing deserialized
data sent by producer and :class:`~kombu.message.Message` instance ``message``. User is also responsible for acknowledging of message when manual
acknowledge is set.

.. code-block:: python

    >>> def callback(body, message):
    ...     print(body)
    ...     message.ack()

    >>> consumer.register_callback(callback)

Draining events from a single consumer. Method ``drain_events`` by default blocks indefinitely. This example sets timeout to 1 second:

.. code-block:: python

    >>> with consumer:
    ...     connection.drain_events(timeout=1)

Draining events from several consumers. Each consumer has its own list of queues. Each consumer accepts `'json'` format of data:

.. code-block:: python

    >>> from kombu.utils.compat import nested

    >>> queues1 = [Queue('queue11', routing_key='queue12')]
    >>> queues2 = [Queue('queue21', routing_key='queue22')]
    >>> with connection.channel(), connection.channel() as (channel1, channel2):
    ...     with nested(Consumer(channel1, queues1, accept=['json']),
    ...                 Consumer(channel2, queues2, accept=['json'])):
    ...         connection.drain_events(timeout=1)

The full example will look as follows:

.. code-block:: python

    from kombu import Connection, Consumer, Queue

    def callback(body, message):
        print('RECEIVED MESSAGE: {0!r}'.format(body))
        message.ack()

    queue1 = Queue('queue1', routing_key='queue1')
    queue2 = Queue('queue2', routing_key='queue2')

    with Connection('amqp://') as conn:
        with conn.channel() as channel:
            consumer = Consumer(conn, [queue1, queue2], accept=['json'])
            consumer.register_callback(callback)
            with consumer:
                conn.drain_events(timeout=1)

Consumer mixin classes
======================

Kombu provides predefined mixin classes in module :py:mod:`~kombu.mixins`. It contains two classes:
:class:`~kombu.mixins.ConsumerMixin` for creating consumers and :class:`~kombu.mixins.ConsumerProducerMixin`
for creating consumers supporting also publishing messages. Consumers can be created just by subclassing
mixin class and overriding some of the methods:

.. code-block:: python

    from kombu.mixins import ConsumerMixin

    class C(ConsumerMixin):

        def __init__(self, connection):
            self.connection = connection

        def get_consumers(self, Consumer, channel):
            return [
                Consumer(queues, callbacks=[self.on_message], accept=['json']),
            ]

        def on_message(self, body, message):
            print('RECEIVED MESSAGE: {0!r}'.format(body))
            message.ack()

    C(connection).run()


and with multiple channels again:

.. code-block:: python

    from kombu import Consumer
    from kombu.mixins import ConsumerMixin

    class C(ConsumerMixin):
        channel2 = None

        def __init__(self, connection):
            self.connection = connection

        def get_consumers(self, _, default_channel):
            self.channel2 = default_channel.connection.channel()
            return [Consumer(default_channel, queues1,
                             callbacks=[self.on_message],
                             accept=['json']),
                    Consumer(self.channel2, queues2,
                             callbacks=[self.on_special_message],
                             accept=['json'])]

        def on_consumer_end(self, connection, default_channel):
            if self.channel2:
                self.channel2.close()

    C(connection).run()


The main use of :class:`~kombu.mixins.ConsumerProducerMixin` is to create consumers
that need to also publish messages on a separate connection (e.g. sending rpc
replies, streaming results):

.. code-block:: python

    from kombu import Producer, Queue
    from kombu.mixins import ConsumerProducerMixin

    rpc_queue = Queue('rpc_queue')

    class Worker(ConsumerProducerMixin):

        def __init__(self, connection):
            self.connection = connection

        def get_consumers(self, Consumer, channel):
            return [Consumer(
                queues=[rpc_queue],
                on_message=self.on_request,
                accept={'application/json'},
                prefetch_count=1,
            )]

        def on_request(self, message):
            n = message.payload['n']
            print(' [.] fib({0})'.format(n))
            result = fib(n)

            self.producer.publish(
                {'result': result},
                exchange='', routing_key=message.properties['reply_to'],
                correlation_id=message.properties['correlation_id'],
                serializer='json',
                retry=True,
            )
            message.ack()

.. seealso::

    :file:`examples/rpc-tut6/` in the Github repository.


Advanced Topics
===============

RabbitMQ
--------

Consumer Priorities
~~~~~~~~~~~~~~~~~~~

RabbitMQ defines a consumer priority extension to the amqp protocol,
that can be enabled by setting the ``x-priority`` argument to
``basic.consume``.

In kombu you can specify this argument on the :class:`~kombu.Queue`, like
this:

.. code-block:: python

    queue = Queue('name', Exchange('exchange_name', type='direct'),
                  consumer_arguments={'x-priority': 10})

Read more about consumer priorities here:
https://www.rabbitmq.com/consumer-priority.html


Reference
=========

.. autoclass:: kombu.Consumer
    :noindex:
    :members:
