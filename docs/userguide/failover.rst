.. _guide-failover:

====================
 Automatic Failover
====================

Automatic failover is functionality for connecting to clustered broker. Application using automatic failover should be able to automatically connect to healthy node and react to unexpected failure of node in cluster.

Connection failover
===================

The :class:`~kombu.Connection` is accepting multiple URLs to several brokers. During connecting to broker, kombu is automatically picking the healthy node from the list. In the example below, kombu uses healthy.example.com broker:

.. code-block:: python

    >>> conn = Connection(
    ...     'amqp://guest:guest@broken.example.com;guest:guest@healthy.example.com'
    ... )
    >>> conn.connect()
    >>> conn
    <Connection: amqp://guest:**@healthy.example.com at 0x6fffff751710>


:class:`~kombu.Connection` also accepts failover_strategy parameter which defines the strategy of trying the nodes:

.. code-block:: python

    >>> Connection(
    ...     'amqp://broker1.example.com;amqp://broker2.example.com',
    ...     failover_strategy='round-robin'
    ... )

The current list of available failver strategies is defined in kombu.connection module:

.. code-block:: python

    >>> import kombu
    >>> kombu.connection.failover_strategies
    {'round-robin': <class 'itertools.cycle'>, 'shuffle': <function shufflecycle at 0x6fffff8547a0>}

Failover during connection handle only failover during calling :attr:`~kombu.Connection.connect()` method of :class:`~kombu.Connection`.


Operation failover
==================

Failover of connection using multiple connection strings in :class:`~kombu.Connection` solves problem when broker is unavailable during creating new connection. But in real world these connections are long lived and hence it is possible that broker fails during lifetime of connection. For this scenario retrying of operation executed against broker is needed. Retrying ensures that failed operation triggers new connection to healthy broker and re-execution of failed operation.


Failover is implemented in :attr:`~kombu.Connection.ensure()` method which tries to execute the function. When contacting broker fails, it reconnects the underlying connection and re-executes the function again. The following example is ensuring that :attr:`~kombu.Producer.publish()` method is re-executed when errors occurred:

.. code-block:: python

    >>> from kombu import Connection, Producer
    >>> conn = Connection('amqp://')
    >>> producer = Producer(conn)
    >>> def errback(exc, interval):
    ...     logger.error('Error: %r', exc, exc_info=1)
    ...     logger.info('Retry in %s seconds.', interval)
    >>> publish = conn.ensure(producer, producer.publish,
    ...                       errback=errback, max_retries=3)
    >>> publish({'hello': 'world'}, routing_key='dest')

Some methods are accepting channel as a parameter, e.g. :attr:`~kombu.Queue.declare()`. Since channel is passed as parameter, it is not refreshed automatically during failover and hence retrying calling of method fails. In this scenarios :attr:`~kombu.Connection.autoretry()` needs to be used which automatically passes channel and refresh it during failover:

.. code-block:: python

    >>> import kombu
    >>> conn = kombu.Connection('amqp://broker1:5672;amqp://broker2:5672')
    >>> conn.connect()
    >>> q = kombu.Queue('test_queue')
    
    >>> declare = conn.autoretry(q.declare)
    >>> declare()


Producer
========

:attr:`~kombu.Producer.publish()` can have automatic failover using :attr:`~kombu.Connection.ensure()` as mentioned before. Moreover, it contains retry parameter as a shortcut for retrying. The following example is retrying publishing when error occurs:

.. code-block:: python

    >>> from kombu import *
    >>> with Connection('amqp://broker1:5672;amqp://broker2:5672') as conn:
    ...     with conn.channel() as channel:
    ...         producer = conn.Producer()
    ...         producer = Producer(channel)
    ...         producer.publish(
    ...             {'hello': 'world'}, routing_key='queue', retry=True
    ...         )

Consumer
========

Consumer with failover functionality can be implemented using following function:

.. code-block:: python

    >>> def consume():
    ...     while True:
    ...         drain_events = conn.ensure(
    ...             conn, conn.drain_events)
    ...         try:
    ...             conn.drain_events(timeout=1)
    ...         except socket.timeout:
    ...             pass

This function is draining events in infinite loop with timeout to avoid blocked connections of unavailable broker. Consumer with failover is implemented by wrapping consume function using :attr:`~kombu.Connection.ensure()` method:

.. code-block:: python

    >>> consume = conn.ensure(conn, consume)
    >>> consume()

The full example implementing consumer with failover is as follows:

.. code-block:: python

    >>> from kombu import *
    >>> import socket
    
    >>> def callback(body, message):
    ...     print(body)
    ...     message.ack()
    
    
    >>> queue = Queue('queue', routing_key='queue')
    >>> with Connection('amqp://broker1:5672;amqp://broker2:5672') as conn:
    ...     def consume():
    ...         while True:
    ...             drain_events = conn.ensure(
    ...                 conn, conn.drain_events)
    ...             try:
    ...                 conn.drain_events(timeout=1)
    ...             except socket.timeout:
    ...                 pass
    ...     with conn.channel() as channel:
    ...         consumer = Consumer(channel, queue)
    ...         consumer.register_callback(callback)
    ...         with consumer:
    ...             while True:
    ...                 consume = conn.ensure(conn, consume)
    ...                 consume()

When implementing consumer as :class:`~kombu.mixins.ConsumerMixin`, the failover functionality is by wrapping consume method with :attr:`~kombu.Connection.ensure()`:

.. code-block:: python

    >>> from kombu import *
    >>> from kombu.mixins import ConsumerMixin
    
    >>> class C(ConsumerMixin):
    ...     def __init__(self, connection):
    ...         self.connection = connection
    ...     def get_consumers(self, Consumer, channel):
    ...         return [
    ...             Consumer(
    ...                  [Queue('queue', routing_key='queue')],
    ...                  callbacks=[self.on_message], accept=['json']
    ...             ),
    ...         ]
    ...     def on_message(self, body, message):
    ...         print('RECEIVED MESSAGE: {0!r}'.format(body))
    ...         message.ack()
    ...     def consume(self, *args, **kwargs):
    ...         consume = conn.ensure(conn, super().consume)
    ...         return consume(*args, **kwargs)
    
    
    >>> with Connection('amqp://broker1:5672;amqp://broker2:5672') as conn:
    ...     C(conn).run()
