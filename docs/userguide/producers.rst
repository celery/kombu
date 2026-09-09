.. _guide-producers:

===========
 Producers
===========

.. _producer-basics:

Basics
======

You can create a producer using a :class:`~kombu.Connection`:

.. code-block:: pycon

    >>> producer = connection.Producer()

You can also instantiate :class:`~kombu.Producer` directly,
it takes a channel or a connection as an argument:

.. code-block:: pycon

    >>> with Connection('amqp://') as conn:
    ...     with conn.channel() as channel:
    ...          producer = Producer(channel)

Having a producer instance you can publish messages:

.. code-block:: pycon

    >>> from kombu import Exchange

    >>> exchange = Exchange('name', type='direct')

    >>> producer.publish(
    ...      {'hello': 'world'},  # message to send
    ...      exchange=exchange,   # destination exchange
    ...      routing_key='rk',    # destination routing key,
    ...      declare=[exchange],  # make sure exchange is declared,
    ... )


Mostly you will be getting a connection from a connection pool,
and this connection can be stale, or you could lose the connection
in the middle of sending the message.   Using retries is a good
way to handle these intermittent failures:

.. code-block:: pycon

    >>> producer.publish({'hello': 'world', ..., retry=True})

In addition a retry policy can be specified, which is a dictionary
of parameters supported by the :func:`~kombu.utils.functional.retry_over_time`
function

.. code-block:: pycon

    >>> producer.publish(
    ...     {'hello': 'world'}, ...,
    ...     retry=True,
    ...     retry_policy={
    ...         'interval_start': 0, # First retry immediately,
    ...         'interval_step': 2,  # then increase by 2s for every retry.
    ...         'interval_max': 30,  # but don't exceed 30s between retries.
    ...         'max_retries': 30,   # give up after 30 tries.
    ...     },
    ... )

The ``declare`` argument lets you pass a list of entities that must be
declared before sending the message.  This is especially important
when using the ``retry`` flag, since the broker may actually restart
during a retry in which case non-durable entities are removed.

Say you are writing a task queue, and the workers may have not started yet
so the queues aren't declared.  In this case you need to define both the
exchange, and the declare the queue so that the message is delivered to
the queue while the workers are offline:

.. code-block:: pycon

    >>> from kombu import Exchange, Queue
    >>> task_queue = Queue('tasks', Exchange('tasks'), routing_key='tasks')

    >>> producer.publish(
    ...     {'hello': 'world'}, ...,
    ...     retry=True,
    ...     exchange=task_queue.exchange,
    ...     routing_key=task_queue.routing_key,
    ...     declare=[task_queue],  # declares exchange, queue and binds.
    ... )

Bypassing routing by using the anon-exchange
--------------------------------------------

You may deliver to a queue directly, bypassing the brokers routing
mechanisms, by using the "anon-exchange": set the exchange parameter to the
empty string, and set the routing key to be the name of the queue:

.. code-block:: pycon

    >>> producer.publish(
    ...     {'hello': 'world'},
    ...     exchange='',
    ...     routing_key=task_queue.name,
    ... )

Batch publishing
----------------

.. versionadded:: 5.7

:meth:`~kombu.Producer.batch` groups normal :meth:`~kombu.Producer.publish`
calls so a supporting transport can send their final broker operations
together:

.. code-block:: python

    with producer.batch(max_size=500) as batch:
        producer.publish(
            {'task': 1},
            exchange='',
            routing_key='tasks',
            declare=[task_queue],
        )
        producer.publish(
            {'task': 2},
            exchange='',
            routing_key='tasks',
        )

        # Optional: send everything accumulated so far and keep batching.
        batch.flush()

Message preparation, serialization, declarations, and routing still use the
normal publishing path. Declarations and routing operations whose results are
needed immediately are not buffered.

The Redis transport buffers its final ``LPUSH``, ``PEXPIRE``, and ``PUBLISH``
commands in a non-transactional redis-py pipeline. This reduces network round
trips without making publication atomic. Commands are added in publication
order, preserving FIFO order within each Redis priority queue.

The batch has the following lifecycle:

* A successful outermost context exit flushes pending operations.
* :meth:`~kombu.Producer.batch` contexts may be nested. They share the
  outermost context's batch and ``max_size``; an inner successful exit does
  not flush.
* An exception leaving any nested batch context aborts the whole batch and
  discards operations that have not already been flushed. Operations sent by
  an explicit or size-triggered flush cannot be rolled back.
* An empty batch performs no transport work.
* ``max_size`` is a positive integer and defaults to 1000. For Redis it limits
  final Redis commands, not calls to :meth:`~kombu.Producer.publish`. One
  publication routed to many queues is kept together and may temporarily
  exceed this limit.
* Transports without batch support publish immediately inside the context.
  Check :attr:`~kombu.Producer.supports_batch_publish` when an application
  requires actual deferred publication.

A Redis pipeline failure raises
:exc:`~kombu.exceptions.BatchPublishError`. If the response to
``pipeline.execute()`` is lost, Redis may have accepted none, some, or all of
the commands. Kombu therefore does not automatically replay the batch, even
when individual calls used ``retry=True``. Applications may retry explicitly
when at-least-once publication and possible duplicates are acceptable.

``max_size`` bounds memory use. Batch contexts do not start a timer or a
background flush; long-running producers should call ``batch.flush()`` at
their own time boundary. Use the Redis ``socket_timeout`` transport option to
bound how long a pipeline network operation may block.

The standard Redis, Redis TLS, and Redis Sentinel transports share this batch
implementation. Standard Redis is covered by integration tests. TLS and
Sentinel use the same channel implementation but are not covered by
real-service batch integration tests. Redis Cluster is not currently an
upstream Kombu transport and is not supported or tested by this API. Redis
publishing also remains immediate when the underlying connection pool enables
automatic retries, including ``retry_on_timeout`` or a custom retry policy,
because replaying a pipeline after an ambiguous failure can publish duplicate
messages.

Serialization
=============

Json is the default serializer when a non-string object is passed
to publish, but you can also specify a different serializer:

.. code-block:: pycon

    >>> producer.publish({'hello': 'world'}, serializer='pickle')

See :ref:`guide-serialization` for more information.


Reference
=========

.. autoclass:: kombu.Producer
    :noindex:
    :members:
