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
