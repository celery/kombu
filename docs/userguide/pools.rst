.. _guide-pools:

===============================
 Connection and Producer Pools
===============================

.. _default-pools:

Default Pools
=============

Kombu ships with two global pools: one connection pool,
and one producer pool.

These are convenient and the fact that they are global
may not be an issue as connections should often be limited
at the process level, rather than per thread/application
and so on, but if you need custom pools per thread
see :ref:`custom-pool-groups`.


.. _default-connections:

The connection pool group
-------------------------

The connection pools are available as :attr:`kombu.pools.connections`.
This is a pool group, which means you give it a connection instance,
and you get a pool instance back. We have one pool per connection
instance to support multiple connections in the same app.
All connection instances with the same connection parameters will
get the same pool:

.. code-block:: pycon

    >>> from kombu import Connection
    >>> from kombu.pools import connections

    >>> connections[Connection('redis://localhost:6379')]
    <kombu.connection.ConnectionPool object at 0x101805650>
    >>> connections[Connection('redis://localhost:6379')]
    <kombu.connection.ConnectionPool object at 0x101805650>

Let's acquire and release a connection:

.. code-block:: python

    from kombu import Connection
    from kombu.pools import connections

    connection = Connection('redis://localhost:6379')

    with connections[connection].acquire(block=True) as conn:
        print('Got connection: {0!r}'.format(connection.as_uri()))

.. note::

    The ``block=True`` here means that the acquire call will block
    until a connection is available in the pool.
    Note that this will block forever in case there is a deadlock
    in your code where a connection is not released. There
    is a ``timeout`` argument you can use to safeguard against this
    (see :meth:`kombu.connection.Resource.acquire`).

    If blocking is disabled and there aren't any connections
    left in the pool an :class:`kombu.exceptions.ConnectionLimitExceeded`
    exception will be raised.

That's about it. If you need to connect to multiple brokers
at once you can do that too:

.. code-block:: python

    from kombu import Connection
    from kombu.pools import connections

    c1 = Connection('amqp://')
    c2 = Connection('redis://')

    with connections[c1].acquire(block=True) as conn1:
        with connections[c2].acquire(block=True) as conn2:
            # ....

.. _default-producers:

The producer pool group
=======================

This is a pool group just like the connections, except
that it manages :class:`~kombu.Producer` instances
used to publish messages.

Here is an example using the producer pool to publish a message
to the ``news`` exchange:

.. code-block:: python

    from kombu import Connection, Exchange
    from kombu.pools import producers

    # The exchange we send our news articles to.
    news_exchange = Exchange('news')

    # The article we want to send
    article = {'title': 'No cellular coverage on the tube for 2012',
               'ingress': 'yadda yadda yadda'}

    # The broker where our exchange is.
    connection = Connection('amqp://guest:guest@localhost:5672//')

    with producers[connection].acquire(block=True) as producer:
        producer.publish(
            article,
            exchange=new_exchange,
            routing_key='domestic',
            declare=[news_exchange],
            serializer='json',
            compression='zlib')

.. _default-pool-limits:

Setting pool limits
-------------------

By default every connection instance has a limit of 200 connections.
You can change this limit using :func:`kombu.pools.set_limit`.
You are able to grow the pool at runtime, but you can't shrink it,
so it is best to set the limit as early as possible after your application
starts:

.. code-block:: pycon

    >>> from kombu import pools
    >>> pools.set_limit()

Resetting all pools
-------------------

You can close all active connections and reset all pool groups by
using the :func:`kombu.pools.reset` function. Note that this
will not respect anything currently using these connections,
so will just drag the connections away from under their feet:
you should be very careful before you use this.

Kombu will reset the pools if the process is forked,
so that forked processes start with clean pool groups.

.. _custom-pool-groups:

Custom Pool Groups
==================

To maintain your own pool groups you should create your own
:class:`~kombu.pools.Connections` and :class:`kombu.pools.Producers`
instances:

.. code-block:: python

    from kombu import pools
    from kombu import Connection

    connections = pools.Connections(limit=100)
    producers = pools.Producers(limit=connections.limit)

    connection = Connection('amqp://guest:guest@localhost:5672//')

    with connections[connection].acquire(block=True):
        # ...


If you want to use the global limit that can be set with
:func:`~kombu.pools.set_limit` you can use a special value as the ``limit``
argument:

.. code-block:: python

    from kombu import pools

    connections = pools.Connections(limit=pools.use_default_limit)
