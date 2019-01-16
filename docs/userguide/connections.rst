.. _guide-connections:

============================
 Connections and transports
============================

.. _connection-basics:

Basics
======

To send and receive messages you need a transport and a connection.
There are several transports to choose from (amqp, librabbitmq, redis, qpid, in-memory, etc.),
and you can even create your own. The default transport is amqp.

Create a connection using the default transport:

.. code-block:: pycon

    >>> from kombu import Connection
    >>> connection = Connection('amqp://guest:guest@localhost:5672//')

The connection will not be established yet, as the connection is established
when needed. If you want to explicitly establish the connection
you have to call the :meth:`~kombu.Connection.connect`
method:

.. code-block:: pycon

    >>> connection.connect()

You can also check whether the connection is connected:

.. code-block:: pycon

    >>> connection.connected
    True

Connections must always be closed after use:

.. code-block:: pycon

    >>> connection.close()

But best practice is to release the connection instead,
this will release the resource if the connection is associated
with a connection pool, or close the connection if not,
and makes it easier to do the transition to connection pools later:

.. code-block:: pycon

    >>> connection.release()

.. seealso::

    :ref:`guide-pools`

Of course, the connection can be used as a context, and you are
encouraged to do so as it makes it harder to forget releasing open
resources:

.. code-block:: python

    with Connection() as connection:
        # work with connection

.. _connection-urls:

URLs
====

Connection parameters can be provided as a URL in the format:

.. code-block:: text

    transport://userid:password@hostname:port/virtual_host

All of these are valid URLs:

.. code-block:: text

    # Specifies using the amqp transport only, default values
    # are taken from the keyword arguments.
    amqp://

    # Using Redis
    redis://localhost:6379/

    # Using Redis over a Unix socket
    redis+socket:///tmp/redis.sock

    # Using Qpid
    qpid://localhost/

    # Using virtual host '/foo'
    amqp://localhost//foo

    # Using virtual host 'foo'
    amqp://localhost/foo

    # Using Pyro with name server running on 'localhost'
    pyro://localhost/kombu.broker


The query part of the URL can also be used to set options, e.g.:

.. code-block:: text

    amqp://localhost/myvhost?ssl=1

See :ref:`connection-options` for a list of supported options.

A connection without options will use the default connection settings,
which is using the localhost host, default port, user name `guest`,
password `guest` and virtual host "/". A connection without arguments
is the same as:

.. code-block:: pycon

    >>> Connection('amqp://guest:guest@localhost:5672//')

The default port is transport specific, for AMQP this is 5672.

Other fields may also have different meaning depending on the transport
used. For example, the Redis transport uses the `virtual_host` argument as
the redis database number.

.. _connection-options:

Keyword arguments
=================

The :class:`~kombu.Connection` class supports additional
keyword arguments, these are:

:hostname: Default host name if not provided in the URL.
:userid: Default user name if not provided in the URL.
:password: Default password if not provided in the URL.
:virtual_host: Default virtual host if not provided in the URL.
:port: Default port if not provided in the URL.
:transport: Default transport if not provided in the URL.
  Can be a string specifying the path to the class. (e.g.
  ``kombu.transport.pyamqp:Transport``), or one of the aliases:
  ``pyamqp``, ``librabbitmq``, ``redis``, ``qpid``, ``memory``, and so on.

:ssl: Use SSL to connect to the server. Default is ``False``.
  Only supported by the amqp and qpid transports.
:insist: Insist on connecting to a server.
  *No longer supported, relic from AMQP 0.8*
:connect_timeout: Timeout in seconds for connecting to the
  server. May not be supported by the specified transport.
:transport_options: A dict of additional connection arguments to
  pass to alternate kombu channel implementations. Consult the transport
  documentation for available options.

AMQP Transports
===============

There are 4 transports available for AMQP use.

1. ``pyamqp`` uses the pure Python library ``amqp``, automatically
   installed with Kombu.
2. ``librabbitmq`` uses the high performance transport written in C.
   This requires the ``librabbitmq`` Python package to be installed, which
   automatically compiles the C library.
3. ``amqp`` tries to use ``librabbitmq`` but falls back to ``pyamqp``.
4. ``qpid`` uses the pure Python library ``qpid.messaging``, automatically
   installed with Kombu. The Qpid library uses AMQP, but uses custom
   extensions specifically supported by the Apache Qpid Broker.

For the highest performance, you should install the ``librabbitmq`` package.
To ensure librabbitmq is used, you can explicitly specify it in the
transport URL, or use ``amqp`` to have the fallback.

Transport Comparison
====================

+---------------+----------+------------+------------+---------------+--------------+
| **Client**    | **Type** | **Direct** | **Topic**  | **Fanout**    | **Priority** |
+---------------+----------+------------+------------+---------------+--------------+
| *amqp*        | Native   | Yes        | Yes        | Yes           | Yes [#f3]_   |
+---------------+----------+------------+------------+---------------+--------------+
| *qpid*        | Native   | Yes        | Yes        | Yes           | No           |
+---------------+----------+------------+------------+---------------+--------------+
| *redis*       | Virtual  | Yes        | Yes        | Yes (PUB/SUB) | Yes          |
+---------------+----------+------------+------------+---------------+--------------+
| *SQS*         | Virtual  | Yes        | Yes [#f1]_ | Yes [#f2]_    | No           |
+---------------+----------+------------+------------+---------------+--------------+
| *zookeeper*   | Virtual  | Yes        | Yes [#f1]_ | No            | Yes          |
+---------------+----------+------------+------------+---------------+--------------+
| *in-memory*   | Virtual  | Yes        | Yes [#f1]_ | No            | No           |
+---------------+----------+------------+------------+---------------+--------------+
| *SLMQ*        | Virtual  | Yes        | Yes [#f1]_ | No            | No           |
+---------------+----------+------------+------------+---------------+--------------+


.. [#f1] Declarations only kept in memory, so exchanges/queues
         must be declared by all clients that needs them.

.. [#f2] Fanout supported via storing routing tables in SimpleDB.
         Disabled by default, but can be enabled by using the
         ``supports_fanout`` transport option.

.. [#f3] AMQP Message priority support depends on broker implementation.
