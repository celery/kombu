.. _changelog:

================
 Change history
================

.. _version-2.5.6:

2.5.6
=====
:release-date: 2013-02-08 01:00 P.M UTC

- Now depends on amqp 1.0.8 which works around a bug found on some
  Python 2.5 installations where 2**32 overflows to 0.

.. _version-2.5.5:

2.5.5
=====
:release-date: 2013-02-07 17:00 P.M UTC

SQS: Now supports long polling (Issue #176).

    The polling interval default has been changed to 0 and a new
    transport option (``wait_time_seconds``) has been added.
    This parameter specifies how long to wait for a message from
    SQS, and defaults to 20 seconds, which is the maximum
    value currently allowed by Amazon SQS.

    Contributed by James Saryerwinnie.

- SQS: Now removes unpickleable fields before restoring messages.

- Consumer.__exit__ now ignores exceptions occurring while
  cancelling the consumer.

- Virtual:  Routing keys can now consist of characters also used
  in regular expressions (e.g. parens) (Issue #194).

- Virtual: Fixed compression header when restoring messages.

    Fix contributed by Alex Koshelev.

- Virtual: ack/reject/requeue now works while using ``basic_get``.

- Virtual: Message.reject is now supported by virtual transports
  (requeue depends on individual transport support).

- Fixed typo in hack used for static analyzers.

    Fix contributed by Basil Mironenko.

.. _version-2.5.4:

2.5.4
=====
:release-date: 2012-12-10 12:35 P.M UTC

- Fixed problem with connection clone and multiple URLs (Issue #182).

    Fix contributed by Dane Guempel.

- zeromq: Now compatible with libzmq 3.2.x.

    Fix contributed by Andrey Antukh.

- Fixed Python 3 installation problem (Issue #187).

.. _version-2.5.3:

2.5.3
=====
:release-date: 2012-11-29 12:35 P.M UTC

- Pidbox: Fixed compatibility with Python 2.6

2.5.2
=====
:release-date: 2012-11-29 12:35 P.M UTC

.. _version-2.5.2:

2.5.2
=====
:release-date: 2012-11-29 12:35 P.M UTC

- [Redis] Fixed connection leak and added a new 'max_connections' transport
  option.

.. _version-2.5.1:

2.5.1
=====
:release-date: 2012-11-28 12:45 P.M UTC

- Fixed bug where return value of Queue.as_dict could not be serialized with
  JSON (Issue #177).

.. _version-2.5.0:

2.5.0
=====
:release-date: 2012-11-27 04:00 P.M UTC

- `py-amqp`_ is now the new default transport, replacing ``amqplib``.

    The new `py-amqp`_ library is a fork of amqplib started with the
    following goals:

        - Uses AMQP 0.9.1 instead of 0.8
        - Support for heartbeats (Issue #79 + Issue #131)
        - Automatically revives channels on channel errors.
        - Support for all RabbitMQ extensions
            - Consumer Cancel Notifications (Issue #131)
            - Publisher Confirms (Issue #131).
            - Exchange-to-exchange bindings: ``exchange_bind`` / ``exchange_unbind``.
        - API compatible with :mod:`librabbitmq` so that it can be used
          as a pure-python replacement in environments where rabbitmq-c cannot
          be compiled.  librabbitmq will be updated to support all the same
          features as py-amqp.

- Support for using multiple connection URL's for failover.

    The first argument to :class:`~kombu.Connection` can now be a list of
    connection URLs:

    .. code-block:: python

        Connection(['amqp://foo', 'amqp://bar'])

    or it can be a single string argument with several URLs separated by
    semicolon:

    .. code-block:: python

        Connection('amqp://foo;amqp://bar')

    There is also a new keyword argument ``failover_strategy`` that defines
    how :meth:`~kombu.Connection.ensure_connection`/
    :meth:`~kombu.Connection.ensure`/:meth:`kombu.Connection.autoretry` will
    reconnect in the event of connection failures.

    The default reconnection strategy is ``round-robin``, which will simply
    cycle through the list forever, and there's also a ``shuffle`` strategy
    that will select random hosts from the list.  Custom strategies can also
    be used, in that case the argument must be a generator yielding the URL
    to connect to.

    Example:

    .. code-block:: python

        Connection('amqp://foo;amqp://bar')

- Now supports PyDev, PyCharm, pylint and other static code analysis tools.

- :class:`~kombu.Queue` now supports multiple bindings.

    You can now have multiple bindings in the same queue by having
    the second argument be a list:

    .. code-block:: python

        from kombu import binding, Queue

        Queue('name', [
            binding(Exchange('E1'), routing_key='foo'),
            binding(Exchange('E1'), routing_key='bar'),
            binding(Exchange('E2'), routing_key='baz'),
        ])

    To enable this, helper methods have been added:

        - :meth:`~kombu.Queue.bind_to`
        - :meth:`~kombu.Queue.unbind_from`

    Contributed by Rumyana Neykova.

- Custom serializers can now be registered using Setuptools entry-points.

    See :ref:`serialization-entrypoints`.

- New :class:`kombu.common.QoS` class used as a thread-safe way to manage
  changes to a consumer or channels prefetch_count.

    This was previously an internal class used in Celery now moved to
    the :mod:`kombu.common` module.

- Consumer now supports a ``on_message`` callback that can be used to process
  raw messages (not decoded).

    Other callbacks specified using the ``callbacks`` argument, and
    the ``receive`` method will be not be called when a on message callback
    is present.

- New utility :func:`kombu.common.ignore_errors` ignores connection and
  channel errors.

    Must only be used for cleanup actions at shutdown or on connection loss.

- Support for exchange-to-exchange bindings.

    The :class:`~kombu.Exchange` entity gained ``bind_to``
    and ``unbind_from`` methods:

    .. code-block:: python

        e1 = Exchange('A')(connection)
        e2 = Exchange('B')(connection)

        e2.bind_to(e1, routing_key='rkey', arguments=None)
        e2.unbind_from(e1, routing_key='rkey', arguments=None)

    This is currently only supported by the ``pyamqp`` transport.

    Contributed by Rumyana Neykova.

.. _version-2.4.10:

2.4.10
======
:release-date: 2012-11-22 06:00 P.M UTC

- The previous versions connection pool changes broke Redis support so that
  it would always connect to localhost (default setting) no matter what
  connection parameters were provided (Issue #176).

.. _version-2.4.9:

2.4.9
=====
:release-date: 2012-11-21 03:00 P.M UTC

- Redis: Fixed race condition that could occur while trying to restore
  messages (Issue #171).

    Fix contributed by Ollie Walsh.

- Redis: Each channel is now using a specific connection pool instance,
  which is disconnected on connection failure.

- ProducerPool: Fixed possible dead-lock in the acquire method.

- ProducerPool: ``force_close_all`` no longer tries to call the non-existent
  ``Producer._close``.

- librabbitmq: Now implements ``transport.verify_connection`` so that
  connection pools will not give back connections that are no longer working.

- New and better ``repr()`` for Queue and Exchange objects.

- Python3:  Fixed problem with running the unit test suite.

- Python3: Fixed problem with JSON codec.

.. _version-2.4.8:

2.4.8
=====
:release-date: 2012-11-02 05:00 P.M UTC

- Redis:  Improved fair queue cycle implementation (Issue #166).

    Contributed by Kevin McCarthy.

- Redis: Unacked message restore limit is now unlimited by default.

    Also, the limit can now be configured using the ``unacked_restore_limit``
    transport option:

    .. code-block:: python

        Connection('redis://', transport_options={
            'unacked_restore_limit': 100,
        })

        A limit of 100 means that the consumer will restore at most 100
        messages at each pass.

- Redis: Now uses a mutex to ensure only one consumer restores messages at a
  time.

    The mutex expires after 5 minutes by default, but can be configured
    using the ``unacked_mutex_expire`` transport option.

- LamportClock.adjust now returns the new clock value.

- Heartbeats can now be specified in URLs.

    Fix contributed by Mher Movsisyan.

- Kombu can now be used with PyDev, PyCharm and other static analysis tools.

- Fixes problem with msgpack on Python 3 (Issue #162).

    Fix contributed by Jasper Bryant-Greene

- amqplib: Fixed bug with timeouts when SSL is used in non-blocking mode.

    Fix contributed by Mher Movsisyan


.. _version-2.4.7:

2.4.7
=====
:release-date: 2012-09-18 03:00 P.M BST

- Virtual: Unknown exchanges now default to 'direct' when sending a message.

- MongoDB: Fixed memory leak when merging keys stored in the db (Issue #159)

    Fix contributed by Michael Korbakov.

- MongoDB: Better index for MongoDB transport (Issue #158).

    This improvement will create a new compund index for queue and _id in order
    to be able to use both indexed fields for getting a new message (using
    queue field) and sorting by _id.  It'll be necessary to manually delete
    the old index from the collection.

    Improvement contributed by rmihael

.. _version-2.4.6:

2.4.6
=====
:release-date: 2012-09-12 03:00 P.M BST

- Adds additional compatibility dependencies:

    - Python <= 2.6:

        - importlib
        - ordereddict

    - Python <= 2.5

        - simplejson

.. _version-2.4.5:

2.4.5
=====
:release-date: 2012-08-30 03:36 P.M BST

- Last version broke installtion on PyPy and Jython due
  to test requirements clean-up.

.. _version-2.4.4:

2.4.4
=====
:release-date: 2012-08-29 04:00 P.M BST

- amqplib: Fixed a bug with asynchronously reading large messages.

- pyamqp: Now requires amqp 0.9.3

- Cleaned up test requirements.

.. _version-2.4.3:

2.4.3
=====
:release-date: 2012-08-25 10:30 P.M BST

- Fixed problem with amqp transport alias (Issue #154).

.. _version-2.4.2:

2.4.2
=====
:release-date: 2012-08-24 05:00 P.M BST

- Having an empty transport name broke in 2.4.1.


.. _version-2.4.1:

2.4.1
=====
:release-date: 2012-08-24 04:00 P.M BST

- Redis: Fixed race condition that could cause the consumer to crash (Issue #151)

    Often leading to the error message ``"could not convert string to float"``

- Connection retry could cause an inifite loop (Issue #145).

- The ``amqp`` alias is now resolved at runtime, so that eventlet detection
  works even if patching was done later.

.. _version-2.4.0:

2.4.0
=====
:release-date: 2012-08-17 08:00 P.M BST

- New experimental :mod:`ZeroMQ <kombu.transport.zmq` transport.

    Contributed by John Watson.

- Redis: Ack timed-out messages were not restored when using the eventloop.

- Now uses pickle protocol 2 by default to be cross-compatible with Python 3.

    The protocol can also now be changed using the :envvar:`PICKLE_PROTOCOL`
    environment variable.

- Adds ``Transport.supports_ev`` attribute.

- Pika: Queue purge was not working properly.

    Fix contributed by Steeve Morin.

- Pika backend was no longer working since Kombu 2.3

    Fix contributed by Steeve Morin.

.. _version-2.3.2:

2.3.2
=====
:release-date: 2012-08-01 06:00 P.M BST

- Fixes problem with deserialization in Python 3.

.. _version-2.3.1:

2.3.1
=====
:release-date: 2012-08-01 04:00 P.M BST

- librabbitmq: Can now handle messages that does not have a
  content_encoding/content_type set (Issue #149).

    Fix contributed by C Anthony Risinger.

- Beanstalk: Now uses localhost by default if the URL does not contain a host.

.. _version-2.3.0:

2.3.0
=====
:release-date: 2012-07-24 03:50 P.M BST

- New ``pyamqp://`` transport!

    The new `py-amqp`_ library is a fork of amqplib started with the
    following goals:

        - Uses AMQP 0.9.1 instead of 0.8
        - Should support all RabbitMQ extensions
        - API compatible with :mod:`librabbitmq` so that it can be used
          as a pure-python replacement in environments where rabbitmq-c cannot
          be compiled.

    .. _`py-amqp`: http://amqp.readthedocs.org/

    If you start using use py-amqp instead of amqplib you can enjoy many
    advantages including:

        - Heartbeat support (Issue #79 + Issue #131)
        - Consumer Cancel Notifications (Issue #131)
        - Publisher Confirms

    amqplib has not been updated in a long while, so maintaining our own fork
    ensures that we can quickly roll out new features and fixes without
    resorting to monkey patching.

    To use the py-amqp transport you must install the :mod:`amqp` library::

        $ pip install amqp

    and change the connection URL to use the correct transport::

        >>> conn = Connection('pyamqp://guest:guest@localhost//')


    The ``pyamqp://`` transport will be the default fallback transport
    in Kombu version 3.0, when :mod:`librabbitmq` is not installed,
    and librabbitmq will also be updated to support the same features.

- Connection now supports heartbeat argument.

    If enabled you must make sure to manually maintain heartbeats
    by calling the ``Connection.heartbeat_check`` at twice the rate
    of the specified heartbeat interval.

    E.g. if you have ``Connection(heartbeat=10)``,
    then you must call ``Connection.heartbeat_check()`` every 5 seconds.

    if the server has not sent heartbeats at a suitable rate then
    the heartbeat check method must raise an error that is listed
    in ``Connection.connection_errors``.

    The attribute ``Connection.supports_heartbeats`` has been added
    for the ability to inspect if a transport supports heartbeats
    or not.

    Calling ``heartbeat_check`` on a transport that does
    not support heartbeats results in a noop operation.

- SQS: Fixed bug with invalid characters in queue names.

    Fix contributed by Zach Smith.

- utils.reprcall: Fixed typo where kwargs argument was an empty tuple by
  default, and not an empty dict.

.. _version-2.2.6:

2.2.6
=====
:release-date: 2012-07-10 17:00 P.M BST

- Adds ``kombu.messaging.entry_to_queue`` for compat with previous versions.

.. _version-2.2.5:

2.2.5
=====
:release-date: 2012-07-10 17:00 P.M BST

- Pidbox: Now sets queue expire at 10 seconds for reply queues.

- EventIO: Now ignores ``ValueError`` raised by epoll unregister.

- MongoDB: Fixes Issue #142

    Fix by Flavio Percoco Premoli

.. _version-2.2.4:

2.2.4
=====
:release-date: 2012-07-05 16:00 P.M BST

- Support for msgpack-python 0.2.0 (Issue #143)

    The latest msgpack version no longer supports Python 2.5, so if you're
    still using that you need to depend on an earlier msgpack-python version.

    Fix contributed by Sebastian Insua

- :func:`~kombu.common.maybe_declare` no longer caches entities with the
  ``auto_delete`` flag set.

- New experimental filesystem transport.

    Contributed by Bobby Beever.

- Virtual Transports: Now support anonymous queues and exchanges.

.. _version-2.2.3:

2.2.3
=====
:release-date: 2012-06-24 17:00 P.M BST

- ``BrokerConnection`` now renamed to ``Connection``.

    The name ``Connection`` has been an alias for a very long time,
    but now the rename is official in the documentation as well.

    The Connection alias has been available since version 1.1.3,
    and ``BrokerConnection`` will still work and is not deprecated.

- ``Connection.clone()`` now works for the sqlalchemy transport.

- :func:`kombu.common.eventloop`, :func:`kombu.utils.uuid`,
  and :func:`kombu.utils.url.parse_url` can now be
  imported from the :mod:`kombu` module directly.

- Pidbox transport callback ``after_reply_message_received`` now happens
  in a finally block.

- Trying to use the ``librabbitmq://`` transport will now show the right
  name in the :exc:`ImportError` if :mod:`librabbitmq` is not installed.

    The librabbitmq falls back to the older ``pylibrabbitmq`` name for
    compatibility reasons and would therefore show ``No module named
    pylibrabbitmq`` instead of librabbitmq.


.. _version-2.2.2:

2.2.2
=====
:release-date: 2012-06-22 02:30 P.M BST

- Now depends on :mod:`anyjson` 0.3.3

- Json serializer: Now passes :class:`buffer` objects directly,
  since this is supported in the latest :mod:`anyjson` version.

- Fixes blocking epoll call if timeout was set to 0.

    Fix contributed by John Watson.

- setup.py now takes requirements from the :file:`requirements/` directory.

- The distribution directory :file:`contrib/` is now renamed to :file:`extra/`

.. _version-2.2.1:

2.2.1
=====
:release-date: 2012-06-21 01:00 P.M BST

- SQS: Default visibility timeout is now 30 minutes.

    Since we have ack emulation the visibility timeout is
    only in effect if the consumer is abrubtly terminated.

- retry argument to ``Producer.publish`` now works properly,
  when the declare argument is specified.

- Json serializer: didn't handle buffer objects (Issue #135).

    Fix contributed by Jens Hoffrichter.

- Virtual: Now supports passive argument to ``exchange_declare``.

- Exchange & Queue can now be bound to connections (which will use the default
  channel):

    >>> exchange = Exchange('name')
    >>> bound_exchange = exchange(connection)
    >>> bound_exchange.declare()

- ``SimpleQueue`` & ``SimpleBuffer`` can now be bound to connections (which
  will use the default channel).

- ``Connection.manager.get_bindings`` now works for librabbitmq and pika.

- Adds new transport info attributes::

    - ``Transport.driver_type``

        Type of underlying driver, e.g. "amqp", "redis", "sql".

    - ``Transport.driver_name``

        Name of library used e.g. "amqplib", "redis", "pymongo".

    - ``Transport.driver_version()``

        Version of underlying library.

.. _version-2.2.0:

2.2.0
=====
:release-date: 2012-06-07 3:10 P.M BST
:by: Ask Solem

.. _v220-important:

Important Notes
---------------

- The canonical source code repository has been moved to

    http://github.com/celery/kombu

- Pidbox: Exchanges used by pidbox are no longer auto_delete.

    Auto delete has been described as a misfeature,
    and therefore we have disabled it.

    For RabbitMQ users old exchanges used by pidbox must be removed,
    these are named ``mailbox_name.pidbox``,
    and ``reply.mailbox_name.pidbox``.

    The following command can be used to clean up these exchanges::

        VHOST=/ URL=amqp:// python -c'import sys,kombu;[kombu.Connection(
            sys.argv[-1]).channel().exchange_delete(x)
                for x in sys.argv[1:-1]]' \
            $(sudo rabbitmqctl -q list_exchanges -p "$VHOST" \
            | grep \.pidbox | awk '{print $1}') "$URL"

    The :envvar:`VHOST` variable must be set to the target RabbitMQ virtual host,
    and the :envvar:`URL` must be the AMQP URL to the server.

- The ``amqp`` transport alias will now use :mod:`librabbitmq`
  if installed.

    `py-librabbitmq`_ is a fast AMQP client for Python
    using the librabbitmq C library.

    It can be installed by::

        $ pip install librabbitmq

    It will not be used if the process is monkey patched by eventlet/gevent.

.. _`py-librabbitmq`: https://github.com/celery/librabbitmq

.. _v220-news:

News
----

- Redis: Ack emulation improvements.

    Reducing the possibility of data loss.

    Acks are now implemented by storing a copy of the message when the message
    is consumed.  The copy is not removed until the consumer acknowledges
    or rejects it.

    This means that unacknowledged messages will be redelivered either
    when the connection is closed, or when the visibility timeout is exceeded.

    - Visibility timeout

        This is a timeout for acks, so that if the consumer
        does not ack the message within this time limit, the message
        is redelivered to another consumer.

        The timeout is set to one hour by default, but
        can be changed by configuring a transport option:

            >>> Connection('redis://', transport_options={
            ...     'visibility_timeout': 1800,  # 30 minutes
            ... })

    **NOTE**: Messages that have not been acked will be redelivered
    if the visibility timeout is exceeded, for Celery users
    this means that ETA/countdown tasks that are scheduled to execute
    with a time that exceeds the visibility timeout will be executed
    twice (or more).  If you plan on using long ETA/countdowns you
    should tweak the visibility timeout accordingly::

        BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 18000}  # 5 hours

    Setting a long timeout means that it will take a long time
    for messages to be redelivered in the event of a power failure,
    but if so happens you could temporarily set the visibility timeout lower
    to flush out messages when you start up the systems again.

- Experimental `Apache ZooKeeper`_ transport

    More information is in the module reference:
    :mod:`kombu.transport.zookeeper`.

    Contributed by Mahendra M.

.. _`Apache ZooKeeper`: http://zookeeper.apache.org/

- Redis: Priority support.

    The message's ``priority`` field is now respected by the Redis
    transport by having multiple lists for each named queue.
    The queues are then consumed by in order of priority.

    The priority field is a number in the range of 0 - 9, where
    0 is the default and highest priority.

    The priority range is collapsed into four steps by default, since it is
    unlikely that nine steps will yield more benefit than using four steps.
    The number of steps can be configured by setting the ``priority_steps``
    transport option, which must be a list of numbers in **sorted order**::

        >>> x = Connection('redis://', transport_options={
        ...     'priority_steps': [0, 2, 4, 6, 8, 9],
        ... })

    Priorities implemented in this way is not as reliable as
    priorities on the server side, which is why
    nickname the feature "quasi-priorities";
    **Using routing is still the suggested way of ensuring
    quality of service**, as client implemented priorities
    fall short in a number of ways, e.g. if the worker
    is busy with long running tasks, has prefetched many messages,
    or the queues are congested.

    Still, it is possible that using priorities in combination
    with routing can be more beneficial than using routing
    or priorities alone.  Experimentation and monitoring
    should be used to prove this.

    Contributed by Germán M. Bravo.

- Redis: Now cycles queues so that consuming is fair.

    This ensures that a very busy queue won't block messages
    from other queues, and ensures that all queues have
    an equal chance of being consumed from.

    This used to be the case before, but the behavior was
    accidentally changed while switching to using blocking pop.

- Redis: Auto delete queues that are bound to fanout exchanges
  is now deleted at channel.close.

- amqplib: Refactored the drain_events implementation.

- Pidbox: Now uses ``connection.default_channel``.

- Pickle serialization: Can now decode buffer objects.

- Exchange/Queue declarations can now be cached even if
  the entity is non-durable.

    This is possible because the list of cached declarations
    are now kept with the connection, so that the entities
    will be redeclared if the connection is lost.

- Kombu source code now only uses one-level of explicit relative imports.

.. _v220-fixes:

Fixes
-----

- eventio: Now ignores ENOENT raised by ``epoll.register``, and
  EEXIST from ``epoll.unregister``.

- eventio: kqueue now ignores :exc:`KeyError` on unregister.

- Redis: ``Message.reject`` now supports the ``requeue`` argument.

- Redis: Remove superfluous pipeline call.

    Fix contributed by Thomas Johansson.

- Redis: Now sets redelivered header for redelivered messages.

- Now always makes sure references to :func:`sys.exc_info` is removed.

- Virtual: The compression header is now removed before restoring messages.

- More tests for the SQLAlchemy backend.

    Contributed by Franck Cuny.

- Url parsing did not handle MongoDB URLs properly.

    Fix contributed by Flavio Percoco Premoli.

- Beanstalk: Ignore default tube when reserving.

    Fix contributed by Zhao Xiaohong.

Nonblocking consume support
---------------------------

librabbitmq, amqplib and redis transports can now be used
non-blocking.

The interface is very manual, and only consuming messages
is non-blocking so far.

The API should not be regarded as stable or final
in any way. It is used by Celery which has very limited
needs at this point. Hopefully we can introduce a proper
callback-based API later.

- ``Transport.eventmap``

    Is a map of ``fd -> callback(fileno, event)``
    to register in an eventloop.

- ``Transport.on_poll_start()``

    Is called before every call to poll.
    The poller must support ``register(fd, callback)``
    and ``unregister(fd)`` methods.

- ``Transport.on_poll_start(poller)``

    Called when the hub is initialized.
    The poller argument must support the same
    interface as :class:`kombu.utils.eventio.poll`.

- ``Connection.ensure_connection`` now takes a callback
  argument which is called for every loop while
  the connection is down.

- Adds ``connection.drain_nowait``

    This is a non-blocking alternative to drain_events,
    but only supported by amqplib/librabbitmq.

- drain_events now sets ``connection.more_to_read`` if
  there is more data to read.

    This is to support eventloops where other things
    must be handled between draining events.

.. _version-2.1.8:

2.1.8
=====
:release-date: 2012-05-06 3:06 P.M BST
:by: Ask Solem

* Bound Exchange/Queue's are now pickleable.

* Consumer/Producer can now be instantiated without a channel,
  and only later bound using ``.revive(channel)``.

* ProducerPool now takes ``Producer`` argument.

* :func:`~kombu.utils.fxrange` now counts forever if the
  stop argument is set to None.
  (fxrange is like xrange but for decimals).

* Auto delete support for virtual transports were incomplete
  and could lead to problems so it was removed.

* Cached declarations (:func:`~kombu.common.maybe_declare`)
  are now bound to the underlying connection, so that
  entities are redeclared if the connection is lost.

    This also means that previously uncacheable entities
    (e.g. non-durable) can now be cached.

* compat ConsumerSet: can now specify channel.

.. _version-2.1.7:

2.1.7
=====
:release-date: 2012-04-27 6:00 P.M BST

* compat consumerset now accepts optional channel argument.

.. _version-2.1.6:

2.1.6
=====
:release-date: 2012-04-23 1:30 P.M BST

* SQLAlchemy transport was not working correctly after URL parser change.

* maybe_declare now stores cached declarations per underlying connection
  instead of globally, in the rare case that data disappears from the
  broker after connection loss.

* Django: Added South migrations.

    Contributed by Joseph Crosland.

.. _version-2.1.5:

2.1.5
=====
:release-date: 2012-04-13 3:30 P.M BST

* The url parser removed more than the first leading slash (Issue #121).

* SQLAlchemy: Can now specify url using + separator

    Example::

        Connection('sqla+mysql://localhost/db')

* Better support for anonymous queues (Issue #116).

    Contributed by Michael Barrett.

* ``Connection.as_uri`` now quotes url parts (Issue #117).

* Beanstalk: Can now set message TTR as a message property.

    Contributed by Andrii Kostenko

.. _version-2.1.4:

2.1.4
=====
:release-date: 2012-04-03 4:00 P.M GMT

* MongoDB:  URL parsing are now delegated to the pymongo library
  (Fixes Issue #103 and Issue #87).

    Fix contributed by Flavio Percoco Premoli and James Sullivan

* SQS:  A bug caused SimpleDB to be used even if sdb persistence
  was not enabled (Issue #108).

    Fix contributed by Anand Kumria.

* Django:  Transaction was committed in the wrong place, causing
  data cleanup to fail (Issue #115).

    Fix contributed by Daisuke Fujiwara.

* MongoDB: Now supports replica set URLs.

    Contributed by Flavio Percoco Premoli.

* Redis: Now raises a channel error if a queue key that is currently
  being consumed from disappears.

    Fix contributed by Stephan Jaekel.

* All transport 'channel_errors' lists now includes
  :exc:`~kombu.exception.StdChannelError`.

* All kombu exceptions now inherit from a common
  :exc:`~kombu.exceptions.KombuError`.

.. _version-2.1.3:

2.1.3
=====
:release-date: 2012-03-20 3:00 P.M GMT
:by: Ask Solem

* Fixes Jython compatibility issues.

* Fixes Python 2.5 compatibility issues.

.. _version-2.1.2:

2.1.2
=====
:release-date: 2012-03-01 01:00 P.M GMT
:by: Ask Solem

* amqplib: Last version broke SSL support.

.. _version-2.1.1:

2.1.1
=====
:release-date: 2012-02-24 02:00 P.M GMT
:by: Ask Solem

* Connection URLs now supports encoded characters.

* Fixed a case where connection pool could not recover from connection loss.

    Fix contributed by Florian Munz.

* We now patch amqplib's ``__del__`` method to skip trying to close the socket
  if it is not connected, as this resulted in an annoying warning.

* Compression can now be used with binary message payloads.

    Fix contributed by Steeve Morin.

.. _version-2.1.0:

2.1.0
=====
:release-date: 2012-02-04 10:38 P.M GMT
:by: Ask Solem

* MongoDB: Now supports fanout (broadcast) (Issue #98).

    Contributed by Scott Lyons.

* amqplib: Now detects broken connections by using ``MSG_PEEK``.

* pylibrabbitmq: Now supports ``basic_get`` (Issue #97).

* gevent: Now always uses the ``select`` polling backend.

* pika transport: Now works with pika 0.9.5 and 0.9.6dev.

    The old pika transport (supporting 0.5.x) is now available
    as alias ``oldpika``.

    (Note terribly latency has been experienced with the new pika
    versions, so this is still an experimental transport).

* Virtual transports: can now set polling interval via the
  transport options (Issue #96).

    Example::

        >>> Connection('sqs://', transport_options={
        ...     'polling_interval': 5.0})

    The default interval is transport specific, but usually
    1.0s (or 5.0s for the Django database transport, which
    can also be set using the ``KOMBU_POLLING_INTERVAL`` setting).

* Adds convenience function: :func:`kombu.common.eventloop`.

.. _version-2.0.0:

2.0.0
=====
:release-date: 2012-01-15 18:34 P.M GMT
:by: Ask Solem

.. _v200-important:

Important Notes
---------------

.. _v200-python-compatibility:

Python Compatibility
~~~~~~~~~~~~~~~~~~~~

* No longer supports Python 2.4.

    Users of Python 2.4 can still use the 1.x series.

    The 1.x series has entered bugfix-only maintenance mode, and will
    stay that way as long as there is demand, and a willingness to
    maintain it.


.. _v200-new-transports:

New Transports
~~~~~~~~~~~~~~

* ``django-kombu`` is now part of Kombu core.

    The Django message transport uses the Django ORM to store messages.

    It uses polling, with a default polling interval of 5 seconds.
    The polling interval can be increased or decreased by configuring the
    ``KOMBU_POLLING_INTERVAL`` Django setting, which is the polling
    interval in seconds as an int or a float.  Note that shorter polling
    intervals can cause extreme strain on the database: if responsiveness
    is needed you shall consider switching to a non-polling transport.

    To use it you must use transport alias ``"django"``,
    or as an URL::

        django://

    and then add ``kombu.transport.django`` to ``INSTALLED_APPS``, and
    run ``manage.py syncdb`` to create the necessary database tables.

    **Upgrading**

    If you have previously used ``django-kombu``, then the entry
    in ``INSTALLED_APPS`` must be changed from ``djkombu``
    to ``kombu.transport.django``::

        INSTALLED_APPS = (…,
                          'kombu.transport.django')

    If you have previously used django-kombu, then there is no need
    to recreate the tables, as the old tables will be fully compatible
    with the new version.

* ``kombu-sqlalchemy`` is now part of Kombu core.

    This change requires no code changes given that the
    ``sqlalchemy`` transport alias is used.

.. _v200-news:

News
----

* :class:`kombu.mixins.ConsumerMixin` is a mixin class that lets you
  easily write consumer programs and threads.

  See :ref:`examples` and :ref:`guide-consumers`.

* SQS Transport: Added support for SQS queue prefixes (Issue #84).

    The queue prefix can be set using the transport option
    ``queue_name_prefix``::

        BrokerTransport('SQS://', transport_options={
            'queue_name_prefix': 'myapp'})

    Contributed by Nitzan Miron.

* ``Producer.publish`` now supports automatic retry.

    Retry is enabled by the ``reply`` argument, and retry options
    set by the ``retry_policy`` argument::

        exchange = Exchange('foo')
        producer.publish(message, exchange=exchange, retry=True,
                         declare=[exchange], retry_policy={
                            'interval_start': 1.0})

    See :meth:`~kombu.Connection.ensure`
    for a list of supported retry policy options.

* ``Producer.publish`` now supports a ``declare`` keyword argument.

    This is a list of entities (:class:`Exchange`, or :class:`Queue`)
    that should be declared before the message is published.

.. _v200-fixes:

Fixes
-----

* Redis transport: Timeout was multiplied by 1000 seconds when using
  ``select`` for event I/O (Issue #86).

.. _version-1.5.1:

1.5.1
=====
:release-date: 2011-11-30 01:00 P.M GMT
:by: Ask Solem

* Fixes issue with ``kombu.compat`` introduced in 1.5.0 (Issue #83).

* Adds the ability to disable content_types in the serializer registry.

    Any message with a content type that is disabled will be refused.
    One example would be to disable the Pickle serializer:

        >>> from kombu.serialization import registry
        # by name
        >>> registry.disable('pickle')
        # or by mime-type.
        >>> registry.disable('application/x-python-serialize')

.. _version-1.5.0:

1.5.0
=====
:release-date: 2011-11-27 06:00 P.M GMT
:by: Ask Solem

* kombu.pools: Fixed a bug resulting in resources not being properly released.

  This was caused by the use of ``__hash__`` to distinguish them.

* Virtual transports: Dead-letter queue is now disabled by default.

    The dead-letter queue was enabled by default to help application
    authors, but now that Kombu is stable it should be removed.
    There are after all many cases where messages should just be dropped
    when there are no queues to buffer them, and keeping them without
    supporting automatic cleanup is rather considered a resource leak
    than a feature.

    If wanted the dead-letter queue can still be enabled, by using
    the ``deadletter_queue`` transport option::

        >>> x = Connection('redis://',
        ...       transport_options={'deadletter_queue': 'ae.undeliver'})

    In addition, an :class:`UndeliverableWarning` is now emitted when
    the dead-letter queue is enabled and a message ends up there.

    Contributed by Ionel Maries Cristian.

* MongoDB transport now supports Replicasets (Issue #81).

    Contributed by Ivan Metzlar.

* The ``Connection.ensure`` methods now accepts a ``max_retries`` value
  of 0.

    A value of 0 now means *do not retry*, which is distinct from :const:`None`
    which means *retry indefinitely*.

    Contributed by Dan McGee.

* SQS Transport: Now has a lowercase ``sqs`` alias, so that it can be
  used with broker URLs (Issue #82).

    Fix contributed by Hong Minhee

* SQS Transport: Fixes KeyError on message acknowledgements (Issue #73).

    The SQS transport now uses UUID's for delivery tags, rather than
    a counter.

    Fix contributed by Brian Bernstein.

* SQS Transport: Unicode related fixes (Issue #82).

    Fix contributed by Hong Minhee.

* Redis version check could crash because of improper handling of types
  (Issue #63).

* Fixed error with `Resource.force_close_all` when resources
  were not yet properly initialized (Issue #78).

.. _version-1.4.3:

1.4.3
=====
:release-date: 2011-10-27 10:00 P.M BST

* Fixes bug in ProducerPool where too many resources would be acquired.

.. _version-1.4.2:

1.4.2
=====
:release-date: 2011-10-26 05:00 P.M BST
:by: Ask Solem

* Eventio: Polling should ignore `errno.EINTR`

* SQS: str.encode did only start accepting kwargs after Py2.7.

* simple_task_queue example didn't run correctly (Issue #72).

    Fix contributed by Stefan Eletzhofer.

* Empty messages would not raise an exception not able to be handled
  by `on_decode_error` (Issue #72)

    Fix contributed by Christophe Chauvet.

* CouchDB: Properly authenticate if user/password set (Issue #70)

    Fix contributed by Rafael Duran Castaneda

* Connection.Consumer had the wrong signature.

    Fix contributed by Pavel Skvazh

.. _version-1.4.1:

1.4.1
=====
:release-date: 2011-09-26 04:00 P.M BST
:by: Ask Solem

* 1.4.0 broke the producer pool, resulting in new connections being
  established for every acquire.


.. _version-1.4.0:

1.4.0
=====
:release-date: 2011-09-22 05:00 P.M BST
:by: Ask Solem

* Adds module :mod:`kombu.mixins`.

    This module contains a :class:`~kombu.mixins.ConsumerMixin` class
    that can be used to easily implement a message consumer
    thread that consumes messages from one or more
    :class:`kombu.Consumer` instances.

* New example: :ref:`task-queue-example`

    Using the ``ConsumerMixin``, default channels and
    the global connection pool to demonstrate new Kombu features.

* MongoDB transport did not work with MongoDB >= 2.0 (Issue #66)

    Fix contributed by James Turk.

* Redis-py version check did not account for beta identifiers
  in version string.

    Fix contributed by David Ziegler.

* Producer and Consumer now accepts a connection instance as the
  first argument.

    The connections default channel will then be used.

    In addition shortcut methods has been added to Connection::

        >>> connection.Producer(exchange)
        >>> connection.Consumer(queues=..., callbacks=...)

* Connection has aquired a ``connected`` attribute that
  can be used to check if the connection instance has established
  a connection.

* ``ConnectionPool.acquire_channel`` now returns the connections
  default channel rather than establising a new channel that
  must be manually handled.

* Added ``kombu.common.maybe_declare``

    ``maybe_declare(entity)`` declares an entity if it has
    not previously been declared in the same process.

* :func:`kombu.compat.entry_to_queue` has been moved to :mod:`kombu.common`

* New module :mod:`kombu.clocks` now contains an implementation
  of Lamports logical clock.

.. _version-1.3.5:

1.3.5
=====
:release-date: 2011-09-16 06:00 P.M BST
:by: Ask Solem

* Python 3: AMQP_PROTOCOL_HEADER must be bytes, not str.

.. _version-1.3.4:

1.3.4
=====
:release-date: 2011-09-16 06:00 P.M BST
:by: Ask Solem

* Fixes syntax error in pools.reset


.. _version-1.3.3:

1.3.3
=====
:release-date: 2011-09-15 02:00 P.M BST
:by: Ask Solem

* pools.reset did not support after forker arguments.

.. _version-1.3.2:

1.3.2
=====
:release-date: 2011-09-10 01:00 P.M BST
:by: Mher Movsisyan

* Broke Python 2.5 compatibility by importing ``parse_qsl`` from ``urlparse``

* Connection.default_channel is now closed when connection is revived
  after connection failures.

* Pika: Channel now supports the ``connection.client`` attribute
  as required by the simple interface.

* pools.set_limit now raises an exception if the limit is lower
  than the previous limit.

* pools.set_limit no longer resets the pools.

.. _version-1.3.1:

1.3.1
=====
:release-date: 2011-10-07 03:00 P.M BST

* Last release broke after fork for pool reinitialization.

* Producer/Consumer now has a ``connection`` attribute,
  giving access to the :class:`Connection` of the
  instance.

* Pika: Channels now have access to the underlying
  :class:`Connection` instance using ``channel.connection.client``.

    This was previously required by the ``Simple`` classes and is now
    also required by :class:`Consumer` and :class:`Producer`.

* Connection.default_channel is now closed at object revival.

* Adds kombu.clocks.LamportClock.

* compat.entry_to_queue has been moved to new module :mod:`kombu.common`.

.. _version-1.3.0:

1.3.0
=====
:release-date: 2011-10-05 01:00 P.M BST

* Broker connection info can be now be specified using URLs

    The broker hostname can now be given as an URL instead, of the format::

        transport://user:password@hostname:port/virtual_host

    for example the default broker is expressed as::

        >>> Connection('amqp://guest:guest@localhost:5672//')

    Transport defaults to amqp, and is not required.
    user, password, port and virtual_host is also not mandatory and
    will default to the corresponding transports default.

    .. note::

        Note that the path component (virtual_host) always starts with a
        forward-slash.  This is necessary to distinguish between the virtual
        host '' (empty) and '/', which are both acceptable virtual host names.

        A virtual host of '/' becomes:

            amqp://guest:guest@localhost:5672//

        and a virtual host of '' (empty) becomes::

            amqp://guest:guest@localhost:5672/

        So the leading slash in the path component is **always required**.

* Now comes with default global connection and producer pools.

    The acquire a connection using the connection parameters
    from a :class:`Connection`::

        >>> from kombu import Connection, connections
        >>> connection = Connection('amqp://guest:guest@localhost//')
        >>> with connections[connection].acquire(block=True):
        ...     # do something with connection

    To acquire a producer using the connection parameters
    from a :class:`Connection`::

        >>> from kombu import Connection, producers
        >>> connection = Connection('amqp://guest:guest@localhost//')
        >>> with producers[connection].acquire(block=True):
        ...     producer.publish({'hello': 'world'}, exchange='hello')

    Acquiring a producer will in turn also acquire a connection
    from the associated pool in ``connections``, so you the number
    of producers is bound the same limit as number of connections.

    The default limit of 100 connections per connection instance
    can be changed by doing::

        >>> from kombu import pools
        >>> pools.set_limit(10)

    The pool can also be forcefully closed by doing::

        >>> from kombu import pools
        >>> pool.reset()

* SQS Transport: Persistence using SimpleDB is now disabled by default,
  after reports of unstable SimpleDB connections leading to errors.

* :class:`Producer` can now be used as a context manager.

* ``Producer.__exit__`` now properly calls ``release`` instead of close.

    The previous behavior would lead to a memory leak when using
    the :class:`kombu.pools.ProducerPool`

* Now silences all exceptions from `import ctypes` to match behaviour
  of the standard Python uuid module, and avoid passing on MemoryError
  exceptions on SELinux-enabled systems (Issue #52 + Issue #53)

* ``amqp`` is now an alias to the ``amqplib`` transport.

* ``kombu.syn.detect_environment`` now returns 'default', 'eventlet', or
  'gevent' depending on what monkey patches have been installed.

* Serialization registry has new attribute ``type_to_name`` so it is
  possible to lookup serializater name by content type.

* Exchange argument to ``Producer.publish`` can now be an :class:`Exchange`
  instance.

* ``compat.Publisher`` now supports the ``channel`` keyword argument.

* Acking a message on some transports could lead to :exc:`KeyError` being
  raised (Issue #57).

* Connection pool:  Connections are no long instantiated when the pool is
  created, but instantiated as needed instead.

* Tests now pass on PyPy.

* ``Connection.as_uri`` now includes the password if the keyword argument
  ``include_password`` is set.

* Virtual transports now comes with a default ``default_connection_params``
  attribute.

.. _version-1.2.1:

1.2.1
=====
:release-date: 2011-07-29 12:52 P.M BST

* Now depends on amqplib >= 1.0.0.

* Redis: Now automatically deletes auto_delete queues at ``basic_cancel``.

* ``serialization.unregister`` added so it is possible to remove unwanted
  seralizers.

* Fixes MemoryError while importing ctypes on SELinux (Issue #52).

* ``Connection.autoretry`` is a version of ``ensure`` that works
  with arbitrary functions (i.e. it does not need an associated object
  that implements the ``revive`` method.

  Example usage:

  .. code-block:: python

        channel = connection.channel()
        try:
            ret, channel = connection.autoretry(send_messages, channel=channel)
        finally:
            channel.close()

* ``ConnectionPool.acquire`` no longer force establishes the connection.

    The connection will be established as needed.

* ``Connection.ensure`` now supports an ``on_revive`` callback
  that is applied whenever the connection is re-established.

* ``Consumer.consuming_from(queue)`` returns True if the Consumer is
  consuming from ``queue``.

* ``Consumer.cancel_by_queue`` did not remove the queue from ``queues``.

* ``compat.ConsumerSet.add_queue_from_dict`` now automatically declared
  the queue if ``auto_declare`` set.

.. _version-1.2.0:

1.2.0
=====
:release-date: 2011-07-15 12:00 P.M BST

* Virtual: Fixes cyclic reference in Channel.close (Issue #49).

* Producer.publish: Can now set additional properties using keyword
  arguments (Issue #48).

* Adds Queue.no_ack option to control the no_ack option for individual queues.

* Recent versions broke pylibrabbitmq support.

* SimpleQueue and SimpleBuffer can now be used as contexts.

* Test requirements specifies PyYAML==3.09 as 3.10 dropped Python 2.4 support

* Now properly reports default values in Connection.info/.as_uri

.. _version-1.1.6:

1.1.6
=====
:release-date: 2011-06-13 04:00 P.M BST

* Redis: Fixes issue introduced in 1.1.4, where a redis connection
  failure could leave consumer hanging forever.

* SQS: Now supports fanout messaging by using SimpleDB to store routing
  tables.

    This can be disabled by setting the `supports_fanout` transport option:

        >>> Connection(transport='SQS',
        ...            transport_options={'supports_fanout': False})

* SQS: Now properly deletes a message when a message is acked.

* SQS: Can now set the Amazon AWS region, by using the ``region``
  transport option.

* amqplib: Now uses `localhost` as default hostname instead of raising an
  error.

.. _version-1.1.5:

1.1.5
=====
:release-date: 2011-06-07 06:00 P.M BST

* Fixes compatibility with redis-py 2.4.4.

.. _version-1.1.4:

1.1.4
=====
:release-date: 2011-06-07 04:00 P.M BST

* Redis transport: Now requires redis-py version 2.4.4 or later.

* New Amazon SQS transport added.

    Usage:

        >>> conn = Connection(transport='SQS',
        ...                   userid=aws_access_key_id,
        ...                   password=aws_secret_access_key)

    The environment variables :envvar:`AWS_ACCESS_KEY_ID` and
    :envvar:`AWS_SECRET_ACCESS_KEY` are also supported.

* librabbitmq transport: Fixes default credentials support.

* amqplib transport: Now supports `login_method` for SSL auth.

    :class:`Connection` now supports the `login_method`
    keyword argument.

    Default `login_method` is ``AMQPLAIN``.

.. _version-1.1.3:

1.1.3
=====
:release-date: 2011-04-21 16:00 P.M CEST

* Redis: Consuming from multiple connections now works with Eventlet.

* Redis: Can now perform channel operations while the channel is in
  BRPOP/LISTEN mode (Issue #35).

    Also the async BRPOP now times out after 1 second, this means that
    cancelling consuming from a queue/starting consuming from additional queues
    has a latency of up to one second (BRPOP does not support subsecond
    timeouts).

* Virtual: Allow channel objects to be closed multiple times without error.

* amqplib: ``AttributeError`` has been added to the list of known
  connection related errors (:attr:`Connection.connection_errors`).

* amqplib: Now converts :exc:`SSLError` timeout errors to
  :exc:`socket.timeout` (http://bugs.python.org/issue10272)

* Ensures cyclic references are destroyed when the connection is closed.

.. _version-1.1.2:

1.1.2
=====
:release-date: 2011-04-06 16:00 P.M CEST

* Redis: Fixes serious issue where messages could be lost.

    The issue could happen if the message exceeded a certain number
    of kilobytes in size.

    It is recommended that all users of the Redis transport should
    upgrade to this version, even if not currently experiencing any
    issues.

.. _version-1.1.1:

1.1.1
=====
:release-date: 2011-04-05 15:51 P.M CEST

* 1.1.0 started using ``Queue.LifoQueue`` which is only available
  in Python 2.6+ (Issue #33).  We now ship with our own LifoQueue.


.. _version-1.1.0:

1.1.0
=====
:release-date: 2011-04-05 01:05 P.M CEST

.. _v110-important:

Important Notes
---------------

* Virtual transports: Message body is now base64 encoded by default
  (Issue #27).

    This should solve problems sending binary data with virtual
    transports.

    Message compatibility is handled by adding a ``body_encoding``
    property, so messages sent by older versions is compatible
    with this release.  However -- If you are accessing the messages
    directly not using Kombu, then you have to respect
    the ``body_encoding`` property.

    If you need to disable base64 encoding then you can do so
    via the transport options::

        Connection(transport='...',
                   transport_options={'body_encoding': None})

    **For transport authors**:

        You don't have to change anything in your custom transports,
        as this is handled automatically by the base class.

        If you want to use a different encoder you can do so by adding
        a key to ``Channel.codecs``.  Default encoding is specified
        by the ``Channel.body_encoding`` attribute.

        A new codec must provide two methods: ``encode(data)`` and
        ``decode(data)``.

* ConnectionPool/ChannelPool/Resource: Setting ``limit=None`` (or 0)
  now disables pool semantics, and will establish and close
  the resource whenever acquired or released.

* ConnectionPool/ChannelPool/Resource: Is now using a LIFO queue
  instead of the previous FIFO behavior.

    This means that the last resource released will be the one
    acquired next.  I.e. if only a single thread is using the pool
    this means only a single connection will ever be used.

* Connection: Cloned connections did not inherit transport_options
  (``__copy__``).

* contrib/requirements is now located in the top directory
  of the distribution.

* MongoDB: Now supports authentication using the ``userid`` and ``password``
  arguments to :class:`Connection` (Issue #30).

* Connection: Default autentication credentials are now delegated to
  the individual transports.

    This means that the ``userid`` and ``password`` arguments to
    Connection is no longer *guest/guest* by default.

    The amqplib and pika transports will still have the default
    credentials.

* :meth:`Consumer.__exit__` did not have the correct signature (Issue #32).

* Channel objects now have a ``channel_id`` attribute.

* MongoDB: Version sniffing broke with development versions of
	mongod (Issue #29).

* New environment variable :envvar:`KOMBU_LOG_CONNECTION` will now emit debug
  log messages for connection related actions.

  :envvar:`KOMBU_LOG_DEBUG` will also enable :envvar:`KOMBU_LOG_CONNECTION`.

.. _version-1.0.7:

1.0.7
=====
:release-date: 2011-03-28 05:45 P.M CEST

* Now depends on anyjson 0.3.1

    cjson is no longer a recommended json implementation, and anyjson
    will now emit a deprecation warning if used.

* Please note that the Pika backend only works with version 0.5.2.

    The latest version (0.9.x) drastically changed API, and it is not
    compatible yet.

* on_decode_error is now called for exceptions in message_to_python
  (Issue #24).

* Redis: did not respect QoS settings.

* Redis: Creating a connection now ensures the connection is established.

    This means ``Connection.ensure_connection`` works properly with
    Redis.

* consumer_tag argument to ``Queue.consume`` can't be :const:`None`
  (Issue #21).

    A None value is now automatically converted to empty string.
    An empty string will make the server generate a unique tag.

* Connection now supports a ``transport_options`` argument.

    This can be used to pass additional arguments to transports.

* Pika: ``drain_events`` raised :exc:`socket.timeout` even if no timeout
  set (Issue #8).

.. version-1.0.6:

1.0.6
=====
:release-date: 2011-03-22 04:00 P.M CET

* The ``delivery_mode`` aliases (persistent/transient) were not automatically
  converted to integer, and would cause a crash if using the amqplib
  transport.

* Redis: The redis-py :exc:`InvalidData` exception suddenly changed name to
  :exc:`DataError`.

* The :envvar:`KOMBU_LOG_DEBUG` environment variable can now be set to log all
  channel method calls.

  Support for the following environment variables have been added:

    * :envvar:`KOMBU_LOG_CHANNEL` will wrap channels in an object that
      logs every method call.

    * :envvar:`KOMBU_LOG_DEBUG` both enables channel logging and configures the
      root logger to emit messages to standard error.

    **Example Usage**::

        $ KOMBU_LOG_DEBUG=1 python
        >>> from kombu import Connection
        >>> conn = Connection()
        >>> channel = conn.channel()
        Start from server, version: 8.0, properties:
            {u'product': 'RabbitMQ',..............  }
        Open OK! known_hosts []
        using channel_id: 1
        Channel open
        >>> channel.queue_declare('myq', passive=True)
        [Kombu channel:1] queue_declare('myq', passive=True)
        (u'myq', 0, 1)

.. _version-1.0.5:

1.0.5
=====
:release-date: 2011-03-17 04:00 P.M CET

* Fixed memory leak when creating virtual channels.  All virtual transports
  affected (redis, mongodb, memory, django, sqlalchemy, couchdb, beanstalk).

* Virtual Transports: Fixed potential race condition when acking messages.

    If you have been affected by this, the error would show itself as an
    exception raised by the OrderedDict implementation. (``object no longer
    exists``).

* MongoDB transport requires the ``findandmodify`` command only available in
  MongoDB 1.3+, so now raises an exception if connected to an incompatible
  server version.

* Virtual Transports: ``basic.cancel`` should not try to remove unknown
  consumer tag.

.. _version-1.0.4:

1.0.4
=====
:release-date: 2011-02-28 04:00 P.M CET

* Added Transport.polling_interval

    Used by django-kombu to increase the time to sleep between SELECTs when
    there are no messages in the queue.

    Users of django-kombu should upgrade to django-kombu v0.9.2.

.. _version-1.0.3:

1.0.3
=====
:release-date: 2011-02-12 04:00 P.M CET

* ConnectionPool: Re-connect if amqplib connection closed

* Adds ``Queue.as_dict`` + ``Exchange.as_dict``.

* Copyright headers updated to include 2011.

.. _version-1.0.2:

1.0.2
=====
:release-date: 2011-01-31 10:45 P.M CET

* amqplib: Message properties were not set properly.
* Ghettoq backend names are now automatically translated to the new names.

.. _version-1.0.1:

1.0.1
=====
:release-date: 2011-01-28 12:00 P.M CET

* Redis: Now works with Linux (epoll)

.. _version-1.0.0:

1.0.0
=====
:release-date: 2011-01-27 12:00 P.M CET

* Initial release

.. _version-0.1.0:

0.1.0
=====
:release-date: 2010-07-22 04:20 P.M CET

* Initial fork of carrot
