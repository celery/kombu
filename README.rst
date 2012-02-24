#############################################
 kombu - AMQP Messaging Framework for Python
#############################################

:Version: 2.1.1

Synopsis
========

`Kombu` is an `AMQP`_ messaging framework for Python.

AMQP is the Advanced Message Queuing Protocol, an open standard protocol
for message orientation, queuing, routing, reliability and security.

One of the most popular implementations of AMQP is `RabbitMQ`_.

The aim of `Kombu` is to make messaging in Python as easy as possible by
providing an idiomatic high-level interface for the AMQP protocol, and also
provide proven and tested solutions to common messaging problems.

Features
========

* Allows application authors to support several message server
  solutions by using pluggable transports.

    * AMQP transports for both the `amqplib`_ (sync) and
      `pika`_ (sync + async) clients.

    * Virtual transports makes it really easy to add support for non-AMQP
      transports.  There is already built-in support for `Redis`_,
      `Beanstalk`_, `Amazon SQS`_, `CouchDB`_, and `MongoDB`_.

    * SQLAlchemy and Django ORM transports exists as plug-ins (
      `kombu-sqlalchemy`_ and `django-kombu`_).

    * In-memory transport for unit testing.

* Supports automatic encoding, serialization and compression of message
  payloads.

* Consistent exception handling across transports.

* The ability to ensure that an operation is performed by gracefully
  handling connection and channel errors.

* Several annoyances with `amqplib`_ has been fixed, like supporting
  timeouts and the ability to wait for events on more than one channel.

* Projects already using `carrot`_ can easily be ported by using
  a compatibility layer.


For an introduction to AMQP you should read the article `Rabbits and warrens`_,
and the `Wikipedia article about AMQP`_.



.. _`RabbitMQ`: http://www.rabbitmq.com/
.. _`AMQP`: http://amqp.org
.. _`Redis`: http://code.google.com/p/redis/
.. _`Amazon SQS`: http://aws.amazon.com/sqs/
.. _`MongoDB`: http://www.mongodb.org/
.. _`CouchDB`: http://couchdb.apache.org/
.. _`Beanstalk`: http://kr.github.com/beanstalkd/
.. _`Rabbits and warrens`: http://blogs.digitar.com/jjww/2009/01/rabbits-and-warrens/
.. _`amqplib`: http://barryp.org/software/py-amqplib/
.. _`pika`: http://github.com/pika/pika
.. _`Wikipedia article about AMQP`: http://en.wikipedia.org/wiki/AMQP
.. _`kombu-sqlalchemy`: http://github.com/ask/kombu-sqlalchemy/
.. _`django-kombu`: http://github.com/ask/django-kombu/
.. _`carrot`: http://pypi.python.org/pypi/carrot/


Transport Comparison
====================

+---------------+----------+------------+------------+---------------+
| **Client**    | **Type** | **Direct** | **Topic**  | **Fanout**    |
+---------------+----------+------------+------------+---------------+
| *amqplib*     | Native   | Yes        | Yes        | Yes           |
+---------------+----------+------------+------------+---------------+
| *pika*        | Native   | Yes        | Yes        | Yes           |
+---------------+----------+------------+------------+---------------+
| *redis*       | Virtual  | Yes        | Yes [#f1]_ | Yes (PUB/SUB) |
+---------------+----------+------------+------------+---------------+
| *beanstalk*   | Virtual  | Yes        | Yes [#f1]_ | No            |
+---------------+----------+------------+------------+---------------+
| *SQS*         | Virtual  | Yes        | Yes [#f1]_ | Yes [#f2]_    |
+---------------+----------+------------+------------+---------------+
| *mongodb*     | Virtual  | Yes        | Yes [#f1]_ | Yes           |
+---------------+----------+------------+------------+---------------+
| *couchdb*     | Virtual  | Yes        | Yes [#f1]_ | No            |
+---------------+----------+------------+------------+---------------+
| *in-memory*   | Virtual  | Yes        | Yes [#f1]_ | No            |
+---------------+----------+------------+------------+---------------+


.. [#f1] Declarations only kept in memory, so exchanges/queues
         must be declared by all clients that needs them.

.. [#f2] Fanout supported via storing routing tables in SimpleDB.
         Disabled by default, but can be enabled by using the
         ``supports_fanout`` transport option.


Documentation
-------------

Kombu is using Sphinx, and the latest documentation is available at GitHub:

    http://ask.github.com/kombu

Quick overview
--------------

::

    from kombu import BrokerConnection, Exchange, Queue

    media_exchange = Exchange("media", "direct", durable=True)
    video_queue = Queue("video", exchange=media_exchange, routing_key="video")

    def process_media(body, message):
        print body
        message.ack()

    # connections
    with BrokerConnection("amqp://guest:guest@localhost//") as conn:

        # Declare the video queue so that the messages can be delivered.
        # It is a best practice in Kombu to have both publishers and
        # consumers declare the queue.
        video_queue(conn.channel()).declare()

        # produce
        with conn.Producer(exchange=media_exchange,
                           serializer="json", routing_key="video") as producer:
            producer.publish({"name": "/tmp/lolcat1.avi", "size": 1301013})

        # consume
        with conn.Consumer(video_queue, callbacks=[process_media]) as consumer:
            # Process messages and handle events on all channels
            while True:
                conn.drain_events()

    # Consume from several queues on the same channel:
    video_queue = Queue("video", exchange=media_exchange, key="video")
    image_queue = Queue("image", exchange=media_exchange, key="image")

    with connection.Consumer([video_queue, image_queue],
                             callbacks=[process_media]) as consumer:
        while True:
            connection.drain_events()


Or handle channels manually::

    with connection.channel() as channel:
        producer = Producer(channel, ...)
        consumer = Producer(channel)


All objects can be used outside of with statements too,
just remember to close the objects after use::

    from kombu import BrokerConnection, Consumer, Producer

    connection = BrokerConnection()
        # ...
    connection.close()

    consumer = Consumer(channel_or_connection, ...)
    consumer.register_callback(my_callback)
    consumer.consume()
        # ....
    consumer.cancel()


    producer = Producer(channel_or_connection, ...)
        # ....
    producer.close()


`Exchange` and `Queue` are simply declarations that can be pickled
and used in configuration files etc.

They also support operations, but to do so they need to be bound
to a channel:

::

    >>> exchange = Exchange("tasks", "direct")

    >>> connection = BrokerConnection()
    >>> channel = connection.channel()
    >>> bound_exchange = exchange(channel)
    >>> bound_exchange.delete()

    # the original exchange is not affected, and stays unbound.
    >>> exchange.delete()
    raise NotBoundError: Can't call delete on Exchange not bound to
        a channel.

Installation
============

You can install `Kombu` either via the Python Package Index (PyPI)
or from source.

To install using `pip`,::

    $ pip install kombu

To install using `easy_install`,::

    $ easy_install kombu

If you have downloaded a source tarball you can install it
by doing the following,::

    $ python setup.py build
    # python setup.py install # as root


Terminology
===========

There are some concepts you should be familiar with before starting:

    * Producers

        Producers sends messages to an exchange.

    * Exchanges

        Messages are sent to exchanges. Exchanges are named and can be
        configured to use one of several routing algorithms. The exchange
        routes the messages to consumers by matching the routing key in the
        message with the routing key the consumer provides when binding to
        the exchange.

    * Consumers

        Consumers declares a queue, binds it to a exchange and receives
        messages from it.

    * Queues

        Queues receive messages sent to exchanges. The queues are declared
        by consumers.

    * Routing keys

        Every message has a routing key.  The interpretation of the routing
        key depends on the exchange type. There are four default exchange
        types defined by the AMQP standard, and vendors can define custom
        types (so see your vendors manual for details).

        These are the default exchange types defined by AMQP/0.8:

            * Direct exchange

                Matches if the routing key property of the message and
                the `routing_key` attribute of the consumer are identical.

            * Fan-out exchange

                Always matches, even if the binding does not have a routing
                key.

            * Topic exchange

                Matches the routing key property of the message by a primitive
                pattern matching scheme. The message routing key then consists
                of words separated by dots (`"."`, like domain names), and
                two special characters are available; star (`"*"`) and hash
                (`"#"`). The star matches any word, and the hash matches
                zero or more words. For example `"*.stock.#"` matches the
                routing keys `"usd.stock"` and `"eur.stock.db"` but not
                `"stock.nasdaq"`.

Getting Help
============

Mailing list
------------

Join the `carrot-users`_ mailing list.

.. _`carrot-users`: http://groups.google.com/group/carrot-users/

Bug tracker
===========

If you have any suggestions, bug reports or annoyances please report them
to our issue tracker at http://github.com/ask/kombu/issues/

Contributing
============

Development of `Kombu` happens at Github: http://github.com/ask/kombu

You are highly encouraged to participate in the development. If you don't
like Github (for some reason) you're welcome to send regular patches.

License
=======

This software is licensed under the `New BSD License`. See the `LICENSE`
file in the top distribution directory for the full license text.
