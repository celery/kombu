============================
 Connections and transports
============================

To send and receive messages you need a transport and a connection.
There are several transports to choose from (amqplib, pika, redis, in-memory),
and you can even create your own. The default transport is amqplib.

Create a connection using the default transport::

    >>> from kombu import BrokerConnection
    >>> connection = BrokerConnection()

The connection will not be established yet, as the connection is established
when needed. If you want to explicitly establish the connection
you have to call the :meth:`~kombu.connection.BrokerConnection.connect`
method::

    >>> connection.connect()

This connection will use the default connection settings, which is using
the localhost host, default port, username `guest`,
password `guest` and virtual host "/". A connection without arguments
is the same as::

    >>> BrokerConnection(hostname="localhost",
    ...                  userid="guest",
    ...                  password="guest",
    ...                  virtual_host="/",
    ...                  port=6379)

The default port is transport specific, for AMQP this is 6379.

Other fields may also have different meaning depending on the transport
used. For example, the Redis transport uses the `virtual_host` argument as
the redis database number.

See the :class:`~kombu.connection.BrokerConnection` reference documentation
for more information and a full list of the arguments supported.
