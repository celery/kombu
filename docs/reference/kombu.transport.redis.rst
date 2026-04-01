=================================================
 Redis Transport - ``kombu.transport.redis``
=================================================

.. currentmodule:: kombu.transport.redis

.. automodule:: kombu.transport.redis

    .. contents::
        :local:

    Transport
    ---------

    .. autoclass:: Transport
        :members:
        :undoc-members:


    .. rubric:: Transport-specific notes
    .. versionadded:: 5.6.0
    Redis now honours the generic
    :attr:`~kombu.transport.virtual.Transport.polling_interval`
    option (present in SQS, etcd, Zookeeper, …).  When you pass

    .. code-block:: python

        app.conf.broker_transport_options = {"polling_interval": 10}

    the worker uses that value as the *timeout* for the underlying
    ``BRPOP`` call, so it issues at most one poll every 10 seconds
    while the queue is empty.  
    The default remains **1 second** to stay backward-compatible.

    .. versionadded:: 5.7.0
    Supports Queue TTL

    Queue arguments
    ---------------
    The following queue argument is supported. Pass it per-queue via
    ``Queue(expires=...)`` or ``Queue(..., queue_arguments={'x-expires': ...})``,
    not as a connection-level transport option.

    ``x-expires`` (int)
        Time in milliseconds for the queue to expire if there is no activity.
        The queue will be automatically deleted after this period of inactivity.

    Channel
    -------

    .. autoclass:: Channel
        :members:
        :undoc-members:

    SentinelChannel
    ---------------

    .. autoclass:: SentinelChannel
        :members:
        :undoc-members:
