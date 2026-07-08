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

    Streaming credentials / automatic re-authentication
    ----------------------------------------------------
    .. versionadded:: 5.7.0

    The transport supports rotating credentials supplied by a
    ``redis.credentials.StreamingCredentialProvider`` (for example the
    Microsoft Entra ID provider from ``redis-entraid``, or an AWS ElastiCache
    IAM provider).  Configure it like any other credential provider:

    .. code-block:: python

        app.conf.broker_transport_options = {
            "credential_provider": my_streaming_credential_provider,
        }

    Such providers emit a fresh authentication token in the background before
    the current one expires.  redis-py delivers these tokens to *pooled*
    connections when they are released back to the pool, but the Redis
    transport keeps two connections busy for the entire lifetime of the
    worker — the ``BRPOP`` connection (used to consume regular queues) and the
    pub/sub ``LISTEN`` connection (used to consume fanout queues) — so they are
    never released and would otherwise never receive a rotated token.  The
    broker then severs them once the original credentials expire (e.g. AWS
    ElastiCache with IAM auth enforces a hard 12-hour connection limit),
    causing redelivered messages, interrupted in-flight tasks and brief worker
    unavailability.

    To avoid this, the transport periodically flushes any pending token onto
    those long-lived connections from the event loop:

    * the ``BRPOP`` connection is re-authenticated in place with an ``AUTH``
      command, sent only when no blocking pop is in flight;
    * the pub/sub ``LISTEN`` connection cannot process ``AUTH`` while
      subscribed under RESP2, so it is transparently reconnected (and
      re-subscribed) to pick up the new credentials.  Under RESP3, redis-py
      re-authenticates pub/sub connections itself and the transport leaves
      them untouched.

    How often the flush runs is controlled by the ``reauth_check_interval``
    transport option (seconds, default ``10``):

    .. code-block:: python

        app.conf.broker_transport_options = {
            "credential_provider": my_streaming_credential_provider,
            "reauth_check_interval": 10,
        }

    When no streaming credential provider is configured this machinery is a
    cheap no-op, so it is always safe to leave enabled.

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
