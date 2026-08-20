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
            "visibility_timeout": 30,
            "unacked_restore_interval": 5,
            "unacked_restore_throttle": 1,
        }

    .. note::

        ``unacked_restore_interval`` only affects the asynchronous path
        (a prefork worker driven by the event loop on Linux/macOS).  On the
        synchronous path — used by ``eventlet``/``gevent`` pools, on Windows,
        or by any plain ``connection.drain_events()`` loop — there is no
        restore timer; a restore is attempted on every empty poll
        (~``brpop_timeout``, 1 second) and only ``unacked_restore_throttle``
        governs how often Redis is actually scanned.

    .. warning::

        ``restore_visible`` uses the *scanning* worker's own
        ``visibility_timeout`` as the cutoff — the sorted set stores only the
        fetch timestamp, not each message's timeout.  Configure the same
        ``visibility_timeout`` on every worker that may run the sweep,
        otherwise restore timing becomes inconsistent: too low re-queues
        messages that are still being processed (duplicate execution), too
        high delays recovery.

    Dedicated "janitor" worker for green pools
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    Under the ``eventlet`` and ``gevent`` pools every greenlet runs
    cooperatively in a single OS thread.  A CPU-bound task that does not yield
    blocks the hub, which also stalls the broker consumer loop — so restore
    sweeps (and heartbeats) stop running until the task finishes.  Visibility
    timeouts may then expire without anything re-queuing the affected messages.

    Because the unacked index is global, you can side-step this by running a
    small dedicated **prefork** worker whose only job is to drive restores.
    Point it at the same Redis database and ``global_keyprefix`` as your real
    workers, have it consume a throwaway queue (so ``active_queues`` is
    non-empty and the restore loop fires), give it the *same*
    ``visibility_timeout`` as the rest of the fleet, and tune its restore
    cadence aggressively:

    .. code-block:: python

        # janitor_app.py
        from celery import Celery

        app = Celery("janitor", broker="redis://localhost:6379/0")
        app.conf.broker_transport_options = {
            "visibility_timeout": 3600,        # MUST match the real workers
            "unacked_restore_interval": 5,
            "unacked_restore_throttle": 1,
        }

    .. code-block:: console

        $ celery -A janitor_app worker -P prefork -c 1 -Q janitor_dummy

    The janitor never needs to consume the real queues: ``restore_visible``
    re-routes each recovered message to its original queue using the bindings
    stored in Redis.  Its hub is independent of the green-pool workers, so it
    keeps reaping expired messages even while they are busy with CPU-bound
    work — and it doubles as a safety net for messages stranded by crashed
    workers.

    .. note::

        The janitor only *drives* the sweep; the per-message restore work is
        unchanged, and all workers still contend for the same ``unacked_mutex``
        so only one sweep runs at a time.  ``ack_emulation`` must stay enabled
        for the unacked index to exist.
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
