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

    Visibility timeout and unacked message restore
    ----------------------------------------------
    .. versionadded:: 5.7.0

    When ack emulation is enabled (the default), a message that has been
    delivered but not yet acknowledged is tracked in a single, *global* Redis
    sorted set (``unacked_index``) scored by the time it was fetched.
    Periodically each consuming worker runs
    :meth:`QoS.restore_visible <kombu.transport.redis.QoS.restore_visible>`,
    which scans that set for entries older than ``now - visibility_timeout``
    and pushes them back onto their original queues so another worker can pick
    them up.

    Because the index is shared by every worker and queue on the same Redis
    database (and ``global_keyprefix``), *any* consuming worker restores
    abandoned messages for the whole fleet — including messages left behind by
    workers that crashed.

    Two transport options control how often the restore scan runs:

    ``unacked_restore_interval`` (int, default ``10``)
        Seconds between periodic restore sweeps on the asynchronous
        (event-loop / prefork) path.  Lower this to recover abandoned messages
        faster when using a low ``visibility_timeout``.

    ``unacked_restore_throttle`` (int, default ``10``)
        Only perform an actual Redis scan on every *N*-th ``restore_visible``
        call.  This protects Redis from being hit too often, especially on the
        synchronous poll path where a restore is *attempted* on every empty
        poll.  Set to ``1`` to scan on every call.

    The effective sweep period on the asynchronous path is roughly::

        unacked_restore_interval * unacked_restore_throttle   (seconds)

    For example, to actually scan every 5 seconds with a 30 second visibility
    timeout:

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
