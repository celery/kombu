=========================================
 Pidbox - ``kombu.pidbox``
=========================================


.. currentmodule:: kombu.pidbox

.. automodule:: kombu.pidbox

    .. contents::
        :local:

    Introduction
    ------------

    Creating the applications Mailbox
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    .. code-block:: python

        >>> mailbox = pidbox.Mailbox('celerybeat', type='direct')

        >>> @mailbox.handler
        >>> def reload_schedule(state, **kwargs):
        ...     state['beat'].reload_schedule()

        >>> @mailbox.handler
        >>> def connection_info(state, **kwargs):
        ...     return {'connection': state['connection'].info()}

    Example Node
    ~~~~~~~~~~~~

    .. code-block:: python

        >>> connection = kombu.Connection()
        >>> state = {'beat': beat,
                     'connection': connection}
        >>> consumer = mailbox(connection).Node(hostname).listen()
        >>> try:
        ...     while True:
        ...         connection.drain_events(timeout=1)
        ... finally:
        ...     consumer.cancel()

    Example Client
    ~~~~~~~~~~~~~~

    .. code-block:: python

        >>> mailbox.cast('reload_schedule')   # cast is async.
        >>> info = celerybeat.call('connection_info', timeout=1)

    Mailbox
    -------

    .. autoclass:: Mailbox

        .. autoattribute:: namespace
        .. autoattribute:: connection
        .. autoattribute:: type
        .. autoattribute:: exchange
        .. autoattribute:: reply_exchange

        .. automethod:: Node
        .. automethod:: call
        .. automethod:: cast
        .. automethod:: abcast
        .. automethod:: multi_call
        .. automethod:: get_reply_queue
        .. automethod:: get_queue

    Mailbox Options
    ~~~~~~~~~~~~~~~

    .. versionadded:: 5.6.0

    The `Mailbox` supports several configuration options that affect
    the behavior of its queues.

    - ``queue_durable``: If True, declares durable queues that survive broker restarts.
      Defaults to False.
    - ``queue_exclusive``: If True, declares exclusive queues (usable by only
      one connection, auto-deleted on disconnect). Defaults to True.

    .. versionchanged:: 5.7.0
       ``queue_exclusive`` now defaults to ``True`` for RabbitMQ 4.3.0
       compatibility. RabbitMQ 4.3.0 rejects transient non-exclusive queues
       by default.

    Node
    ----

    .. autoclass:: Node

        .. autoattribute:: hostname
        .. autoattribute:: mailbox
        .. autoattribute:: handlers
        .. autoattribute:: state
        .. autoattribute:: channel

        .. automethod:: Consumer
        .. automethod:: handler
        .. automethod:: listen
        .. automethod:: dispatch
        .. automethod:: dispatch_from_message
        .. automethod:: handle_call
        .. automethod:: handle_cast
        .. automethod:: handle
        .. automethod:: handle_message
        .. automethod:: reply
