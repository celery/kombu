===================================
 Kombu - ``kombu``
===================================

.. currentmodule:: kombu

.. contents::
    :local:

.. automodule:: kombu

    .. autofunction:: enable_insecure_serializers

    .. autofunction:: disable_insecure_serializers

    Connection
    ----------

    .. autoclass:: Connection

        .. admonition:: Attributes

            .. autoattribute:: hostname
            .. autoattribute:: port
            .. autoattribute:: userid
            .. autoattribute:: password
            .. autoattribute:: virtual_host
            .. autoattribute:: ssl
            .. autoattribute:: login_method
            .. autoattribute:: failover_strategy
            .. autoattribute:: connect_timeout
            .. autoattribute:: heartbeat

            .. autoattribute:: default_channel
            .. autoattribute:: connected
            .. autoattribute:: recoverable_connection_errors
            .. autoattribute:: recoverable_channel_errors
            .. autoattribute:: connection_errors
            .. autoattribute:: channel_errors
            .. autoattribute:: transport
            .. autoattribute:: connection
            .. autoattribute:: uri_prefix
            .. autoattribute:: declared_entities
            .. autoattribute:: cycle
            .. autoattribute:: host
            .. autoattribute:: manager
            .. autoattribute:: supports_heartbeats
            .. autoattribute:: is_evented

        .. admonition:: Methods

            .. automethod:: as_uri
            .. automethod:: connect
            .. automethod:: channel
            .. automethod:: drain_events
            .. automethod:: release
            .. automethod:: autoretry
            .. automethod:: ensure_connection
            .. automethod:: ensure
            .. automethod:: revive
            .. automethod:: create_transport
            .. automethod:: get_transport_cls
            .. automethod:: clone
            .. automethod:: info
            .. automethod:: switch
            .. automethod:: maybe_switch_next
            .. automethod:: heartbeat_check
            .. automethod:: maybe_close_channel
            .. automethod:: register_with_event_loop
            .. automethod:: close
            .. automethod:: _close
            .. automethod:: completes_cycle
            .. automethod:: get_manager

            .. automethod:: Producer
            .. automethod:: Consumer
            .. automethod:: Pool
            .. automethod:: ChannelPool
            .. automethod:: SimpleQueue
            .. automethod:: SimpleBuffer

    Exchange
    --------

    Example creating an exchange declaration::

        >>> news_exchange = Exchange('news', type='topic')

    For now `news_exchange` is just a declaration, you can't perform
    actions on it. It just describes the name and options for the exchange.

    The exchange can be bound or unbound. Bound means the exchange is
    associated with a channel and operations can be performed on it.
    To bind the exchange you call the exchange with the channel as argument::

        >>> bound_exchange = news_exchange(channel)

    Now you can perform operations like :meth:`declare` or :meth:`delete`::

        >>> # Declare exchange manually
        >>> bound_exchange.declare()

        >>> # Publish raw string message using low-level exchange API
        >>> bound_exchange.publish(
        ...     'Cure for cancer found!',
        ...     routing_key='news.science',
        ... )

        >>> # Delete exchange.
        >>> bound_exchange.delete()

    .. autoclass:: Exchange
        :members:
        :undoc-members:

        .. automethod:: maybe_bind

    Queue
    -----

    Example creating a queue using our exchange in the :class:`Exchange`
    example::

        >>> science_news = Queue('science_news',
        ...                      exchange=news_exchange,
        ...                      routing_key='news.science')

    For now `science_news` is just a declaration, you can't perform
    actions on it. It just describes the name and options for the queue.

    The queue can be bound or unbound. Bound means the queue is
    associated with a channel and operations can be performed on it.
    To bind the queue you call the queue instance with the channel as
    an argument::

        >>> bound_science_news = science_news(channel)

    Now you can perform operations like :meth:`declare` or :meth:`purge`:

    .. code-block:: python

        >>> bound_science_news.declare()
        >>> bound_science_news.purge()
        >>> bound_science_news.delete()

    .. autoclass:: Queue
        :members:
        :undoc-members:

        .. automethod:: maybe_bind

    Message Producer
    ----------------

    .. autoclass:: Producer

        .. autoattribute:: channel
        .. autoattribute:: exchange
        .. autoattribute:: routing_key
        .. autoattribute:: serializer
        .. autoattribute:: compression
        .. autoattribute:: auto_declare
        .. autoattribute:: on_return
        .. autoattribute:: connection

        .. automethod:: declare
        .. automethod:: maybe_declare
        .. automethod:: publish
        .. automethod:: revive

    Message Consumer
    ----------------

    .. autoclass:: Consumer

        .. autoattribute:: channel
        .. autoattribute:: queues
        .. autoattribute:: no_ack
        .. autoattribute:: auto_declare
        .. autoattribute:: callbacks
        .. autoattribute:: on_message
        .. autoattribute:: on_decode_error
        .. autoattribute:: connection

        .. automethod:: declare
        .. automethod:: register_callback
        .. automethod:: add_queue
        .. automethod:: consume
        .. automethod:: cancel
        .. automethod:: cancel_by_queue
        .. automethod:: consuming_from
        .. automethod:: purge
        .. automethod:: flow
        .. automethod:: qos
        .. automethod:: recover
        .. automethod:: receive
        .. automethod:: revive
