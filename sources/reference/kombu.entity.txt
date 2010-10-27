.. currentmodule:: kombu.entity

.. automodule:: kombu.entity

    .. contents::
        :local:

    Exchange
    --------

    Example creating an exchange declaration::

        >>> news_exchange = Exchange("news", type="topic")

    For now `news_exchange` is just a declaration, you can't perform
    actions on it. It just describes the name and options for the exchange.

    The exchange can be bound or unbound. Bound means the exchange is
    associated with a channel and operations can be performed on it.
    To bind the exchange you call the exchange with the channel as argument::

        >>> bound_exchange = news_exchange(channel)

    Now you can perform operations like :meth:`declare` or :meth:`delete`::

        >>> bound_exchange.declare()
        >>> message = bound_exchange.Message("Cure for cancer found!")
        >>> bound_exchange.publish(message, routing_key="news.science")
        >>> bound_exchange.delete()

    .. autoclass:: Exchange
        :members:
        :undoc-members:

        .. automethod:: maybe_bind

    Queue
    -----

    Example creating a queue using our exchange in the :class:`Exchange`
    example::

        >>> science_news = Queue("science_news",
        ...                      exchange=news_exchange,
        ...                      routing_key="news.science")

    For now `science_news` is just a declaration, you can't perform
    actions on it. It just describes the name and options for the queue.

    The queue can be bound or unbound. Bound means the queue is
    associated with a channel and operations can be performed on it.
    To bind the queue you call the queue instance with the channel as
    an argument::

        >>> bound_science_news = science_news(channel)

    Now you can perform operations like :meth:`declare` or :meth:`purge`::

        >>> bound_sicence_news.declare()
        >>> bound_science_news.purge()
        >>> bound_science_news.delete()

    .. autoclass:: Queue
        :members:
        :undoc-members:

        .. automethod:: maybe_bind
