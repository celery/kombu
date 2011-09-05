

.. currentmodule:: kombu.connection

.. automodule:: kombu.connection

    .. contents::
        :local:

    Connection
    ----------

    .. autoclass:: BrokerConnection

        .. admonition:: Attributes

            .. autoattribute:: connection_errors
            .. autoattribute:: channel_errors
            .. autoattribute:: transport
            .. autoattribute:: host
            .. autoattribute:: connection

        .. admonition:: Methods

            .. automethod:: connect
            .. automethod:: channel
            .. automethod:: drain_events
            .. automethod:: release
            .. automethod:: ensure_connection
            .. automethod:: ensure
            .. automethod:: create_transport
            .. automethod:: get_transport_cls
            .. automethod:: clone
            .. automethod:: info

            .. automethod:: Pool
            .. automethod:: ChannelPool
            .. automethod:: SimpleQueue
            .. automethod:: SimpleBuffer


    Pools
    -----

    .. seealso::

        The shortcut methods :meth:`BrokerConnection.Pool` and
        :meth:`BrokerConnection.ChannelPool` is the recommended way
        to instantiate these classes.

    .. autoclass:: ConnectionPool

        .. autoattribute:: LimitExceeded

        .. automethod:: acquire
        .. automethod:: release

    .. autoclass:: ChannelPool

        .. autoattribute:: LimitExceeded

        .. automethod:: acquire
        .. automethod:: release
