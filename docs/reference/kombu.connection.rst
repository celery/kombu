=======================================
  Connection - ``kombu.connection``
=======================================

.. currentmodule:: kombu.connection

.. automodule:: kombu.connection

    .. contents::
        :local:

    Connection
    ----------

    .. autoclass:: Connection
        :members:
        :undoc-members:

    Pools
    -----

    .. seealso::

        The shortcut methods :meth:`Connection.Pool` and
        :meth:`Connection.ChannelPool` is the recommended way
        to instantiate these classes.

    .. autoclass:: ConnectionPool

        .. autoattribute:: LimitExceeded

        .. automethod:: acquire
        .. automethod:: release
        .. automethod:: force_close_all

    .. autoclass:: ChannelPool

        .. autoattribute:: LimitExceeded

        .. automethod:: acquire
        .. automethod:: release
        .. automethod:: force_close_all
