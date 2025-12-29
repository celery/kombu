============================================================
 Virtual Transport Base Class - ``kombu.transport.virtual``
============================================================

.. currentmodule:: kombu.transport.virtual

.. automodule:: kombu.transport.virtual

    .. contents::
        :local:

    Transports
    ----------

    .. autoclass:: Transport

        .. autoattribute:: Channel

        .. autoattribute:: Cycle

        .. autoattribute:: polling_interval

        .. autoattribute:: default_port

        .. autoattribute:: state

        .. autoattribute:: cycle

        .. automethod:: establish_connection

        .. automethod:: close_connection

        .. automethod:: create_channel

        .. automethod:: close_channel

        .. automethod:: drain_events

    Channel
    -------

    .. autoclass:: AbstractChannel
        :members:

    .. autoclass:: Channel

        .. autoattribute:: Message

        .. autoattribute:: state

        .. autoattribute:: qos

        .. autoattribute:: do_restore

        .. autoattribute:: exchange_types

        .. automethod:: exchange_declare

        .. automethod:: exchange_delete

        .. automethod:: queue_declare

        .. automethod:: queue_delete

        .. automethod:: queue_bind

        .. automethod:: queue_purge

        .. automethod:: basic_publish

        .. automethod:: basic_consume

        .. automethod:: basic_cancel

        .. automethod:: basic_get

        .. automethod:: basic_ack

        .. automethod:: basic_recover

        .. automethod:: basic_reject

        .. automethod:: basic_qos

        .. automethod:: get_table

        .. automethod:: typeof

        .. automethod:: drain_events

        .. automethod:: prepare_message

        .. automethod:: message_to_python

        .. automethod:: flow

        .. automethod:: close

    Message
    -------

    .. autoclass:: Message
        :members:
        :undoc-members:
        :inherited-members:

    Quality Of Service
    ------------------

    .. autoclass:: QoS
        :members:
        :undoc-members:
        :inherited-members:

    In-memory State
    ---------------

    .. autoclass:: BrokerState
        :members:
        :undoc-members:
        :inherited-members:
