==================================================
 Transport Base Class - ``kombu.transport.base``
==================================================

.. currentmodule:: kombu.transport.base

.. automodule:: kombu.transport.base

    .. contents::
        :local:

    Message
    -------

    .. autoclass:: Message

        .. autoattribute:: payload
        .. autoattribute:: channel
        .. autoattribute:: delivery_tag
        .. autoattribute:: content_type
        .. autoattribute:: content_encoding
        .. autoattribute:: delivery_info
        .. autoattribute:: headers
        .. autoattribute:: properties
        .. autoattribute:: body
        .. autoattribute:: acknowledged

        .. automethod:: ack
        .. automethod:: reject
        .. automethod:: requeue
        .. automethod:: decode

    Transport
    ---------

    .. autoclass:: Transport

        .. autoattribute:: client
        .. autoattribute:: default_port

        .. attribute:: recoverable_connection_errors

            Optional list of connection related exceptions that can be
            recovered from, but where the connection must be closed
            and re-established first.

            If not defined then all :attr:`connection_errors` and
            :class:`channel_errors` will be regarded as recoverable,
            but needing to close the connection first.

        .. attribute:: recoverable_channel_errors

            Optional list of channel related exceptions that can be
            automatically recovered from without re-establishing the
            connection.

        .. autoattribute:: connection_errors
        .. autoattribute:: channel_errors

        .. automethod:: establish_connection
        .. automethod:: close_connection
        .. automethod:: create_channel
        .. automethod:: close_channel
        .. automethod:: drain_events


