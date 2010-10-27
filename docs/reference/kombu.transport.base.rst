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
        .. autoattribute:: connection_errors
        .. autoattribute:: channel_errors

        .. automethod:: establish_connection
        .. automethod:: close_connection
        .. automethod:: create_channel
        .. automethod:: close_channel
        .. automethod:: drain_events


