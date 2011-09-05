.. currentmodule:: kombu.messaging

.. automodule:: kombu.messaging

    .. contents::
        :local:

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

        .. automethod:: declare
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
        .. autoattribute:: on_decode_error

        .. automethod:: declare
        .. automethod:: register_callback
        .. automethod:: consume
        .. automethod:: cancel
        .. automethod:: cancel_by_queue
        .. automethod:: purge
        .. automethod:: flow
        .. automethod:: qos
        .. automethod:: recover
        .. automethod:: receive
        .. automethod:: revive
