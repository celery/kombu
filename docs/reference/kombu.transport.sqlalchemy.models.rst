=====================================================================
 SQLAlchemy Transport Model - ``kombu.transport.sqlalchemy.models``
=====================================================================


.. currentmodule:: kombu.transport.sqlalchemy.models

.. automodule:: kombu.transport.sqlalchemy.models

    .. contents::
        :local:

    Models
    ------

    .. autoclass:: Queue

        .. autoattribute:: Queue.id

        .. autoattribute:: Queue.name

    .. autoclass:: Message

        .. autoattribute:: Message.id

        .. autoattribute:: Message.visible

        .. autoattribute:: Message.sent_at

        .. autoattribute:: Message.payload

        .. autoattribute:: Message.version
