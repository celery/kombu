.. _guide-producers:

===========
 Producers
===========

.. _producer-basics:

Basics
======

The :class:`Producer` takes a connection (or channel) and an exchange to
produce to.

Draining events from a single producer:

.. code-block:: python

    with Producer(connection, exchange) as producer:
        producer.publish(data, content_type=['json'])


Serialization
=============

See :ref:`guide-serialization`.


Reference
=========

.. autoclass:: kombu.Producer
    :noindex:
    :members:
