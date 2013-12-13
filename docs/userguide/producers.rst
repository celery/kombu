.. _guide-producers:

===========
 Producers
===========

.. _producer-basics:

Basics
======

The :class:`Producer` takes a connection (or channel) and a list of queues to
produce to.

Draining events from a single producer:

.. code-block:: python

    with Producer(connection, queues, content_type=['json']):
        connection.drain_events(timeout=1)


Serialization
=============

See :ref:`guide-serialization`.


Reference
=========

.. autoclass:: kombu.Producer
    :noindex:
    :members:
