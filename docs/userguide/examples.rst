.. _examples:

========================
 Examples
========================

.. _hello-world-example:

Hello World Example
===================

Below example uses
:ref:`guide-simple`
to send helloworld message through
message broker (rabbitmq) and print received message


:file:`hello_publisher.py`:

.. literalinclude:: ../../examples/hello_publisher.py
    :language: python

:file:`hello_consumer.py`:

.. literalinclude:: ../../examples/hello_consumer.py
    :language: python


.. _task-queue-example:

Task Queue Example
==================

Very simple task queue using pickle, with primitive support
for priorities using different queues.


:file:`queues.py`:

.. literalinclude:: ../../examples/simple_task_queue/queues.py
    :language: python

:file:`worker.py`:

.. literalinclude:: ../../examples/simple_task_queue/worker.py
    :language: python

:file:`tasks.py`:

.. literalinclude:: ../../examples/simple_task_queue/tasks.py
    :language: python

:file:`client.py`:

.. literalinclude:: ../../examples/simple_task_queue/client.py

.. _native-delayed-delivery-example:

Native Delayed Delivery
=======================

This example demonstrates how to declare native delayed delivery queues and exchanges and publish a message using
the native delayed delivery mechanism.

:file:`delayed_infra.py`:

.. literalinclude:: ../../examples/delayed_infra.py
    :language: python
