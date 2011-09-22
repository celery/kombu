.. _examples:

========================
 Examples
========================

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

.. code-block:: python

:file:`client.py`:

.. literalinclude:: ../../examples/simple_task_queue/client.py
