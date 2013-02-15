.. _examples:

========================
 Examples
========================

.. _hello-world-example:

Hello World Example
==================

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

.. code-block:: python

:file:`client.py`:

.. literalinclude:: ../../examples/simple_task_queue/client.py

Django Celery Example
=====================

Simple task queue using Django and Celery with a Procfile
to run a worker process as a consumer of the queue.

:file:`Procfile`:

.. literalinclude:: ../../examples/django_celery_task_queue/Procfile

:file:`models.py`:

.. literalinclude:: ../../examples/django_celery_task_queue/models.py

:file:`tasks.py`:

.. literalinclude:: ../../examples/django_celery_task_queue/tasks.py

