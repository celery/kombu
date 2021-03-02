================================================
 Amazon SQS Transport - ``kombu.transport.SQS``
================================================

.. currentmodule:: kombu.transport.SQS

.. automodule:: kombu.transport.SQS

    .. contents::
        :local:

    Transport
    ---------

    .. autoclass:: Transport
        :members:
        :undoc-members:

    Channel
    -------

    .. autoclass:: Channel
        :members:
        :undoc-members:

Back-off policy
------------------------
Back-off policy is using SQS visibility timeout mechanism altering the time difference between task retries.
Number of retries is managed by SQS (``ApproximateReceiveCount`` message attribute), no need adding count of some sort.

Configuring the queues and backoff policy::

    broker_transport_options = {
        'predefined_queues': {
            'my-q': {
                'url': 'https://ap-southeast-2.queue.amazonaws.com/123456/my-q',
                'access_key_id': 'xxx',
                'secret_access_key': 'xxx',
                'backoff_policy': {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640},
                'backoff_tasks': ['svc.tasks.tasks.task1']
            }
        }
    }


``backoff_policy`` dictionary where key is number of retries, and value is delay seconds between retries (i.e
SQS visibility timeout)
``backoff_tasks`` list of task names to apply the above policy

The above policy:

+-----------------------------------------+--------------------------------------------+
| **Attempt**                             | **Delay**                                  |
+-----------------------------------------+--------------------------------------------+
| ``2nd attempt``                         | 20 seconds                                 |
+-----------------------------------------+--------------------------------------------+
| ``3rd attempt``                         | 40 seconds                                 |
+-----------------------------------------+--------------------------------------------+
| ``4th attempt``                         | 80 seconds                                 |
+-----------------------------------------+--------------------------------------------+
| ``5th attempt``                         | 320 seconds                                |
+-----------------------------------------+--------------------------------------------+
| ``6th attempt``                         | 640 seconds                                |
+-----------------------------------------+--------------------------------------------+
