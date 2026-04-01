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
The mechanism changes message specific ``visibility timeout`` from queue ``Default visibility timeout`` to policy configured timeout.
The number of retries is managed by SQS (specifically by the ``ApproximateReceiveCount`` message attribute) and no further action is required by the user.

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


Message Attributes
------------------------

SQS supports sending message attributes along with the message body.
To use this feature, you can pass a 'message_attributes' as keyword argument
to `basic_publish` method.

Fair Queue Support (only available from version 5.7.0+)
------------------------

Kombu supports Amazon SQS Fair Queues, which provide improved message processing fairness by ensuring that messages from different message groups
are processed in a balanced manner.

Fair Queues are designed to prevent a single message group (or tenant) from monopolizing
consumer resources, which can happen with standard queues that handle multi-tenant
workloads with unbalanced message distribution.

When publishing messages to a Fair Queue, you should provide a `MessageGroupId`. This can be done by passing it as a
keyword argument to the `publish` method. While the Kombu implementation only sends `MessageGroupId` if it is present,
AWS requires it for FIFO and Fair Queues. If omitted, (a) FIFO: Kombu will assign a default group id,
(b) standard fair queues: group id is needed to get fairness but omission shouldn't imply AWS rejection.
Example:
    producer.publish(
        message,
        routing_key='my-fair-queue',
        MessageGroupId='customer-123'  # Required for FIFO queues, if not provided, Kombu will assign a default group id; needed for Fair queue functionality on standard queues, else fairness will not be guaranteed.
    )

Benefits of using Fair Queues with Kombu:
- Improved message processing fairness across message groups
- Better workload distribution among consumers
- Eliminates noisy neighbor problem

For more information, refer to the AWS documentation on Fair Queues: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-fair-queues.html




Amazon SQS Transport - ``kombu.transport.SQS.exceptions``
=========================================================

.. automodule:: kombu.transport.SQS.exceptions
   :members:
   :show-inheritance:
   :undoc-members:


Amazon SQS Transport - ``kombu.transport.SQS.SNS``
==================================================

.. automodule:: kombu.transport.SQS.SNS
   :members:
   :show-inheritance:
   :undoc-members:
