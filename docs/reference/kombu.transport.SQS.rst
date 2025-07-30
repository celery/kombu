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

Large Message Support
------------------------

SQS has a maximum message size limit of 256KB. To handle larger messages,
the SQS transport automatically supports the Amazon SQS Extended Client Library,
which uses S3 to store message payloads that exceed the SQS size limit.

This feature is automatically available when using the SQS transport - no
additional installation or configuration is required as the necessary
dependencies are included with the SQS extras.

**How it works:**

- When sending a message larger than 256KB, the transport automatically stores
  the message body in S3
- SQS receives a reference pointer to the S3 object instead of the actual message
- When receiving the message, the transport transparently retrieves the payload
  from S3

**IAM Permissions:**

To use this feature, your AWS credentials need appropriate S3 permissions in
addition to standard SQS permissions:

- ``s3:GetObject`` - for retrieving large messages
- ``s3:PutObject`` - for storing large messages

The S3 bucket used for storage is managed by the SQS Extended Client Library.