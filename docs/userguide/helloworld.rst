.. _guide-hello-world:

==================
 Hello world uses simple interface
==================


Below example uses simple interface to send helloworld message through 
message broker (rabbitmq) and print received message


:file:`simple_publisher.py`:
.. code-block:: python
    from __future__ import with_statement
    from kombu import Connection
    import datetime
    
    with Connection('amqp://guest:guest@localhost:5672//') as conn:
        simple_queue = conn.SimpleQueue('simple_queue')
        message = 'helloword, sent at %s' % datetime.datetime.today()
        simple_queue.put(message)
        print('Sent: %s' % message)
        simple_queue.close()

    
:file:`simple_consumer.py`:
.. code-block:: python
    from __future__ import with_statement
    from kombu import Connection
    
    with Connection('amqp://guest:guest@localhost:5672//') as conn:
        simple_queue = conn.SimpleQueue('simple_queue')
        message = simple_queue.get(block=True, timeout=1)
        print("Received: %s" % message.payload)
        message.ack()
        simple_queue.close()
