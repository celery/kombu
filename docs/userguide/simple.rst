.. _guide-simple:

==================
 Simple Interface
==================

.. contents::
    :local:


:mod:`kombu.simple` is a simple interface to AMQP queueing.
It is only slightly different from the :class:`~Queue.Queue` class in the
Python Standard Library, which makes it excellent for users with basic
messaging needs.

Instead of defining exchanges and queues, the simple classes only requires
two arguments, a connection channel and a name. The name is used as the
queue, exchange and routing key. If the need arises, you can specify
a :class:`~kombu.Queue` as the name argument instead.

In addition, the :class:`~kombu.Connection` comes with
shortcuts to create simple queues using the current connection:

.. code-block:: pycon

    >>> queue = connection.SimpleQueue('myqueue')
    >>> # ... do something with queue
    >>> queue.close()


This is equivalent to:

.. code-block:: pycon

    >>> from kombu.simple import SimpleQueue, SimpleBuffer

    >>> channel = connection.channel()
    >>> queue = SimpleBuffer(channel)
    >>> # ... do something with queue
    >>> channel.close()
    >>> queue.close()

.. _simple-send-receive:

Sending and receiving messages
==============================

The simple interface defines two classes; :class:`~kombu.simple.SimpleQueue`,
and :class:`~kombu.simple.SimpleBuffer`. The former is used for persistent
messages, and the latter is used for transient, buffer-like queues.
They both have the same interface, so you can use them interchangeably.

Here is an example using the :class:`~kombu.simple.SimpleQueue` class
to produce and consume logging messages:

.. code-block:: python

    import socket
    import datetime
    from time import time
    from kombu import Connection


    class Logger(object):

        def __init__(self, connection, queue_name='log_queue',
                serializer='json', compression=None):
            self.queue = connection.SimpleQueue(queue_name)
            self.serializer = serializer
            self.compression = compression

        def log(self, message, level='INFO', context={}):
            self.queue.put({'message': message,
                            'level': level,
                            'context': context,
                            'hostname': socket.gethostname(),
                            'timestamp': time()},
                            serializer=self.serializer,
                            compression=self.compression)

        def process(self, callback, n=1, timeout=1):
            for i in xrange(n):
                log_message = self.queue.get(block=True, timeout=1)
                entry = log_message.payload # deserialized data.
                callback(entry)
                log_message.ack() # remove message from queue

        def close(self):
            self.queue.close()


    if __name__ == '__main__':
        from contextlib import closing

        with Connection('amqp://guest:guest@localhost:5672//') as conn:
            with closing(Logger(conn)) as logger:

                # Send message
                logger.log('Error happened while encoding video',
                            level='ERROR',
                            context={'filename': 'cutekitten.mpg'})

                # Consume and process message

                # This is the callback called when a log message is
                # received.
                def dump_entry(entry):
                    date = datetime.datetime.fromtimestamp(entry['timestamp'])
                    print('[%s %s %s] %s %r' % (date,
                                                entry['hostname'],
                                                entry['level'],
                                                entry['message'],
                                                entry['context']))

                # Process a single message using the callback above.
                logger.process(dump_entry, n=1)
