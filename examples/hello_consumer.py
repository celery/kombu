from __future__ import absolute_import, unicode_literals, print_function

from kombu import Connection  # noqa


with Connection('amqp://guest:guest@localhost:5672//') as conn:
    simple_queue = conn.SimpleQueue('simple_queue')
    message = simple_queue.get(block=True, timeout=1)
    print('Received: {0}'.format(message.payload))
    message.ack()
    simple_queue.close()
