"""
Example of simple consumer that waits for a single message, acknowledges it
and exits.
"""
from kombu import Connection, Exchange, Queue, Consumer, eventloop
from pprint import pformat

#: By default messages sent to exchanges are persistent (delivery_mode=2),
#: and queues and exchanges are durable.
exchange = Exchange('kombu_demo', type='direct')
queue = Queue('kombu_demo', exchange, routing_key='kombu_demo')


def pretty(obj):
    return pformat(obj, indent=4)


#: This is the callback applied when a message is received.
def handle_message(body, message):
    print('Received message: %r' % (body, ))
    print('  properties:\n%s' % (pretty(message.properties), ))
    print('  delivery_info:\n%s' % (pretty(message.delivery_info), ))
    message.ack()

#: Create a connection and a channel.
#: If hostname, userid, password and virtual_host is not specified
#: the values below are the default, but listed here so it can
#: be easily changed.
with Connection('amqp://guest:guest@localhost:5672//') as connection:

    #: Create consumer using our callback and queue.
    #: Second argument can also be a list to consume from
    #: any number of queues.
    with Consumer(connection, queue, callbacks=[handle_message]):

        #: Each iteration waits for a single event.  Note that this
        #: event may not be a message, or a message that is to be
        #: delivered to the consumers channel, but any event received
        #: on the connection.
        for _ in eventloop(connection):
            pass
