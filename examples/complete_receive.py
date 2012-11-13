"""
Example of simple consumer that waits for a single message, acknowledges it
and exits.
"""

from __future__ import with_statement

from kombu import Connection, Exchange, Queue, Consumer, eventloop
from pprint import pformat

#: By default messages sent to exchanges are persistent (delivery_mode=2),
#: and queues and exchanges are durable.


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
with Connection('pyamqp://guest:guest@localhost:5672//') as connection:
    # The configuration of the message flow is as follows:
    #   gateway_kombu_exchange -> internal_kombu_exchange -> kombu_demo queue
    gateway_exchange = Exchange('gateway_kombu_demo')(connection)
    exchange = Exchange('internal_kombu_demo')(connection)
    gateway_exchange.declare()
    exchange.declare()
    exchange.bind_to(gateway_exchange, routing_key='kombu_demo')

    queue = Queue('kombu_demo', exchange, routing_key='kombu_demo')

    #: Create consumer using our callback and queue.
    #: Second argument can also be a list to consume from
    #: any number of queues.
    with Consumer(connection, queue, callbacks=[handle_message]):

        #: This waits for a single event.  Note that this event may not
        #: be a message, or a message that is to be delivered to the consumers
        #: channel, but any event received on the connection.
        recv = eventloop(connection)
        while True:
            recv.next()
