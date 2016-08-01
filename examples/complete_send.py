"""

Example producer that sends a single message and exits.

You can use `complete_receive.py` to receive the message sent.

"""
from __future__ import absolute_import, unicode_literals

from kombu import Connection, Producer, Exchange, Queue

#: By default messages sent to exchanges are persistent (delivery_mode=2),
#: and queues and exchanges are durable.
exchange = Exchange('kombu_demo', type='direct')
queue = Queue('kombu_demo', exchange, routing_key='kombu_demo')


with Connection('amqp://guest:guest@localhost:5672//') as connection:

    #: Producers are used to publish messages.
    #: a default exchange and routing key can also be specified
    #: as arguments the Producer, but we rather specify this explicitly
    #: at the publish call.
    producer = Producer(connection)

    #: Publish the message using the json serializer (which is the default),
    #: and zlib compression.  The kombu consumer will automatically detect
    #: encoding, serialization and compression used and decode accordingly.
    producer.publish({'hello': 'world'},
                     exchange=exchange,
                     routing_key='kombu_demo',
                     serializer='json', compression='zlib')
