"""

Example producer that sends a single message and exits.

You can use `complete_receive.py` to receive the message sent.

"""
from __future__ import with_statement

from kombu import Connection, Producer, Exchange, Queue

#: By default messages sent to exchanges are persistent (delivery_mode=2),
#: and queues and exchanges are durable.
gateway_exchange = Exchange('gateway_kombu_demo', type='direct')

with Connection('pyamqp://guest:guest@localhost:5672//') as connection:
    #binded = exchange.bind(connection.channel())
    #binded.exchange_bind(gateway_exchange, routing_key = 'kombu_demo')
    #: Producers are used to publish messages.
    #: a default exchange and routing key can also be specifed
    #: as arguments the Producer, but we rather specify this explicitly
    #: at the publish call.
    producer = Producer(connection)

    #: Publish the message using the json serializer (which is the default),
    #: and zlib compression.  The kombu consumer will automatically detect
    #: encoding, serialization and compression used and decode accordingly.
    producer.publish({'hello': 'world'},
                     exchange=gateway_exchange,
                     routing_key='kombu_demo',
                     serializer='json', compression='zlib')
