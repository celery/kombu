"""

Example producer that sends a single message and exits.

You can use `complete_receive.py` to receive the message sent.

"""

from kombu import BrokerConnection, Exchange, Queue, Producer

#: By default messages sent to exchanges are persistent (delivery_mode=2),
#: and queues and exchanges are durable.
exchange = Exchange("kombu_demo", type="direct")
queue = Queue("kombu_demo", exchange, routing_key="kombu_demo")


#: Create connection and channel.
#: If hostname, userid, password and virtual_host is not specified
#: the values below are the default, but listed here so it can
#: be easily changed.
connection = BrokerConnection(hostname="localhost",
                              userid="guest",
                              password="guest",
                              virtual_host="/")
channel = connection.channel()

#: Producers are used to publish messages.
#: Routing keys can also be specifed as an argument to `publish`.
producer = Producer(channel, exchange, routing_key="kombu_demo")

#: Publish the message using the json serializer (which is the default),
#: and zlib compression.  The kombu consumer will automatically detect
#: encoding, serializiation and compression used and decode accordingly.
producer.publish({"hello": "world"}, serializer="json",
                                     compression="zlib")
