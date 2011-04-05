"""

Example that sends a single message and exits using the simple interface.

You can use `simple_receive.py` (or `complete_receive.py`) to receive the
message sent.

"""

from kombu import BrokerConnection

#: Create connection
#: If hostname, userid, password and virtual_host is not specified
#: the values below are the default, but listed here so it can
#: be easily changed.
connection = BrokerConnection(hostname="localhost",
                              userid="guest",
                              password="guest",
                              virtual_host="/")


#: SimpleQueue mimics the interface of the Python Queue module.
#: First argument can either be a queue name or a kombu.Queue object.
#: If a name, then the queue will be declared with the name as the queue
#: name, exchange name and routing key.
queue = connection.SimpleQueue("kombu_demo")
queue.put({"hello": "world"}, serializer="json", compression="zlib")

# Always remember to close channels and connections.
queue.close()
connection.close()
