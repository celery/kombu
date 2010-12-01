"""
Example receiving a message using the SimpleQueue interface.
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
message = queue.get(block=True, timeout=10)
message.ack()
print(message.payload)
