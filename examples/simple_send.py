"""

Example that sends a single message and exits using the simple interface.

You can use `simple_receive.py` (or `complete_receive.py`) to receive the
message sent.

"""
from __future__ import with_statement

from kombu import BrokerConnection

#: Create connection
#: If hostname, userid, password and virtual_host is not specified
#: the values below are the default, but listed here so it can
#: be easily changed.
with BrokerConnection("amqp://guest:guest@localhost:5672//") as conn:

    #: SimpleQueue mimics the interface of the Python Queue module.
    #: First argument can either be a queue name or a kombu.Queue object.
    #: If a name, then the queue will be declared with the name as the queue
    #: name, exchange name and routing key.
    with conn.SimpleQueue("kombu_demo") as queue:
        queue.put({"hello": "world"}, serializer="json", compression="zlib")


#####
# If you don't use the with statement, you must always
# remember to close objects.
#   queue.close()
#   connection.close()
