"""

Example that sends a single message and exits using the simple interface.

You can use `simple_receive.py` (or `complete_receive.py`) to receive the
message sent.

"""
import eventlet

from kombu import BrokerConnection

eventlet.monkey_patch()


def send_many(n):

    #: Create connection
    #: If hostname, userid, password and virtual_host is not specified
    #: the values below are the default, but listed here so it can
    #: be easily changed.
    connection = BrokerConnection("amqp://guest:guest@localhost:5672//")

    #: SimpleQueue mimics the interface of the Python Queue module.
    #: First argument can either be a queue name or a kombu.Queue object.
    #: If a name, then the queue will be declared with the name as the queue
    #: name, exchange name and routing key.
    queue = connection.SimpleQueue("kombu_demo")

    def send_message(i):
        queue.put({"hello": "world%s" % (i, )})

    pool = eventlet.GreenPool(10)
    for i in xrange(n):
        pool.spawn(send_message, i)
    pool.waitall()

    queue.close()
    connection.close()

if __name__ == "__main__":
    send_many(10)
