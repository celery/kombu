"""

Example that sends a single message and exits using the simple interface.

You can use `simple_receive.py` (or `complete_receive.py`) to receive the
message sent.

"""

import eventlet

from kombu import Connection

eventlet.monkey_patch()


def wait_many(timeout=1):

    #: Create connection
    #: If hostname, userid, password and virtual_host is not specified
    #: the values below are the default, but listed here so it can
    #: be easily changed.
    with Connection('amqp://guest:guest@localhost:5672//') as connection:

        #: SimpleQueue mimics the interface of the Python Queue module.
        #: First argument can either be a queue name or a kombu.Queue object.
        #: If a name, then the queue will be declared with the name as the
        #: queue name, exchange name and routing key.
        with connection.SimpleQueue('kombu_demo') as queue:

            while True:
                try:
                    message = queue.get(block=False, timeout=timeout)
                except queue.Empty:
                    break
                else:
                    message.ack()
                    print(message.payload)

eventlet.spawn(wait_many).wait()
