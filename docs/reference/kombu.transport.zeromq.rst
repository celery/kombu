.. currentmodule:: kombu.transport.zeromq

.. automodule:: kombu.transport.zeromq

    .. contents::
        :local:

    Currently, the ZeroMQ transport does not provide routing via queues. The
    queue name is sent along with the message, but dropped upon consumption.

    Messages sent to direct/topic exchanges are sent round-robin to connected
    peers regardless of the designated queue. However, since fanout queues
    are based on PUB/SUB, some peers don't have to subscribe to all topics.

    By default, consuming channels listen on ports starting at 5555. To modify
    the starting port, pass a port when creating a `~kombu.connection.Connection`.

    transport_options
    -----------------

    +-------------+---------+------------------------------------+
    | Option      | Default | Description                        |
    +=============+=========+====================================+
    | hwm         | 1000    | HWM_ of the zmq_socket             |
    +-------------+---------+------------------------------------+
    | swap_size   | None    | `Swap size`_ on the zmq_socket     |
    +-------------+---------+------------------------------------+
    | enable_sink | True    | Disable to not consume messages    |
    +-------------+---------+------------------------------------+
    | port_incr   | 2       | Incr binding port for each channel |
    +-------------+---------+------------------------------------+

    .. _HWM: http://api.zeromq.org/2-1:zmq-setsockopt#toc3
    .. _Swap size: http://api.zeromq.org/2-1:zmq-setsockopt#toc4

    Transport
    ---------

    .. autoclass:: Transport
        :members:
        :undoc-members:

    Channel
    -------

    .. autoclass:: Channel
        :members:
        :undoc-members:
