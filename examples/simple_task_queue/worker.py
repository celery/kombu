from __future__ import with_statement

from kombu.mixins import ConsumerMixin
from kombu.utils import kwdict

from queues import task_queues


class Worker(ConsumerMixin):

    def __init__(self, connection):
        self.connection = connection

    def get_consumers(self, Consumer, channel):
        return [Consumer(queues=task_queues,
                        callbacks=[self.process_task])]

    def process_task(self, body, message):
        fun = body["fun"]
        args = body["args"]
        kwargs = body["kwargs"]
        fun(*args, **kwdict(kwargs))
        message.ack()

if __name__ == "__main__":
    from kombu import BrokerConnection
    from kombu.utils.debug import setup_logging
    setup_logging(loglevel="INFO")

    with BrokerConnection("amqp://guest:guest@localhost:5672//") as conn:
        Worker(conn).run()
