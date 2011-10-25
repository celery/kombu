from __future__ import with_statement

from kombu.common import maybe_declare
from kombu.pools import producers

from queues import task_exchange

priority_to_routing_key = {"high": "hipri",
                           "mid": "midpri",
                           "low": "lopri"}


def send_as_task(connection, fun, args=(), kwargs={}, priority="mid"):
    payload = {"fun": fun, "args": args, "kwargs": kwargs}
    routing_key = priority_to_routing_key[priority]

    with producers[connection].acquire(block=True) as producer:
        maybe_declare(task_exchange, producer.channel)
        producer.publish(payload, serializer="pickle",
                                  compression="bzip2",
                                  routing_key=routing_key)

if __name__ == "__main__":
    from kombu import BrokerConnection
    from tasks import hello_task

    connection = BrokerConnection("amqp://guest:guest@localhost:5672//")
    send_as_task(connection, fun=hello_task, args=("Kombu", ), kwargs={},
                 priority="high")
