"""
kombu.common
============

Common Utilities.

:copyright: (c) 2009 - 2011 by Ask Solem.
:license: BSD, see LICENSE for more details.

"""
from collections import defaultdict

from kombu import entity
from kombu.utils import uuid

declared_entities = defaultdict(lambda: set())


def maybe_declare(entity, channel):
    declared = declared_entities[channel.connection.client]
    if not entity.is_bound:
        entity = entity(channel)
    if entity not in declared:
        entity.declare()
        declared.add(entity)
        return True
    return False


class Broadcast(entity.Queue):
    """Convenience class used to define broadcast queues.

    Every queue instance will have a unique name,
    and both the queue and exchange is configued with auto deletion.

    :keyword name: This is used as the name of the exchange.
    :keyword queue: By default a unique id is used for the queue
       name for every consumer.  You can specify a custom queue
       name here.
    :keyword \*\*kwargs: See :class:`~kombu.entity.Queue` for a list
        of additional keyword arguments supported.

    """

    def __init__(self, name=None, queue=None, **kwargs):
        kwargs.setdefault("exchange", entity.Exchange(name, type="fanout",
                                                      auto_delete=True))
        kwargs.setdefault("auto_delete", True)
        kwargs.setdefault("alias", name)
        return super(Broadcast, self).__init__(
                name=queue or "bcast.%s" % (uuid(), ), **kwargs)


def entry_to_queue(queue, **options):
    binding_key = options.get("binding_key") or options.get("routing_key")

    e_durable = options.get("exchange_durable")
    if e_durable is None:
        e_durable = options.get("durable")

    e_auto_delete = options.get("exchange_auto_delete")
    if e_auto_delete is None:
        e_auto_delete = options.get("auto_delete")

    q_durable = options.get("queue_durable")
    if q_durable is None:
        q_durable = options.get("durable")

    q_auto_delete = options.get("queue_auto_delete")
    if q_auto_delete is None:
        q_auto_delete = options.get("auto_delete")

    e_arguments = options.get("exchange_arguments")
    q_arguments = options.get("queue_arguments")
    b_arguments = options.get("binding_arguments")

    exchange = entity.Exchange(options.get("exchange"),
                               type=options.get("exchange_type"),
                               delivery_mode=options.get("delivery_mode"),
                               routing_key=options.get("routing_key"),
                               durable=e_durable,
                               auto_delete=e_auto_delete,
                               arguments=e_arguments)

    return entity.Queue(queue,
                        exchange=exchange,
                        routing_key=binding_key,
                        durable=q_durable,
                        exclusive=options.get("exclusive"),
                        auto_delete=q_auto_delete,
                        no_ack=options.get("no_ack"),
                        queue_arguments=q_arguments,
                        binding_arguments=b_arguments)
